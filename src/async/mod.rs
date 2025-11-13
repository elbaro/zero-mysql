use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::col::ColumnDefinition;
use crate::constant::CapabilityFlags;
use crate::error::{Error, Result};
use crate::protocol::command::prepared::{read_execute_response, write_execute, ExecuteResponse};
use crate::protocol::command::resultset::{read_binary_row, read_column_definition};
use crate::protocol::connection::handshake::{
    auth_caching_sha2_password, auth_mysql_native_password,
    read_auth_switch_request, read_caching_sha2_password_fast_auth_result,
    read_initial_handshake, write_auth_switch_response, write_handshake_response,
    CachingSha2PasswordFastAuthResult, HandshakeResponse41,
};
use crate::protocol::packet::write_packet_header_array;
use crate::protocol::r#trait::{params::Params, RowsDecoder};
use crate::protocol::packet::{ErrPayloadBytes, OkPayloadBytes};
use crate::protocol::response::{detect_packet_type, ErrPayload, OkPayload, PacketType};
use crate::row::Row;

/// A MySQL connection with a buffered async TCP stream
///
/// This struct holds the connection state including server information
/// obtained during the handshake phase.
pub struct Conn {
    stream: BufReader<TcpStream>,
    server_version: String,
    capability_flags: CapabilityFlags,
}

impl Conn {
    /// Create a new MySQL connection from a URL (async)
    ///
    /// This performs the complete MySQL handshake protocol:
    /// 1. Parses the MySQL URL
    /// 2. Connects to the MySQL server via TCP
    /// 3. Reads initial handshake from server
    /// 4. Sends handshake response with authentication
    /// 5. Handles auth plugin switching if needed
    /// 6. Returns ready-to-use connection
    ///
    /// # Arguments
    /// * `url` - MySQL connection URL (e.g., "mysql://user:pass@host:3306/db")
    ///
    /// # URL Format
    /// ```text
    /// mysql://[username[:password]@]host[:port][/database]
    /// ```
    ///
    /// Examples:
    /// - `mysql://localhost`
    /// - `mysql://root:password@localhost:3306`
    /// - `mysql://user:pass@127.0.0.1:3306/mydb`
    ///
    /// # Returns
    /// * `Ok(Conn)` - Authenticated connection ready for queries
    /// * `Err(Error)` - Connection or authentication failed
    pub async fn new(url: &str) -> Result<Self> {
        // Parse URL
        let parsed = url::Url::parse(url).map_err(|e| {
            Error::BadInputError(format!("Failed to parse MySQL URL: {}", e))
        })?;

        // Verify scheme
        if parsed.scheme() != "mysql" {
            return Err(Error::BadInputError(format!(
                "Invalid URL scheme '{}', expected 'mysql'",
                parsed.scheme()
            )));
        }

        // Extract host
        let host = parsed.host_str().ok_or_else(|| {
            Error::BadInputError("Missing host in MySQL URL".to_string())
        })?;

        // Extract port (default 3306)
        let port = parsed.port().unwrap_or(3306);

        // Extract username (default empty)
        let username = if parsed.username().is_empty() {
            ""
        } else {
            parsed.username()
        };

        // Extract password (default empty)
        let password = parsed.password().unwrap_or("");

        // Extract database from path
        let database = parsed.path().trim_start_matches('/');
        let database = if database.is_empty() {
            None
        } else {
            Some(database)
        };

        // Connect to server
        let addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect(&addr).await?;

        Self::new_with_stream(stream, username, password, database).await
    }

    /// Create a new MySQL connection with an existing TCP stream (async)
    ///
    /// This is useful when you need more control over the TCP connection,
    /// such as setting socket options before connecting.
    ///
    /// # Arguments
    /// * `stream` - TCP stream connected to MySQL server
    /// * `username` - MySQL username
    /// * `password` - MySQL password (plain text)
    /// * `database` - Optional database name to connect to
    ///
    /// # Returns
    /// * `Ok(Conn)` - Authenticated connection ready for queries
    /// * `Err(Error)` - Connection or authentication failed
    pub async fn new_with_stream(
        stream: TcpStream,
        username: &str,
        password: &str,
        database: Option<&str>,
    ) -> Result<Self> {
        let mut conn_stream = BufReader::new(stream);
        let mut buffer = Vec::new();

        // Step 1: Read initial handshake from server
        let _seq = read_payload(&mut conn_stream, &mut buffer).await?;
        let handshake = read_initial_handshake(&buffer)?;

        // Step 2: Compute client capabilities
        use crate::constant::CAPABILITIES_ALWAYS_ENABLED;
        let mut client_caps = CAPABILITIES_ALWAYS_ENABLED;

        // Add CLIENT_CONNECT_WITH_DB if we have a database name and server supports it
        if database.is_some() && handshake.capability_flags.contains(CapabilityFlags::CLIENT_CONNECT_WITH_DB) {
            client_caps |= CapabilityFlags::CLIENT_CONNECT_WITH_DB;
        }

        let negotiated_caps = client_caps & handshake.capability_flags;

        // Step 3: Compute auth response based on plugin
        let (auth_response, auth_plugin_name) = match handshake.auth_plugin_name.as_str() {
            "mysql_native_password" => {
                let response = auth_mysql_native_password(password, &handshake.auth_plugin_data);
                (response.to_vec(), "mysql_native_password")
            }
            "caching_sha2_password" => {
                let response = auth_caching_sha2_password(password, &handshake.auth_plugin_data);
                (response.to_vec(), "caching_sha2_password")
            }
            unknown => {
                return Err(Error::UnsupportedAuthPlugin(unknown.to_string()));
            }
        };

        // Step 4: Send handshake response
        let handshake_resp = HandshakeResponse41 {
            capability_flags: negotiated_caps,
            max_packet_size: 16_777_216, // 16MB
            charset: 45,                 // utf8mb4_general_ci (widely compatible)
            username,
            auth_response: &auth_response,
            database,
            auth_plugin_name: Some(auth_plugin_name),
        };

        let mut out = Vec::new();
        write_handshake_response(&mut out, &handshake_resp);
        write_payload(conn_stream.get_mut(), 1, &out).await?;

        // Step 5: Read server response
        let _seq = read_payload(&mut conn_stream, &mut buffer).await?;

        // Check packet type
        let packet_type = detect_packet_type(&buffer, negotiated_caps)?;

        match packet_type {
            PacketType::Ok => {
                // Authentication successful
                let ok_bytes = OkPayloadBytes::from_payload(&buffer)
                    .ok_or(Error::InvalidPacket)?;
                let _ok = OkPayload::try_from(ok_bytes)?;
            }
            PacketType::Err => {
                // Authentication failed
                let err_bytes = ErrPayloadBytes::from_payload(&buffer)
                    .ok_or(Error::InvalidPacket)?;
                let err = ErrPayload::try_from(err_bytes)?;
                return Err(Error::ServerError {
                    error_code: err.error_code,
                    sql_state: err.sql_state,
                    message: err.message,
                });
            }
            PacketType::Eof => {
                // For caching_sha2_password, this might be fast auth result
                if handshake.auth_plugin_name == "caching_sha2_password" {
                    let result = read_caching_sha2_password_fast_auth_result(&buffer)?;
                    match result {
                        CachingSha2PasswordFastAuthResult::Success => {
                            // Read final OK packet
                            let _seq = read_payload(&mut conn_stream, &mut buffer).await?;
                            let ok_bytes = OkPayloadBytes::from_payload(&buffer)
                                .ok_or(Error::InvalidPacket)?;
                            let _ok = OkPayload::try_from(ok_bytes)?;
                        }
                        CachingSha2PasswordFastAuthResult::FullAuthRequired => {
                            // Would need to send password over SSL or RSA
                            // For now, return error
                            return Err(Error::UnknownProtocolError(
                                "Full authentication required (SSL/RSA not implemented)".to_string(),
                            ));
                        }
                    }
                } else {
                    // Auth switch request
                    let auth_switch = read_auth_switch_request(&buffer)?;

                    // Compute new auth response
                    let new_auth_response = match auth_switch.plugin_name.as_str() {
                        "mysql_native_password" => {
                            auth_mysql_native_password(password, &auth_switch.plugin_data).to_vec()
                        }
                        "caching_sha2_password" => {
                            auth_caching_sha2_password(password, &auth_switch.plugin_data).to_vec()
                        }
                        unknown => {
                            return Err(Error::UnsupportedAuthPlugin(unknown.to_string()));
                        }
                    };

                    // Send auth switch response
                    let mut out = Vec::new();
                    write_auth_switch_response(&mut out, &new_auth_response);
                    write_payload(conn_stream.get_mut(), 3, &out).await?;

                    // Read final response
                    let _seq = read_payload(&mut conn_stream, &mut buffer).await?;
                    let packet_type = detect_packet_type(&buffer, negotiated_caps)?;

                    match packet_type {
                        PacketType::Ok => {
                            let ok_bytes = OkPayloadBytes::from_payload(&buffer)
                                .ok_or(Error::InvalidPacket)?;
                            let _ok = OkPayload::try_from(ok_bytes)?;
                        }
                        PacketType::Err => {
                            let err_bytes = ErrPayloadBytes::from_payload(&buffer)
                                .ok_or(Error::InvalidPacket)?;
                            let err = ErrPayload::try_from(err_bytes)?;
                            return Err(Error::ServerError {
                                error_code: err.error_code,
                                sql_state: err.sql_state,
                                message: err.message,
                            });
                        }
                        _ => {
                            return Err(Error::InvalidPacket);
                        }
                    }
                }
            }
            _ => {
                return Err(Error::InvalidPacket);
            }
        }

        Ok(Self {
            stream: conn_stream,
            server_version: handshake.server_version,
            capability_flags: negotiated_caps,
        })
    }

    /// Get the server version string
    pub fn server_version(&self) -> &str {
        &self.server_version
    }

    /// Get the negotiated capability flags
    pub fn capability_flags(&self) -> CapabilityFlags {
        self.capability_flags
    }

    /// Execute a query with parameters using binary protocol and decode all rows at once (async)
    ///
    /// This function reads all rows into the provided `rows_buffer` and then passes
    /// a reference to the decoder. The decoder can then parse the rows with zero-copy.
    ///
    /// # Arguments
    /// * `statement_id` - The prepared statement ID
    /// * `params` - Parameters implementing the Params trait
    /// * `decoder` - Mutable reference to a RowsDecoder implementation
    /// * `buffer` - Reusable buffer for reading packets
    /// * `rows_buffer` - Buffer to store all row data (will be cleared and filled)
    ///
    /// # Returns
    /// * `Ok(D::Output)` - The decoded result from the RowsDecoder
    /// * `Err(Error)` - Query execution or decoding failed
    pub async fn exec_with_decoder<'a, P, D>(
        &mut self,
        statement_id: u32,
        params: &P,
        decoder: &mut D,
        buffer: &mut Vec<u8>,
        rows_buffer: &'a mut Vec<u8>,
    ) -> Result<<D as RowsDecoder<'a>>::Output>
    where
        P: Params,
        D: RowsDecoder<'a>,
    {
        // Write COM_STMT_EXECUTE
        let mut out = Vec::new();
        write_execute(&mut out, statement_id, params);
        write_payload(self.stream.get_mut(), 0, &out).await?;

        // Read response
        let _seq = read_payload(&mut self.stream, buffer).await?;
        let response = read_execute_response(&buffer)?;

        match response {
            ExecuteResponse::Ok(_ok) => {
                // No rows to decode, return empty result
                rows_buffer.clear();
                decoder.decode_rows(rows_buffer.as_slice(), 0)
            }
            ExecuteResponse::ResultSet { column_count } => {
                let num_columns = column_count as usize;

                // Read column definitions
                let mut _columns = Vec::with_capacity(num_columns);
                for _ in 0..num_columns {
                    let _seq = read_payload(&mut self.stream, buffer).await?;
                    let col_def = read_column_definition(&buffer)?;
                    _columns.push(col_def);
                }

                // Read EOF packet after column definitions (if present in older protocols)
                let _seq = read_payload(&mut self.stream, buffer).await?;

                // Read all row packets into rows_buffer
                rows_buffer.clear();
                loop {
                    let _seq = read_payload(&mut self.stream, buffer).await?;

                    // Check for EOF/OK packet
                    if !buffer.is_empty()
                        && (buffer[0] == 0xFE || buffer[0] == 0x00)
                        && buffer.len() < 9
                    {
                        break;
                    }

                    rows_buffer.extend_from_slice(&buffer);
                }

                // Decode all rows at once using the decoder
                decoder.decode_rows(rows_buffer.as_slice(), num_columns)
            }
        }
    }

    /// Execute a query with parameters using binary protocol and return an async iterator
    ///
    /// # Arguments
    /// * `statement_id` - The prepared statement ID
    /// * `params` - Parameters implementing the Params trait
    ///
    /// # Returns
    /// * `Ok(QueryResult)` - An iterator over the result rows
    /// * `Err(Error)` - Query execution failed
    pub async fn exec_iter_with_decoder<'a, P>(
        &'a mut self,
        statement_id: u32,
        params: &P,
    ) -> Result<QueryResult<'a>>
    where
        P: Params,
    {
        // Write COM_STMT_EXECUTE
        let mut out = Vec::new();
        write_execute(&mut out, statement_id, params);
        write_payload(self.stream.get_mut(), 0, &out).await?;

        // Read response
        let mut buffer = Vec::new();
        let _seq = read_payload(&mut self.stream, &mut buffer).await?;
        let response = read_execute_response(&buffer)?;

        match response {
            ExecuteResponse::Ok(_ok) => {
                // No rows, return finished iterator
                Ok(QueryResult::new(&mut self.stream, 0, Vec::new()))
            }
            ExecuteResponse::ResultSet { column_count } => {
                let num_columns = column_count as usize;

                // Read column definitions
                let mut columns = Vec::with_capacity(num_columns);
                for _ in 0..num_columns {
                    let _seq = read_payload(&mut self.stream, &mut buffer).await?;
                    let col_def = read_column_definition(&buffer)?;
                    columns.push(col_def);
                }

                // Read EOF packet after column definitions (if present)
                let _seq = read_payload(&mut self.stream, &mut buffer).await?;

                Ok(QueryResult::new(&mut self.stream, num_columns, columns))
            }
        }
    }
}

/// Read a complete MySQL payload asynchronously, concatenating packets if they span multiple 16MB chunks.
/// This function performs minimal copies and uses buffered reads to reduce syscalls.
///
/// # Arguments
/// * `reader` - A buffered async reader (e.g., BufReader<TcpStream>)
/// * `buffer` - Reusable buffer for storing the payload (to minimize allocations)
///
/// # Returns
/// * `Ok(sequence_id)` - The sequence ID; the payload is stored in `buffer`
/// * `Err(Error)` - IO error or protocol error
pub async fn read_payload<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    buffer: &mut Vec<u8>,
) -> Result<u8> {
    buffer.clear();

    // Read first packet header (4 bytes)
    // Note: fill_buf() doesn't guarantee 4 bytes will be available, so we use read_exact
    let mut header = [0u8; 4];
    reader.read_exact(&mut header).await.map_err(|e| Error::IoError(e))?;

    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    let sequence_id = header[3];

    // Reserve space for the payload
    buffer.reserve(length);

    // Read first packet payload directly
    let start = buffer.len();
    buffer.resize(start + length, 0);
    reader
        .read_exact(&mut buffer[start..])
        .await
        .map_err(|e| Error::IoError(e))?;

    // If packet is exactly 16MB (0xFFFFFF bytes), there may be more packets
    let mut current_length = length;
    while current_length == 0xFFFFFF {
        // Read next packet header
        reader.read_exact(&mut header).await.map_err(|e| Error::IoError(e))?;

        current_length = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
        // sequence_id should increment but we don't verify it (non-priority)

        // Read and append next packet payload
        let prev_len = buffer.len();
        buffer.resize(prev_len + current_length, 0);
        reader
            .read_exact(&mut buffer[prev_len..])
            .await
            .map_err(|e| Error::IoError(e))?;
    }

    Ok(sequence_id)
}

/// Write a MySQL packet asynchronously, splitting it into 16MB chunks if necessary
///
/// # Arguments
/// * `stream` - The async TCP stream to write to
/// * `sequence_id` - Starting sequence ID (will auto-increment for multi-packet)
/// * `payload` - The payload bytes to send
async fn write_payload<W: AsyncWrite + Unpin>(
    stream: &mut W,
    mut sequence_id: u8,
    payload: &[u8],
) -> Result<()> {
    let mut remaining = payload;
    let mut chunk_size = 0;

    while !remaining.is_empty() {
        chunk_size = remaining.len().min(0xFFFFFF);
        let chunk;
        (chunk, remaining) = remaining.split_at(chunk_size);

        // Write header using a stack-allocated buffer
        let header = write_packet_header_array(sequence_id, chunk_size);

        // Write header and payload separately without allocating
        stream
            .write_all(&header)
            .await
            .map_err(|e| Error::IoError(e))?;
        stream
            .write_all(chunk)
            .await
            .map_err(|e| Error::IoError(e))?;

        sequence_id = sequence_id.wrapping_add(1);
    }

    // If the last chunk was exactly 0xFFFFFF bytes, send an empty packet to signal EOF
    if chunk_size == 0xFFFFFF {
        let header = write_packet_header_array(sequence_id, 0);
        stream
            .write_all(&header)
            .await
            .map_err(|e| Error::IoError(e))?;
    }

    Ok(())
}
