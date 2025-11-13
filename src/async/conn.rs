use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::constant::CapabilityFlags;
use crate::error::{Error, Result};
use crate::protocol::command::prepared::{read_prepare_ok, write_execute, write_prepare};
use crate::protocol::connection::handshake::{Handshake, HandshakeResult};
use crate::protocol::packet::write_packet_header_array;
use crate::protocol::r#trait::{params::Params, ResultSetHandler, TextResultSetHandler};
use crate::protocol::packet::ErrPayloadBytes;

/// A MySQL connection with a buffered async TCP stream
///
/// This struct holds the connection state including server information
/// obtained during the handshake phase.
pub struct Conn {
    stream: BufReader<TcpStream>,
    server_version: String,
    capability_flags: CapabilityFlags,
    /// Reusable buffer for reading payloads (reduces heap allocations)
    read_buffer: Vec<u8>,
    /// Reusable buffer for building outgoing commands (reduces heap allocations)
    write_buffer: Vec<u8>,
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

        // Create handshake state machine
        let mut handshake = Handshake::new(
            username.to_string(),
            password.to_string(),
            database.map(|s| s.to_string()),
        );

        // Drive the handshake state machine
        let (server_version, capability_flags) = loop {
            // Read next packet
            buffer.clear();
            let mut seq = read_payload(&mut conn_stream, &mut buffer).await?;

            // Drive state machine with the payload
            match handshake.drive(&buffer)? {
                HandshakeResult::Write(packet_data) => {
                    // Write packet to server
                    if !packet_data.is_empty() {
                        seq = seq.wrapping_add(1);
                        write_payload(conn_stream.get_mut(), seq, &packet_data).await?;
                    }
                    // Continue to read next response
                }
                HandshakeResult::Connected {
                    server_version,
                    capability_flags,
                } => {
                    // Handshake complete
                    break (server_version, capability_flags);
                }
            }
        };

        Ok(Self {
            stream: conn_stream,
            server_version,
            capability_flags,
            read_buffer: Vec::new(),
            write_buffer: Vec::new(),
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

    /// Prepare a statement and return the statement ID (async)
    ///
    /// This sends a COM_STMT_PREPARE command to the server and returns the statement ID
    /// that can be used with exec_* methods.
    ///
    /// # Arguments
    /// * `sql` - The SQL statement to prepare
    ///
    /// # Returns
    /// * `Ok(statement_id)` - The prepared statement ID
    /// * `Err(Error)` - Prepare failed
    pub async fn prepare(&mut self, sql: &str) -> Result<u32> {
        // Reuse struct buffers to avoid heap allocations
        self.read_buffer.clear();
        self.write_buffer.clear();

        // Write COM_STMT_PREPARE
        write_prepare(&mut self.write_buffer, sql);
        write_payload(self.stream.get_mut(), 0, &self.write_buffer).await?;

        // Read response
        let _seq = read_payload(&mut self.stream, &mut self.read_buffer).await?;

        // Check for error
        if !self.read_buffer.is_empty() && self.read_buffer[0] == 0xFF {
            Err(ErrPayloadBytes(&self.read_buffer))?
        }

        // Parse PrepareOk
        let prepare_ok = read_prepare_ok(&self.read_buffer)?;
        let statement_id = prepare_ok.statement_id.get();
        let num_params = prepare_ok.num_params.get();
        let num_columns = prepare_ok.num_columns.get();

        // Skip parameter definitions if present
        if num_params > 0 {
            for _ in 0..num_params {
                let _seq = read_payload(&mut self.stream, &mut self.read_buffer).await?;
            }
            // Read EOF packet after params (if CLIENT_DEPRECATE_EOF not set)
            if !self
                .capability_flags
                .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF)
            {
                let _seq = read_payload(&mut self.stream, &mut self.read_buffer).await?;
            }
        }

        // Skip column definitions if present
        if num_columns > 0 {
            for _ in 0..num_columns {
                let _seq = read_payload(&mut self.stream, &mut self.read_buffer).await?;
            }
            // Read EOF packet after columns (if CLIENT_DEPRECATE_EOF not set)
            if !self
                .capability_flags
                .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF)
            {
                let _seq = read_payload(&mut self.stream, &mut self.read_buffer).await?;
            }
        }

        Ok(statement_id)
    }

    /// Execute a prepared statement with a result set handler (async)
    ///
    /// This method provides a streaming, callback-based API for processing query results.
    /// It drives the exec state machine and calls handler methods for each event.
    ///
    /// # Arguments
    /// * `statement_id` - The prepared statement ID
    /// * `params` - Parameters implementing the Params trait
    /// * `handler` - Mutable reference to a ResultSetHandler implementation
    /// * `buffer` - Reusable buffer for reading packets (to minimize allocations)
    ///
    /// # Returns
    /// * `Ok(())` - Query execution completed successfully
    /// * `Err(Error)` - Query execution or handler callback failed
    pub async fn exec_fold<'a, P, H>(
        &mut self,
        statement_id: u32,
        params: &P,
        handler: &mut H,
        buffer: &mut Vec<u8>,
    ) -> Result<()>
    where
        P: Params,
        H: ResultSetHandler<'a>,
    {
        use crate::protocol::command::prepared::{Exec, ExecResult};

        // Write COM_STMT_EXECUTE - reuse struct buffer to avoid heap allocations
        self.write_buffer.clear();
        write_execute(&mut self.write_buffer, statement_id, params)?;
        write_payload(self.stream.get_mut(), 0, &self.write_buffer).await?;

        // Create the state machine
        let mut exec_fold = Exec::new();

        // Drive the state machine: read payloads and drive
        loop {
            // Read the next packet from network
            buffer.clear();
            read_payload(&mut self.stream, buffer).await?;

            // Drive state machine with the payload and handle events
            let result = exec_fold.drive(&buffer[..])?;
            match result {
                ExecResult::NeedPayload => {
                    continue;
                }
                ExecResult::NoResultSet(ok_bytes) => {
                    handler.no_result_set(ok_bytes)?;
                    return Ok(());
                }

                ExecResult::ResultSetStart { num_columns } => {
                    handler.resultset_start(num_columns)?;
                }
                ExecResult::Column(col) => {
                    handler.col(col)?;
                }
                ExecResult::Row(row) => {
                    handler.row(&row)?;
                }
                ExecResult::Eof(eof_bytes) => {
                    handler.resultset_end(eof_bytes)?;
                    return Ok(());
                }
            }
        }
    }

    /// Execute a text protocol SQL query (async)
    ///
    /// # Arguments
    /// * `sql` - SQL query to execute
    /// * `handler` - Handler for result set events
    /// * `buffer` - Reusable buffer for reading payloads
    ///
    /// # Returns
    /// * `Ok(())` - Query executed successfully
    /// * `Err(Error)` - Query failed
    pub async fn query<'a, H>(
        &mut self,
        sql: &str,
        handler: &mut H,
        buffer: &mut Vec<u8>,
    ) -> Result<()>
    where
        H: TextResultSetHandler<'a>,
    {
        use crate::protocol::command::query::{write_query, Query, QueryResult};

        // Write COM_QUERY - reuse struct buffer to avoid heap allocations
        self.write_buffer.clear();
        write_query(&mut self.write_buffer, sql);
        write_payload(self.stream.get_mut(), 0, &self.write_buffer).await?;

        // Create the state machine
        let mut query_fold = Query::new();

        // Drive the state machine: read payloads and drive
        loop {
            // Read the next packet from network
            buffer.clear();
            read_payload(&mut self.stream, buffer).await?;

            // Drive state machine with the payload and handle events
            let result = query_fold.drive(&buffer[..])?;
            match result {
                QueryResult::NeedPayload => {
                    continue;
                }
                QueryResult::NoResultSet(ok_bytes) => {
                    handler.no_result_set(ok_bytes)?;
                    return Ok(());
                }
                QueryResult::ResultSetStart { num_columns } => {
                    handler.resultset_start(num_columns)?;
                }
                QueryResult::Column(col) => {
                    handler.col(col)?;
                }
                QueryResult::Row(row) => {
                    handler.row(&row)?;
                }
                QueryResult::Eof(eof_bytes) => {
                    handler.resultset_end(eof_bytes)?;
                    return Ok(());
                }
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
