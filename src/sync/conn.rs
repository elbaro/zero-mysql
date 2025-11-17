use std::io::{BufRead, BufReader, IoSlice, Write};
use std::net::TcpStream;

use crate::constant::CapabilityFlags;
use crate::error::{Error, Result};
use crate::protocol::command::prepared::write_execute;
use crate::protocol::command::prepared::{read_prepare_ok, write_prepare};
use crate::protocol::packet::write_packet_header_array;
use crate::protocol::packet::ErrPayloadBytes;
use crate::protocol::r#trait::{params::Params, ResultSetHandler, TextResultSetHandler};

/// A MySQL connection with a buffered TCP stream
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
    /// Reusable buffer for packet headers when writing payloads (reduces heap allocations)
    write_headers_buffer: Vec<[u8; 4]>,
    /// Reusable buffer for IoSlice when writing payloads (reduces heap allocations)
    ioslice_buffer: Vec<IoSlice<'static>>,
}

impl Conn {
    /// Create a new MySQL connection from a URL
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
    pub fn new(url: &str) -> Result<Self> {
        // Parse URL
        let parsed = url::Url::parse(url)
            .map_err(|e| Error::BadInputError(format!("Failed to parse MySQL URL: {}", e)))?;

        // Verify scheme
        if parsed.scheme() != "mysql" {
            return Err(Error::BadInputError(format!(
                "Invalid URL scheme '{}', expected 'mysql'",
                parsed.scheme()
            )));
        }

        // Extract host
        let host = parsed
            .host_str()
            .ok_or_else(|| Error::BadInputError("Missing host in MySQL URL".to_string()))?;

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
        let stream = TcpStream::connect(&addr)?;
        stream.set_nodelay(true)?;

        Self::new_with_stream(stream, username, password, database)
    }

    /// Create a new MySQL connection with an existing TCP stream
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
    pub fn new_with_stream(
        stream: TcpStream,
        username: &str,
        password: &str,
        database: Option<&str>,
    ) -> Result<Self> {
        use crate::protocol::connection::handshake::{Handshake, HandshakeResult};

        let mut conn_stream = BufReader::new(stream);
        let mut buffer = Vec::new();
        let mut headers_buffer = Vec::new();
        let mut ioslice_buffer = Vec::new();

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
            let mut seq = read_payload(&mut conn_stream, &mut buffer)?;

            // Drive state machine with the payload
            match handshake.drive(&buffer)? {
                HandshakeResult::Write(packet_data) => {
                    // Write packet to server
                    if !packet_data.is_empty() {
                        seq = seq.wrapping_add(1);
                        write_handshake_payload(
                            &mut conn_stream.get_mut(),
                            seq,
                            &packet_data,
                            &mut headers_buffer,
                            &mut ioslice_buffer,
                        )?;
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
            write_headers_buffer: Vec::new(),
            ioslice_buffer: Vec::new(),
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

    /// Write a MySQL packet from write_buffer, splitting it into 16MB chunks if necessary
    ///
    /// # Arguments
    /// * `sequence_id` - Starting sequence ID (will auto-increment for multi-packet)
    fn write_payload(&mut self, mut sequence_id: u8) -> Result<()> {
        self.write_headers_buffer.clear();
        self.ioslice_buffer.clear();

        let payload = self.write_buffer.as_slice();
        let mut remaining = payload;
        let mut chunk_size = 0;

        // Build all headers
        while !remaining.is_empty() {
            chunk_size = remaining.len().min(0xFFFFFF);
            let (_chunk, rest) = remaining.split_at(chunk_size);
            remaining = rest;

            // Write header using a stack-allocated buffer
            let header = write_packet_header_array(sequence_id, chunk_size);
            self.write_headers_buffer.push(header);

            sequence_id = sequence_id.wrapping_add(1);
        }

        // If the last chunk was exactly 0xFFFFFF bytes, add an empty packet to signal EOF
        if chunk_size == 0xFFFFFF {
            let header = write_packet_header_array(sequence_id, 0);
            self.write_headers_buffer.push(header);
        }

        // Build IoSlice array with all headers and chunks
        remaining = payload;
        for header in self.write_headers_buffer.iter() {
            let chunk_size = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;

            // Safety: We extend the lifetime of IoSlice to 'static for storage in the Vec.
            // This is safe because:
            // 1. The IoSlice references are only used in write_all_vectored() below
            // 2. Both header and payload outlive this function call
            // 3. ioslice_buffer is cleared at the start of each write_payload call
            self.ioslice_buffer
                .push(unsafe { std::mem::transmute(IoSlice::new(header)) });

            if chunk_size > 0 {
                let chunk;
                (chunk, remaining) = remaining.split_at(chunk_size);
                self.ioslice_buffer
                    .push(unsafe { std::mem::transmute(IoSlice::new(chunk)) });
            }
        }

        // Write all chunks at once
        self.stream
            .get_mut()
            .write_all_vectored(&mut self.ioslice_buffer)
            .map_err(|e| Error::IoError(e))?;

        self.stream.get_mut().flush()?;

        Ok(())
    }

    /// Prepare a SQL statement
    ///
    /// # Arguments
    /// * `sql` - SQL statement to prepare
    ///
    /// # Returns
    /// * `Ok(statement_id)` - Statement ID for use in execute
    /// * `Err(Error)` - Preparation failed
    pub fn prepare(&mut self, sql: &str) -> Result<u32> {
        // Reuse struct buffers to avoid heap allocations
        self.read_buffer.clear();
        self.write_buffer.clear();

        // Write COM_STMT_PREPARE
        write_prepare(&mut self.write_buffer, sql);

        self.write_payload(0)?;
        // Read response
        let _seq = read_payload(&mut self.stream, &mut self.read_buffer)?;

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
                let _seq = read_payload(&mut self.stream, &mut self.read_buffer)?;
            }
            // Read EOF packet after params (if CLIENT_DEPRECATE_EOF not set)
            if !self
                .capability_flags
                .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF)
            {
                let _seq = read_payload(&mut self.stream, &mut self.read_buffer)?;
            }
        }

        // Skip column definitions if present
        if num_columns > 0 {
            for _ in 0..num_columns {
                let _seq = read_payload(&mut self.stream, &mut self.read_buffer)?;
            }
            // Read EOF packet after columns (if CLIENT_DEPRECATE_EOF not set)
            if !self
                .capability_flags
                .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF)
            {
                let _seq = read_payload(&mut self.stream, &mut self.read_buffer)?;
            }
        }

        Ok(statement_id)
    }

    pub fn exec<'a, P, H>(&mut self, statement_id: u32, params: P, handler: &mut H) -> Result<()>
    where
        P: Params,
        H: ResultSetHandler<'a>,
    {
        use crate::protocol::command::prepared::{Exec, ExecResult};

        // Write COM_STMT_EXECUTE - reuse struct buffer to avoid heap allocations
        self.write_buffer.clear();
        write_execute(&mut self.write_buffer, statement_id, params)?;

        self.write_payload(0)?;

        // Create the state machine
        let mut exec = Exec::new();

        // Drive the state machine: read payloads and drive
        loop {
            // Read the next packet from network
            self.read_buffer.clear();
            read_payload(&mut self.stream, &mut self.read_buffer)?;

            // Drive state machine with the payload and handle events
            let result = exec.drive(&self.read_buffer[..])?;
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

    /// Execute a prepared statement and return only the first row, dropping the rest
    ///
    /// This is optimized for queries where you only need the first result.
    /// After receiving the first row, it efficiently discards remaining rows without
    /// processing them through the handler.
    ///
    /// # Arguments
    /// * `statement_id` - The prepared statement ID
    /// * `params` - Parameters implementing the Params trait
    /// * `handler` - Mutable reference to a ResultSetHandler implementation
    ///
    /// # Returns
    /// * `Ok(true)` - First row was found and processed
    /// * `Ok(false)` - No rows in result set
    /// * `Err(Error)` - Query execution or handler callback failed
    pub fn exec_first<'a, P, H>(
        &mut self,
        statement_id: u32,
        params: P,
        handler: &mut H,
    ) -> Result<bool>
    where
        P: Params,
        H: ResultSetHandler<'a>,
    {
        use crate::protocol::command::prepared::{Exec, ExecResult};

        // Write COM_STMT_EXECUTE - reuse struct buffer to avoid heap allocations
        self.write_buffer.clear();
        write_execute(&mut self.write_buffer, statement_id, params)?;

        self.write_payload(0)?;

        // Create the state machine
        let mut exec = Exec::new();
        let mut first_row_found = false;

        // Drive the state machine: read payloads and drive
        loop {
            // Read the next packet from network
            self.read_buffer.clear();
            read_payload(&mut self.stream, &mut self.read_buffer)?;

            // Drive state machine with the payload and handle events
            let result = exec.drive(&self.read_buffer[..])?;
            match result {
                ExecResult::NeedPayload => {
                    continue;
                }
                ExecResult::NoResultSet(ok_bytes) => {
                    handler.no_result_set(ok_bytes)?;
                    return Ok(false);
                }

                ExecResult::ResultSetStart { num_columns } => {
                    handler.resultset_start(num_columns)?;
                }
                ExecResult::Column(col) => {
                    handler.col(col)?;
                }
                ExecResult::Row(row) => {
                    if !first_row_found {
                        handler.row(&row)?;
                        first_row_found = true;
                        // Continue reading to drain remaining packets but don't process them
                    }
                    // Skip processing subsequent rows
                }
                ExecResult::Eof(eof_bytes) => {
                    handler.resultset_end(eof_bytes)?;
                    return Ok(first_row_found);
                }
            }
        }
    }

    /// Execute a prepared statement and discard all results
    ///
    /// This is optimized for queries where you don't need to process any results,
    /// such as INSERT/UPDATE/DELETE statements or when you only care about whether
    /// the query succeeded.
    ///
    /// # Arguments
    /// * `statement_id` - The prepared statement ID
    /// * `params` - Parameters implementing the Params trait
    ///
    /// # Returns
    /// * `Ok(())` - Query executed successfully
    /// * `Err(Error)` - Query execution failed
    pub fn exec_drop<P>(&mut self, statement_id: u32, params: P) -> Result<()>
    where
        P: Params,
    {
        use crate::protocol::command::prepared::{Exec, ExecResult};

        // Write COM_STMT_EXECUTE - reuse struct buffer to avoid heap allocations
        self.write_buffer.clear();
        write_execute(&mut self.write_buffer, statement_id, params)?;

        self.write_payload(0)?;

        // Create the state machine
        let mut exec = Exec::new();

        // Drive the state machine: read payloads and drive, but don't process results
        loop {
            // Read the next packet from network
            self.read_buffer.clear();
            read_payload(&mut self.stream, &mut self.read_buffer)?;

            // Drive state machine with the payload
            let result = exec.drive(&self.read_buffer[..])?;

            match result {
                ExecResult::NeedPayload => {
                    continue;
                }
                ExecResult::NoResultSet(_ok_bytes) => {
                    // No result set, query complete
                    return Ok(());
                }
                ExecResult::ResultSetStart { .. } => {
                    // Start of result set, continue to drain
                }
                ExecResult::Column(_) => {
                    // Column definition, skip
                }
                ExecResult::Row(_) => {
                    // Row data, skip
                }
                ExecResult::Eof(_eof_bytes) => {
                    // End of result set
                    return Ok(());
                }
            }
        }
    }

    /// Execute a text protocol SQL query
    ///
    /// # Arguments
    /// * `sql` - SQL query to execute
    /// * `handler` - Handler for result set events
    ///
    /// # Returns
    /// * `Ok(())` - Query executed successfully
    /// * `Err(Error)` - Query failed
    pub fn query<'a, H>(&mut self, sql: &str, handler: &mut H) -> Result<()>
    where
        H: TextResultSetHandler<'a>,
    {
        use crate::protocol::command::query::{write_query, Query, QueryResult};

        // Write COM_QUERY - reuse struct buffer to avoid heap allocations
        self.write_buffer.clear();
        write_query(&mut self.write_buffer, sql);

        self.write_payload(0)?;

        // Create the state machine
        let mut query_fold = Query::new();

        // Drive the state machine: read payloads and drive
        loop {
            // Read the next packet from network
            self.read_buffer.clear();
            read_payload(&mut self.stream, &mut self.read_buffer)?;

            // Drive state machine with the payload and handle events
            let result = query_fold.drive(&self.read_buffer[..])?;
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

    /// Execute a text protocol SQL query and discard all results
    ///
    /// This is optimized for queries where you don't need to process any results,
    /// such as DDL statements (CREATE, DROP, ALTER), DML statements without results
    /// (INSERT, UPDATE, DELETE), or when you only care about whether the query succeeded.
    ///
    /// # Arguments
    /// * `sql` - SQL query to execute
    ///
    /// # Returns
    /// * `Ok(())` - Query executed successfully
    /// * `Err(Error)` - Query execution failed
    pub fn query_drop(&mut self, sql: &str) -> Result<()> {
        use crate::protocol::command::query::{write_query, Query, QueryResult};

        // Write COM_QUERY - reuse struct buffer to avoid heap allocations
        self.write_buffer.clear();
        write_query(&mut self.write_buffer, sql);

        self.write_payload(0)?;

        // Create the state machine
        let mut query = Query::new();

        // Drive the state machine: read payloads and drive, but don't process results
        loop {
            // Read the next packet from network
            self.read_buffer.clear();
            read_payload(&mut self.stream, &mut self.read_buffer)?;

            // Drive state machine with the payload
            let result = query.drive(&self.read_buffer[..])?;
            match result {
                QueryResult::NeedPayload => {
                    continue;
                }
                QueryResult::NoResultSet(_ok_bytes) => {
                    // No result set, query complete
                    return Ok(());
                }
                QueryResult::ResultSetStart { .. } => {
                    // Start of result set, continue to drain
                }
                QueryResult::Column(_) => {
                    // Column definition, skip
                }
                QueryResult::Row(_) => {
                    // Row data, skip
                }
                QueryResult::Eof(_eof_bytes) => {
                    // End of result set
                    return Ok(());
                }
            }
        }
    }
}

/// Read a complete MySQL payload, concatenating packets if they span multiple 16MB chunks.
/// This function performs minimal copies and uses buffered reads to reduce syscalls.
///
/// # Arguments
/// * `reader` - A buffered reader (e.g., BufReader<TcpStream>)
/// * `buffer` - Reusable buffer for storing the payload (to minimize allocations)
///
/// # Returns
/// * `Ok(sequence_id)` - The sequence ID; the payload is stored in `buffer`
/// * `Err(Error)` - IO error or protocol error
pub fn read_payload<R: BufRead>(reader: &mut R, buffer: &mut Vec<u8>) -> Result<u8> {
    buffer.clear();

    // Read first packet header (4 bytes)
    let mut header = [0u8; 4];
    reader
        .read_exact(&mut header)
        .map_err(|e| Error::IoError(e))?;

    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    let sequence_id = header[3];

    // Reserve space for the payload
    buffer.reserve(length);

    // Read first packet payload directly
    let start = buffer.len();
    buffer.resize(start + length, 0);
    reader
        .read_exact(&mut buffer[start..])
        .map_err(|e| Error::IoError(e))?;

    // If packet is exactly 16MB (0xFFFFFF bytes), there may be more packets
    let mut current_length = length;
    while current_length == 0xFFFFFF {
        // Read next packet header
        let mut next_header = [0u8; 4];
        reader
            .read_exact(&mut next_header)
            .map_err(|e| Error::IoError(e))?;

        current_length =
            u32::from_le_bytes([next_header[0], next_header[1], next_header[2], 0]) as usize;
        // sequence_id should increment but we don't verify it (non-priority)

        // Read and append next packet payload
        let prev_len = buffer.len();
        buffer.resize(prev_len + current_length, 0);
        reader
            .read_exact(&mut buffer[prev_len..])
            .map_err(|e| Error::IoError(e))?;
    }

    Ok(sequence_id)
}

/// Write a MySQL packet during handshake, splitting it into 16MB chunks if necessary
/// (standalone version for use before Conn is fully initialized)
///
/// # Arguments
/// * `stream` - The TCP stream to write to
/// * `sequence_id` - Starting sequence ID (will auto-increment for multi-packet)
/// * `payload` - The payload bytes to send
/// * `headers_buffer` - Reusable buffer for packet headers (reduces heap allocations)
/// * `ioslice_buffer` - Reusable buffer for IoSlice (reduces heap allocations)
fn write_handshake_payload<W: Write>(
    stream: &mut W,
    mut sequence_id: u8,
    payload: &[u8],
    headers_buffer: &mut Vec<[u8; 4]>,
    ioslice_buffer: &mut Vec<IoSlice<'static>>,
) -> Result<()> {
    headers_buffer.clear();
    ioslice_buffer.clear();

    let mut remaining = payload;
    let mut chunk_size = 0;

    // Build all headers
    while !remaining.is_empty() {
        chunk_size = remaining.len().min(0xFFFFFF);
        let (_chunk, rest) = remaining.split_at(chunk_size);
        remaining = rest;

        // Write header using a stack-allocated buffer
        let header = write_packet_header_array(sequence_id, chunk_size);
        headers_buffer.push(header);

        sequence_id = sequence_id.wrapping_add(1);
    }

    // If the last chunk was exactly 0xFFFFFF bytes, add an empty packet to signal EOF
    if chunk_size == 0xFFFFFF {
        let header = write_packet_header_array(sequence_id, 0);
        headers_buffer.push(header);
    }

    // Build IoSlice array with all headers and chunks
    remaining = payload;
    for header in headers_buffer.iter() {
        let chunk_size = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;

        // Safety: We extend the lifetime of IoSlice to 'static for storage in the Vec.
        // This is safe because:
        // 1. The IoSlice references are only used in write_all_vectored() below
        // 2. Both header and payload outlive this function call
        // 3. ioslice_buffer is cleared at the start of each write_payload call
        ioslice_buffer.push(unsafe { std::mem::transmute(IoSlice::new(header)) });

        if chunk_size > 0 {
            let chunk;
            (chunk, remaining) = remaining.split_at(chunk_size);
            ioslice_buffer.push(unsafe { std::mem::transmute(IoSlice::new(chunk)) });
        }
    }

    // Write all chunks at once
    stream
        .write_all_vectored(ioslice_buffer)
        .map_err(|e| Error::IoError(e))?;

    stream.flush()?;

    Ok(())
}
