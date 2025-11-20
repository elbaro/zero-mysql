use crate::protocol::packet::PacketHeader;
use compio::buf::BufResult;
use compio::buf::IntoInner;
use compio::buf::IoBuf;
use compio::io::{AsyncReadExt, AsyncWriteExt};
use compio::net::TcpStream;
use tracing::instrument;

use crate::constant::CapabilityFlags;
use crate::error::{Error, Result};
use crate::protocol::command::prepared::{read_prepare_ok, write_execute, write_prepare};
use crate::protocol::connection::handshake::{Handshake, HandshakeResult};
use crate::protocol::packet::ErrPayloadBytes;
use crate::protocol::r#trait::{ResultSetHandler, TextResultSetHandler, params::Params};

use zerocopy::IntoBytes;

/// A MySQL connection with an async TCP stream (compio runtime)
///
/// This struct holds the connection state including server information
/// obtained during the handshake phase.
pub struct Conn {
    stream: TcpStream,
    server_version: String,
    capability_flags: CapabilityFlags,
    /// Buffer pool for reading payloads (reduces heap allocations)
    buffer_pool: Vec<Vec<u8>>,
    /// Reusable buffer for building outgoing commands (reduces heap allocations)
    write_buffer: Vec<u8>,
    /// Reusable buffer for assembling complete packets with headers (reduces heap allocations)
    packet_buf: Vec<u8>,
}

impl Conn {
    /// Create a new MySQL connection from connection options (async)
    ///
    /// This performs the complete MySQL handshake protocol:
    /// 1. Parses the connection options
    /// 2. Connects to the MySQL server via TCP or Unix socket
    /// 3. Reads initial handshake from server
    /// 4. Sends handshake response with authentication
    /// 5. Handles auth plugin switching if needed
    /// 6. Returns ready-to-use connection
    ///
    /// # Arguments
    /// * `opts` - Connection options (can be a URL string or an Opts struct)
    ///
    /// # Examples
    /// ```
    /// // Using a URL string
    /// let conn = Conn::new("mysql://root:password@localhost:3306/mydb").await?;
    ///
    /// // Using an Opts struct
    /// let opts = Opts {
    ///     host: Some("localhost".to_string()),
    ///     port: 3306,
    ///     user: "root".to_string(),
    ///     password: Some("password".to_string()),
    ///     db: Some("mydb".to_string()),
    ///     ..Default::default()
    /// };
    /// let conn = Conn::new(opts).await?;
    /// ```
    ///
    /// # Returns
    /// * `Ok(Conn)` - Authenticated connection ready for queries
    /// * `Err(Error)` - Connection or authentication failed
    pub async fn new<O: TryInto<crate::opts::Opts>>(opts: O) -> Result<Self>
    where
        Error: From<O::Error>,
    {
        let opts: crate::opts::Opts = opts.try_into()?;

        // Handle socket connection
        if let Some(_socket) = &opts.socket {
            todo!("Unix socket connections not yet implemented");
        }

        // Extract host
        let host = opts.host.as_ref().ok_or_else(|| {
            Error::BadConfigError("Missing host in connection options".to_string())
        })?;

        // Connect to server
        let stream = TcpStream::connect((host.as_str(), opts.port)).await?;
        // TODO: Set TCP_NODELAY using socket options if needed

        Self::new_with_stream(
            stream,
            &opts.user,
            opts.password.as_deref().unwrap_or(""),
            opts.db.as_deref(),
        )
        .await
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
        mut stream: TcpStream,
        username: &str,
        password: &str,
        database: Option<&str>,
    ) -> Result<Self> {
        let mut buffer_pool = vec![Vec::new()];

        // Create handshake state machine
        let mut handshake = Handshake::new(
            username.to_string(),
            password.to_string(),
            database.map(|s| s.to_string()),
        );

        // Drive the handshake state machine
        let (server_version, capability_flags) = loop {
            // Get buffer from pool or create new one
            let buffer = buffer_pool.pop().unwrap_or_else(Vec::new);

            // Read next packet
            let (seq, buffer) = read_payload(&mut stream, buffer).await?;

            // Drive state machine with the payload
            match handshake.drive(&buffer)? {
                HandshakeResult::Write(packet_data) => {
                    // Write packet to server
                    if !packet_data.is_empty() {
                        let next_seq = seq.wrapping_add(1);
                        write_handshake_payload(&mut stream, next_seq, &packet_data).await?;
                    }
                    // Return buffer to pool
                    buffer_pool.push(buffer);
                    // Continue to read next response
                }
                HandshakeResult::Connected {
                    server_version,
                    capability_flags,
                } => {
                    // Return buffer to pool
                    buffer_pool.push(buffer);
                    // Handshake complete
                    break (server_version, capability_flags);
                }
            }
        };

        Ok(Self {
            stream,
            server_version,
            capability_flags,
            buffer_pool,
            write_buffer: Vec::new(),
            packet_buf: Vec::new(),
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

    /// Get a buffer from the pool or create a new one
    fn get_buffer(&mut self) -> Vec<u8> {
        self.buffer_pool.pop().unwrap_or_default()
    }

    /// Return a buffer to the pool
    fn return_buffer(&mut self, buffer: Vec<u8>) {
        if self.buffer_pool.len() < 8 {
            // Keep pool size reasonable
            self.buffer_pool.push(buffer);
        }
    }

    /// Write a MySQL packet from write_buffer asynchronously, splitting it into 16MB chunks if necessary
    ///
    /// # Arguments
    /// * `sequence_id` - Starting sequence ID (will auto-increment for multi-packet)
    #[instrument(skip_all)]
    async fn write_payload(&mut self, mut sequence_id: u8) -> Result<()> {
        let payload = self.write_buffer.as_slice();

        // Calculate number of chunks needed
        let num_chunks = payload.len() / 0xFFFFFF + 1;
        let needs_empty_packet = payload.len().is_multiple_of(0xFFFFFF) && !payload.is_empty();
        let total_size = num_chunks * 4 + payload.len();

        // Reuse packet buffer, reserve capacity if needed
        self.packet_buf.clear();
        if self.packet_buf.capacity() < total_size {
            self.packet_buf
                .reserve(total_size - self.packet_buf.capacity());
        }

        // Build packet with headers and chunks
        let mut remaining = payload;
        while !remaining.is_empty() {
            let chunk_size = remaining.len().min(0xFFFFFF);
            let (chunk, rest) = remaining.split_at(chunk_size);

            // Write header
            let header = PacketHeader::encode(chunk_size, sequence_id);
            self.packet_buf.extend_from_slice(header.as_bytes());

            // Write chunk
            self.packet_buf.extend_from_slice(chunk);

            remaining = rest;
            sequence_id = sequence_id.wrapping_add(1);
        }

        // Add empty packet if last chunk was exactly 0xFFFFFF bytes
        if needs_empty_packet {
            let header = PacketHeader::encode(0, sequence_id);
            self.packet_buf.extend_from_slice(header.as_bytes());
        }

        // Write all data - take ownership of packet_buf
        let packet_buf = std::mem::take(&mut self.packet_buf);
        let BufResult(result, packet_buf) = self.stream.write_all(packet_buf).await;
        result.map_err(Error::IoError)?;
        self.packet_buf = packet_buf;

        Ok(())
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
        // Clear write buffer
        self.write_buffer.clear();

        // Write COM_STMT_PREPARE
        write_prepare(&mut self.write_buffer, sql);

        self.write_payload(0).await?;

        // Get buffer from pool
        let buffer = self.get_buffer();

        // Read response
        let (_seq, buffer) = read_payload(&mut self.stream, buffer).await?;

        // Check for error
        if !buffer.is_empty() && buffer[0] == 0xFF {
            // Clone error payload before returning buffer
            let err = ErrPayloadBytes(&buffer).into();
            self.return_buffer(buffer);
            return Err(err);
        }

        // Parse PrepareOk
        let prepare_ok = read_prepare_ok(&buffer)?;
        let statement_id = prepare_ok.statement_id.get();
        let num_params = prepare_ok.num_params.get();
        let num_columns = prepare_ok.num_columns.get();

        // Return buffer to pool
        self.return_buffer(buffer);

        // Skip parameter definitions if present
        for _ in 0..num_params {
            let buffer = self.get_buffer();
            let (_seq, buffer) = read_payload(&mut self.stream, buffer).await?;
            self.return_buffer(buffer);
        }

        // Skip column definitions if present
        for _ in 0..num_columns {
            let buffer = self.get_buffer();
            let (_seq, buffer) = read_payload(&mut self.stream, buffer).await?;
            self.return_buffer(buffer);
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
    ///
    /// # Returns
    /// * `Ok(())` - Query execution completed successfully
    /// * `Err(Error)` - Query execution or handler callback failed
    pub async fn exec<'a, P, H>(
        &mut self,
        statement_id: u32,
        params: P,
        handler: &mut H,
    ) -> Result<()>
    where
        P: Params,
        H: ResultSetHandler<'a>,
    {
        use crate::protocol::command::prepared::{Exec, ExecResult};

        // Write COM_STMT_EXECUTE
        self.write_buffer.clear();
        write_execute(&mut self.write_buffer, statement_id, params)?;

        self.write_payload(0).await?;

        // Create the state machine
        let mut exec = Exec::default();

        // Drive the state machine: read payloads and drive
        loop {
            // Get buffer from pool
            let buffer = self.get_buffer();

            // Read the next packet from network
            let (_, buffer) = read_payload(&mut self.stream, buffer).await?;

            // Drive state machine with the payload and handle events
            let result = exec.drive(&buffer[..])?;
            match result {
                ExecResult::NeedPayload => {
                    self.return_buffer(buffer);
                    continue;
                }
                ExecResult::NoResultSet(ok_bytes) => {
                    handler.no_result_set(ok_bytes)?;
                    self.return_buffer(buffer);
                    return Ok(());
                }

                ExecResult::ResultSetStart { num_columns } => {
                    handler.resultset_start(num_columns)?;
                    self.return_buffer(buffer);
                }
                ExecResult::Column(col) => {
                    handler.col(col)?;
                    self.return_buffer(buffer);
                }
                ExecResult::Row(row) => {
                    handler.row(&row)?;
                    self.return_buffer(buffer);
                }
                ExecResult::Eof(eof_bytes) => {
                    handler.resultset_end(eof_bytes)?;
                    self.return_buffer(buffer);
                    return Ok(());
                }
            }
        }
    }

    /// Execute a prepared statement and return only the first row, dropping the rest (async)
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
    pub async fn exec_first<'a, P, H>(
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

        // Write COM_STMT_EXECUTE
        self.write_buffer.clear();
        write_execute(&mut self.write_buffer, statement_id, params)?;

        self.write_payload(0).await?;

        // Create the state machine
        let mut exec = Exec::default();
        let mut first_row_found = false;

        // Drive the state machine: read payloads and drive
        loop {
            // Get buffer from pool
            let buffer = self.get_buffer();

            // Read the next packet from network
            let (_, buffer) = read_payload(&mut self.stream, buffer).await?;

            // Drive state machine with the payload and handle events
            let result = exec.drive(&buffer[..])?;
            match result {
                ExecResult::NeedPayload => {
                    self.return_buffer(buffer);
                    continue;
                }
                ExecResult::NoResultSet(ok_bytes) => {
                    handler.no_result_set(ok_bytes)?;
                    self.return_buffer(buffer);
                    return Ok(false);
                }

                ExecResult::ResultSetStart { num_columns } => {
                    handler.resultset_start(num_columns)?;
                    self.return_buffer(buffer);
                }
                ExecResult::Column(col) => {
                    handler.col(col)?;
                    self.return_buffer(buffer);
                }
                ExecResult::Row(row) => {
                    if !first_row_found {
                        handler.row(&row)?;
                        first_row_found = true;
                        // Continue reading to drain remaining packets but don't process them
                    }
                    self.return_buffer(buffer);
                    // Skip processing subsequent rows
                }
                ExecResult::Eof(eof_bytes) => {
                    handler.resultset_end(eof_bytes)?;
                    self.return_buffer(buffer);
                    return Ok(first_row_found);
                }
            }
        }
    }

    /// Execute a prepared statement and discard all results (async)
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
    #[instrument(skip_all)]
    pub async fn exec_drop<P>(&mut self, statement_id: u32, params: P) -> Result<()>
    where
        P: Params,
    {
        use crate::protocol::command::prepared::{Exec, ExecResult};

        // Write COM_STMT_EXECUTE
        self.write_buffer.clear();
        write_execute(&mut self.write_buffer, statement_id, params)?;

        self.write_payload(0).await?;

        // Create the state machine
        let mut exec = Exec::default();

        // Drive the state machine: read payloads and drive, but don't process results
        loop {
            // Get buffer from pool
            let mut buffer = self.get_buffer();
            buffer.clear();

            // Read the next packet from network
            let (_, buffer) = read_payload(&mut self.stream, buffer).await?;

            // Drive state machine with the payload
            let result = exec.drive(&buffer[..])?;
            match result {
                ExecResult::NeedPayload => {
                    self.return_buffer(buffer);
                    continue;
                }
                ExecResult::NoResultSet(_ok_bytes) => {
                    // No result set, query complete
                    self.return_buffer(buffer);
                    return Ok(());
                }
                ExecResult::ResultSetStart { .. } => {
                    // Start of result set, continue to drain
                    self.return_buffer(buffer);
                }
                ExecResult::Column(_) => {
                    // Column definition, skip
                    self.return_buffer(buffer);
                }
                ExecResult::Row(_) => {
                    // Row data, skip
                    self.return_buffer(buffer);
                }
                ExecResult::Eof(_eof_bytes) => {
                    // End of result set
                    self.return_buffer(buffer);
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
    ///
    /// # Returns
    /// * `Ok(())` - Query executed successfully
    /// * `Err(Error)` - Query failed
    pub async fn query<'a, H>(&mut self, sql: &str, handler: &mut H) -> Result<()>
    where
        H: TextResultSetHandler<'a>,
    {
        use crate::protocol::command::query::{Query, QueryResult, write_query};

        // Write COM_QUERY
        self.write_buffer.clear();
        write_query(&mut self.write_buffer, sql);

        self.write_payload(0).await?;

        // Create the state machine
        let mut query_fold = Query::default();

        // Drive the state machine: read payloads and drive
        loop {
            // Get buffer from pool
            let buffer = self.get_buffer();

            // Read the next packet from network
            let (_, buffer) = read_payload(&mut self.stream, buffer).await?;

            // Drive state machine with the payload and handle events
            let result = query_fold.drive(&buffer[..])?;
            match result {
                QueryResult::NeedPayload => {
                    self.return_buffer(buffer);
                    continue;
                }
                QueryResult::NoResultSet(ok_bytes) => {
                    handler.no_result_set(ok_bytes)?;
                    self.return_buffer(buffer);
                    return Ok(());
                }
                QueryResult::ResultSetStart { num_columns } => {
                    handler.resultset_start(num_columns)?;
                    self.return_buffer(buffer);
                }
                QueryResult::Column(col) => {
                    handler.col(col)?;
                    self.return_buffer(buffer);
                }
                QueryResult::Row(row) => {
                    handler.row(&row)?;
                    self.return_buffer(buffer);
                }
                QueryResult::Eof(eof_bytes) => {
                    handler.resultset_end(eof_bytes)?;
                    self.return_buffer(buffer);
                    return Ok(());
                }
            }
        }
    }

    /// Execute a text protocol SQL query and discard all results (async)
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
    #[instrument(skip_all)]
    pub async fn query_drop(&mut self, sql: &str) -> Result<()> {
        use crate::protocol::command::query::{Query, QueryResult, write_query};

        // Write COM_QUERY
        self.write_buffer.clear();
        write_query(&mut self.write_buffer, sql);

        self.write_payload(0).await?;

        // Create the state machine
        let mut query = Query::default();

        // Drive the state machine: read payloads and drive, but don't process results
        loop {
            // Get buffer from pool
            let mut buffer = self.get_buffer();
            buffer.clear();

            // Read the next packet from network
            let (_, buffer) = read_payload(&mut self.stream, buffer).await?;

            // Drive state machine with the payload
            let result = query.drive(&buffer[..])?;
            match result {
                QueryResult::NeedPayload => {
                    self.return_buffer(buffer);
                    continue;
                }
                QueryResult::NoResultSet(_ok_bytes) => {
                    // No result set, query complete
                    self.return_buffer(buffer);
                    return Ok(());
                }
                QueryResult::ResultSetStart { .. } => {
                    // Start of result set, continue to drain
                    self.return_buffer(buffer);
                }
                QueryResult::Column(_) => {
                    // Column definition, skip
                    self.return_buffer(buffer);
                }
                QueryResult::Row(_) => {
                    // Row data, skip
                    self.return_buffer(buffer);
                }
                QueryResult::Eof(_eof_bytes) => {
                    // End of result set
                    self.return_buffer(buffer);
                    return Ok(());
                }
            }
        }
    }

    /// Send a ping to the server to check if the connection is alive (async)
    ///
    /// This sends a COM_PING command to the MySQL server and waits for an OK response.
    /// It's useful for checking connection health or preventing connection timeouts.
    ///
    /// # Returns
    /// * `Ok(())` - Server responded successfully (connection is alive)
    /// * `Err(Error)` - Ping failed (connection may be dead or network issue)
    pub async fn ping(&mut self) -> Result<()> {
        use crate::protocol::command::utility::write_ping;

        // Write COM_PING
        self.write_buffer.clear();
        write_ping(&mut self.write_buffer);

        self.write_payload(0).await?;

        // Read OK packet response (MySQL always returns OK for COM_PING)
        let buffer = self.get_buffer();
        let (_, buffer) = read_payload(&mut self.stream, buffer).await?;
        self.return_buffer(buffer);

        Ok(())
    }
}

/// Read a complete MySQL payload asynchronously, concatenating packets if they span multiple 16MB chunks.
/// This function uses compio's completion-based I/O with owned buffers.
///
/// # Arguments
/// * `reader` - An async reader (e.g., TcpStream)
/// * `buffer` - Buffer for storing the payload (ownership is transferred and returned)
///
/// # Returns
/// * `Ok((sequence_id, buffer))` - The sequence ID and buffer with the payload
/// * `Err(Error)` - IO error or protocol error
#[instrument(skip_all)]
pub async fn read_payload<R>(reader: &mut R, mut buffer: Vec<u8>) -> Result<(u8, Vec<u8>)>
where
    R: AsyncReadExt + Unpin,
{
    // Read first packet header (4 bytes)
    let header = [0u8; 4];
    let BufResult(result, header) = reader.read_exact(header).await;
    result.map_err(Error::IoError)?;
    let mut length = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    let sequence_id = header[3];

    // Read first packet payload
    buffer.clear();
    if buffer.capacity() < length {
        buffer.reserve(length - buffer.capacity());
    }
    let BufResult(result, slice) = reader.read_exact(buffer.slice(..length)).await;
    result.map_err(Error::IoError)?;
    buffer = slice.into_inner();

    // If packet is exactly 16MB (0xFFFFFF bytes), there may be more packets
    while length == 0xFFFFFF {
        // Read next packet header
        let BufResult(result, header) = reader.read_exact(header).await;
        result.map_err(Error::IoError)?;

        length = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
        if buffer.capacity() < length {
            buffer.reserve(length - buffer.capacity());
        }
        let BufResult(result, slice) = reader.read_exact(buffer.slice(..length)).await;
        buffer = slice.into_inner();
        result.map_err(Error::IoError)?;
    }

    Ok((sequence_id, buffer))
}

/// Write a MySQL packet during handshake asynchronously, splitting it into 16MB chunks if necessary
/// (standalone version for use before Conn is fully initialized)
///
/// # Arguments
/// * `stream` - The async TCP stream to write to
/// * `sequence_id` - Starting sequence ID (will auto-increment for multi-packet)
/// * `payload` - The payload bytes to send
async fn write_handshake_payload<W>(
    stream: &mut W,
    mut sequence_id: u8,
    payload: &[u8],
) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    // Build complete packet with headers
    let mut packet_buf = Vec::new();

    let mut remaining = payload;
    let mut last_chunk_size = 0;

    while !remaining.is_empty() {
        let chunk_size = remaining.len().min(0xFFFFFF);
        let (chunk, rest) = remaining.split_at(chunk_size);

        // Write header
        let header = PacketHeader::encode(chunk_size, sequence_id);
        packet_buf.extend_from_slice(header.as_bytes());

        // Write chunk
        packet_buf.extend_from_slice(chunk);

        remaining = rest;
        last_chunk_size = chunk_size;
        sequence_id = sequence_id.wrapping_add(1);
    }

    // If the last chunk was exactly 0xFFFFFF bytes, add an empty packet to signal EOF
    if last_chunk_size == 0xFFFFFF {
        let header = PacketHeader::encode(0, sequence_id);
        packet_buf.extend_from_slice(header.as_bytes());
    }

    // Write all data
    let write_result = stream.write_all(packet_buf).await;
    write_result.0.map_err(Error::IoError)?;

    Ok(())
}
