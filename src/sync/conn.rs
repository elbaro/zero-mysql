use zerocopy::IntoBytes;

use crate::constant::CapabilityFlags;
use crate::error::{Error, Result};
use crate::protocol::command::prepared::write_execute;
use crate::protocol::command::prepared::{read_prepare_ok, write_prepare};
use crate::protocol::connection::{Handshake, HandshakeResult};
use crate::protocol::packet::PacketHeader;
use crate::protocol::response::ErrPayloadBytes;
use crate::protocol::r#trait::{ResultSetHandler, TextResultSetHandler, params::Params};
use std::hint::unlikely;
use std::io::{BufRead, BufReader, IoSlice, Write};
use std::net::TcpStream;

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
    write_headers_buffer: Vec<PacketHeader>,
    /// Reusable buffer for IoSlice when writing payloads (reduces heap allocations)
    ioslice_buffer: Vec<IoSlice<'static>>,
    /// Tracks whether a transaction is currently active
    pub(crate) in_transaction: bool,
}

impl Conn {
    /// Create a new MySQL connection from connection options
    ///
    /// # Examples
    /// ```
    /// // Using a URL string
    /// let conn = Conn::new("mysql://root:password@localhost:3306/mydb")?;
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
    /// let conn = Conn::new(opts)?;
    ///
    /// // Reuse with connection pool
    /// let mut opts = Opts::from_url("mysql://root:password@localhost:3306/mydb")?;
    /// let pool = MyCustomPool::new(opts);
    /// ```
    pub fn new<O: TryInto<crate::opts::Opts>>(opts: O) -> Result<Self>
    where
        Error: From<O::Error>,
    {
        let opts: crate::opts::Opts = opts.try_into()?;

        if let Some(_socket) = &opts.socket {
            todo!("Unix socket connections not yet implemented");
        }

        let host = opts.host.as_ref().ok_or_else(|| {
            Error::BadConfigError("Missing host in connection options".to_string())
        })?;

        let addr = format!("{}:{}", host, opts.port);
        let stream = TcpStream::connect(&addr)?;
        stream.set_nodelay(opts.tcp_nodelay)?;

        Self::new_with_stream(
            stream,
            &opts.user,
            opts.password.as_deref().unwrap_or(""),
            opts.db.as_deref(),
        )
    }

    /// Create a new MySQL connection with an existing TCP stream
    pub fn new_with_stream(
        stream: TcpStream,
        username: &str,
        password: &str,
        database: Option<&str>,
    ) -> Result<Self> {
        let mut conn_stream = BufReader::new(stream);
        let mut buffer = Vec::new();
        let mut headers_buffer = Vec::new();
        let mut ioslice_buffer = Vec::new();

        let mut handshake = Handshake::new(
            username.to_string(),
            password.to_string(),
            database.map(|s| s.to_string()),
        );

        let (server_version, capability_flags) = loop {
            buffer.clear();
            read_payload(&mut conn_stream, &mut buffer)?;
            let mut seq: u8 = 0;

            match handshake.drive(&buffer)? {
                HandshakeResult::Write(packet_data) => {
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
                }
                HandshakeResult::Connected {
                    server_version,
                    capability_flags,
                } => {
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
            in_transaction: false,
        })
    }

    pub fn server_version(&self) -> &str {
        &self.server_version
    }

    /// Get the negotiated capability flags
    pub fn capability_flags(&self) -> CapabilityFlags {
        self.capability_flags
    }

    /// Write abytes in write_buffer to stream, splitting it into 16MB chunks if necessary
    #[tracing::instrument(skip_all)]
    fn write_payload(&mut self, mut sequence_id: u8) -> Result<()> {
        self.write_headers_buffer.clear();
        self.ioslice_buffer.clear();

        let payload = self.write_buffer.as_slice();
        let mut remaining = payload;
        let mut chunk_size = 0;

        while !remaining.is_empty() {
            chunk_size = remaining.len().min(0xFFFFFF);
            let (_chunk, rest) = remaining.split_at(chunk_size);
            remaining = rest;

            let header = PacketHeader::encode(chunk_size, sequence_id);
            self.write_headers_buffer.push(header);

            sequence_id = sequence_id.wrapping_add(1);
        }

        if chunk_size == 0xFFFFFF {
            let header = PacketHeader::encode(0, sequence_id);
            self.write_headers_buffer.push(header);
        }

        remaining = payload;
        for header in self.write_headers_buffer.iter() {
            let chunk_size = header.length();
            self.ioslice_buffer.push(unsafe {
                std::mem::transmute::<IoSlice<'_>, IoSlice<'static>>(IoSlice::new(
                    header.as_bytes(),
                ))
            });

            if chunk_size > 0 {
                let chunk;
                (chunk, remaining) = remaining.split_at(chunk_size);
                self.ioslice_buffer.push(unsafe {
                    std::mem::transmute::<IoSlice<'_>, IoSlice<'static>>(IoSlice::new(chunk))
                });
            }
        }

        self.stream
            .get_mut()
            .write_all_vectored(&mut self.ioslice_buffer)?;

        self.stream.get_mut().flush()?;

        Ok(())
    }

    /// Returns `Ok(statement_id) on success
    pub fn prepare(&mut self, sql: &str) -> Result<u32> {
        self.read_buffer.clear();
        self.write_buffer.clear();

        write_prepare(&mut self.write_buffer, sql);

        self.write_payload(0)?;
        read_payload(&mut self.stream, &mut self.read_buffer)?;

        if unlikely(!self.read_buffer.is_empty() && self.read_buffer[0] == 0xFF) {
            Err(ErrPayloadBytes(&self.read_buffer))?
        }

        let prepare_ok = read_prepare_ok(&self.read_buffer)?;
        let statement_id = prepare_ok.statement_id();
        let num_params = prepare_ok.num_params();
        let num_columns = prepare_ok.num_columns();

        if num_params > 0 {
            // TODO: && !CLIENT_OPTIONAL_RESULTSET_METADATA || medatdata_follows==RESULTSET_METADATA_FULL
            for _ in 0..num_params {
                read_payload(&mut self.stream, &mut self.read_buffer)?;
            }
        }

        if num_columns > 0 {
            // TODO: && !CLIENT_OPTIONAL_RESULTSET_METADATA || medatdata_follows==RESULTSET_METADATA_FULL
            for _ in 0..num_columns {
                read_payload(&mut self.stream, &mut self.read_buffer)?;
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

        self.write_buffer.clear();
        write_execute(&mut self.write_buffer, statement_id, params)?;

        self.write_payload(0)?;

        let mut exec = Exec::default();

        loop {
            self.read_buffer.clear();
            read_payload(&mut self.stream, &mut self.read_buffer)?;

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

        self.write_buffer.clear();
        write_execute(&mut self.write_buffer, statement_id, params)?;

        self.write_payload(0)?;

        let mut exec = Exec::default();
        let mut first_row_found = false;

        loop {
            self.read_buffer.clear();
            read_payload(&mut self.stream, &mut self.read_buffer)?;

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
                    }
                }
                ExecResult::Eof(eof_bytes) => {
                    handler.resultset_end(eof_bytes)?;
                    return Ok(first_row_found);
                }
            }
        }
    }

    /// Execute a prepared statement and discard all results
    #[tracing::instrument(skip_all)]
    pub fn exec_drop<P>(&mut self, statement_id: u32, params: P) -> Result<()>
    where
        P: Params,
    {
        use crate::protocol::command::prepared::{Exec, ExecResult};

        self.write_buffer.clear();
        write_execute(&mut self.write_buffer, statement_id, params)?;

        self.write_payload(0)?;

        let mut exec = Exec::default();

        loop {
            self.read_buffer.clear();
            read_payload(&mut self.stream, &mut self.read_buffer)?;

            let result = exec.drive(&self.read_buffer[..])?;

            match result {
                ExecResult::NeedPayload => {
                    continue;
                }
                ExecResult::NoResultSet(_ok_bytes) => {
                    return Ok(());
                }
                ExecResult::ResultSetStart { .. } => {}
                ExecResult::Column(_) => {}
                ExecResult::Row(_) => {}
                ExecResult::Eof(_eof_bytes) => {
                    return Ok(());
                }
            }
        }
    }

    /// Execute a text protocol SQL query
    pub fn query<'a, H>(&mut self, sql: &str, handler: &mut H) -> Result<()>
    where
        H: TextResultSetHandler<'a>,
    {
        use crate::protocol::command::query::{Query, QueryResult, write_query};

        self.write_buffer.clear();
        write_query(&mut self.write_buffer, sql);

        self.write_payload(0)?;

        let mut query_fold = Query::default();

        loop {
            self.read_buffer.clear();
            read_payload(&mut self.stream, &mut self.read_buffer)?;

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

    /// Execute a text protocol SQL query and discard the result
    pub fn query_drop(&mut self, sql: &str) -> Result<()> {
        use crate::protocol::command::query::{Query, QueryResult, write_query};

        self.write_buffer.clear();
        write_query(&mut self.write_buffer, sql);

        self.write_payload(0)?;

        let mut query = Query::default();

        loop {
            self.read_buffer.clear();
            read_payload(&mut self.stream, &mut self.read_buffer)?;

            let result = query.drive(&self.read_buffer[..])?;
            match result {
                QueryResult::NeedPayload => {
                    continue;
                }
                QueryResult::NoResultSet(_ok_bytes) => {
                    return Ok(());
                }
                QueryResult::ResultSetStart { .. } => {}
                QueryResult::Column(_) => {}
                QueryResult::Row(_) => {}
                QueryResult::Eof(_eof_bytes) => {
                    return Ok(());
                }
            }
        }
    }

    /// Send a ping to the server to check if the connection is alive
    ///
    /// This sends a COM_PING command to the MySQL server and waits for an OK response.
    pub fn ping(&mut self) -> Result<()> {
        use crate::protocol::command::utility::write_ping;

        self.write_buffer.clear();
        write_ping(&mut self.write_buffer);

        self.write_payload(0)?;

        self.read_buffer.clear();
        read_payload(&mut self.stream, &mut self.read_buffer)?;

        Ok(())
    }

    /// Execute a closure within a transaction
    ///
    /// # Example
    /// ```
    /// conn.run_transaction(|conn, tx| {
    ///     conn.query_drop("INSERT INTO users (name) VALUES ('Alice')")?;
    ///     conn.query_drop("INSERT INTO users (name) VALUES ('Bob')")?;
    ///     tx.commit(conn)?;
    ///     Ok(())
    /// })?;
    /// ```
    ///
    /// # Panics
    /// Panics if called while already in a transaction
    pub fn run_transaction<F, R>(&mut self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Conn, super::transaction::Transaction) -> Result<R>,
    {
        assert!(
            !self.in_transaction,
            "Cannot nest transactions - a transaction is already active"
        );

        self.in_transaction = true;

        if let Err(e) = self.query_drop("BEGIN") {
            self.in_transaction = false;
            return Err(e);
        }

        let tx = super::transaction::Transaction::new();
        let result = f(self, tx);

        // If the transaction was not explicitly committed or rolled back, roll it back
        if self.in_transaction {
            let rollback_result = self.query_drop("ROLLBACK");
            self.in_transaction = false;

            // Return the first error (either from closure or rollback)
            if let Err(e) = result {
                return Err(e);
            }
            rollback_result?;
        }

        result
    }
}

/// Read a complete MySQL payload, concatenating packets if they span multiple 16MB chunks
#[tracing::instrument(skip_all)]
pub fn read_payload<R: BufRead>(reader: &mut R, buffer: &mut Vec<u8>) -> Result<()> {
    buffer.clear();

    let mut header = [0u8; 4];
    reader.read_exact(&mut header)?;

    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;

    if buffer.capacity() < length {
        buffer.reserve(length - buffer.capacity());
    }

    let start = buffer.len();
    buffer.resize(start + length, 0);
    reader.read_exact(&mut buffer[start..])?;

    let mut current_length = length;
    while current_length == 0xFFFFFF {
        let mut next_header = [0u8; 4];
        reader.read_exact(&mut next_header)?;

        current_length =
            u32::from_le_bytes([next_header[0], next_header[1], next_header[2], 0]) as usize;

        let prev_len = buffer.len();
        buffer.resize(prev_len + current_length, 0);
        reader.read_exact(&mut buffer[prev_len..])?;
    }

    Ok(())
}

/// Write a MySQL packet during handshake, splitting it into 16MB chunks if necessary
fn write_handshake_payload<W: Write>(
    stream: &mut W,
    mut sequence_id: u8,
    payload: &[u8],
    headers_buffer: &mut Vec<PacketHeader>,
    ioslice_buffer: &mut Vec<IoSlice<'static>>,
) -> Result<()> {
    headers_buffer.clear();
    ioslice_buffer.clear();

    let mut remaining = payload;
    let mut chunk_size = 0;

    while !remaining.is_empty() {
        chunk_size = remaining.len().min(0xFFFFFF);
        let (_chunk, rest) = remaining.split_at(chunk_size);
        remaining = rest;

        let header = PacketHeader::encode(chunk_size, sequence_id);
        headers_buffer.push(header);

        sequence_id = sequence_id.wrapping_add(1);
    }

    if chunk_size == 0xFFFFFF {
        let header = PacketHeader::encode(0, sequence_id);
        headers_buffer.push(header);
    }

    remaining = payload;
    for header in headers_buffer.iter() {
        let chunk_size = header.length();

        ioslice_buffer.push(unsafe {
            std::mem::transmute::<IoSlice<'_>, IoSlice<'static>>(IoSlice::new(header.as_bytes()))
        });

        if chunk_size > 0 {
            let chunk;
            (chunk, remaining) = remaining.split_at(chunk_size);
            ioslice_buffer.push(unsafe {
                std::mem::transmute::<IoSlice<'_>, IoSlice<'static>>(IoSlice::new(chunk))
            });
        }
    }

    stream.write_all_vectored(ioslice_buffer)?;
    stream.flush()?;

    Ok(())
}
