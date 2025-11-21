use zerocopy::IntoBytes;

use crate::buffer::BufferSet;
use crate::constant::CapabilityFlags;
use crate::error::{Error, Result};
use crate::protocol::command::prepared::write_execute;
use crate::protocol::command::prepared::{read_prepare_ok, write_prepare};
use crate::protocol::connection::{Handshake, HandshakeResult, InitialHandshake};
use crate::protocol::packet::PacketHeader;
use crate::protocol::response::ErrPayloadBytes;
use crate::protocol::r#trait::{ResultSetHandler, TextResultSetHandler, params::Params};
use std::hint::unlikely;
use std::io::{BufRead, BufReader, IoSlice, Write};
use std::net::TcpStream;

pub struct Conn {
    stream: BufReader<TcpStream>,
    buffer_set: BufferSet,
    initial_handshake: InitialHandshake,
    capability_flags: CapabilityFlags,
    pub(crate) in_transaction: bool,
}

impl Conn {
    /// Create a new MySQL connection from connection options
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
        let mut buffer_set = BufferSet::new();
        let mut initial_handshake = None;

        let mut handshake = Handshake::new(
            username.to_string(),
            password.to_string(),
            database.map(|s| s.to_string()),
        );

        let capability_flags = loop {
            let buffer = if matches!(handshake, Handshake::Start { .. }) {
                &mut buffer_set.initial_handshake
            } else {
                &mut buffer_set.read_buffer
            };
            buffer.clear();
            let mut last_sequence_id = read_payload(&mut conn_stream, buffer)?;

            match handshake.drive(buffer)? {
                HandshakeResult::InitialHandshake {
                    handshake_response,
                    initial_handshake: hs,
                } => {
                    initial_handshake = Some(hs);
                    if !handshake_response.is_empty() {
                        write_handshake_payload(
                            conn_stream.get_mut(),
                            &mut last_sequence_id,
                            &handshake_response,
                            &mut buffer_set.write_headers_buffer,
                            &mut buffer_set.ioslice_buffer,
                        )?;
                    }
                }
                HandshakeResult::Write(packet_data) => {
                    if !packet_data.is_empty() {
                        write_handshake_payload(
                            conn_stream.get_mut(),
                            &mut last_sequence_id,
                            &packet_data,
                            &mut buffer_set.write_headers_buffer,
                            &mut buffer_set.ioslice_buffer,
                        )?;
                    }
                }
                HandshakeResult::Connected { capability_flags } => {
                    break capability_flags;
                }
            }
        };

        Ok(Self {
            stream: conn_stream,
            buffer_set,
            initial_handshake: initial_handshake.unwrap(),
            capability_flags,
            in_transaction: false,
        })
    }

    pub fn server_version(&self) -> &[u8] {
        &self.buffer_set.initial_handshake[self.initial_handshake.server_version.clone()]
    }

    /// Get the negotiated capability flags
    pub fn capability_flags(&self) -> CapabilityFlags {
        self.capability_flags
    }

    /// Get the connection ID assigned by the server
    pub fn connection_id(&self) -> u64 {
        self.initial_handshake.connection_id as u64
    }

    /// Get the server status flags from the initial handshake
    pub fn status_flags(&self) -> crate::constant::ServerStatusFlags {
        self.initial_handshake.status_flags
    }

    #[tracing::instrument(skip_all)]
    fn write_payload(&mut self) -> Result<()> {
        let mut sequence_id = 0;
        self.buffer_set.write_headers_buffer.clear();
        self.buffer_set.ioslice_buffer.clear();

        let payload = self.buffer_set.write_buffer.as_slice();
        let mut remaining = payload;
        let mut chunk_size = 0;

        while !remaining.is_empty() {
            chunk_size = remaining.len().min(0xFFFFFF);
            let (_chunk, rest) = remaining.split_at(chunk_size);
            remaining = rest;

            let header = PacketHeader::encode(chunk_size, sequence_id);
            self.buffer_set.write_headers_buffer.push(header);

            sequence_id = sequence_id.wrapping_add(1);
        }

        if chunk_size == 0xFFFFFF {
            let header = PacketHeader::encode(0, sequence_id);
            self.buffer_set.write_headers_buffer.push(header);
        }

        remaining = payload;
        for header in self.buffer_set.write_headers_buffer.iter() {
            let chunk_size = header.length();
            self.buffer_set.ioslice_buffer.push(unsafe {
                std::mem::transmute::<IoSlice<'_>, IoSlice<'static>>(IoSlice::new(
                    header.as_bytes(),
                ))
            });

            if chunk_size > 0 {
                let chunk;
                (chunk, remaining) = remaining.split_at(chunk_size);
                self.buffer_set.ioslice_buffer.push(unsafe {
                    std::mem::transmute::<IoSlice<'_>, IoSlice<'static>>(IoSlice::new(chunk))
                });
            }
        }

        self.stream
            .get_mut()
            .write_all_vectored(&mut self.buffer_set.ioslice_buffer)?;

        self.stream.get_mut().flush()?;

        Ok(())
    }

    /// Returns `Ok(statement_id) on success
    pub fn prepare(&mut self, sql: &str) -> Result<u32> {
        self.buffer_set.read_buffer.clear();
        self.buffer_set.write_buffer.clear();

        write_prepare(&mut self.buffer_set.write_buffer, sql);

        self.write_payload()?;
        let _ = read_payload(&mut self.stream, &mut self.buffer_set.read_buffer)?;

        if unlikely(
            !self.buffer_set.read_buffer.is_empty() && self.buffer_set.read_buffer[0] == 0xFF,
        ) {
            Err(ErrPayloadBytes(&self.buffer_set.read_buffer))?
        }

        let prepare_ok = read_prepare_ok(&self.buffer_set.read_buffer)?;
        let statement_id = prepare_ok.statement_id();
        let num_params = prepare_ok.num_params();
        let num_columns = prepare_ok.num_columns();

        if num_params > 0 {
            for _ in 0..num_params {
                let _ = read_payload(&mut self.stream, &mut self.buffer_set.read_buffer)?;
            }
        }

        if num_columns > 0 {
            for _ in 0..num_columns {
                let _ = read_payload(&mut self.stream, &mut self.buffer_set.read_buffer)?;
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

        self.buffer_set.write_buffer.clear();
        write_execute(&mut self.buffer_set.write_buffer, statement_id, params)?;

        self.write_payload()?;

        let mut exec = Exec::default();

        loop {
            self.buffer_set.read_buffer.clear();
            let _ = read_payload(&mut self.stream, &mut self.buffer_set.read_buffer)?;

            let result = exec.drive(&self.buffer_set.read_buffer[..])?;
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

        self.buffer_set.write_buffer.clear();
        write_execute(&mut self.buffer_set.write_buffer, statement_id, params)?;

        self.write_payload()?;

        let mut exec = Exec::default();
        let mut first_row_found = false;

        loop {
            self.buffer_set.read_buffer.clear();
            let _ = read_payload(&mut self.stream, &mut self.buffer_set.read_buffer)?;

            let result = exec.drive(&self.buffer_set.read_buffer[..])?;
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

        self.buffer_set.write_buffer.clear();
        write_execute(&mut self.buffer_set.write_buffer, statement_id, params)?;

        self.write_payload()?;

        let mut exec = Exec::default();

        loop {
            self.buffer_set.read_buffer.clear();
            let _ = read_payload(&mut self.stream, &mut self.buffer_set.read_buffer)?;

            let result = exec.drive(&self.buffer_set.read_buffer[..])?;

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

        self.buffer_set.write_buffer.clear();
        write_query(&mut self.buffer_set.write_buffer, sql);

        self.write_payload()?;

        let mut query_fold = Query::default();

        loop {
            self.buffer_set.read_buffer.clear();
            let _ = read_payload(&mut self.stream, &mut self.buffer_set.read_buffer)?;

            let result = query_fold.drive(&self.buffer_set.read_buffer[..])?;
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

        self.buffer_set.write_buffer.clear();
        write_query(&mut self.buffer_set.write_buffer, sql);

        self.write_payload()?;

        let mut query = Query::default();

        loop {
            self.buffer_set.read_buffer.clear();
            let _ = read_payload(&mut self.stream, &mut self.buffer_set.read_buffer)?;

            let result = query.drive(&self.buffer_set.read_buffer[..])?;
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

        self.buffer_set.write_buffer.clear();
        write_ping(&mut self.buffer_set.write_buffer);

        self.write_payload()?;

        self.buffer_set.read_buffer.clear();
        let _ = read_payload(&mut self.stream, &mut self.buffer_set.read_buffer)?;

        Ok(())
    }

    /// Reset the connection to its initial state
    pub fn reset(&mut self) -> Result<()> {
        use crate::protocol::command::utility::write_reset_connection;

        self.buffer_set.write_buffer.clear();
        write_reset_connection(&mut self.buffer_set.write_buffer);

        self.write_payload()?;

        self.buffer_set.read_buffer.clear();
        let _ = read_payload(&mut self.stream, &mut self.buffer_set.read_buffer)?;

        self.in_transaction = false;

        Ok(())
    }

    /// Execute a closure within a transaction
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
/// Returns the sequence_id of the last packet read.
#[tracing::instrument(skip_all)]
pub fn read_payload<R: BufRead>(reader: &mut R, buffer: &mut Vec<u8>) -> Result<u8> {
    use zerocopy::FromZeros;

    buffer.clear();

    let mut header = PacketHeader::new_zeroed();
    reader.read_exact(header.as_mut_bytes())?;

    let length = header.length();
    let mut sequence_id = header.sequence_id;

    if buffer.capacity() < length {
        buffer.reserve(length - buffer.capacity());
    }

    let start = buffer.len();
    buffer.resize(start + length, 0);
    reader.read_exact(&mut buffer[start..])?;

    let mut current_length = length;
    while current_length == 0xFFFFFF {
        reader.read_exact(header.as_mut_bytes())?;

        current_length = header.length();
        sequence_id = header.sequence_id;

        let prev_len = buffer.len();
        buffer.resize(prev_len + current_length, 0);
        reader.read_exact(&mut buffer[prev_len..])?;
    }

    Ok(sequence_id)
}

/// Write a MySQL packet during handshake, splitting it into 16MB chunks if necessary
fn write_handshake_payload<W: Write>(
    stream: &mut W,
    last_sequence_id: &mut u8,
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

        *last_sequence_id = last_sequence_id.wrapping_add(1);
        let header = PacketHeader::encode(chunk_size, *last_sequence_id);
        headers_buffer.push(header);
    }

    if chunk_size == 0xFFFFFF {
        *last_sequence_id = last_sequence_id.wrapping_add(1);
        let header = PacketHeader::encode(0, *last_sequence_id);
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
