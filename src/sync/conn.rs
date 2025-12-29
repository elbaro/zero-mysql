use crate::PreparedStatement;
use crate::buffer::BufferSet;
use crate::buffer_pool::PooledBufferSet;
use crate::constant::CapabilityFlags;
use crate::error::{Error, Result};
use crate::protocol::TextRowPayload;
use crate::protocol::command::Action;
use crate::protocol::command::ColumnDefinition;
use crate::protocol::command::bulk_exec::{BulkExec, BulkFlags, BulkParamsSet, write_bulk_execute};
use crate::protocol::command::prepared::Exec;
use crate::protocol::command::prepared::write_execute;
use crate::protocol::command::prepared::{read_prepare_ok, write_prepare};
use crate::protocol::command::query::Query;
use crate::protocol::command::query::write_query;
use crate::protocol::command::utility::DropHandler;
use crate::protocol::command::utility::FirstRowHandler;
use crate::protocol::command::utility::write_ping;
use crate::protocol::command::utility::write_reset_connection;
use crate::protocol::connection::{Handshake, HandshakeAction, InitialHandshake};
use crate::protocol::packet::PacketHeader;
use crate::protocol::primitive::read_string_lenenc;
use crate::protocol::response::{ErrPayloadBytes, OkPayloadBytes};
use crate::protocol::r#trait::{BinaryResultSetHandler, TextResultSetHandler, param::Params};
use core::hint::unlikely;
use core::io::BorrowedBuf;
use std::net::TcpStream;
use std::os::unix::net::UnixStream;
use zerocopy::FromZeros;
use zerocopy::{FromBytes, IntoBytes};

use super::stream::Stream;

pub struct Conn {
    stream: Stream,
    buffer_set: PooledBufferSet,
    initial_handshake: InitialHandshake,
    capability_flags: CapabilityFlags,
    mariadb_capabilities: crate::constant::MariadbCapabilityFlags,
    in_transaction: bool,
    is_broken: bool,
}

impl Conn {
    pub(crate) fn set_in_transaction(&mut self, value: bool) {
        self.in_transaction = value;
    }

    /// Create a new MySQL connection from connection options
    pub fn new<O: TryInto<crate::opts::Opts>>(opts: O) -> Result<Self>
    where
        Error: From<O::Error>,
    {
        let opts: crate::opts::Opts = opts.try_into()?;

        let stream = if let Some(socket_path) = &opts.socket {
            let stream = UnixStream::connect(socket_path)?;
            Stream::unix(stream)
        } else {
            if opts.host.is_empty() {
                return Err(Error::BadUsageError(
                    "Missing host in connection options".to_string(),
                ));
            }
            let addr = format!("{}:{}", opts.host, opts.port);
            let stream = TcpStream::connect(&addr)?;
            stream.set_nodelay(opts.tcp_nodelay)?;
            Stream::tcp(stream)
        };

        Self::new_with_stream(stream, &opts)
    }

    /// Create a new MySQL connection with an existing stream
    pub fn new_with_stream(stream: Stream, opts: &crate::opts::Opts) -> Result<Self> {
        let mut conn_stream = stream;
        let mut buffer_set = opts.buffer_pool.get_buffer_set();

        #[cfg(feature = "sync-tls")]
        let host = opts.host.clone();

        let mut handshake = Handshake::new(opts);

        loop {
            match handshake.step(&mut buffer_set)? {
                HandshakeAction::ReadPacket(buffer) => {
                    buffer.clear();
                    read_payload(&mut conn_stream, buffer)?;
                }
                HandshakeAction::WritePacket { sequence_id } => {
                    write_handshake_payload(&mut conn_stream, &mut buffer_set, sequence_id)?;
                    buffer_set.read_buffer.clear();
                    read_payload(&mut conn_stream, &mut buffer_set.read_buffer)?;
                }
                #[cfg(feature = "sync-tls")]
                HandshakeAction::UpgradeTls { sequence_id } => {
                    write_handshake_payload(&mut conn_stream, &mut buffer_set, sequence_id)?;
                    conn_stream = conn_stream.upgrade_to_tls(&host)?;
                }
                #[cfg(not(feature = "sync-tls"))]
                HandshakeAction::UpgradeTls { .. } => {
                    return Err(Error::BadUsageError(
                        "TLS requested but sync-tls feature is not enabled".to_string(),
                    ));
                }
                HandshakeAction::Finished => break,
            }
        }

        let (initial_handshake, capability_flags, mariadb_capabilities) = handshake.finish()?;

        let conn = Self {
            stream: conn_stream,
            buffer_set,
            initial_handshake,
            capability_flags,
            mariadb_capabilities,
            in_transaction: false,
            is_broken: false,
        };

        // Upgrade to Unix socket if connected via TCP to loopback
        let mut conn = if opts.upgrade_to_unix_socket && conn.stream.is_tcp_loopback() {
            conn.try_upgrade_to_unix_socket(opts)
        } else {
            conn
        };

        // Execute init command if specified
        if let Some(init_command) = &opts.init_command {
            conn.query_drop(init_command)?;
        }

        Ok(conn)
    }

    /// Example: `"11.4.8-MariaDB"`
    pub fn server_version(&self) -> &[u8] {
        &self.buffer_set.initial_handshake[self.initial_handshake.server_version.clone()]
    }

    /// Get the negotiated capability flags
    pub fn capability_flags(&self) -> CapabilityFlags {
        self.capability_flags
    }

    /// Check if the server is MySQL (as opposed to MariaDB)
    pub fn is_mysql(&self) -> bool {
        self.capability_flags.is_mysql()
    }

    /// Check if the server is MariaDB (as opposed to MySQL)
    pub fn is_mariadb(&self) -> bool {
        self.capability_flags.is_mariadb()
    }

    /// Get the connection ID assigned by the server
    pub fn connection_id(&self) -> u64 {
        self.initial_handshake.connection_id as u64
    }

    /// Get the server status flags from the initial handshake
    pub fn status_flags(&self) -> crate::constant::ServerStatusFlags {
        self.initial_handshake.status_flags
    }

    /// Indicates if the connection is broken by errors
    ///
    /// This state is used by Pool to decide if this Conn can be reset and reused or dropped.
    pub fn is_broken(&self) -> bool {
        self.is_broken
    }

    #[inline]
    fn check_error<T>(&mut self, result: Result<T>) -> Result<T> {
        if let Err(e) = &result
            && e.is_conn_broken()
        {
            self.is_broken = true;
        }
        result
    }

    /// Try to upgrade to Unix socket connection.
    /// Returns upgraded conn on success, original conn on failure.
    fn try_upgrade_to_unix_socket(mut self, opts: &crate::opts::Opts) -> Self {
        // Query the server for its Unix socket path
        let mut handler = SocketPathHandler { path: None };
        if self.query("SELECT @@socket", &mut handler).is_err() {
            return self;
        }

        let socket_path = match handler.path {
            Some(p) if !p.is_empty() => p,
            _ => return self,
        };

        // Connect via Unix socket
        let unix_stream = match UnixStream::connect(&socket_path) {
            Ok(s) => s,
            Err(_) => return self,
        };
        let stream = Stream::unix(unix_stream);

        // Create new connection over Unix socket (re-handshakes)
        // Disable upgrade_to_unix_socket to prevent infinite recursion
        let mut opts_unix = opts.clone();
        opts_unix.upgrade_to_unix_socket = false;

        match Self::new_with_stream(stream, &opts_unix) {
            Ok(new_conn) => new_conn,
            Err(_) => self,
        }
    }

    fn write_payload(&mut self) -> Result<()> {
        let mut sequence_id = 0_u8;
        let mut buffer = self.buffer_set.write_buffer_mut().as_mut_slice();

        loop {
            let chunk_size = buffer[4..].len().min(0xFFFFFF);
            PacketHeader::mut_from_bytes(&mut buffer[0..4])?
                .encode_in_place(chunk_size, sequence_id);
            self.stream.write_all(&buffer[..4 + chunk_size])?;

            if chunk_size < 0xFFFFFF {
                break;
            }

            sequence_id = sequence_id.wrapping_add(1);
            buffer = &mut buffer[0xFFFFFF..];
        }
        self.stream.flush()?;
        Ok(())
    }

    /// Returns `Ok(statement_id)` on success
    pub fn prepare(&mut self, sql: &str) -> Result<PreparedStatement> {
        let result = self.prepare_inner(sql);
        self.check_error(result)
    }

    fn prepare_inner(&mut self, sql: &str) -> Result<PreparedStatement> {
        use crate::protocol::command::ColumnDefinitions;

        self.buffer_set.read_buffer.clear();

        write_prepare(self.buffer_set.new_write_buffer(), sql);

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

        // Skip param definitions (we don't cache them)
        if num_params > 0 {
            for _ in 0..num_params {
                let _ = read_payload(&mut self.stream, &mut self.buffer_set.read_buffer)?;
            }
        }

        // Read and cache column definitions for MARIADB_CLIENT_CACHE_METADATA support
        let column_definitions = if num_columns > 0 {
            read_column_definition_packets(
                &mut self.stream,
                &mut self.buffer_set.column_definition_buffer,
                num_columns as usize,
            )?;
            Some(ColumnDefinitions::new(
                num_columns as usize,
                std::mem::take(&mut self.buffer_set.column_definition_buffer),
            )?)
        } else {
            None
        };

        let mut stmt = PreparedStatement::new(statement_id);
        if let Some(col_defs) = column_definitions {
            stmt.set_column_definitions(col_defs);
        }
        Ok(stmt)
    }

    fn drive_exec<H: BinaryResultSetHandler>(
        &mut self,
        stmt: &mut PreparedStatement,
        handler: &mut H,
    ) -> Result<()> {
        let cache_metadata = self
            .mariadb_capabilities
            .contains(crate::constant::MariadbCapabilityFlags::MARIADB_CLIENT_CACHE_METADATA);
        let mut exec = Exec::new(handler, stmt, cache_metadata);

        loop {
            match exec.step(&mut self.buffer_set)? {
                Action::NeedPacket(buffer) => {
                    buffer.clear();
                    let _ = read_payload(&mut self.stream, buffer)?;
                }
                Action::ReadColumnMetadata { num_columns } => {
                    read_column_definition_packets(
                        &mut self.stream,
                        &mut self.buffer_set.column_definition_buffer,
                        num_columns,
                    )?;
                }
                Action::Finished => return Ok(()),
            }
        }
    }

    /// Executes a prepared statement with parameters.
    ///
    /// This is the most general version of exec_*() methods.
    pub fn exec<'conn, P, H>(
        &'conn mut self,
        stmt: &'conn mut PreparedStatement,
        params: P,
        handler: &mut H,
    ) -> Result<()>
    where
        P: Params,
        H: BinaryResultSetHandler,
    {
        let result = self.exec_inner(stmt, params, handler);
        self.check_error(result)
    }

    fn exec_inner<'conn, P, H>(
        &'conn mut self,
        stmt: &'conn mut PreparedStatement,
        params: P,
        handler: &mut H,
    ) -> Result<()>
    where
        P: Params,
        H: BinaryResultSetHandler,
    {
        write_execute(self.buffer_set.new_write_buffer(), stmt.id(), params)?;
        self.write_payload()?;
        self.drive_exec(stmt, handler)
    }

    fn drive_bulk_exec<H: BinaryResultSetHandler>(
        &mut self,
        stmt: &mut PreparedStatement,
        handler: &mut H,
    ) -> Result<()> {
        let cache_metadata = self
            .mariadb_capabilities
            .contains(crate::constant::MariadbCapabilityFlags::MARIADB_CLIENT_CACHE_METADATA);
        let mut bulk_exec = BulkExec::new(handler, stmt, cache_metadata);

        loop {
            match bulk_exec.step(&mut self.buffer_set)? {
                Action::NeedPacket(buffer) => {
                    buffer.clear();
                    let _ = read_payload(&mut self.stream, buffer)?;
                }
                Action::ReadColumnMetadata { num_columns } => {
                    read_column_definition_packets(
                        &mut self.stream,
                        &mut self.buffer_set.column_definition_buffer,
                        num_columns,
                    )?;
                }
                Action::Finished => return Ok(()),
            }
        }
    }

    /// Execute a bulk prepared statement with a result set handler
    pub fn exec_bulk_insert_or_update<P, I, H>(
        &mut self,
        stmt: &mut PreparedStatement,
        params: P,
        flags: BulkFlags,
        handler: &mut H,
    ) -> Result<()>
    where
        P: BulkParamsSet + IntoIterator<Item = I>,
        I: Params,
        H: BinaryResultSetHandler,
    {
        let result = self.exec_bulk_insert_or_update_inner(stmt, params, flags, handler);
        self.check_error(result)
    }

    fn exec_bulk_insert_or_update_inner<P, I, H>(
        &mut self,
        stmt: &mut PreparedStatement,
        params: P,
        flags: BulkFlags,
        handler: &mut H,
    ) -> Result<()>
    where
        P: BulkParamsSet + IntoIterator<Item = I>,
        I: Params,
        H: BinaryResultSetHandler,
    {
        if !self.is_mariadb() {
            // Fallback to multiple exec_drop for non-MariaDB servers
            for param in params {
                self.exec_inner(stmt, param, &mut DropHandler::default())?;
            }
            Ok(())
        } else {
            // Use MariaDB bulk execute protocol
            write_bulk_execute(self.buffer_set.new_write_buffer(), stmt.id(), params, flags)?;
            self.write_payload()?;
            self.drive_bulk_exec(stmt, handler)
        }
    }

    /// Execute a prepared statement and return only the first row, dropping the rest
    ///
    /// # Returns
    /// * `Ok(true)` - First row was found and processed
    /// * `Ok(false)` - No rows in result set
    /// * `Err(Error)` - Query execution or handler callback failed
    pub fn exec_first<'conn, P, H>(
        &'conn mut self,
        stmt: &'conn mut PreparedStatement,
        params: P,
        handler: &mut H,
    ) -> Result<bool>
    where
        P: Params,
        H: BinaryResultSetHandler,
    {
        let result = self.exec_first_inner(stmt, params, handler);
        self.check_error(result)
    }

    fn exec_first_inner<'conn, P, H>(
        &'conn mut self,
        stmt: &'conn mut PreparedStatement,
        params: P,
        handler: &mut H,
    ) -> Result<bool>
    where
        P: Params,
        H: BinaryResultSetHandler,
    {
        write_execute(self.buffer_set.new_write_buffer(), stmt.id(), params)?;
        self.write_payload()?;
        let mut first_row_handler = FirstRowHandler::new(handler);
        self.drive_exec(stmt, &mut first_row_handler)?;
        Ok(first_row_handler.found_row)
    }

    /// Execute a prepared statement and discard all results
    pub fn exec_drop<P>(&mut self, stmt: &mut PreparedStatement, params: P) -> Result<()>
    where
        P: Params,
    {
        self.exec(stmt, params, &mut DropHandler::default())
    }

    /// Execute a prepared statement and collect all rows into a Vec
    pub fn exec_rows<Row, P>(&mut self, stmt: &mut PreparedStatement, params: P) -> Result<Vec<Row>>
    where
        Row: for<'buf> crate::raw::FromRawRow<'buf>,
        P: Params,
    {
        let mut handler = crate::handler::CollectHandler::<Row>::default();
        self.exec(stmt, params, &mut handler)?;
        Ok(handler.into_rows())
    }

    fn drive_query<H: TextResultSetHandler>(&mut self, handler: &mut H) -> Result<()> {
        let mut query = Query::new(handler);

        loop {
            match query.step(&mut self.buffer_set)? {
                Action::NeedPacket(buffer) => {
                    buffer.clear();
                    let _ = read_payload(&mut self.stream, buffer)?;
                }
                Action::ReadColumnMetadata { num_columns } => {
                    read_column_definition_packets(
                        &mut self.stream,
                        &mut self.buffer_set.column_definition_buffer,
                        num_columns,
                    )?;
                }
                Action::Finished => return Ok(()),
            }
        }
    }

    /// Execute a text protocol SQL query
    pub fn query<H>(&mut self, sql: &str, handler: &mut H) -> Result<()>
    where
        H: TextResultSetHandler,
    {
        let result = self.query_inner(sql, handler);
        self.check_error(result)
    }

    fn query_inner<H>(&mut self, sql: &str, handler: &mut H) -> Result<()>
    where
        H: TextResultSetHandler,
    {
        write_query(self.buffer_set.new_write_buffer(), sql);
        self.write_payload()?;
        self.drive_query(handler)
    }

    /// Execute a text protocol SQL query and discard the result
    pub fn query_drop(&mut self, sql: &str) -> Result<()> {
        let result = self.query_drop_inner(sql);
        self.check_error(result)
    }

    fn query_drop_inner(&mut self, sql: &str) -> Result<()> {
        write_query(self.buffer_set.new_write_buffer(), sql);
        self.write_payload()?;
        self.drive_query(&mut DropHandler::default())
    }

    /// Send a ping to the server to check if the connection is alive
    ///
    /// This sends a COM_PING command to the MySQL server and waits for an OK response.
    pub fn ping(&mut self) -> Result<()> {
        let result = self.ping_inner();
        self.check_error(result)
    }

    fn ping_inner(&mut self) -> Result<()> {
        write_ping(self.buffer_set.new_write_buffer());
        self.write_payload()?;
        self.buffer_set.read_buffer.clear();
        let _ = read_payload(&mut self.stream, &mut self.buffer_set.read_buffer)?;
        Ok(())
    }

    /// Reset the connection to its initial state
    pub fn reset(&mut self) -> Result<()> {
        let result = self.reset_inner();
        self.check_error(result)
    }

    fn reset_inner(&mut self) -> Result<()> {
        write_reset_connection(self.buffer_set.new_write_buffer());
        self.write_payload()?;
        self.buffer_set.read_buffer.clear();
        let _ = read_payload(&mut self.stream, &mut self.buffer_set.read_buffer)?;
        self.in_transaction = false;
        Ok(())
    }

    /// Execute a closure within a transaction
    ///
    /// # Errors
    /// Returns `Error::NestedTransaction` if called while already in a transaction
    pub fn run_transaction<F, R>(&mut self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Conn, super::transaction::Transaction) -> Result<R>,
    {
        if self.in_transaction {
            return Err(Error::NestedTransaction);
        }

        self.in_transaction = true;

        if let Err(e) = self.query_drop("BEGIN") {
            self.in_transaction = false;
            return Err(e);
        }

        let tx = super::transaction::Transaction::new(self.connection_id());
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

/// Read a complete MySQL payload, concatenating payloads if they span multiple 16MB chunks
/// Returns the sequence_id of the last packet read.
fn read_payload(reader: &mut Stream, buffer: &mut Vec<u8>) -> Result<u8> {
    buffer.clear();

    let mut header = PacketHeader::new_zeroed();
    reader.read_exact(header.as_mut_bytes())?;

    let length = header.length();
    let mut sequence_id = header.sequence_id;

    buffer.reserve(length);

    {
        let spare = buffer.spare_capacity_mut();
        let mut buf: BorrowedBuf<'_> = (&mut spare[..length]).into();
        reader.read_buf_exact(buf.unfilled())?;
        // SAFETY: read_buf_exact filled exactly `length` bytes
        unsafe {
            buffer.set_len(length);
        }
    }

    let mut current_length = length;
    while current_length == 0xFFFFFF {
        reader.read_exact(header.as_mut_bytes())?;

        current_length = header.length();
        sequence_id = header.sequence_id;

        buffer.reserve(current_length);
        let spare = buffer.spare_capacity_mut();
        let mut buf: BorrowedBuf<'_> = (&mut spare[..current_length]).into();
        reader.read_buf_exact(buf.unfilled())?;
        // SAFETY: read_buf_exact filled exactly `current_length` bytes
        unsafe {
            buffer.set_len(buffer.len() + current_length);
        }
    }

    Ok(sequence_id)
}

fn read_column_definition_packets(
    reader: &mut Stream,
    out: &mut Vec<u8>,
    num_columns: usize,
) -> Result<u8> {
    out.clear();
    let mut header = PacketHeader::new_zeroed();

    // For each column, write [4 bytes len][payload]
    for _ in 0..num_columns {
        reader.read_exact(header.as_mut_bytes())?;
        let length = header.length();
        out.extend((length as u32).to_ne_bytes());

        out.reserve(length);
        let spare = out.spare_capacity_mut();
        let mut buf: BorrowedBuf<'_> = (&mut spare[..length]).into();
        reader.read_buf_exact(buf.unfilled())?;
        // SAFETY: read_buf_exact filled exactly `length` bytes
        unsafe {
            out.set_len(out.len() + length);
        }
    }

    Ok(header.sequence_id)
}

fn write_handshake_payload(
    stream: &mut Stream,
    buffer_set: &mut BufferSet,
    sequence_id: u8,
) -> Result<()> {
    let mut buffer = buffer_set.write_buffer_mut().as_mut_slice();
    let mut seq_id = sequence_id;

    loop {
        let chunk_size = buffer[4..].len().min(0xFFFFFF);
        PacketHeader::mut_from_bytes(&mut buffer[0..4])?.encode_in_place(chunk_size, seq_id);
        stream.write_all(&buffer[..4 + chunk_size])?;

        if chunk_size < 0xFFFFFF {
            break;
        }

        seq_id = seq_id.wrapping_add(1);
        buffer = &mut buffer[0xFFFFFF..];
    }
    stream.flush()?;
    Ok(())
}

/// Handler to capture socket path from SELECT @@socket query
struct SocketPathHandler {
    path: Option<String>,
}

impl TextResultSetHandler for SocketPathHandler {
    fn no_result_set(&mut self, _: OkPayloadBytes) -> Result<()> {
        Ok(())
    }
    fn resultset_start(&mut self, _: &[ColumnDefinition<'_>]) -> Result<()> {
        Ok(())
    }
    fn resultset_end(&mut self, _: OkPayloadBytes) -> Result<()> {
        Ok(())
    }
    fn row(&mut self, _: &[ColumnDefinition<'_>], row: TextRowPayload<'_>) -> Result<()> {
        // 0xFB indicates NULL value
        if row.0.first() == Some(&0xFB) {
            return Ok(());
        }
        // Parse the first length-encoded string
        let (value, _) = read_string_lenenc(row.0)?;
        if !value.is_empty() {
            self.path = Some(String::from_utf8_lossy(value).into_owned());
        }
        Ok(())
    }
}
