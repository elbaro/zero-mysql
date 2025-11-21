use crate::buffer::BufferSet;
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
use crate::protocol::connection::{Handshake, HandshakeResult, InitialHandshake};
use crate::protocol::response::ErrPayloadBytes;
use crate::protocol::r#trait::{ResultSetHandler, TextResultSetHandler, params::Params};

use zerocopy::IntoBytes;

pub struct Conn {
    stream: TcpStream,
    buffer_set: BufferSet,
    initial_handshake: InitialHandshake,
    capability_flags: CapabilityFlags,
}

impl Conn {
    /// Create a new MySQL connection from connection options (async)
    pub async fn new<O: TryInto<crate::opts::Opts>>(opts: O) -> Result<Self>
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

        let stream = TcpStream::connect((host.as_str(), opts.port)).await?;

        Self::new_with_stream(
            stream,
            &opts.user,
            opts.password.as_deref().unwrap_or(""),
            opts.db.as_deref(),
        )
        .await
    }

    /// Create a new MySQL connection with an existing TCP stream (async)
    pub async fn new_with_stream(
        mut stream: TcpStream,
        username: &str,
        password: &str,
        database: Option<&str>,
    ) -> Result<Self> {
        let mut buffer_set = BufferSet::new();
        buffer_set.buffer_pool.push(Vec::new());
        let mut initial_handshake = None;

        let mut handshake = Handshake::new(
            username.to_string(),
            password.to_string(),
            database.map(|s| s.to_string()),
        );

        let capability_flags = loop {
            let is_start = matches!(handshake, Handshake::Start { .. });
            let buffer = if is_start {
                std::mem::take(&mut buffer_set.initial_handshake)
            } else {
                buffer_set.get_pooled_buffer()
            };

            let (mut last_sequence_id, buffer) = read_payload(&mut stream, buffer).await?;

            match handshake.drive(&buffer)? {
                HandshakeResult::InitialHandshake { handshake_response, initial_handshake: hs } => {
                    initial_handshake = Some(hs);
                    buffer_set.initial_handshake = buffer;
                    if !handshake_response.is_empty() {
                        write_handshake_payload(&mut stream, &mut last_sequence_id, &handshake_response).await?;
                    }
                }
                HandshakeResult::Write(packet_data) => {
                    if !packet_data.is_empty() {
                        write_handshake_payload(&mut stream, &mut last_sequence_id, &packet_data).await?;
                    }
                    buffer_set.return_pooled_buffer(buffer);
                }
                HandshakeResult::Connected { capability_flags } => {
                    buffer_set.return_pooled_buffer(buffer);
                    break capability_flags;
                }
            }
        };

        Ok(Self {
            stream,
            buffer_set,
            initial_handshake: initial_handshake.unwrap(),
            capability_flags,
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

    #[instrument(skip_all)]
    async fn write_payload(&mut self) -> Result<()> {
        let mut sequence_id = 0u8;
        let payload = self.buffer_set.write_buffer.as_slice();

        let num_chunks = payload.len() / 0xFFFFFF + 1;
        let needs_empty_packet = payload.len().is_multiple_of(0xFFFFFF) && !payload.is_empty();
        let total_size = num_chunks * 4 + payload.len();

        self.buffer_set.packet_buf.clear();
        if self.buffer_set.packet_buf.capacity() < total_size {
            self.buffer_set.packet_buf
                .reserve(total_size - self.buffer_set.packet_buf.capacity());
        }

        let mut remaining = payload;
        while !remaining.is_empty() {
            let chunk_size = remaining.len().min(0xFFFFFF);
            let (chunk, rest) = remaining.split_at(chunk_size);

            let header = PacketHeader::encode(chunk_size, sequence_id);
            self.buffer_set.packet_buf.extend_from_slice(header.as_bytes());

            self.buffer_set.packet_buf.extend_from_slice(chunk);

            remaining = rest;
            sequence_id = sequence_id.wrapping_add(1);
        }

        if needs_empty_packet {
            let header = PacketHeader::encode(0, sequence_id);
            self.buffer_set.packet_buf.extend_from_slice(header.as_bytes());
        }

        let packet_buf = std::mem::take(&mut self.buffer_set.packet_buf);
        let BufResult(result, packet_buf) = self.stream.write_all(packet_buf).await;
        result.map_err(Error::IoError)?;
        self.buffer_set.packet_buf = packet_buf;

        Ok(())
    }

    /// Prepare a statement and return the statement ID (async)
    pub async fn prepare(&mut self, sql: &str) -> Result<u32> {
        self.buffer_set.write_buffer.clear();

        write_prepare(&mut self.buffer_set.write_buffer, sql);

        self.write_payload().await?;

        let buffer = self.buffer_set.get_pooled_buffer();

        let (_seq, buffer) = read_payload(&mut self.stream, buffer).await?;

        if !buffer.is_empty() && buffer[0] == 0xFF {
            let err = ErrPayloadBytes(&buffer).into();
            self.buffer_set.return_pooled_buffer(buffer);
            return Err(err);
        }

        let prepare_ok = read_prepare_ok(&buffer)?;
        let statement_id = prepare_ok.statement_id();
        let num_params = prepare_ok.num_params();
        let num_columns = prepare_ok.num_columns();

        self.buffer_set.return_pooled_buffer(buffer);

        for _ in 0..num_params {
            let buffer = self.buffer_set.get_pooled_buffer();
            let (_seq, buffer) = read_payload(&mut self.stream, buffer).await?;
            self.buffer_set.return_pooled_buffer(buffer);
        }

        for _ in 0..num_columns {
            let buffer = self.buffer_set.get_pooled_buffer();
            let (_seq, buffer) = read_payload(&mut self.stream, buffer).await?;
            self.buffer_set.return_pooled_buffer(buffer);
        }

        Ok(statement_id)
    }

    /// Execute a prepared statement with a result set handler (async)
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

        self.buffer_set.write_buffer.clear();
        write_execute(&mut self.buffer_set.write_buffer, statement_id, params)?;

        self.write_payload().await?;

        let mut exec = Exec::default();

        loop {
            let buffer = self.buffer_set.get_pooled_buffer();

            let (_, buffer) = read_payload(&mut self.stream, buffer).await?;

            let result = exec.drive(&buffer[..])?;
            match result {
                ExecResult::NeedPayload => {
                    self.buffer_set.return_pooled_buffer(buffer);
                    continue;
                }
                ExecResult::NoResultSet(ok_bytes) => {
                    handler.no_result_set(ok_bytes)?;
                    self.buffer_set.return_pooled_buffer(buffer);
                    return Ok(());
                }

                ExecResult::ResultSetStart { num_columns } => {
                    handler.resultset_start(num_columns)?;
                    self.buffer_set.return_pooled_buffer(buffer);
                }
                ExecResult::Column(col) => {
                    handler.col(col)?;
                    self.buffer_set.return_pooled_buffer(buffer);
                }
                ExecResult::Row(row) => {
                    handler.row(&row)?;
                    self.buffer_set.return_pooled_buffer(buffer);
                }
                ExecResult::Eof(eof_bytes) => {
                    handler.resultset_end(eof_bytes)?;
                    self.buffer_set.return_pooled_buffer(buffer);
                    return Ok(());
                }
            }
        }
    }

    /// Execute a prepared statement and return only the first row, dropping the rest (async)
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

        self.buffer_set.write_buffer.clear();
        write_execute(&mut self.buffer_set.write_buffer, statement_id, params)?;

        self.write_payload().await?;

        let mut exec = Exec::default();
        let mut first_row_found = false;

        loop {
            let buffer = self.buffer_set.get_pooled_buffer();

            let (_, buffer) = read_payload(&mut self.stream, buffer).await?;

            let result = exec.drive(&buffer[..])?;
            match result {
                ExecResult::NeedPayload => {
                    self.buffer_set.return_pooled_buffer(buffer);
                    continue;
                }
                ExecResult::NoResultSet(ok_bytes) => {
                    handler.no_result_set(ok_bytes)?;
                    self.buffer_set.return_pooled_buffer(buffer);
                    return Ok(false);
                }

                ExecResult::ResultSetStart { num_columns } => {
                    handler.resultset_start(num_columns)?;
                    self.buffer_set.return_pooled_buffer(buffer);
                }
                ExecResult::Column(col) => {
                    handler.col(col)?;
                    self.buffer_set.return_pooled_buffer(buffer);
                }
                ExecResult::Row(row) => {
                    if !first_row_found {
                        handler.row(&row)?;
                        first_row_found = true;
                    }
                    self.buffer_set.return_pooled_buffer(buffer);
                }
                ExecResult::Eof(eof_bytes) => {
                    handler.resultset_end(eof_bytes)?;
                    self.buffer_set.return_pooled_buffer(buffer);
                    return Ok(first_row_found);
                }
            }
        }
    }

    /// Execute a prepared statement and discard all results (async)
    #[instrument(skip_all)]
    pub async fn exec_drop<P>(&mut self, statement_id: u32, params: P) -> Result<()>
    where
        P: Params,
    {
        use crate::protocol::command::prepared::{Exec, ExecResult};

        self.buffer_set.write_buffer.clear();
        write_execute(&mut self.buffer_set.write_buffer, statement_id, params)?;

        self.write_payload().await?;

        let mut exec = Exec::default();

        loop {
            let mut buffer = self.buffer_set.get_pooled_buffer();
            buffer.clear();

            let (_, buffer) = read_payload(&mut self.stream, buffer).await?;

            let result = exec.drive(&buffer[..])?;
            match result {
                ExecResult::NeedPayload => {
                    self.buffer_set.return_pooled_buffer(buffer);
                    continue;
                }
                ExecResult::NoResultSet(_ok_bytes) => {
                    self.buffer_set.return_pooled_buffer(buffer);
                    return Ok(());
                }
                ExecResult::ResultSetStart { .. } => {
                    self.buffer_set.return_pooled_buffer(buffer);
                }
                ExecResult::Column(_) => {
                    self.buffer_set.return_pooled_buffer(buffer);
                }
                ExecResult::Row(_) => {
                    self.buffer_set.return_pooled_buffer(buffer);
                }
                ExecResult::Eof(_eof_bytes) => {
                    self.buffer_set.return_pooled_buffer(buffer);
                    return Ok(());
                }
            }
        }
    }

    /// Execute a text protocol SQL query (async)
    pub async fn query<'a, H>(&mut self, sql: &str, handler: &mut H) -> Result<()>
    where
        H: TextResultSetHandler<'a>,
    {
        use crate::protocol::command::query::{Query, QueryResult, write_query};

        self.buffer_set.write_buffer.clear();
        write_query(&mut self.buffer_set.write_buffer, sql);

        self.write_payload().await?;

        let mut query_fold = Query::default();

        loop {
            let buffer = self.buffer_set.get_pooled_buffer();

            let (_, buffer) = read_payload(&mut self.stream, buffer).await?;

            let result = query_fold.drive(&buffer[..])?;
            match result {
                QueryResult::NeedPayload => {
                    self.buffer_set.return_pooled_buffer(buffer);
                    continue;
                }
                QueryResult::NoResultSet(ok_bytes) => {
                    handler.no_result_set(ok_bytes)?;
                    self.buffer_set.return_pooled_buffer(buffer);
                    return Ok(());
                }
                QueryResult::ResultSetStart { num_columns } => {
                    handler.resultset_start(num_columns)?;
                    self.buffer_set.return_pooled_buffer(buffer);
                }
                QueryResult::Column(col) => {
                    handler.col(col)?;
                    self.buffer_set.return_pooled_buffer(buffer);
                }
                QueryResult::Row(row) => {
                    handler.row(&row)?;
                    self.buffer_set.return_pooled_buffer(buffer);
                }
                QueryResult::Eof(eof_bytes) => {
                    handler.resultset_end(eof_bytes)?;
                    self.buffer_set.return_pooled_buffer(buffer);
                    return Ok(());
                }
            }
        }
    }

    /// Execute a text protocol SQL query and discard all results (async)
    #[instrument(skip_all)]
    pub async fn query_drop(&mut self, sql: &str) -> Result<()> {
        use crate::protocol::command::query::{Query, QueryResult, write_query};

        self.buffer_set.write_buffer.clear();
        write_query(&mut self.buffer_set.write_buffer, sql);

        self.write_payload().await?;

        let mut query = Query::default();

        loop {
            let mut buffer = self.buffer_set.get_pooled_buffer();
            buffer.clear();

            let (_, buffer) = read_payload(&mut self.stream, buffer).await?;

            let result = query.drive(&buffer[..])?;
            match result {
                QueryResult::NeedPayload => {
                    self.buffer_set.return_pooled_buffer(buffer);
                    continue;
                }
                QueryResult::NoResultSet(_ok_bytes) => {
                    self.buffer_set.return_pooled_buffer(buffer);
                    return Ok(());
                }
                QueryResult::ResultSetStart { .. } => {
                    self.buffer_set.return_pooled_buffer(buffer);
                }
                QueryResult::Column(_) => {
                    self.buffer_set.return_pooled_buffer(buffer);
                }
                QueryResult::Row(_) => {
                    self.buffer_set.return_pooled_buffer(buffer);
                }
                QueryResult::Eof(_eof_bytes) => {
                    self.buffer_set.return_pooled_buffer(buffer);
                    return Ok(());
                }
            }
        }
    }

    /// Send a ping to the server to check if the connection is alive (async)
    pub async fn ping(&mut self) -> Result<()> {
        use crate::protocol::command::utility::write_ping;

        self.buffer_set.write_buffer.clear();
        write_ping(&mut self.buffer_set.write_buffer);

        self.write_payload().await?;

        let buffer = self.buffer_set.get_pooled_buffer();
        let (_, buffer) = read_payload(&mut self.stream, buffer).await?;
        self.buffer_set.return_pooled_buffer(buffer);

        Ok(())
    }

    /// Reset the connection to its initial state (async)
    pub async fn reset(&mut self) -> Result<()> {
        use crate::protocol::command::utility::write_reset_connection;

        self.buffer_set.write_buffer.clear();
        write_reset_connection(&mut self.buffer_set.write_buffer);

        self.write_payload().await?;

        let buffer = self.buffer_set.get_pooled_buffer();
        let (_, buffer) = read_payload(&mut self.stream, buffer).await?;
        self.buffer_set.return_pooled_buffer(buffer);

        Ok(())
    }
}

/// Read a complete MySQL payload asynchronously, concatenating packets if they span multiple 16MB chunks.
#[instrument(skip_all)]
pub async fn read_payload<R>(reader: &mut R, mut buffer: Vec<u8>) -> Result<(u8, Vec<u8>)>
where
    R: AsyncReadExt + Unpin,
{
    let header = [0u8; 4];
    let BufResult(result, header) = reader.read_exact(header).await;
    result.map_err(Error::IoError)?;
    let mut length = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    let sequence_id = header[3];

    buffer.clear();
    if buffer.capacity() < length {
        buffer.reserve(length - buffer.capacity());
    }
    let BufResult(result, slice) = reader.read_exact(buffer.slice(..length)).await;
    result.map_err(Error::IoError)?;
    buffer = slice.into_inner();

    while length == 0xFFFFFF {
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

/// Write a MySQL packet during handshake asynchronously
async fn write_handshake_payload<W>(
    stream: &mut W,
    last_sequence_id: &mut u8,
    payload: &[u8],
) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    let mut packet_buf = Vec::new();

    let mut remaining = payload;
    let mut last_chunk_size = 0;

    while !remaining.is_empty() {
        let chunk_size = remaining.len().min(0xFFFFFF);
        let (chunk, rest) = remaining.split_at(chunk_size);

        *last_sequence_id = last_sequence_id.wrapping_add(1);
        let header = PacketHeader::encode(chunk_size, *last_sequence_id);
        packet_buf.extend_from_slice(header.as_bytes());

        packet_buf.extend_from_slice(chunk);

        remaining = rest;
        last_chunk_size = chunk_size;
    }

    if last_chunk_size == 0xFFFFFF {
        *last_sequence_id = last_sequence_id.wrapping_add(1);
        let header = PacketHeader::encode(0, *last_sequence_id);
        packet_buf.extend_from_slice(header.as_bytes());
    }

    let write_result = stream.write_all(packet_buf).await;
    write_result.0.map_err(Error::IoError)?;

    Ok(())
}
