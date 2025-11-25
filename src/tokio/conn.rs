use tokio::net::{TcpStream, UnixStream};
use tracing::instrument;
use zerocopy::{FromBytes, FromZeros, IntoBytes};

use crate::buffer::BufferSet;
use crate::constant::CapabilityFlags;
use crate::error::{Error, Result};
use crate::protocol::command::prepared::{read_prepare_ok, write_execute, write_prepare};
use crate::protocol::connection::{Handshake, HandshakeResult, InitialHandshake};
use crate::protocol::packet::PacketHeader;
use crate::protocol::response::ErrPayloadBytes;
use crate::protocol::r#trait::{BinaryResultSetHandler, TextResultSetHandler, params::Params};

use super::stream::Stream;

pub struct Conn {
    stream: Stream,
    buffer_set: BufferSet,
    initial_handshake: InitialHandshake,
    capability_flags: CapabilityFlags,
    pub(crate) in_transaction: bool,
}

impl Conn {
    /// Create a new MySQL connection from connection options (async)
    pub async fn new<O: TryInto<crate::opts::Opts>>(opts: O) -> Result<Self>
    where
        Error: From<O::Error>,
    {
        let opts: crate::opts::Opts = opts.try_into()?;

        let stream = if let Some(socket_path) = &opts.socket {
            let stream = UnixStream::connect(socket_path).await?;
            Stream::unix(stream)
        } else {
            let host = opts.host.as_ref().ok_or_else(|| {
                Error::BadConfigError("Missing host in connection options".to_string())
            })?;

            let addr = format!("{}:{}", host, opts.port);
            let stream = TcpStream::connect(&addr).await?;
            stream.set_nodelay(opts.tcp_nodelay)?;
            Stream::tcp(stream)
        };

        Self::new_with_stream(stream, &opts).await
    }

    /// Create a new MySQL connection with an existing stream (async)
    pub async fn new_with_stream(stream: Stream, opts: &crate::opts::Opts) -> Result<Self> {
        let mut conn_stream = stream;
        let mut buffer_set = BufferSet::new();
        let mut initial_handshake = None;

        #[cfg(feature = "tls")]
        let host = opts.host.clone().unwrap_or_default();

        let mut handshake = Handshake::new(
            opts.user.clone(),
            opts.password.clone().unwrap_or_default(),
            opts.db.clone(),
            opts.capabilities,
            opts.tls,
        );

        let mut last_sequence_id;
        let capability_flags = loop {
            let buffer = if matches!(handshake, Handshake::Start { .. }) {
                &mut buffer_set.initial_handshake
            } else {
                &mut buffer_set.read_buffer
            };
            buffer.clear();
            last_sequence_id = read_payload(&mut conn_stream, buffer).await?;

            match handshake.drive(&mut buffer_set)? {
                HandshakeResult::InitialHandshake {
                    initial_handshake: hs,
                } => {
                    initial_handshake = Some(hs);
                    write_handshake_payload(
                        &mut conn_stream,
                        &mut buffer_set,
                        &mut last_sequence_id,
                    )
                    .await?;
                }
                #[cfg(feature = "tls")]
                HandshakeResult::SslRequest {
                    initial_handshake: hs,
                } => {
                    initial_handshake = Some(hs);
                    write_handshake_payload(
                        &mut conn_stream,
                        &mut buffer_set,
                        &mut last_sequence_id,
                    )
                    .await?;

                    // Upgrade to TLS
                    conn_stream = conn_stream.upgrade_to_tls(&host).await?;

                    // Continue handshake after TLS upgrade
                    let HandshakeResult::Write = handshake.drive_after_tls(&mut buffer_set)? else {
                        return Err(Error::InvalidPacket);
                    };
                    write_handshake_payload(
                        &mut conn_stream,
                        &mut buffer_set,
                        &mut last_sequence_id,
                    )
                    .await?;
                }
                #[cfg(not(feature = "tls"))]
                HandshakeResult::SslRequest { .. } => {
                    return Err(Error::BadConfigError(
                        "TLS requested but tls feature is not enabled".to_string(),
                    ));
                }
                HandshakeResult::Write => {
                    write_handshake_payload(
                        &mut conn_stream,
                        &mut buffer_set,
                        &mut last_sequence_id,
                    )
                    .await?;
                }
                HandshakeResult::Read => {
                    // Nothing to write, just continue to read next packet
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

    /// Check if the server is MySQL (as opposed to MariaDB)
    pub fn is_mysql(&self) -> bool {
        self.capability_flags.is_mysql()
    }

    /// Get the connection ID assigned by the server
    pub fn connection_id(&self) -> u64 {
        self.initial_handshake.connection_id as u64
    }

    /// Get the server status flags from the initial handshake
    pub fn status_flags(&self) -> crate::constant::ServerStatusFlags {
        self.initial_handshake.status_flags
    }

    /// Write a MySQL packet from write_buffer asynchronously, splitting it into 16MB chunks if necessary
    #[instrument(skip_all)]
    async fn write_payload(&mut self) -> Result<()> {
        let mut sequence_id = 0u8;
        let mut buffer = self.buffer_set.write_buffer_mut().as_mut_slice();

        loop {
            let chunk_size = buffer[4..].len().min(0xFFFFFF);
            PacketHeader::mut_from_bytes(&mut buffer[0..4])
                .unwrap()
                .encode_in_place(chunk_size, sequence_id);
            self.stream.write_all(&buffer[..4 + chunk_size]).await?;

            if chunk_size < 0xFFFFFF {
                break;
            }

            sequence_id = sequence_id.wrapping_add(1);
            buffer = &mut buffer[0xFFFFFF..];
        }
        self.stream.flush().await?;
        Ok(())
    }

    /// Prepare a statement and return the statement ID (async)
    ///
    /// Returns `Ok(statement_id)` on success.
    pub async fn prepare(&mut self, sql: &str) -> Result<u32> {
        self.buffer_set.read_buffer.clear();

        write_prepare(self.buffer_set.new_write_buffer(), sql);

        self.write_payload().await?;

        let _ = read_payload(&mut self.stream, &mut self.buffer_set.read_buffer).await?;

        if !self.buffer_set.read_buffer.is_empty() && self.buffer_set.read_buffer[0] == 0xFF {
            Err(ErrPayloadBytes(&self.buffer_set.read_buffer))?
        }

        let prepare_ok = read_prepare_ok(&self.buffer_set.read_buffer)?;
        let statement_id = prepare_ok.statement_id();
        let num_params = prepare_ok.num_params();
        let num_columns = prepare_ok.num_columns();

        for _ in 0..num_params {
            let _ = read_payload(&mut self.stream, &mut self.buffer_set.read_buffer).await?;
        }

        for _ in 0..num_columns {
            let _ = read_payload(&mut self.stream, &mut self.buffer_set.read_buffer).await?;
        }

        Ok(statement_id)
    }

    /// Execute a prepared statement with a result set handler (async)
    pub async fn exec<P, H>(&mut self, statement_id: u32, params: P, handler: &mut H) -> Result<()>
    where
        P: Params,
        H: BinaryResultSetHandler,
    {
        use crate::protocol::command::Action;
        use crate::protocol::command::prepared::Exec;

        write_execute(self.buffer_set.new_write_buffer(), statement_id, params)?;

        self.write_payload().await?;

        let mut exec = Exec::new(handler);

        loop {
            match exec.step(&mut self.buffer_set)? {
                Action::NeedPacket(buffer) => {
                    buffer.clear();
                    let _ = read_payload(&mut self.stream, buffer).await?;
                }
                Action::Finished => return Ok(()),
            }
        }
    }

    /// Execute a prepared statement and return only the first row, dropping the rest (async)
    ///
    /// # Returns
    /// * `Ok(true)` - First row was found and processed
    /// * `Ok(false)` - No rows in result set
    /// * `Err(Error)` - Query execution or handler callback failed
    pub async fn exec_first<P, H>(
        &mut self,
        statement_id: u32,
        params: P,
        handler: &mut H,
    ) -> Result<bool>
    where
        P: Params,
        H: BinaryResultSetHandler,
    {
        use crate::protocol::command::Action;
        use crate::protocol::command::prepared::Exec;
        use crate::protocol::command::utility::FirstRowHandler;

        write_execute(self.buffer_set.new_write_buffer(), statement_id, params)?;

        self.write_payload().await?;

        let mut first_row_handler = FirstRowHandler::new(handler);
        let mut exec = Exec::new(&mut first_row_handler);

        loop {
            match exec.step(&mut self.buffer_set)? {
                Action::NeedPacket(buffer) => {
                    buffer.clear();
                    let _ = read_payload(&mut self.stream, buffer).await?;
                }
                Action::Finished => return Ok(first_row_handler.found_row),
            }
        }
    }

    /// Execute a prepared statement and discard all results (async)
    #[instrument(skip_all)]
    pub async fn exec_drop<P>(&mut self, statement_id: u32, params: P) -> Result<()>
    where
        P: Params,
    {
        use crate::protocol::command::Action;
        use crate::protocol::command::prepared::Exec;
        use crate::protocol::command::utility::DropHandler;

        write_execute(self.buffer_set.new_write_buffer(), statement_id, params)?;

        self.write_payload().await?;

        let mut drop_handler = DropHandler::new();
        let mut exec = Exec::new(&mut drop_handler);

        loop {
            match exec.step(&mut self.buffer_set)? {
                Action::NeedPacket(buffer) => {
                    buffer.clear();
                    let _ = read_payload(&mut self.stream, buffer).await?;
                }
                Action::Finished => return Ok(()),
            }
        }
    }

    /// Execute a text protocol SQL query (async)
    pub async fn query<H>(&mut self, sql: &str, handler: &mut H) -> Result<()>
    where
        H: TextResultSetHandler,
    {
        use crate::protocol::command::Action;
        use crate::protocol::command::query::{Query, write_query};

        write_query(self.buffer_set.new_write_buffer(), sql);

        self.write_payload().await?;

        let mut query_sm = Query::new(handler);

        loop {
            match query_sm.step(&mut self.buffer_set)? {
                Action::NeedPacket(buffer) => {
                    buffer.clear();
                    let _ = read_payload(&mut self.stream, buffer).await?;
                }
                Action::Finished => return Ok(()),
            }
        }
    }

    /// Execute a text protocol SQL query and discard all results (async)
    #[instrument(skip_all)]
    pub async fn query_drop(&mut self, sql: &str) -> Result<()> {
        use crate::protocol::command::Action;
        use crate::protocol::command::query::{Query, write_query};
        use crate::protocol::command::utility::DropHandler;

        write_query(self.buffer_set.new_write_buffer(), sql);

        self.write_payload().await?;

        let mut drop_handler = DropHandler::new();
        let mut query_sm = Query::new(&mut drop_handler);

        loop {
            match query_sm.step(&mut self.buffer_set)? {
                Action::NeedPacket(buffer) => {
                    buffer.clear();
                    let _ = read_payload(&mut self.stream, buffer).await?;
                }
                Action::Finished => return Ok(()),
            }
        }
    }

    /// Send a ping to the server to check if the connection is alive (async)
    ///
    /// This sends a COM_PING command to the MySQL server and waits for an OK response.
    pub async fn ping(&mut self) -> Result<()> {
        use crate::protocol::command::utility::write_ping;

        write_ping(self.buffer_set.new_write_buffer());

        self.write_payload().await?;

        self.buffer_set.read_buffer.clear();
        let _ = read_payload(&mut self.stream, &mut self.buffer_set.read_buffer).await?;

        Ok(())
    }

    /// Reset the connection to its initial state (async)
    pub async fn reset(&mut self) -> Result<()> {
        use crate::protocol::command::utility::write_reset_connection;

        write_reset_connection(self.buffer_set.new_write_buffer());

        self.write_payload().await?;

        self.buffer_set.read_buffer.clear();
        let _ = read_payload(&mut self.stream, &mut self.buffer_set.read_buffer).await?;

        self.in_transaction = false;

        Ok(())
    }

    /// Execute a closure within a transaction (async)
    ///
    /// # Panics
    /// Panics if called while already in a transaction (nested transactions are not supported).
    pub async fn run_transaction<F, Fut, R>(&mut self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Conn, super::transaction::Transaction) -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        assert!(
            !self.in_transaction,
            "Cannot nest transactions - a transaction is already active"
        );

        self.in_transaction = true;

        if let Err(e) = self.query_drop("BEGIN").await {
            self.in_transaction = false;
            return Err(e);
        }

        let tx = super::transaction::Transaction::new(self.connection_id());
        let result = f(self, tx).await;

        // If the transaction was not explicitly committed or rolled back, roll it back
        if self.in_transaction {
            let rollback_result = self.query_drop("ROLLBACK").await;
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

/// Read a complete MySQL payload asynchronously, concatenating packets if they span multiple 16MB chunks
/// Returns the sequence_id of the last packet read.
#[instrument(skip_all)]
async fn read_payload(reader: &mut Stream, buffer: &mut Vec<u8>) -> Result<u8> {
    let mut packet_header = PacketHeader::new_zeroed();

    buffer.clear();
    reader.read_exact(packet_header.as_mut_bytes()).await?;

    let length = packet_header.length();
    let mut sequence_id = packet_header.sequence_id;

    buffer.reserve(length);

    let start = buffer.len();
    buffer.resize(start + length, 0);
    reader.read_exact(&mut buffer[start..]).await?;

    let mut current_length = length;
    while current_length == 0xFFFFFF {
        reader.read_exact(packet_header.as_mut_bytes()).await?;

        current_length = packet_header.length();
        sequence_id = packet_header.sequence_id;

        let prev_len = buffer.len();
        buffer.resize(prev_len + current_length, 0);
        reader.read_exact(&mut buffer[prev_len..]).await?;
    }

    Ok(sequence_id)
}

async fn write_handshake_payload(
    stream: &mut Stream,
    buffer_set: &mut BufferSet,
    last_sequence_id: &mut u8,
) -> Result<()> {
    let mut buffer = buffer_set.write_buffer_mut().as_mut_slice();

    loop {
        let chunk_size = buffer[4..].len().min(0xFFFFFF);
        *last_sequence_id = last_sequence_id.wrapping_add(1);
        PacketHeader::mut_from_bytes(&mut buffer[0..4])
            .unwrap()
            .encode_in_place(chunk_size, *last_sequence_id);
        stream.write_all(&buffer[..4 + chunk_size]).await?;

        if chunk_size < 0xFFFFFF {
            break;
        }

        buffer = &mut buffer[0xFFFFFF..];
    }
    stream.flush().await?;
    Ok(())
}
