use crate::buffer::BufferSet;
use crate::constant::CommandByte;
use crate::error::{Error, Result, eyre};
use crate::protocol::BinaryRowPayload;
use crate::protocol::primitive::*;
use crate::protocol::response::{ErrPayloadBytes, OkPayloadBytes};
use crate::protocol::r#trait::param::Params;
use zerocopy::byteorder::little_endian::{U16 as U16LE, U32 as U32LE};
use zerocopy::{FromBytes, Immutable, KnownLayout};

/// Prepared statement OK response (zero-copy)
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
pub struct PrepareOk {
    statement_id: U32LE,
    num_columns: U16LE,
    num_params: U16LE,
    _reserved: u8,
    warning_count: U16LE, // MySQL >= 5.7 and MariaDB all expect at least 12 bytes: https://github.com/launchbadge/sqlx/issues/3335
}

impl PrepareOk {
    /// Get the statement ID
    pub fn statement_id(&self) -> u32 {
        self.statement_id.get()
    }

    /// Get the number of columns in the result set
    pub fn num_columns(&self) -> u16 {
        self.num_columns.get()
    }

    /// Get the number of parameters in the prepared statement
    pub fn num_params(&self) -> u16 {
        self.num_params.get()
    }

    /// Get the warning count
    pub fn warning_count(&self) -> u16 {
        self.warning_count.get()
    }
}

/// Write COM_STMT_PREPARE command
pub fn write_prepare(out: &mut Vec<u8>, sql: &str) {
    write_int_1(out, CommandByte::StmtPrepare as u8);
    out.extend_from_slice(sql.as_bytes());
}

/// Read COM_STMT_PREPARE response
pub fn read_prepare_ok(payload: &[u8]) -> Result<&PrepareOk> {
    let (status, data) = read_int_1(payload)?;
    debug_assert_eq!(status, 0x00);
    PrepareOk::ref_from_bytes(&data[..11]).map_err(Error::from_debug)
}

/// Write COM_STMT_EXECUTE command
pub fn write_execute<P: Params>(out: &mut Vec<u8>, statement_id: u32, params: P) -> Result<()> {
    write_int_1(out, CommandByte::StmtExecute as u8);
    write_int_4(out, statement_id);

    // flags (1 byte) - CURSOR_TYPE_NO_CURSOR
    write_int_1(out, 0x00);

    // iteration count (4 bytes) - always 1
    write_int_4(out, 1);

    let num_params = params.len();

    if num_params > 0 {
        // NULL bitmap: (num_params + 7) / 8 bytes
        params.encode_null_bitmap(out);

        // new-params-bound-flag (1 byte)
        let send_types_to_server = true;
        if send_types_to_server {
            write_int_1(out, 0x01);
            params.encode_types(out);
        } else {
            write_int_1(out, 0x00);
        }

        params.encode_values(out)?; // Ignore errors for now (non-priority)
    }
    Ok(())
}

/// Read COM_STMT_EXECUTE response
/// This can be either an OK packet or a result set
pub fn read_execute_response(payload: &[u8], cache_metadata: bool) -> Result<ExecuteResponse<'_>> {
    if payload.is_empty() {
        return Err(Error::LibraryBug(eyre!(
            "read_execute_response: empty payload"
        )));
    }

    match payload[0] {
        0x00 => Ok(ExecuteResponse::Ok(OkPayloadBytes(payload))),
        0xFF => Err(ErrPayloadBytes(payload).into()),
        _ => {
            let (column_count, rest) = read_int_lenenc(payload)?;

            // If MARIADB_CLIENT_CACHE_METADATA is set, read the metadata_follows flag
            let has_column_metadata = if cache_metadata {
                if rest.is_empty() {
                    return Err(Error::LibraryBug(eyre!(
                        "read_execute_response: missing metadata_follows flag"
                    )));
                }
                rest[0] != 0
            } else {
                // Without caching, metadata always follows
                true
            };

            Ok(ExecuteResponse::ResultSet {
                column_count,
                has_column_metadata,
            })
        }
    }
}

/// Execute response variants
#[derive(Debug)]
pub enum ExecuteResponse<'a> {
    Ok(OkPayloadBytes<'a>),
    ResultSet {
        column_count: u64,
        has_column_metadata: bool,
    },
}

/// Read binary protocol row from execute response
pub fn read_binary_row<'a>(payload: &'a [u8], num_columns: usize) -> Result<BinaryRowPayload<'a>> {
    crate::protocol::command::resultset::read_binary_row(payload, num_columns)
}

/// Write COM_STMT_CLOSE command
pub fn write_close_statement(out: &mut Vec<u8>, statement_id: u32) {
    write_int_1(out, CommandByte::StmtClose as u8);
    write_int_4(out, statement_id);
}

/// Write COM_STMT_RESET command
pub fn write_reset_statement(out: &mut Vec<u8>, statement_id: u32) {
    write_int_1(out, CommandByte::StmtReset as u8);
    write_int_4(out, statement_id);
}

// ============================================================================
// State Machine API for exec_fold
// ============================================================================

use crate::PreparedStatement;
use crate::protocol::command::ColumnDefinitions;
use crate::protocol::r#trait::BinaryResultSetHandler;

/// Internal state of the Exec state machine
enum ExecState {
    /// Initial state - need to read first packet
    Start,
    /// Reading the first response packet
    ReadingFirstPacket,
    /// Reading column definitions (processing the buffer after reading all packets)
    ReadingColumns { num_columns: usize },
    /// Reading rows
    ReadingRows { num_columns: usize },
    /// Finished
    Finished,
}

/// State machine for executing prepared statements (binary protocol) with integrated handler
///
/// The handler is provided at construction and called directly by the state machine.
/// The `drive()` method returns actions indicating what I/O operation is needed next.
pub struct Exec<'h, 'stmt, H> {
    state: ExecState,
    handler: &'h mut H,
    stmt: &'stmt mut PreparedStatement,
    cache_metadata: bool,
}

impl<'h, 'stmt, H: BinaryResultSetHandler> Exec<'h, 'stmt, H> {
    /// Create a new Exec state machine with the given handler and prepared statement
    pub fn new(
        handler: &'h mut H,
        stmt: &'stmt mut PreparedStatement,
        cache_metadata: bool,
    ) -> Self {
        Self {
            state: ExecState::Start,
            handler,
            stmt,
            cache_metadata,
        }
    }

    /// Drive the state machine forward
    ///
    /// # Arguments
    /// * `buffer_set` - The buffer set containing buffers to read from/write to
    ///
    /// # Returns
    /// * `Action::NeedPacket(&mut Vec<u8>)` - Needs more data in the specified buffer
    /// * `Action::Finished` - Processing complete
    pub fn step<'buf>(
        &mut self,
        buffer_set: &'buf mut BufferSet,
    ) -> Result<crate::protocol::command::Action<'buf>> {
        use crate::protocol::command::Action;
        match &mut self.state {
            ExecState::Start => {
                // Request the first packet
                self.state = ExecState::ReadingFirstPacket;
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }

            ExecState::ReadingFirstPacket => {
                let payload = &buffer_set.read_buffer[..];
                let response = read_execute_response(payload, self.cache_metadata)?;

                match response {
                    ExecuteResponse::Ok(ok_bytes) => {
                        // Parse OK packet to check status flags
                        use crate::constant::ServerStatusFlags;
                        use crate::protocol::response::OkPayload;

                        let ok_payload = OkPayload::try_from(ok_bytes)?;
                        self.handler.no_result_set(ok_bytes)?;

                        // Check if there are more results to come
                        if ok_payload
                            .status_flags
                            .contains(ServerStatusFlags::SERVER_MORE_RESULTS_EXISTS)
                        {
                            // More resultsets coming, go to ReadingFirstPacket to process next result
                            self.state = ExecState::ReadingFirstPacket;
                            Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
                        } else {
                            // No more results, we're done
                            self.state = ExecState::Finished;
                            Ok(Action::Finished)
                        }
                    }
                    ExecuteResponse::ResultSet {
                        column_count,
                        has_column_metadata,
                    } => {
                        let num_columns = column_count as usize;

                        if has_column_metadata {
                            // Server sent metadata, signal that we need to read N column packets
                            self.state = ExecState::ReadingColumns { num_columns };
                            Ok(Action::ReadColumnMetadata { num_columns })
                        } else {
                            // No metadata from server, use cached definitions
                            if let Some(cols) = self.stmt.column_definitions() {
                                self.handler.resultset_start(cols)?;
                                self.state = ExecState::ReadingRows { num_columns };
                                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
                            } else {
                                // No cache available but server didn't send metadata - error
                                Err(Error::LibraryBug(eyre!(
                                    "no cached column definitions available"
                                )))
                            }
                        }
                    }
                }
            }

            ExecState::ReadingColumns { num_columns } => {
                // Parse all column definitions from the buffer
                // The buffer contains [len(u32)][payload][len(u32)][payload]...
                let column_defs = ColumnDefinitions::new(
                    *num_columns,
                    std::mem::take(&mut buffer_set.column_definition_buffer),
                )?;

                // Cache the column definitions in the prepared statement
                self.handler.resultset_start(column_defs.definitions())?;
                self.stmt.set_column_definitions(column_defs);

                // Move to reading rows
                self.state = ExecState::ReadingRows {
                    num_columns: *num_columns,
                };
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }

            ExecState::ReadingRows { num_columns } => {
                let payload = &buffer_set.read_buffer[..];
                match payload[0] {
                    0x00 => {
                        let row = read_binary_row(payload, *num_columns)?;
                        let cols = self.stmt.column_definitions().ok_or_else(|| {
                            Error::LibraryBug(eyre!("no column definitions while reading rows"))
                        })?;
                        self.handler.row(cols, row)?;
                        Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
                    }
                    0xFE => {
                        // Parse OK packet to check status flags
                        use crate::constant::ServerStatusFlags;
                        use crate::protocol::response::OkPayload;

                        let eof_bytes = OkPayloadBytes(payload);
                        eof_bytes.assert_eof()?;
                        let ok_payload = OkPayload::try_from(eof_bytes)?;
                        self.handler.resultset_end(eof_bytes)?;

                        // Check if there are more results to come
                        if ok_payload
                            .status_flags
                            .contains(ServerStatusFlags::SERVER_MORE_RESULTS_EXISTS)
                        {
                            // More resultsets coming, go to ReadingFirstPacket to process next result
                            self.state = ExecState::ReadingFirstPacket;
                            Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
                        } else {
                            // No more results, we're done
                            self.state = ExecState::Finished;
                            Ok(Action::Finished)
                        }
                    }
                    header => Err(Error::LibraryBug(eyre!(
                        "unexpected row packet header: 0x{:02X}",
                        header
                    ))),
                }
            }

            ExecState::Finished => Err(Error::LibraryBug(eyre!(
                "Exec::step called after finished"
            ))),
        }
    }
}
