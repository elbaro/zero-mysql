use crate::col::ColumnDefinitionBytes;
use crate::constant::CommandByte;
use crate::error::{Error, Result};
use crate::protocol::packet::{ErrPayloadBytes, OkPayloadBytes};
use crate::protocol::primitive::*;
use crate::protocol::r#trait::params::Params;
use crate::row::RowPayload;
use zerocopy::byteorder::little_endian::{U16 as U16LE, U32 as U32LE};
use zerocopy::{FromBytes, Immutable, KnownLayout};

/// Prepared statement OK response (zero-copy)
///
/// Layout matches MySQL wire protocol after status byte:
/// - statement_id: 4 bytes (little-endian)
/// - num_columns: 2 bytes (little-endian)
/// - num_params: 2 bytes (little-endian)
/// - reserved: 1 byte (0x00)
/// - warning_count: 2 bytes (little-endian)
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
pub struct PrepareOk {
    pub statement_id: U32LE,
    pub num_columns: U16LE,
    pub num_params: U16LE,
    pub _reserved: u8,
    pub warning_count: U16LE,
}

/// Write COM_STMT_PREPARE command
pub fn write_prepare(out: &mut Vec<u8>, sql: &str) {
    write_int_1(out, CommandByte::StmtPrepare as u8);
    out.extend_from_slice(sql.as_bytes());
}

/// Read COM_STMT_PREPARE response (zero-copy)
pub fn read_prepare_ok(payload: &[u8]) -> Result<&PrepareOk> {
    let (status, data) = read_int_1(payload)?;
    if status != 0x00 {
        return Err(Error::InvalidPacket);
    }

    // PrepareOk is 11 bytes (4 + 2 + 2 + 1 + 2)
    if data.len() < 11 {
        return Err(Error::UnexpectedEof);
    }

    // Zero-copy cast using zerocopy
    PrepareOk::ref_from_bytes(&data[..11]).map_err(|_| Error::InvalidPacket)

    // Note: If num_params > 0, server will send param definitions
    // Note: If num_columns > 0, server will send column definitions
    // These are read separately by the caller
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
        params.write_null_bitmap(out);

        // new-params-bound-flag (1 byte)
        if params.send_types_to_server() {
            write_int_1(out, 0x01);
            // Write parameter types
            params.write_types(out);
        } else {
            write_int_1(out, 0x00);
        }

        // Write parameter values
        params.write_values(out)?; // Ignore errors for now (non-priority)
    }
    Ok(())
}

/// Read COM_STMT_EXECUTE response
/// This can be either an OK packet or a result set
pub fn read_execute_response(payload: &[u8]) -> Result<ExecuteResponse<'_>> {
    if payload.is_empty() {
        return Err(Error::InvalidPacket);
    }

    match payload[0] {
        0x00 => Ok(ExecuteResponse::Ok(OkPayloadBytes(payload))),
        0xFF => {
            // Error packet - convert to Error
            Err(ErrPayloadBytes(payload).into())
        }
        _ => {
            // Result set - first byte is column count
            let (column_count, _rest) = read_int_lenenc(payload)?;
            Ok(ExecuteResponse::ResultSet { column_count })
        }
    }
}

/// Execute response variants
#[derive(Debug)]
pub enum ExecuteResponse<'a> {
    Ok(OkPayloadBytes<'a>),
    ResultSet { column_count: u64 },
}

/// Read binary protocol row from execute response
pub fn read_binary_row<'a>(payload: &'a [u8], num_columns: usize) -> Result<RowPayload<'a>> {
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

/// Result of driving the exec_fold state machine
///
/// Returns events that the caller should handle
#[derive(Debug)]
pub enum ExecResult<'a> {
    /// Need more payload data
    NeedPayload,
    /// Execute returned OK (no result set)
    NoResultSet(OkPayloadBytes<'a>),
    ResultSetStart {
        num_columns: usize,
    },
    /// Result set started with column definition packets (raw bytes)
    /// Caller should parse these into ColumnDefinition using read_column_definition
    Column(ColumnDefinitionBytes<'a>),
    /// Row data received
    Row(RowPayload<'a>),
    /// Result set finished with EOF
    Eof(OkPayloadBytes<'a>),
}

/// State machine for exec_fold
///
/// Pure parsing state machine without handler dependencies.
/// Each call to `drive()` can accept a payload with its own independent lifetime.
pub enum Exec {
    /// Waiting for initial execute response
    Start,
    /// Reading column definitions
    ReadingColumns {
        num_columns: usize,
        remaining: usize,
    },
    /// Reading rows
    ReadingRows { num_columns: usize },
    /// Finished
    Finished,
}

impl Exec {
    /// Create a new exec_fold state machine
    pub fn new() -> Self {
        Self::Start
    }

    /// Drive the state machine with the next payload
    ///
    /// # Arguments
    /// * `payload` - The next packet payload to process
    ///
    /// # Returns
    /// * `Ok(ExecFoldResult)` - Event to handle
    /// * `Err(Error)` - An error occurred
    pub fn drive<'a>(&mut self, payload: &'a [u8]) -> Result<ExecResult<'a>> {
        match self {
            Self::Start => {
                // Parse execute response
                let response = read_execute_response(payload)?;

                match response {
                    ExecuteResponse::Ok(ok_bytes) => {
                        // No rows to process
                        *self = Self::Finished;
                        Ok(ExecResult::NoResultSet(ok_bytes))
                    }
                    ExecuteResponse::ResultSet { column_count } => {
                        let num_columns = column_count as usize;
                        *self = Self::ReadingColumns {
                            num_columns,
                            remaining: num_columns,
                        };
                        Ok(ExecResult::NeedPayload)
                    }
                }
            }

            Self::ReadingColumns {
                num_columns,
                remaining,
            } => {
                // Store the raw packet bytes
                *remaining -= 1;

                if *remaining == 0 {
                    *self = Self::ReadingRows {
                        num_columns: *num_columns,
                    };
                }
                Ok(ExecResult::Column(ColumnDefinitionBytes(payload)))
            }

            Self::ReadingRows { num_columns } => {
                match payload[0] {
                    0x00 => {
                        // Row packet
                        let row = read_binary_row(payload, *num_columns)?;
                        Ok(ExecResult::Row(row))
                    }
                    0xFE => {
                        // EOF packet
                        let eof_bytes = OkPayloadBytes(payload);
                        eof_bytes.assert_eof()?;
                        *self = Self::Finished;
                        Ok(ExecResult::Eof(eof_bytes))
                    }
                    _ => Err(Error::InvalidPacket),
                }
            }

            Self::Finished => {
                // Should not receive more data after done
                Err(Error::InvalidPacket)
            }
        }
    }
}
