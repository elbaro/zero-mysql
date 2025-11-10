use crate::constant::CommandByte;
use crate::error::{Error, Result};
use crate::protocol::primitive::*;
use crate::protocol::r#trait::params::Params;
use crate::protocol::response::{read_ok_packet, OkPacket};
use crate::row::Row;
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
    PrepareOk::ref_from_bytes(&data[..11])
        .map_err(|_| Error::InvalidPacket)

    // Note: If num_params > 0, server will send param definitions
    // Note: If num_columns > 0, server will send column definitions
    // These are read separately by the caller
}

/// Write COM_STMT_EXECUTE command
pub fn write_execute<P: Params>(out: &mut Vec<u8>, statement_id: u32, params: &P) {
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
        params.write_values(out).ok(); // Ignore errors for now (non-priority)
    }
}

/// Read COM_STMT_EXECUTE response
/// This can be either an OK packet or a result set
pub fn read_execute_response(payload: &[u8]) -> Result<ExecuteResponse> {
    if payload.is_empty() {
        return Err(Error::InvalidPacket);
    }

    match payload[0] {
        0x00 => {
            let ok = read_ok_packet(payload)?;
            Ok(ExecuteResponse::Ok(ok))
        }
        0xFF => {
            // Error packet - convert to Error
            let err = crate::protocol::response::read_err_packet(payload)?;
            Err(Error::ServerError {
                error_code: err.error_code,
                sql_state: err.sql_state,
                message: err.message,
            })
        }
        _ => {
            // Result set - first byte is column count
            let (column_count, _rest) = read_int_lenenc(payload)?;
            Ok(ExecuteResponse::ResultSet { column_count })
        }
    }
}

/// Execute response variants
#[derive(Debug, Clone)]
pub enum ExecuteResponse {
    Ok(OkPacket),
    ResultSet { column_count: u64 },
}

/// Read binary protocol row from execute response
pub fn read_binary_row<'a>(payload: &'a [u8], num_columns: usize) -> Result<Option<Row<'a>>> {
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
