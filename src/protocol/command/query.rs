use crate::constant::CommandByte;
use crate::error::{Error, Result};
use crate::protocol::TextRowPayload;
use crate::protocol::connection::ColumnDefinitionBytes;
use crate::protocol::packet::{ErrPayloadBytes, OkPayloadBytes};
use crate::protocol::primitive::*;

const MAX_PAYLOAD_LENGTH: usize = (1 << 24) - 4;

/// Write COM_QUERY command
pub fn write_query(out: &mut Vec<u8>, sql: &str) {
    write_int_1(out, CommandByte::Query as u8);
    out.extend_from_slice(sql.as_bytes());
}

/// Read COM_QUERY response
/// This can be:
/// - 0xFF: ERR packet (error occurred)
/// - 0x00: OK packet (query succeeded without result set)
/// - 0xFB: LOCAL INFILE packet (not yet supported)
/// - Otherwise: Result set (first byte is column count as length-encoded integer)
pub fn read_query_response(payload: &[u8]) -> Result<QueryResponse<'_>> {
    if payload.is_empty() {
        return Err(Error::InvalidPacket);
    }

    match payload[0] {
        0xFF => Err(ErrPayloadBytes(payload).into()),
        0x00 => Ok(QueryResponse::Ok(OkPayloadBytes(payload))),
        0xFB => Err(Error::BadConfigError(
            "LOCAL INFILE queries are not yet supported".to_string(),
        )),
        _ => {
            let (column_count, _rest) = read_int_lenenc(payload)?;
            Ok(QueryResponse::ResultSet { column_count })
        }
    }
}

/// Query response variants
#[derive(Debug)]
pub enum QueryResponse<'a> {
    Ok(OkPayloadBytes<'a>),
    ResultSet { column_count: u64 },
}

// ============================================================================
// State Machine API for Query
// ============================================================================

/// Result of driving the query state machine
///
/// Returns events that the caller should handle
#[derive(Debug)]
pub enum QueryResult<'a> {
    /// Need more payload data
    NeedPayload,
    /// Query returned OK (no result set)
    NoResultSet(OkPayloadBytes<'a>),
    /// Result set started with column count
    ResultSetStart { num_columns: usize },
    /// Column definition packet received
    Column(ColumnDefinitionBytes<'a>),
    /// Row data received
    Row(TextRowPayload<'a>),
    /// Result set finished with EOF
    Eof(OkPayloadBytes<'a>),
}

/// State machine for Query (text protocol)
///
/// Pure parsing state machine without handler dependencies.
/// Each call to `drive()` can accept a payload with its own independent lifetime.
#[derive(Default)]
pub enum Query {
    /// Waiting for initial query response
    #[default]
    Start,
    /// Reading column definitions
    ReadingColumns { remaining: usize },
    /// Reading rows
    ReadingRows,
    /// Finished
    Finished,
}

impl Query {
    /// Drive the state machine with the next payload
    ///
    /// # Arguments
    /// * `payload` - The next packet payload to process
    ///
    /// # Returns
    /// * `Ok(QueryResult)` - Event to handle
    /// * `Err(Error)` - An error occurred
    pub fn drive<'a>(&mut self, payload: &'a [u8]) -> Result<QueryResult<'a>> {
        match self {
            Self::Start => {
                let response = read_query_response(payload)?;

                match response {
                    QueryResponse::Ok(ok_bytes) => {
                        *self = Self::Finished;
                        Ok(QueryResult::NoResultSet(ok_bytes))
                    }
                    QueryResponse::ResultSet { column_count } => {
                        let num_columns = column_count as usize;
                        *self = Self::ReadingColumns {
                            remaining: num_columns,
                        };
                        Ok(QueryResult::NeedPayload)
                    }
                }
            }

            Self::ReadingColumns { remaining } => {
                *remaining -= 1;

                if *remaining == 0 {
                    *self = Self::ReadingRows;
                }
                Ok(QueryResult::Column(ColumnDefinitionBytes(payload)))
            }

            Self::ReadingRows => {
                // A valid row's first item is NULL (0xFB) or string<lenenc>.
                // string<lenenc> starts with int<lenenc> which cannot start with 0xFF (ErrPacket header).
                // Hence, 0xFF always means Err.
                //
                // Similarly, string<lenenc> starting with 0xFE means that the length of a string is at least 2^24, which means the packet is of the size 2^24.
                // The Ok-Packet for EOF cannot be this long, therefore 0xFE with payload.len() determines the payload length.
                match payload.first() {
                    Some(0xFF) => Err(ErrPayloadBytes(payload))?,
                    Some(0xFE) if payload.len() != MAX_PAYLOAD_LENGTH => {
                        *self = Self::Finished;
                        Ok(QueryResult::Eof(OkPayloadBytes(payload)))
                    }
                    _ => Ok(QueryResult::Row(TextRowPayload(payload))),
                }
            }

            Self::Finished => Err(Error::InvalidPacket),
        }
    }
}
