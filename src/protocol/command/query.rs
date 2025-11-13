use crate::col::ColumnDefinitionBytes;
use crate::constant::CommandByte;
use crate::error::{Error, Result};
use crate::protocol::packet::{ErrPayloadBytes, OkPayloadBytes};
use crate::protocol::primitive::*;
use crate::row::TextRowPayload;

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
        0xFF => {
            // Error packet - convert to Error
            Err(ErrPayloadBytes(payload).into())
        }
        0x00 => {
            // OK packet - query succeeded without result set
            let ok_bytes =
                OkPayloadBytes::try_from_payload(payload).ok_or(Error::InvalidPacket)?;
            Ok(QueryResponse::Ok(ok_bytes))
        }
        0xFB => {
            // LOCAL INFILE packet - not yet supported
            Err(Error::BadInputError(
                "LOCAL INFILE queries are not yet supported".to_string(),
            ))
        }
        _ => {
            // Result set - first byte is column count (length-encoded integer)
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

/// Read text protocol row from query response
pub fn read_text_row<'a>(payload: &'a [u8], num_columns: usize) -> Result<TextRowPayload<'a>> {
    Ok(TextRowPayload {
        data: payload,
        num_columns,
    })
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
pub enum Query {
    /// Waiting for initial query response
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

impl Query {
    /// Create a new query state machine
    pub fn new() -> Self {
        Self::Start
    }

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
                // Parse query response
                let response = read_query_response(payload)?;

                match response {
                    QueryResponse::Ok(ok_bytes) => {
                        // No rows to process
                        *self = Self::Finished;
                        Ok(QueryResult::NoResultSet(ok_bytes))
                    }
                    QueryResponse::ResultSet { column_count } => {
                        let num_columns = column_count as usize;
                        *self = Self::ReadingColumns {
                            num_columns,
                            remaining: num_columns,
                        };
                        Ok(QueryResult::NeedPayload)
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
                Ok(QueryResult::Column(ColumnDefinitionBytes(payload)))
            }

            Self::ReadingRows { num_columns } => {
                // After all rows, the server sends an end-of-result-set packet:
                // - With CLIENT_DEPRECATE_EOF: OK packet (0x00 or 0xFE)
                // - Without CLIENT_DEPRECATE_EOF: EOF packet (0xFE, length < 9)
                //
                // Challenge: Text protocol rows are length-encoded strings that can start
                // with ANY byte value, including 0x00 or 0xFE. We must distinguish:
                // - OK/EOF packets (structured packets marking end of result set)
                // - Text row data (raw column values as length-encoded strings)
                //
                // Solution: Check if payload[0] is 0x00 or 0xFE, then try to parse as
                // OK/EOF packet. If parsing succeeds, it's end of result set. Otherwise,
                // it's a text row that happens to start with those bytes.
                match payload[0] {
                    0x00 | 0xFE => {
                        // Might be OK/EOF packet or a text row starting with these bytes
                        // Try to parse as OK/EOF packet - if it succeeds, we're done
                        if let Some(ok_bytes) = OkPayloadBytes::try_from_payload(payload) {
                            *self = Self::Finished;
                            Ok(QueryResult::Eof(ok_bytes))
                        } else {
                            // Not a valid OK/EOF packet, must be a text row
                            let row = read_text_row(payload, *num_columns)?;
                            Ok(QueryResult::Row(row))
                        }
                    }
                    0xFF => {
                        // Error during result set (unlikely but possible)
                        Err(ErrPayloadBytes(payload).into())
                    }
                    _ => {
                        // Text protocol row (doesn't start with 0x00, 0xFE, or 0xFF)
                        let row = read_text_row(payload, *num_columns)?;
                        Ok(QueryResult::Row(row))
                    }
                }
            }

            Self::Finished => {
                // Should not receive more data after done
                Err(Error::InvalidPacket)
            }
        }
    }
}
