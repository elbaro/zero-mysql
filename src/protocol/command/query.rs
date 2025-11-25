use crate::buffer::BufferSet;
use crate::constant::CommandByte;
use crate::error::{Error, Result};
use crate::protocol::TextRowPayload;
use crate::protocol::command::ColumnDefinitionBytes;
use crate::protocol::primitive::*;
use crate::protocol::response::{ErrPayloadBytes, OkPayloadBytes};

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

use crate::protocol::r#trait::TextResultSetHandler;

/// Internal state of the Query state machine
enum QueryState {
    /// Initial state - need to read first packet
    Start,
    /// Reading the first response packet
    ReadingFirstPacket,
    /// Reading column definitions
    ReadingColumns { remaining: usize },
    /// Reading rows
    ReadingRows,
    /// Finished
    Finished,
}

/// State machine for Query (text protocol) with integrated handler
///
/// The handler is provided at construction and called directly by the state machine.
/// The `drive()` method returns actions indicating what I/O operation is needed next.
pub struct Query<'h, H> {
    state: QueryState,
    handler: &'h mut H,
}

impl<'h, H: TextResultSetHandler> Query<'h, H> {
    /// Create a new Query state machine with the given handler
    pub fn new(handler: &'h mut H) -> Self {
        Self {
            state: QueryState::Start,
            handler,
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
            QueryState::Start => {
                // Request the first packet
                self.state = QueryState::ReadingFirstPacket;
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }

            QueryState::ReadingFirstPacket => {
                let payload = &buffer_set.read_buffer[..];
                let response = read_query_response(payload)?;

                match response {
                    QueryResponse::Ok(ok_bytes) => {
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
                            self.state = QueryState::ReadingFirstPacket;
                            Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
                        } else {
                            // No more results, we're done
                            self.state = QueryState::Finished;
                            Ok(Action::Finished)
                        }
                    }
                    QueryResponse::ResultSet { column_count } => {
                        let num_columns = column_count as usize;
                        self.handler.resultset_start(num_columns)?;
                        self.state = QueryState::ReadingColumns {
                            remaining: num_columns,
                        };
                        Ok(Action::NeedPacket(&mut buffer_set.column_definition_buffer))
                    }
                }
            }

            QueryState::ReadingColumns { remaining } => {
                // Read from column_definition_buffer (no copy needed!)
                let payload = &buffer_set.column_definition_buffer[..];
                let col = ColumnDefinitionBytes(payload);
                self.handler.col(col)?;

                *remaining -= 1;

                if *remaining == 0 {
                    self.state = QueryState::ReadingRows;
                    Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
                } else {
                    Ok(Action::NeedPacket(&mut buffer_set.column_definition_buffer))
                }
            }

            QueryState::ReadingRows => {
                let payload = &buffer_set.read_buffer[..];
                // A valid row's first item is NULL (0xFB) or string<lenenc>.
                // string<lenenc> starts with int<lenenc> which cannot start with 0xFF (ErrPacket header).
                // Hence, 0xFF always means Err.
                //
                // Similarly, string<lenenc> starting with 0xFE means that the length of a string is at least 2^24, which means the packet is of the size 2^24.
                // The Ok-Packet for EOF cannot be this long, therefore 0xFE with payload.len() determines the payload length.
                match payload.first() {
                    Some(0xFF) => Err(ErrPayloadBytes(payload))?,
                    Some(0xFE) if payload.len() != MAX_PAYLOAD_LENGTH => {
                        // Parse OK packet to check status flags
                        use crate::constant::ServerStatusFlags;
                        use crate::protocol::response::OkPayload;

                        let ok_bytes = OkPayloadBytes(payload);
                        let ok_payload = OkPayload::try_from(ok_bytes)?;
                        self.handler.resultset_end(ok_bytes)?;

                        // Check if there are more results to come
                        if ok_payload
                            .status_flags
                            .contains(ServerStatusFlags::SERVER_MORE_RESULTS_EXISTS)
                        {
                            // More resultsets coming, go to ReadingFirstPacket to process next result
                            self.state = QueryState::ReadingFirstPacket;
                            Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
                        } else {
                            // No more results, we're done
                            self.state = QueryState::Finished;
                            Ok(Action::Finished)
                        }
                    }
                    _ => {
                        let row = TextRowPayload(payload);
                        self.handler.row(&row)?;
                        Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
                    }
                }
            }

            QueryState::Finished => Err(Error::InvalidPacket),
        }
    }
}
