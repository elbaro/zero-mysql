use crate::PreparedStatement;
use crate::buffer::BufferSet;
use crate::constant::CommandByte;
use crate::error::{Error, Result};
use crate::protocol::command::ColumnDefinitions;
use crate::protocol::command::prepared::read_binary_row;
use crate::protocol::primitive::*;
use crate::protocol::response::{ErrPayloadBytes, OkPayloadBytes};
use crate::protocol::r#trait::BinaryResultSetHandler;
use crate::protocol::r#trait::param::TypedParams;

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BulkFlags: u16 {
        const SEND_UNIT_RESULTS = 64;
        const SEND_TYPES_TO_SERVER = 128;
    }
}

pub trait BulkParamsSet {
    fn encode_types(&self, out: &mut Vec<u8>);
    fn encode_rows(self, out: &mut Vec<u8>) -> Result<()>;
}

impl<P: TypedParams> BulkParamsSet for &[P] {
    fn encode_types(&self, out: &mut Vec<u8>) {
        P::encode_types(out);
    }

    fn encode_rows(self, out: &mut Vec<u8>) -> Result<()> {
        for params in self {
            params.encode_values_for_bulk(out)?;
        }
        Ok(())
    }
}

pub fn write_bulk_execute<P: BulkParamsSet>(
    out: &mut Vec<u8>,
    statement_id: u32,
    params: P,
    flags: BulkFlags,
) -> Result<()> {
    write_int_1(out, CommandByte::StmtBulkExecute as u8);
    write_int_4(out, statement_id);
    write_int_2(out, flags.bits());

    if flags.contains(BulkFlags::SEND_TYPES_TO_SERVER) {
        params.encode_types(out);
    }

    params.encode_rows(out)?;
    Ok(())
}

pub fn read_bulk_execute_response(
    payload: &[u8],
    cache_metadata: bool,
) -> Result<BulkExecuteResponse<'_>> {
    if payload.is_empty() {
        return Err(Error::InvalidPacket);
    }

    match payload[0] {
        0x00 => Ok(BulkExecuteResponse::Ok(OkPayloadBytes(payload))),
        0xFF => Err(ErrPayloadBytes(payload).into()),
        _ => {
            let (column_count, rest) = read_int_lenenc(payload)?;

            // If MARIADB_CLIENT_CACHE_METADATA is set, read the metadata_follows flag
            let has_column_metadata = if cache_metadata {
                if rest.is_empty() {
                    return Err(Error::InvalidPacket);
                }
                rest[0] != 0
            } else {
                // Without caching, metadata always follows
                true
            };

            Ok(BulkExecuteResponse::ResultSet {
                column_count,
                has_column_metadata,
            })
        }
    }
}

#[derive(Debug)]
pub enum BulkExecuteResponse<'a> {
    Ok(OkPayloadBytes<'a>),
    ResultSet {
        column_count: u64,
        has_column_metadata: bool,
    },
}

enum BulkExecState {
    Start,
    ReadingFirstPacket,
    ReadingColumns { num_columns: usize },
    ReadingRows { num_columns: usize },
    Finished,
}

pub struct BulkExec<'h, 'stmt, H> {
    state: BulkExecState,
    handler: &'h mut H,
    stmt: &'stmt mut PreparedStatement,
    cache_metadata: bool,
}

impl<'h, 'stmt, H: BinaryResultSetHandler> BulkExec<'h, 'stmt, H> {
    pub fn new(
        handler: &'h mut H,
        stmt: &'stmt mut PreparedStatement,
        cache_metadata: bool,
    ) -> Self {
        Self {
            state: BulkExecState::Start,
            handler,
            stmt,
            cache_metadata,
        }
    }

    pub fn step<'buf>(
        &mut self,
        buffer_set: &'buf mut BufferSet,
    ) -> Result<crate::protocol::command::Action<'buf>> {
        use crate::protocol::command::Action;
        match &mut self.state {
            BulkExecState::Start => {
                self.state = BulkExecState::ReadingFirstPacket;
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }

            BulkExecState::ReadingFirstPacket => {
                let payload = &buffer_set.read_buffer[..];
                let response = read_bulk_execute_response(payload, self.cache_metadata)?;

                match response {
                    BulkExecuteResponse::Ok(ok_bytes) => {
                        self.handler.no_result_set(ok_bytes)?;
                        self.state = BulkExecState::Finished;
                        Ok(Action::Finished)
                    }
                    BulkExecuteResponse::ResultSet {
                        column_count,
                        has_column_metadata,
                    } => {
                        let num_columns = column_count as usize;

                        if has_column_metadata {
                            // Server sent metadata, signal that we need to read N column packets
                            self.state = BulkExecState::ReadingColumns { num_columns };
                            Ok(Action::ReadColumnMetadata { num_columns })
                        } else {
                            // No metadata from server, use cached definitions
                            if let Some(cache) = self.stmt.column_definitions() {
                                self.handler.resultset_start(cache)?;
                                self.state = BulkExecState::ReadingRows { num_columns };
                                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
                            } else {
                                // No cache available but server didn't send metadata - error
                                Err(Error::InvalidPacket)
                            }
                        }
                    }
                }
            }

            BulkExecState::ReadingColumns { num_columns } => {
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
                self.state = BulkExecState::ReadingRows {
                    num_columns: *num_columns,
                };
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }

            BulkExecState::ReadingRows { num_columns } => {
                let payload = &buffer_set.read_buffer[..];
                match payload[0] {
                    0x00 => {
                        let row = read_binary_row(payload, *num_columns)?;
                        self.handler.row(&row)?;
                        Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
                    }
                    0xFE => {
                        let eof_bytes = OkPayloadBytes(payload);
                        self.handler.resultset_end(eof_bytes)?;
                        self.state = BulkExecState::Finished;
                        Ok(Action::Finished)
                    }
                    _ => Err(Error::InvalidPacket),
                }
            }

            BulkExecState::Finished => Err(Error::InvalidPacket),
        }
    }
}
