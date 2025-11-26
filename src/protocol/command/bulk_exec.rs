use crate::buffer::BufferSet;
use crate::constant::CommandByte;
use crate::error::{Error, Result};
use crate::protocol::command::ColumnDefinitionBytes;
use crate::protocol::primitive::*;
use crate::protocol::response::{ErrPayloadBytes, OkPayloadBytes};
use crate::protocol::r#trait::BinaryResultSetHandler;

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BulkFlags: u16 {
        const SEND_UNIT_RESULTS = 64;
        const SEND_TYPES_TO_SERVER = 128;
    }
}

pub trait BulkParams {
    fn num_params(&self) -> usize;
    fn num_rows(&self) -> usize;
    fn encode_types(&self, out: &mut Vec<u8>);
    fn encode_rows(&self, out: &mut Vec<u8>) -> Result<()>;
}

pub fn write_bulk_execute<P: BulkParams>(
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

pub fn read_bulk_execute_response(payload: &[u8]) -> Result<BulkExecuteResponse<'_>> {
    if payload.is_empty() {
        return Err(Error::InvalidPacket);
    }

    match payload[0] {
        0x00 => Ok(BulkExecuteResponse::Ok(OkPayloadBytes(payload))),
        0xFF => Err(ErrPayloadBytes(payload).into()),
        _ => {
            let (column_count, _rest) = read_int_lenenc(payload)?;
            Ok(BulkExecuteResponse::ResultSet { column_count })
        }
    }
}

#[derive(Debug)]
pub enum BulkExecuteResponse<'a> {
    Ok(OkPayloadBytes<'a>),
    ResultSet { column_count: u64 },
}

enum BulkExecState {
    Start,
    ReadingFirstPacket,
    ReadingColumns {
        num_columns: usize,
        remaining: usize,
    },
    ReadingRows {
        num_columns: usize,
    },
    Finished,
}

pub struct BulkExec<'h, H> {
    state: BulkExecState,
    handler: &'h mut H,
}

impl<'h, H: BinaryResultSetHandler> BulkExec<'h, H> {
    pub fn new(handler: &'h mut H) -> Self {
        Self {
            state: BulkExecState::Start,
            handler,
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
                let response = read_bulk_execute_response(payload)?;

                match response {
                    BulkExecuteResponse::Ok(ok_bytes) => {
                        use crate::constant::ServerStatusFlags;
                        use crate::protocol::response::OkPayload;

                        let ok_payload = OkPayload::try_from(ok_bytes)?;
                        self.handler.no_result_set(ok_bytes)?;

                        if ok_payload
                            .status_flags
                            .contains(ServerStatusFlags::SERVER_MORE_RESULTS_EXISTS)
                        {
                            self.state = BulkExecState::ReadingFirstPacket;
                            Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
                        } else {
                            self.state = BulkExecState::Finished;
                            Ok(Action::Finished)
                        }
                    }
                    BulkExecuteResponse::ResultSet { column_count } => {
                        let num_columns = column_count as usize;
                        self.handler.resultset_start(num_columns)?;
                        self.state = BulkExecState::ReadingColumns {
                            num_columns,
                            remaining: num_columns,
                        };
                        Ok(Action::NeedPacket(&mut buffer_set.column_definition_buffer))
                    }
                }
            }

            BulkExecState::ReadingColumns {
                num_columns,
                remaining,
            } => {
                let payload = &buffer_set.column_definition_buffer[..];
                let col = ColumnDefinitionBytes(payload);
                self.handler.col(col)?;

                *remaining -= 1;

                if *remaining == 0 {
                    self.state = BulkExecState::ReadingRows {
                        num_columns: *num_columns,
                    };
                    Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
                } else {
                    Ok(Action::NeedPacket(&mut buffer_set.column_definition_buffer))
                }
            }

            BulkExecState::ReadingRows { num_columns } => {
                let payload = &buffer_set.read_buffer[..];
                match payload[0] {
                    0x00 => {
                        use crate::protocol::command::prepared::read_binary_row;
                        let row = read_binary_row(payload, *num_columns)?;
                        self.handler.row(&row)?;
                        Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
                    }
                    0xFE => {
                        use crate::constant::ServerStatusFlags;
                        use crate::protocol::response::OkPayload;

                        let eof_bytes = OkPayloadBytes(payload);
                        eof_bytes.assert_eof()?;
                        let ok_payload = OkPayload::try_from(eof_bytes)?;
                        self.handler.resultset_end(eof_bytes)?;

                        if ok_payload
                            .status_flags
                            .contains(ServerStatusFlags::SERVER_MORE_RESULTS_EXISTS)
                        {
                            self.state = BulkExecState::ReadingFirstPacket;
                            Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
                        } else {
                            self.state = BulkExecState::Finished;
                            Ok(Action::Finished)
                        }
                    }
                    _ => Err(Error::InvalidPacket),
                }
            }

            BulkExecState::Finished => Err(Error::InvalidPacket),
        }
    }
}
