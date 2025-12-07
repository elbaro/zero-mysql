use crate::error::Result;
use crate::protocol::command::ColumnDefinition;
use crate::protocol::r#trait::{BinaryResultSetHandler, TextResultSetHandler};
use crate::protocol::response::{OkPayload, OkPayloadBytes};
use crate::protocol::{BinaryRowPayload, TextRowPayload};

/// A handler that ignores all result set data but captures affected_rows and last_insert_id
///
/// Useful for `exec_drop()` and `query_drop()` methods that discard results but need metadata.
#[derive(Default)]
pub struct DropHandler {
    affected_rows: u64,
    last_insert_id: u64,
}

impl DropHandler {
    /// Get the number of affected rows from the last operation
    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    /// Get the last insert ID from the last operation
    pub fn last_insert_id(&self) -> u64 {
        self.last_insert_id
    }
}

impl BinaryResultSetHandler for DropHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let payload = OkPayload::try_from(ok)?;
        self.affected_rows = payload.affected_rows;
        self.last_insert_id = payload.last_insert_id;
        Ok(())
    }

    fn resultset_start(&mut self, _: &[ColumnDefinition<'_>]) -> Result<()> {
        Ok(())
    }

    fn row(&mut self, _: &[ColumnDefinition<'_>], _: BinaryRowPayload<'_>) -> Result<()> {
        Ok(())
    }

    fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()> {
        let payload = OkPayload::try_from(eof)?;
        self.affected_rows = payload.affected_rows;
        self.last_insert_id = payload.last_insert_id;
        Ok(())
    }
}

impl TextResultSetHandler for DropHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let payload = OkPayload::try_from(ok)?;
        self.affected_rows = payload.affected_rows;
        self.last_insert_id = payload.last_insert_id;
        Ok(())
    }

    fn resultset_start(&mut self, _: &[ColumnDefinition<'_>]) -> Result<()> {
        Ok(())
    }

    fn row(&mut self, _: &[ColumnDefinition<'_>], _: TextRowPayload<'_>) -> Result<()> {
        Ok(())
    }

    fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()> {
        let payload = OkPayload::try_from(eof)?;
        self.affected_rows = payload.affected_rows;
        self.last_insert_id = payload.last_insert_id;
        Ok(())
    }
}

/// A wrapper handler that forwards calls to an inner handler but stops after the first row
///
/// Useful for `exec_first()` methods that only process the first row.
pub struct FirstRowHandler<'a, H> {
    pub inner: &'a mut H,
    pub found_row: bool,
}

impl<'a, H> FirstRowHandler<'a, H> {
    pub fn new(inner: &'a mut H) -> Self {
        Self {
            inner,
            found_row: false,
        }
    }
}

impl<'a, H: BinaryResultSetHandler> BinaryResultSetHandler for FirstRowHandler<'a, H> {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        self.inner.no_result_set(ok)
    }

    fn resultset_start(&mut self, cols: &[ColumnDefinition<'_>]) -> Result<()> {
        self.inner.resultset_start(cols)
    }

    fn row(&mut self, cols: &[ColumnDefinition<'_>], row: BinaryRowPayload<'_>) -> Result<()> {
        if !self.found_row {
            self.found_row = true;
            self.inner.row(cols, row)
        } else {
            Ok(()) // Ignore subsequent rows
        }
    }

    fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()> {
        self.inner.resultset_end(eof)
    }
}
