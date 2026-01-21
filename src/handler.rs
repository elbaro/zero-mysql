use crate::error::Result;
use crate::protocol::command::ColumnDefinition;
use crate::protocol::response::{OkPayload, OkPayloadBytes};
use crate::protocol::r#trait::{BinaryResultSetHandler, TextResultSetHandler};
use crate::protocol::{BinaryRowPayload, TextRowPayload};
use crate::raw::FromRow;
use smart_default::SmartDefault;

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

/// A handler that stores only the first row.
///
/// Useful for `exec_first()` methods that return `Option<Row>`.
#[derive(SmartDefault)]
pub struct FirstHandler<Row> {
    row: Option<Row>,
}

impl<Row> FirstHandler<Row> {
    /// Take the stored row, if any.
    pub fn take(&mut self) -> Option<Row> {
        self.row.take()
    }
}

impl<Row: for<'buf> FromRow<'buf>> BinaryResultSetHandler for FirstHandler<Row> {
    fn no_result_set(&mut self, _ok: OkPayloadBytes) -> Result<()> {
        Ok(())
    }

    fn resultset_start(&mut self, _cols: &[ColumnDefinition<'_>]) -> Result<()> {
        Ok(())
    }

    fn row(&mut self, cols: &[ColumnDefinition<'_>], row: BinaryRowPayload<'_>) -> Result<()> {
        if self.row.is_none() {
            self.row = Some(Row::from_row(cols, row)?);
        }
        Ok(())
    }

    fn resultset_end(&mut self, _eof: OkPayloadBytes) -> Result<()> {
        Ok(())
    }
}

/// A handler that collects all rows into a Vec<Row>
///
/// Useful for `exec()` methods that need to return all rows as a collection.
#[derive(SmartDefault)]
pub struct CollectHandler<Row> {
    rows: Vec<Row>,
    affected_rows: u64,
    last_insert_id: u64,
}

impl<Row> CollectHandler<Row> {
    pub fn take_rows(&mut self) -> Vec<Row> {
        std::mem::take(&mut self.rows)
    }
    pub fn into_rows(self) -> Vec<Row> {
        self.rows
    }
    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }
    pub fn last_insert_id(&self) -> u64 {
        self.last_insert_id
    }
}

impl<Row: for<'buf> FromRow<'buf>> BinaryResultSetHandler for CollectHandler<Row> {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let payload = OkPayload::try_from(ok)?;
        self.affected_rows = payload.affected_rows;
        self.last_insert_id = payload.last_insert_id;
        Ok(())
    }

    fn resultset_start(&mut self, _cols: &[ColumnDefinition<'_>]) -> Result<()> {
        Ok(())
    }

    fn row(&mut self, cols: &[ColumnDefinition], row: BinaryRowPayload) -> Result<()> {
        self.rows.push(Row::from_row(cols, row)?);
        Ok(())
    }

    fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()> {
        let payload = OkPayload::try_from(eof)?;
        self.affected_rows = payload.affected_rows;
        self.last_insert_id = payload.last_insert_id;
        Ok(())
    }
}

/// A handler that calls a closure for each row.
///
/// Useful for `exec_foreach()` methods that process rows without collecting.
pub struct ForEachHandler<Row, F> {
    f: F,
    _marker: std::marker::PhantomData<Row>,
}

impl<Row, F> ForEachHandler<Row, F> {
    pub fn new(f: F) -> Self {
        Self {
            f,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Row, F> BinaryResultSetHandler for ForEachHandler<Row, F>
where
    Row: for<'buf> FromRow<'buf>,
    F: FnMut(Row) -> Result<()>,
{
    fn no_result_set(&mut self, _ok: OkPayloadBytes) -> Result<()> {
        Ok(())
    }

    fn resultset_start(&mut self, _cols: &[ColumnDefinition<'_>]) -> Result<()> {
        Ok(())
    }

    fn row(&mut self, cols: &[ColumnDefinition], row: BinaryRowPayload) -> Result<()> {
        let parsed = Row::from_row(cols, row)?;
        (self.f)(parsed)
    }

    fn resultset_end(&mut self, _eof: OkPayloadBytes) -> Result<()> {
        Ok(())
    }
}
