use std::sync::Arc;

use diesel::backend::Backend;
use diesel::mysql::{Mysql, MysqlValue};
use diesel::row::{Field, PartialRow, Row, RowIndex, RowSealed};

use super::cursor::ColumnInfo;

#[expect(clippy::field_scoped_visibility_modifiers)]
pub struct ZeroMysqlRow {
    pub(in crate::diesel) columns: Arc<[ColumnInfo]>,
    pub(in crate::diesel) values: Vec<Option<Vec<u8>>>,
}

impl RowSealed for ZeroMysqlRow {}

impl<'a> Row<'a, Mysql> for ZeroMysqlRow {
    type Field<'f>
        = ZeroMysqlField<'f>
    where
        'a: 'f,
        Self: 'f;
    type InnerPartialRow = Self;

    fn field_count(&self) -> usize {
        self.columns.len()
    }

    fn get<'b, I>(&'b self, idx: I) -> Option<Self::Field<'b>>
    where
        'a: 'b,
        Self: RowIndex<I>,
    {
        let idx = self.idx(idx)?;
        Some(ZeroMysqlField {
            col_info: &self.columns[idx],
            value: self.values[idx].as_deref(),
        })
    }

    fn partial_row(&self, range: std::ops::Range<usize>) -> PartialRow<'_, Self::InnerPartialRow> {
        PartialRow::new(self, range)
    }
}

impl RowIndex<usize> for ZeroMysqlRow {
    fn idx(&self, idx: usize) -> Option<usize> {
        (idx < self.columns.len()).then_some(idx)
    }
}

impl<'a> RowIndex<&'a str> for ZeroMysqlRow {
    fn idx(&self, idx: &'a str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == idx)
    }
}

pub struct ZeroMysqlField<'a> {
    col_info: &'a ColumnInfo,
    value: Option<&'a [u8]>,
}

impl<'a> Field<'a, Mysql> for ZeroMysqlField<'a> {
    fn field_name(&self) -> Option<&str> {
        Some(&self.col_info.name)
    }

    fn value(&self) -> Option<<Mysql as Backend>::RawValue<'_>> {
        self.value
            .map(|raw| MysqlValue::new(raw, self.col_info.tpe))
    }

    fn is_null(&self) -> bool {
        self.value.is_none()
    }
}
