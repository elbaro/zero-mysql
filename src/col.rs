use crate::constant::{ColumnFlags, ColumnType};

/// Column definition from MySQL protocol
#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub org_table: String,
    pub name: String,
    pub org_name: String,
    pub charset: u16,
    pub column_length: u32,
    pub column_type: ColumnType,
    pub flags: ColumnFlags,
    pub decimals: u8,
}

impl ColumnDefinition {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog: String,
        schema: String,
        table: String,
        org_table: String,
        name: String,
        org_name: String,
        charset: u16,
        column_length: u32,
        column_type: ColumnType,
        flags: ColumnFlags,
        decimals: u8,
    ) -> Self {
        Self {
            catalog,
            schema,
            table,
            org_table,
            name,
            org_name,
            charset,
            column_length,
            column_type,
            flags,
            decimals,
        }
    }
}
