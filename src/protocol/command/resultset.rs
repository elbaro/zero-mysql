use crate::col::ColumnDefinition;
use crate::constant::{ColumnFlags, ColumnType};
use crate::error::{Error, Result};
use crate::protocol::primitive::*;
use crate::row::RowPayload;

/// Result set metadata header
#[derive(Debug, Clone)]
pub struct ResultSetHeader {
    pub column_count: u64,
}

/// Read binary protocol result set header (column count)
pub fn read_binary_resultset_header(payload: &[u8]) -> Result<ResultSetHeader> {
    let (column_count, _rest) = read_int_lenenc(payload)?;
    Ok(ResultSetHeader { column_count })
}

/// Read column definition packet (Protocol::ColumnDefinition41)
pub fn read_column_definition(payload: &[u8]) -> Result<ColumnDefinition> {
    let mut data = payload;

    // catalog (length-encoded string)
    let (catalog_bytes, rest) = read_string_lenenc(data)?;
    let catalog = String::from_utf8_lossy(catalog_bytes).to_string();
    data = rest;

    // schema (length-encoded string)
    let (schema_bytes, rest) = read_string_lenenc(data)?;
    let schema = String::from_utf8_lossy(schema_bytes).to_string();
    data = rest;

    // table (length-encoded string)
    let (table_bytes, rest) = read_string_lenenc(data)?;
    let table = String::from_utf8_lossy(table_bytes).to_string();
    data = rest;

    // org_table (length-encoded string)
    let (org_table_bytes, rest) = read_string_lenenc(data)?;
    let org_table = String::from_utf8_lossy(org_table_bytes).to_string();
    data = rest;

    // name (length-encoded string)
    let (name_bytes, rest) = read_string_lenenc(data)?;
    let name = String::from_utf8_lossy(name_bytes).to_string();
    data = rest;

    // org_name (length-encoded string)
    let (org_name_bytes, rest) = read_string_lenenc(data)?;
    let org_name = String::from_utf8_lossy(org_name_bytes).to_string();
    data = rest;

    // length of fixed-length fields (0x0c = 12)
    let (_fixed_len, rest) = read_int_lenenc(data)?;
    data = rest;

    // character set (2 bytes)
    let (charset, rest) = read_int_2(data)?;
    data = rest;

    // column length (4 bytes)
    let (column_length, rest) = read_int_4(data)?;
    data = rest;

    // column type (1 byte)
    let (type_byte, rest) = read_int_1(data)?;
    data = rest;

    let column_type = ColumnType::from_u8(type_byte).ok_or_else(|| {
        Error::UnknownProtocolError(format!("Unknown column type: {}", type_byte))
    })?;

    // flags (2 bytes)
    let (flags, rest) = read_int_2(data)?;
    data = rest;

    // decimals (1 byte)
    let (decimals, _rest) = read_int_1(data)?;

    Ok(ColumnDefinition::new(
        catalog,
        schema,
        table,
        org_table,
        name,
        org_name,
        charset,
        column_length,
        column_type,
        ColumnFlags::from_bits_truncate(flags),
        decimals,
    ))
}

/// Read binary protocol row or EOF
/// Returns None if this is an EOF packet
pub fn read_binary_row<'a>(payload: &'a [u8], num_columns: usize) -> Result<RowPayload<'a>> {
    // Binary protocol row packet starts with 0x00
    let (header, mut data) = read_int_1(payload)?;
    if header != 0x00 {
        return Err(Error::InvalidPacket);
    }

    // NULL bitmap: (num_columns + 7 + 2) / 8 bytes
    // The +2 offset is for binary protocol
    let null_bitmap_len = (num_columns + 7 + 2) / 8;
    let (null_bitmap, rest) = read_string_fix(data, null_bitmap_len)?;
    data = rest;

    // Remaining data is the values
    Ok(RowPayload::new(null_bitmap, data, num_columns))
}
