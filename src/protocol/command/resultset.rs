use crate::error::Result;
use crate::protocol::BinaryRowPayload;
use crate::protocol::primitive::*;
use crate::protocol::value::NullBitmap;

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

/// Read binary protocol row or EOF
/// Returns None if this is an EOF packet
pub fn read_binary_row<'a>(payload: &'a [u8], num_columns: usize) -> Result<BinaryRowPayload<'a>> {
    // Binary protocol row packet starts with 0x00
    let (header, mut data) = read_int_1(payload)?;
    debug_assert_eq!(header, 0x00);

    // NULL bitmap: (num_columns + 7 + 2) / 8 bytes
    // The +2 offset is for binary protocol
    let null_bitmap_len = (num_columns + 7 + 2) >> 3;
    let (null_bitmap, rest) = read_string_fix(data, null_bitmap_len)?;
    data = rest;

    // Remaining data is the values
    Ok(BinaryRowPayload::new(
        NullBitmap::for_result_set(null_bitmap),
        data,
        num_columns,
    ))
}
