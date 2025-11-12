use crate::constant::CommandByte;
use crate::error::Result;
use crate::protocol::primitive::*;
use crate::row::RowPayload;

/// Write COM_QUERY command
pub fn write_query(out: &mut Vec<u8>, sql: &str) {
    write_int_1(out, CommandByte::Query as u8);
    out.extend_from_slice(sql.as_bytes());
}

/// Result set metadata header
#[derive(Debug, Clone)]
pub struct ResultSetHeader {
    pub column_count: u64,
}

/// Read text protocol result set header (column count)
pub fn read_text_resultset_header(payload: &[u8]) -> Result<ResultSetHeader> {
    let (column_count, _rest) = read_int_lenenc(payload)?;
    Ok(ResultSetHeader { column_count })
}

/// Read a text protocol row
/// Returns None if this is an EOF packet
pub fn read_text_row<'a>(payload: &'a [u8], num_columns: usize) -> Result<Option<RowPayload<'a>>> {
    // Check for EOF packet (0xFE and length < 9)
    if !payload.is_empty() && payload[0] == 0xFE && payload.len() < 9 {
        return Ok(None);
    }

    // Text protocol doesn't have null bitmap at the start
    // Values are length-encoded strings, NULL is encoded as 0xFB
    // We just return the raw payload for external parsing
    // Ok(Some(RowPayload::new(&[], payload, num_columns)))
    todo!()
}
