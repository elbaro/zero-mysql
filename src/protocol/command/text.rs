use crate::constant::CommandByte;
use crate::error::Result;
use crate::protocol::primitive::*;

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
