pub mod params;

use crate::col::ColumnDefinition;
use crate::error::Result;
use crate::protocol::packet::OkPayloadBytes;
use crate::row::RowPayload;

/// Trait for decoding a single row from raw bytes
///
/// Implementations can maintain state and decode rows into their own structures
pub trait RowDecoder<'a> {
    /// The output type produced by decoding a row
    type Output;

    /// Decode a single row from byte slice
    ///
    /// # Arguments
    /// * `row` - The raw row data to decode
    ///
    /// # Returns
    /// * `Ok(Self::Output)` - Successfully decoded row
    /// * `Err(Error)` - Decoding failed
    fn decode_row(&mut self, row: RowPayload<'a>) -> Result<Self::Output>;
}

/// Trait that defines event callbacks
pub trait ResultSetHandler<'a> {
    fn ok(&mut self, ok: OkPayloadBytes) -> Result<()>;
    // fn err(&mut self, err: &ErrPayload) -> Result<()>;
    fn start(&mut self, column_count: usize, column_defs: &[ColumnDefinition]) -> Result<()>;
    fn row(&mut self, row: &RowPayload) -> Result<()>;
    fn finish(&mut self, eof: &OkPayloadBytes) -> Result<()>;
}
