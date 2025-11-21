pub mod param;
pub mod params;

use crate::error::Result;
use crate::protocol::connection::ColumnDefinitionBytes;
use crate::protocol::response::OkPayloadBytes;
use crate::protocol::{BinaryRowPayload, TextRowPayload};

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
    fn decode_row(&mut self, row: BinaryRowPayload<'a>) -> Result<Self::Output>;
}

/// Trait that defines event callbacks for binary protocol result sets
pub trait BinaryResultSetHandler<'a> {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()>;
    // fn err(&mut self, err: &ErrPayload) -> Result<()>;
    fn resultset_start(&mut self, num_columns: usize) -> Result<()>;
    fn col(&mut self, col: ColumnDefinitionBytes) -> Result<()>;
    fn row(&mut self, row: &BinaryRowPayload) -> Result<()>;
    fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()>;
}

/// Trait that defines event callbacks for text protocol result sets
pub trait TextResultSetHandler<'a> {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()>;
    fn resultset_start(&mut self, num_columns: usize) -> Result<()>;
    fn col(&mut self, col: ColumnDefinitionBytes) -> Result<()>;
    fn row(&mut self, row: &TextRowPayload) -> Result<()>;
    fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()>;
}
