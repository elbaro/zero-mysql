pub mod param;

use crate::error::Result;
use crate::protocol::command::ColumnDefinition;
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
pub trait BinaryResultSetHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()>;
    fn resultset_start(&mut self, num_columns: usize) -> Result<()>;
    fn row<'a>(&mut self, cols: &[ColumnDefinition<'a>], row: &'a BinaryRowPayload<'a>) -> Result<()>;
    fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()>;
}

/// Trait that defines event callbacks for text protocol result sets
///
/// The lifetime parameter `'buffers` is bound to the BufferSet, allowing handlers
/// to store references to column definitions without cloning.
pub trait TextResultSetHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()>;
    fn resultset_start(&mut self, num_columns: usize) -> Result<()>;
    fn col<'buffers>(&mut self, col: &ColumnDefinition<'buffers>) -> Result<()>;
    fn row(&mut self, row: &TextRowPayload) -> Result<()>;
    fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()>;
}
