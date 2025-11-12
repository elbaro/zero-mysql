use crate::protocol::value::NullBitmap;

/// Zero-copy row structure that wraps raw bytes from MySQL binary or text protocol.
/// The actual parsing is delegated to external libraries.
#[derive(Debug, Clone)]
pub struct RowPayload<'a> {
    /// NULL bitmap (binary protocol only)
    pub(crate) null_bitmap: NullBitmap<'a>,
    /// Raw value bytes
    pub(crate) values: &'a [u8],
    /// Number of columns in this row
    pub(crate) num_columns: usize,
}

impl<'a> RowPayload<'a> {
    /// Get the NULL bitmap bytes (binary protocol)
    pub fn null_bitmap(&self) -> NullBitmap<'_> {
        self.null_bitmap
    }

    /// Get the raw values bytes (external library parses this)
    pub fn values(&self) -> &[u8] {
        self.values
    }

    /// Get number of columns
    pub fn num_columns(&self) -> usize {
        self.num_columns
    }
}
