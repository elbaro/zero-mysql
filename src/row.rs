/// Zero-copy row structure that wraps raw bytes from MySQL binary or text protocol.
/// The actual parsing is delegated to external libraries.
#[derive(Debug, Clone)]
pub struct RowPayload<'a> {
    /// NULL bitmap (binary protocol only)
    pub(crate) null_bitmap: &'a [u8],
    /// Raw value bytes
    pub(crate) values: &'a [u8],
    /// Number of columns in this row
    pub(crate) num_columns: usize,
}

impl<'a> RowPayload<'a> {
    /// Create a new Row from raw components
    pub fn new(null_bitmap: &'a [u8], values: &'a [u8], num_columns: usize) -> Self {
        Self {
            null_bitmap,
            values,
            num_columns,
        }
    }

    /// Get the raw values bytes (external library parses this)
    pub fn raw_values(&self) -> &[u8] {
        self.values
    }

    /// Get the NULL bitmap bytes (binary protocol)
    pub fn null_bitmap(&self) -> &[u8] {
        self.null_bitmap
    }

    /// Get number of columns
    pub fn num_columns(&self) -> usize {
        self.num_columns
    }
}
