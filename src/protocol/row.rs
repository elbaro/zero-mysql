use crate::protocol::value::NullBitmap;

/// The payload part of a row packet.
#[derive(Debug, Clone)]
pub struct BinaryRowPayload<'a> {
    pub(crate) null_bitmap: NullBitmap<'a>,
    pub(crate) values: &'a [u8],
    pub(crate) num_columns: usize,
}

impl<'a> BinaryRowPayload<'a> {
    pub fn null_bitmap(&self) -> NullBitmap<'_> {
        self.null_bitmap
    }

    pub fn values(&self) -> &[u8] {
        self.values
    }

    pub fn num_columns(&self) -> usize {
        self.num_columns
    }
}

/// The payload part of a row packet.
#[derive(Debug, Clone)]
pub struct TextRowPayload<'a>(pub &'a [u8]);
