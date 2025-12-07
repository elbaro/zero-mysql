use crate::value::NullBitmap;

/// The payload part of a row packet.
#[derive(Debug, Clone)]
pub struct BinaryRowPayload<'a> {
    null_bitmap: NullBitmap<'a>,
    values: &'a [u8],
    num_columns: usize,
}

impl<'a> BinaryRowPayload<'a> {
    pub fn new(null_bitmap: NullBitmap<'a>, values: &'a [u8], num_columns: usize) -> Self {
        Self {
            null_bitmap,
            values,
            num_columns,
        }
    }

    pub fn null_bitmap(&self) -> NullBitmap<'_> {
        self.null_bitmap
    }

    pub fn values(&self) -> &'a [u8] {
        self.values
    }

    pub fn num_columns(&self) -> usize {
        self.num_columns
    }
}

/// The payload part of a row packet.
#[derive(Debug, Clone, Copy)]
pub struct TextRowPayload<'a>(pub &'a [u8]);
