/// MySQL Binary Protocol Value Types
use zerocopy::byteorder::little_endian::{U16 as U16LE, U32 as U32LE};
use zerocopy::{FromBytes, Immutable, KnownLayout};

#[derive(Debug, Clone, Copy)]
pub enum Value<'a> {
    /// NULL value
    Null,
    /// Signed integer (TINYINT, SMALLINT, INT, BIGINT)
    SignedInt(i64),
    /// Unsigned integer (TINYINT UNSIGNED, SMALLINT UNSIGNED, INT UNSIGNED, BIGINT UNSIGNED)
    UnsignedInt(u64),
    /// FLOAT - 4-byte floating point
    Float(f32),
    /// DOUBLE - 8-byte floating point
    Double(f64),
    /// DATE - 0 bytes (0000-00-00)
    Date0,
    /// DATE - 4 bytes (ymd)
    Date4(&'a Timestamp4),
    /// DATETIME/TIMESTAMP - 0 bytes (0000-00-00 00:00:00)
    Datetime0,
    /// DATETIME/TIMESTAMP - 4 bytes (ymd)
    Datetime4(&'a Timestamp4),
    /// DATETIME/TIMESTAMP - 7 bytes (ymd + hms)
    Datetime7(&'a Timestamp7),
    /// DATETIME/TIMESTAMP - 11 bytes (ymd + hms + microseconds)
    Datetime11(&'a Timestamp11),
    /// TIME - 0 bytes (00:00:00)
    Time0,
    /// TIME - 8 bytes (without microseconds)
    Time8(&'a Time8),
    /// TIME - 12 bytes (with microseconds)
    Time12(&'a Time12),
    /// BLOB, GEOMETRY, STRING, VARCHAR, VAR_STRING, ..
    Byte(&'a [u8]),
}

// ============================================================================
// Temporal Types
// ============================================================================

/// TIMESTAMP - 4 bytes (DATE/DATETIME/TIMESTAMP with date only)
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
pub struct Timestamp4 {
    pub year: U16LE,
    pub month: u8,
    pub day: u8,
}

impl Timestamp4 {
    pub fn year(&self) -> u16 {
        self.year.get()
    }
}

/// TIMESTAMP - 7 bytes (DATE/DATETIME/TIMESTAMP without microseconds)
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
pub struct Timestamp7 {
    pub year: U16LE,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
}

impl Timestamp7 {
    pub fn year(&self) -> u16 {
        self.year.get()
    }
}

/// TIMESTAMP - 11 bytes (DATE/DATETIME/TIMESTAMP with microseconds)
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
pub struct Timestamp11 {
    pub year: U16LE,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub microsecond: U32LE,
}

impl Timestamp11 {
    pub fn year(&self) -> u16 {
        self.year.get()
    }

    pub fn microsecond(&self) -> u32 {
        self.microsecond.get()
    }
}

/// TIME - 8 bytes
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
pub struct Time8 {
    pub is_negative: u8,
    pub days: U32LE,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
}

impl Time8 {
    pub fn is_negative(&self) -> bool {
        self.is_negative != 0
    }

    pub fn days(&self) -> u32 {
        self.days.get()
    }
}

/// TIME - 12 bytesative (1), days (4 LE), hour (1), minute (1), second (1), microsecond (4 LE)
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
pub struct Time12 {
    pub is_negative: u8,
    pub days: U32LE,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub microsecond: U32LE,
}

impl Time12 {
    pub fn is_negative(&self) -> bool {
        self.is_negative != 0
    }

    pub fn days(&self) -> u32 {
        self.days.get()
    }

    pub fn microsecond(&self) -> u32 {
        self.microsecond.get()
    }
}

// ============================================================================
// NULL Bitmap
// ============================================================================

/// NULL bitmap for binary protocol
///
/// In MySQL binary protocol, NULL values are indicated by a bitmap where each bit
/// represents whether a column is NULL (1 = NULL, 0 = not NULL).
///
/// For result sets (COM_STMT_EXECUTE response), the bitmap has an offset of 2 bits.
/// For prepared statement parameters, the offset is 0 bits.
#[derive(Debug, Clone, Copy)]
pub struct NullBitmap<'a> {
    bitmap: &'a [u8],
    offset: usize,
}

impl<'a> NullBitmap<'a> {
    /// Create a NULL bitmap for result sets (offset = 2)
    pub fn for_result_set(bitmap: &'a [u8]) -> Self {
        Self { bitmap, offset: 2 }
    }

    /// Create a NULL bitmap for parameters (offset = 0)
    pub fn for_parameters(bitmap: &'a [u8]) -> Self {
        Self { bitmap, offset: 0 }
    }

    /// Check if the column at the given index is NULL
    ///
    /// # Arguments
    /// * `idx` - Column index (0-based)
    ///
    /// # Returns
    /// `true` if the column is NULL, `false` otherwise
    pub fn is_null(&self, idx: usize) -> bool {
        let bit_pos = idx + self.offset;
        let byte_pos = bit_pos >> 3;
        let bit_offset = bit_pos & 7;

        if byte_pos >= self.bitmap.len() {
            return false;
        }

        (self.bitmap[byte_pos] & (1 << bit_offset)) != 0
    }

    /// Get the raw bitmap bytes
    pub fn as_bytes(&self) -> &'a [u8] {
        self.bitmap
    }
}
