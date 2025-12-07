/// MySQL Binary Protocol Value Types
use crate::constant::{ColumnFlags, ColumnType};
use crate::error::{Error, Result, eyre};
use crate::protocol::command::ColumnTypeAndFlags;
use crate::protocol::primitive::*;
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
    /// DATE/DATETIME/TIMESTAMP - 0 bytes (0000-00-00 00:00:00)
    Timestamp0,
    /// DATE/DATETIME/TIMESTAMP - 4 bytes (ymd)
    Timestamp4(&'a Timestamp4),
    /// DATE/DATETIME/TIMESTAMP - 7 bytes (ymd + hms)
    Timestamp7(&'a Timestamp7),
    /// DATE/DATETIME/TIMESTAMP - 11 bytes (ymd + hms + microseconds)
    Timestamp11(&'a Timestamp11),
    /// TIME - 0 bytes (00:00:00)
    Time0,
    /// TIME - 8 bytes (without microseconds)
    Time8(&'a Time8),
    /// TIME - 12 bytes (with microseconds)
    Time12(&'a Time12),
    /// BLOB, GEOMETRY, STRING, VARCHAR, VAR_STRING, ..
    Byte(&'a [u8]),
}

impl<'a> Value<'a> {
    /// Parse a single binary protocol value based on column type and flags
    ///
    /// Returns the parsed value and the remaining bytes
    pub fn parse(type_and_flags: &ColumnTypeAndFlags, data: &'a [u8]) -> Result<(Self, &'a [u8])> {
        let is_unsigned = type_and_flags.flags.contains(ColumnFlags::UNSIGNED_FLAG);

        match type_and_flags.column_type {
            ColumnType::MYSQL_TYPE_NULL => Ok((Value::Null, data)),

            // Integer types
            ColumnType::MYSQL_TYPE_TINY => {
                let (val, rest) = read_int_1(data)?;
                let value = if is_unsigned {
                    Value::UnsignedInt(val as u64)
                } else {
                    Value::SignedInt(val as i8 as i64)
                };
                Ok((value, rest))
            }

            ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
                let (val, rest) = read_int_2(data)?;
                let value = if is_unsigned {
                    Value::UnsignedInt(val as u64)
                } else {
                    Value::SignedInt(val as i16 as i64)
                };
                Ok((value, rest))
            }

            ColumnType::MYSQL_TYPE_INT24 | ColumnType::MYSQL_TYPE_LONG => {
                let (val, rest) = read_int_4(data)?;
                let value = if is_unsigned {
                    Value::UnsignedInt(val as u64)
                } else {
                    Value::SignedInt(val as i32 as i64)
                };
                Ok((value, rest))
            }

            ColumnType::MYSQL_TYPE_LONGLONG => {
                let (val, rest) = read_int_8(data)?;
                let value = if is_unsigned {
                    Value::UnsignedInt(val)
                } else {
                    Value::SignedInt(val as i64)
                };
                Ok((value, rest))
            }

            // Floating point types
            ColumnType::MYSQL_TYPE_FLOAT => {
                let (val, rest) = read_int_4(data)?;
                Ok((Value::Float(f32::from_bits(val)), rest))
            }

            ColumnType::MYSQL_TYPE_DOUBLE => {
                let (val, rest) = read_int_8(data)?;
                Ok((Value::Double(f64::from_bits(val)), rest))
            }

            // Temporal types - use length-encoded format
            ColumnType::MYSQL_TYPE_DATE
            | ColumnType::MYSQL_TYPE_DATETIME
            | ColumnType::MYSQL_TYPE_TIMESTAMP
            | ColumnType::MYSQL_TYPE_TIMESTAMP2
            | ColumnType::MYSQL_TYPE_DATETIME2
            | ColumnType::MYSQL_TYPE_NEWDATE => {
                let (len, mut rest) = read_int_1(data)?;
                match len {
                    0 => Ok((Value::Timestamp0, rest)),
                    4 => {
                        let ts = Timestamp4::ref_from_bytes(&rest[..4])?;
                        rest = &rest[4..];
                        Ok((Value::Timestamp4(ts), rest))
                    }
                    7 => {
                        let ts = Timestamp7::ref_from_bytes(&rest[..7])?;
                        rest = &rest[7..];
                        Ok((Value::Timestamp7(ts), rest))
                    }
                    11 => {
                        let ts = Timestamp11::ref_from_bytes(&rest[..11])?;
                        rest = &rest[11..];
                        Ok((Value::Timestamp11(ts), rest))
                    }
                    _ => Err(Error::LibraryBug(eyre!(
                        "invalid timestamp length: {}",
                        len
                    ))),
                }
            }

            // TIME types
            ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => {
                let (len, mut rest) = read_int_1(data)?;
                match len {
                    0 => Ok((Value::Time0, rest)),
                    8 => {
                        let time = Time8::ref_from_bytes(&rest[..8])?;
                        rest = &rest[8..];
                        Ok((Value::Time8(time), rest))
                    }
                    12 => {
                        let time = Time12::ref_from_bytes(&rest[..12])?;
                        rest = &rest[12..];
                        Ok((Value::Time12(time), rest))
                    }
                    _ => Err(Error::LibraryBug(eyre!("invalid time length: {}", len))),
                }
            }

            // String and BLOB types - length-encoded
            ColumnType::MYSQL_TYPE_VARCHAR
            | ColumnType::MYSQL_TYPE_VAR_STRING
            | ColumnType::MYSQL_TYPE_STRING
            | ColumnType::MYSQL_TYPE_BLOB
            | ColumnType::MYSQL_TYPE_TINY_BLOB
            | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
            | ColumnType::MYSQL_TYPE_LONG_BLOB
            | ColumnType::MYSQL_TYPE_GEOMETRY
            | ColumnType::MYSQL_TYPE_JSON
            | ColumnType::MYSQL_TYPE_DECIMAL
            | ColumnType::MYSQL_TYPE_NEWDECIMAL
            | ColumnType::MYSQL_TYPE_ENUM
            | ColumnType::MYSQL_TYPE_SET
            | ColumnType::MYSQL_TYPE_BIT
            | ColumnType::MYSQL_TYPE_TYPED_ARRAY => {
                let (bytes, rest) = read_string_lenenc(data)?;
                Ok((Value::Byte(bytes), rest))
            }
        }
    }
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

