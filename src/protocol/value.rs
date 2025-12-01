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
                        let ts = Timestamp4::ref_from_bytes(&rest[..4]).map_err(Error::from_debug)?;
                        rest = &rest[4..];
                        Ok((Value::Timestamp4(ts), rest))
                    }
                    7 => {
                        let ts = Timestamp7::ref_from_bytes(&rest[..7]).map_err(Error::from_debug)?;
                        rest = &rest[7..];
                        Ok((Value::Timestamp7(ts), rest))
                    }
                    11 => {
                        let ts =
                            Timestamp11::ref_from_bytes(&rest[..11]).map_err(Error::from_debug)?;
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
                        let time = Time8::ref_from_bytes(&rest[..8]).map_err(Error::from_debug)?;
                        rest = &rest[8..];
                        Ok((Value::Time8(time), rest))
                    }
                    12 => {
                        let time = Time12::ref_from_bytes(&rest[..12]).map_err(Error::from_debug)?;
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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_parse_signed_integers() {
        // TINYINT (-42)
        let type_and_flags = ColumnTypeAndFlags {
            column_type: ColumnType::MYSQL_TYPE_TINY,
            flags: ColumnFlags::empty(),
        };
        let data = [214u8]; // -42 as i8
        let (value, rest) = Value::parse(&type_and_flags, &data).unwrap();
        assert!(matches!(value, Value::SignedInt(-42)));
        assert_eq!(rest.len(), 0);

        // SMALLINT (-1000)
        let type_and_flags = ColumnTypeAndFlags {
            column_type: ColumnType::MYSQL_TYPE_SHORT,
            flags: ColumnFlags::empty(),
        };
        let data = [0x18, 0xFC]; // -1000 as i16 LE
        let (value, rest) = Value::parse(&type_and_flags, &data).unwrap();
        assert!(matches!(value, Value::SignedInt(-1000)));
        assert_eq!(rest.len(), 0);

        // INT (-100000)
        let type_and_flags = ColumnTypeAndFlags {
            column_type: ColumnType::MYSQL_TYPE_LONG,
            flags: ColumnFlags::empty(),
        };
        let data = [0x60, 0x79, 0xFE, 0xFF]; // -100000 as i32 LE
        let (value, rest) = Value::parse(&type_and_flags, &data).unwrap();
        assert!(matches!(value, Value::SignedInt(-100000)));
        assert_eq!(rest.len(), 0);
    }

    #[test]
    fn test_value_parse_unsigned_integers() {
        // TINYINT UNSIGNED (200)
        let type_and_flags = ColumnTypeAndFlags {
            column_type: ColumnType::MYSQL_TYPE_TINY,
            flags: ColumnFlags::UNSIGNED_FLAG,
        };
        let data = [200_u8];
        let (value, rest) = Value::parse(&type_and_flags, &data).unwrap();
        assert!(matches!(value, Value::UnsignedInt(200)));
        assert_eq!(rest.len(), 0);

        // BIGINT UNSIGNED (large number)
        let type_and_flags = ColumnTypeAndFlags {
            column_type: ColumnType::MYSQL_TYPE_LONGLONG,
            flags: ColumnFlags::UNSIGNED_FLAG,
        };
        let data = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]; // i64::MAX
        let (value, rest) = Value::parse(&type_and_flags, &data).unwrap();
        assert!(matches!(value, Value::UnsignedInt(9223372036854775807)));
        assert_eq!(rest.len(), 0);
    }

    #[test]
    fn test_value_parse_float_double() {
        // FLOAT (3.14)
        let type_and_flags = ColumnTypeAndFlags {
            column_type: ColumnType::MYSQL_TYPE_FLOAT,
            flags: ColumnFlags::empty(),
        };
        let data = 3.14f32.to_le_bytes();
        let (value, rest) = Value::parse(&type_and_flags, &data).unwrap();
        if let Value::Float(f) = value {
            assert!((f - 3.14).abs() < 0.001);
        } else {
            panic!("Expected Float value");
        }
        assert_eq!(rest.len(), 0);

        // DOUBLE (3.141592653589793)
        let type_and_flags = ColumnTypeAndFlags {
            column_type: ColumnType::MYSQL_TYPE_DOUBLE,
            flags: ColumnFlags::empty(),
        };
        let data = std::f64::consts::PI.to_le_bytes();
        let (value, rest) = Value::parse(&type_and_flags, &data).unwrap();
        if let Value::Double(d) = value {
            assert!((d - std::f64::consts::PI).abs() < 0.0000001);
        } else {
            panic!("Expected Double value");
        }
        assert_eq!(rest.len(), 0);
    }

    #[test]
    fn test_value_parse_timestamp() {
        let type_and_flags = ColumnTypeAndFlags {
            column_type: ColumnType::MYSQL_TYPE_DATETIME,
            flags: ColumnFlags::empty(),
        };

        // Timestamp0 (0000-00-00 00:00:00)
        let data = [0_u8]; // length = 0
        let (value, rest) = Value::parse(&type_and_flags, &data).unwrap();
        assert!(matches!(value, Value::Timestamp0));
        assert_eq!(rest.len(), 0);

        // Timestamp4 (2024-12-25)
        let mut data = vec![4u8]; // length = 4
        data.extend_from_slice(&2024u16.to_le_bytes()); // year
        data.push(12); // month
        data.push(25); // day
        let (value, rest) = Value::parse(&type_and_flags, &data).unwrap();
        if let Value::Timestamp4(ts) = value {
            assert_eq!(ts.year(), 2024);
            assert_eq!(ts.month, 12);
            assert_eq!(ts.day, 25);
        } else {
            panic!("Expected Timestamp4 value");
        }
        assert_eq!(rest.len(), 0);

        // Timestamp7 (2024-12-25 15:30:45)
        let mut data = vec![7u8]; // length = 7
        data.extend_from_slice(&2024u16.to_le_bytes()); // year
        data.push(12); // month
        data.push(25); // day
        data.push(15); // hour
        data.push(30); // minute
        data.push(45); // second
        let (value, rest) = Value::parse(&type_and_flags, &data).unwrap();
        if let Value::Timestamp7(ts) = value {
            assert_eq!(ts.year(), 2024);
            assert_eq!(ts.month, 12);
            assert_eq!(ts.day, 25);
            assert_eq!(ts.hour, 15);
            assert_eq!(ts.minute, 30);
            assert_eq!(ts.second, 45);
        } else {
            panic!("Expected Timestamp7 value");
        }
        assert_eq!(rest.len(), 0);
    }

    #[test]
    fn test_value_parse_time() {
        let type_and_flags = ColumnTypeAndFlags {
            column_type: ColumnType::MYSQL_TYPE_TIME,
            flags: ColumnFlags::empty(),
        };

        // Time0 (00:00:00)
        let data = [0_u8]; // length = 0
        let (value, rest) = Value::parse(&type_and_flags, &data).unwrap();
        assert!(matches!(value, Value::Time0));
        assert_eq!(rest.len(), 0);

        // Time8 (negative, 1 day 12:30:45)
        let mut data = vec![8u8]; // length = 8
        data.push(1); // is_negative
        data.extend_from_slice(&1u32.to_le_bytes()); // days
        data.push(12); // hour
        data.push(30); // minute
        data.push(45); // second
        let (value, rest) = Value::parse(&type_and_flags, &data).unwrap();
        if let Value::Time8(time) = value {
            assert_eq!(time.is_negative(), true);
            assert_eq!(time.days(), 1);
            assert_eq!(time.hour, 12);
            assert_eq!(time.minute, 30);
            assert_eq!(time.second, 45);
        } else {
            panic!("Expected Time8 value");
        }
        assert_eq!(rest.len(), 0);
    }

    #[test]
    fn test_value_parse_string() {
        let type_and_flags = ColumnTypeAndFlags {
            column_type: ColumnType::MYSQL_TYPE_VAR_STRING,
            flags: ColumnFlags::empty(),
        };

        // Length-encoded string "Hello"
        let mut data = vec![5u8]; // length = 5
        data.extend_from_slice(b"Hello");
        let (value, rest) = Value::parse(&type_and_flags, &data).unwrap();
        if let Value::Byte(bytes) = value {
            assert_eq!(bytes, b"Hello");
        } else {
            panic!("Expected Byte value");
        }
        assert_eq!(rest.len(), 0);
    }

    #[test]
    fn test_value_parse_blob() {
        let type_and_flags = ColumnTypeAndFlags {
            column_type: ColumnType::MYSQL_TYPE_BLOB,
            flags: ColumnFlags::empty(),
        };

        // Length-encoded binary data
        let mut data = vec![4u8]; // length = 4
        data.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        let (value, rest) = Value::parse(&type_and_flags, &data).unwrap();
        if let Value::Byte(bytes) = value {
            assert_eq!(bytes, &[0xDE, 0xAD, 0xBE, 0xEF]);
        } else {
            panic!("Expected Byte value");
        }
        assert_eq!(rest.len(), 0);
    }

    #[test]
    fn test_value_parse_null() {
        let type_and_flags = ColumnTypeAndFlags {
            column_type: ColumnType::MYSQL_TYPE_NULL,
            flags: ColumnFlags::empty(),
        };

        let data = []; // NULL takes no bytes
        let (value, rest) = Value::parse(&type_and_flags, &data).unwrap();
        assert!(matches!(value, Value::Null));
        assert_eq!(rest.len(), 0);
    }

    #[test]
    fn test_value_parse_with_remaining_data() {
        let type_and_flags = ColumnTypeAndFlags {
            column_type: ColumnType::MYSQL_TYPE_TINY,
            flags: ColumnFlags::UNSIGNED_FLAG,
        };

        let data = [42u8, 0xFF, 0xFF]; // 42 followed by extra data
        let (value, rest) = Value::parse(&type_and_flags, &data).unwrap();
        assert!(matches!(value, Value::UnsignedInt(42)));
        assert_eq!(rest, &[0xFF, 0xFF]);
    }

    #[test]
    fn test_null_bitmap_result_set() {
        // Example bitmap for 8 columns with offset=2
        // Bitmap bytes: [0b00000100, 0b00010000]
        // With offset=2, this means:
        // - Bit 0 (column -2, ignored) = 0
        // - Bit 1 (column -1, ignored) = 0
        // - Bit 2 (column 0) = 1 -> NULL
        // - Bit 3 (column 1) = 0
        // - Bit 4 (column 2) = 0
        // - ...
        // - Bit 12 (column 10) = 1 -> NULL
        let bitmap = [0b00000100, 0b00010000];
        let null_bitmap = NullBitmap::for_result_set(&bitmap);

        assert!(null_bitmap.is_null(0)); // Bit 2
        assert!(!null_bitmap.is_null(1)); // Bit 3
        assert!(!null_bitmap.is_null(2)); // Bit 4
        assert!(null_bitmap.is_null(10)); // Bit 12
    }

    #[test]
    fn test_null_bitmap_parameters() {
        // Example bitmap for parameters with offset=0
        // Bitmap: [0b00000101]
        // - Bit 0 (param 0) = 1 -> NULL
        // - Bit 1 (param 1) = 0
        // - Bit 2 (param 2) = 1 -> NULL
        let bitmap = [0b00000101];
        let null_bitmap = NullBitmap::for_parameters(&bitmap);

        assert!(null_bitmap.is_null(0)); // Bit 0
        assert!(!null_bitmap.is_null(1)); // Bit 1
        assert!(null_bitmap.is_null(2)); // Bit 2
        assert!(!null_bitmap.is_null(3)); // Bit 3
    }
}
