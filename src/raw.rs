//! Flexible decoding API for MySQL binary protocol values.
//!
//! This module provides traits for decoding MySQL values directly into target types
//! without intermediate `Value` allocation.

use crate::constant::{ColumnFlags, ColumnType};
use crate::error::{Error, Result, eyre};
use crate::protocol::BinaryRowPayload;
use crate::protocol::command::{ColumnDefinition, ColumnDefinitionTail};
use crate::protocol::primitive::*;
use crate::value::{Time8, Time12, Timestamp4, Timestamp7, Timestamp11, Value};
use simdutf8::basic::from_utf8;
use zerocopy::FromBytes;

/// MySQL binary charset number - indicates binary/non-text data
const BINARY_CHARSET: u16 = 63;

/// Trait for types that can be decoded from MySQL binary protocol values.
///
/// Each method corresponds to a MySQL wire format. Implementations should
/// return `Err` for unsupported conversions.
pub trait FromRawValue<'buf>: Sized {
    fn from_null() -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type NULL to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_i8(_v: i8) -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type TINYINT (i8) to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_i16(_v: i16) -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type SMALLINT (i16) to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_i32(_v: i32) -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type INT (i32) to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_i64(_v: i64) -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type BIGINT (i64) to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_u8(_v: u8) -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type TINYINT UNSIGNED (u8) to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_u16(_v: u16) -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type SMALLINT UNSIGNED (u16) to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_u32(_v: u32) -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type INT UNSIGNED (u32) to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_u64(_v: u64) -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type BIGINT UNSIGNED (u64) to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_float(_v: f32) -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type FLOAT (f32) to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_double(_v: f64) -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type DOUBLE (f64) to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_bytes(_v: &'buf [u8]) -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type BYTES to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_str(_v: &'buf [u8]) -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type STRING to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_decimal(_v: &'buf [u8]) -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type DECIMAL to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_date0() -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type DATE to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_date4(_v: &'buf Timestamp4) -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type DATE to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_datetime0() -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type DATETIME to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_datetime4(_v: &'buf Timestamp4) -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type DATETIME to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_datetime7(_v: &'buf Timestamp7) -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type DATETIME to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_datetime11(_v: &'buf Timestamp11) -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type DATETIME to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_time0() -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type TIME to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_time8(_v: &'buf Time8) -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type TIME to {}",
            std::any::type_name::<Self>()
        )))
    }

    fn from_time12(_v: &'buf Time12) -> Result<Self> {
        Err(Error::BadUsageError(format!(
            "Cannot decode MySQL type TIME to {}",
            std::any::type_name::<Self>()
        )))
    }
}

/// Parse a single value from binary data into target type `T`.
///
/// Returns the parsed value and remaining bytes.
pub fn parse_value<'buf, T: FromRawValue<'buf>>(
    col: &ColumnDefinitionTail,
    is_null: bool,
    data: &'buf [u8],
) -> Result<(T, &'buf [u8])> {
    if is_null {
        return Ok((T::from_null()?, data));
    }
    let is_unsigned = col.flags()?.contains(ColumnFlags::UNSIGNED_FLAG);
    let is_binary_charset = col.charset() == BINARY_CHARSET;

    match col.column_type()? {
        ColumnType::MYSQL_TYPE_NULL => Ok((T::from_null()?, data)),

        // Integer types
        ColumnType::MYSQL_TYPE_TINY => {
            let (val, rest) = read_int_1(data)?;
            let out = if is_unsigned {
                T::from_u8(val)?
            } else {
                T::from_i8(val as i8)?
            };
            Ok((out, rest))
        }

        ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
            let (val, rest) = read_int_2(data)?;
            let out = if is_unsigned {
                T::from_u16(val)?
            } else {
                T::from_i16(val as i16)?
            };
            Ok((out, rest))
        }

        ColumnType::MYSQL_TYPE_INT24 | ColumnType::MYSQL_TYPE_LONG => {
            let (val, rest) = read_int_4(data)?;
            let out = if is_unsigned {
                T::from_u32(val)?
            } else {
                T::from_i32(val as i32)?
            };
            Ok((out, rest))
        }

        ColumnType::MYSQL_TYPE_LONGLONG => {
            let (val, rest) = read_int_8(data)?;
            let out = if is_unsigned {
                T::from_u64(val)?
            } else {
                T::from_i64(val as i64)?
            };
            Ok((out, rest))
        }

        // Floating point types
        ColumnType::MYSQL_TYPE_FLOAT => {
            let (val, rest) = read_int_4(data)?;
            Ok((T::from_float(f32::from_bits(val))?, rest))
        }

        ColumnType::MYSQL_TYPE_DOUBLE => {
            let (val, rest) = read_int_8(data)?;
            Ok((T::from_double(f64::from_bits(val))?, rest))
        }

        // DATE types
        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => {
            let (len, mut rest) = read_int_1(data)?;
            match len {
                0 => Ok((T::from_date0()?, rest)),
                4 => {
                    let ts = Timestamp4::ref_from_bytes(&rest[..4])?;
                    rest = &rest[4..];
                    Ok((T::from_date4(ts)?, rest))
                }
                _ => Err(Error::LibraryBug(eyre!("invalid date length: {}", len))),
            }
        }

        // DATETIME/TIMESTAMP types
        ColumnType::MYSQL_TYPE_DATETIME
        | ColumnType::MYSQL_TYPE_TIMESTAMP
        | ColumnType::MYSQL_TYPE_TIMESTAMP2
        | ColumnType::MYSQL_TYPE_DATETIME2 => {
            let (len, mut rest) = read_int_1(data)?;
            match len {
                0 => Ok((T::from_datetime0()?, rest)),
                4 => {
                    let ts = Timestamp4::ref_from_bytes(&rest[..4])?;
                    rest = &rest[4..];
                    Ok((T::from_datetime4(ts)?, rest))
                }
                7 => {
                    let ts = Timestamp7::ref_from_bytes(&rest[..7])?;
                    rest = &rest[7..];
                    Ok((T::from_datetime7(ts)?, rest))
                }
                11 => {
                    let ts = Timestamp11::ref_from_bytes(&rest[..11])?;
                    rest = &rest[11..];
                    Ok((T::from_datetime11(ts)?, rest))
                }
                _ => Err(Error::LibraryBug(eyre!("invalid datetime length: {}", len))),
            }
        }

        // TIME types
        ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => {
            let (len, mut rest) = read_int_1(data)?;
            match len {
                0 => Ok((T::from_time0()?, rest)),
                8 => {
                    let time = Time8::ref_from_bytes(&rest[..8])?;
                    rest = &rest[8..];
                    Ok((T::from_time8(time)?, rest))
                }
                12 => {
                    let time = Time12::ref_from_bytes(&rest[..12])?;
                    rest = &rest[12..];
                    Ok((T::from_time12(time)?, rest))
                }
                _ => Err(Error::LibraryBug(eyre!("invalid time length: {}", len))),
            }
        }

        // DECIMAL types
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            let (bytes, rest) = read_string_lenenc(data)?;
            Ok((T::from_decimal(bytes)?, rest))
        }

        // String and BLOB types
        ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_VAR_STRING
        | ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_BLOB
        | ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB
        | ColumnType::MYSQL_TYPE_GEOMETRY
        | ColumnType::MYSQL_TYPE_JSON
        | ColumnType::MYSQL_TYPE_ENUM
        | ColumnType::MYSQL_TYPE_SET
        | ColumnType::MYSQL_TYPE_BIT
        | ColumnType::MYSQL_TYPE_TYPED_ARRAY => {
            let (bytes, rest) = read_string_lenenc(data)?;
            let out = if is_binary_charset {
                T::from_bytes(bytes)?
            } else {
                T::from_str(bytes)?
            };
            Ok((out, rest))
        }
    }
}

/// Trait for types that can be decoded from a MySQL row.
pub trait FromRawRow<'buf>: Sized {
    fn from_raw_row(cols: &[ColumnDefinition<'_>], row: BinaryRowPayload<'buf>) -> Result<Self>;
}

// ============================================================================
// FromRawValue implementations for Value<'a>
// ============================================================================

impl<'buf, 'value> FromRawValue<'buf> for Value<'value>
where
    'buf: 'value,
{
    fn from_null() -> Result<Self> {
        Ok(Value::Null)
    }

    fn from_i8(v: i8) -> Result<Self> {
        Ok(Value::SignedInt(v as i64))
    }

    fn from_i16(v: i16) -> Result<Self> {
        Ok(Value::SignedInt(v as i64))
    }

    fn from_i32(v: i32) -> Result<Self> {
        Ok(Value::SignedInt(v as i64))
    }

    fn from_i64(v: i64) -> Result<Self> {
        Ok(Value::SignedInt(v))
    }

    fn from_u8(v: u8) -> Result<Self> {
        Ok(Value::UnsignedInt(v as u64))
    }

    fn from_u16(v: u16) -> Result<Self> {
        Ok(Value::UnsignedInt(v as u64))
    }

    fn from_u32(v: u32) -> Result<Self> {
        Ok(Value::UnsignedInt(v as u64))
    }

    fn from_u64(v: u64) -> Result<Self> {
        Ok(Value::UnsignedInt(v))
    }

    fn from_float(v: f32) -> Result<Self> {
        Ok(Value::Float(v))
    }

    fn from_double(v: f64) -> Result<Self> {
        Ok(Value::Double(v))
    }

    fn from_bytes(v: &'buf [u8]) -> Result<Self> {
        Ok(Value::Byte(v))
    }

    fn from_str(v: &'buf [u8]) -> Result<Self> {
        Ok(Value::Byte(v))
    }

    fn from_decimal(v: &'buf [u8]) -> Result<Self> {
        Ok(Value::Byte(v))
    }

    fn from_date0() -> Result<Self> {
        Ok(Value::Date0)
    }

    fn from_date4(v: &'buf Timestamp4) -> Result<Self> {
        Ok(Value::Date4(v))
    }

    fn from_datetime0() -> Result<Self> {
        Ok(Value::Datetime0)
    }

    fn from_datetime4(v: &'buf Timestamp4) -> Result<Self> {
        Ok(Value::Datetime4(v))
    }

    fn from_datetime7(v: &'buf Timestamp7) -> Result<Self> {
        Ok(Value::Datetime7(v))
    }

    fn from_datetime11(v: &'buf Timestamp11) -> Result<Self> {
        Ok(Value::Datetime11(v))
    }

    fn from_time0() -> Result<Self> {
        Ok(Value::Time0)
    }

    fn from_time8(v: &'buf Time8) -> Result<Self> {
        Ok(Value::Time8(v))
    }

    fn from_time12(v: &'buf Time12) -> Result<Self> {
        Ok(Value::Time12(v))
    }
}

// ============================================================================
// FromRawValue implementations for primitive types
// ============================================================================

impl FromRawValue<'_> for i8 {
    fn from_i8(v: i8) -> Result<Self> {
        Ok(v)
    }
}

impl FromRawValue<'_> for i16 {
    fn from_i8(v: i8) -> Result<Self> {
        Ok(v as i16)
    }

    fn from_i16(v: i16) -> Result<Self> {
        Ok(v)
    }
}

impl FromRawValue<'_> for i32 {
    fn from_i8(v: i8) -> Result<Self> {
        Ok(v as i32)
    }

    fn from_i16(v: i16) -> Result<Self> {
        Ok(v as i32)
    }

    fn from_i32(v: i32) -> Result<Self> {
        Ok(v)
    }
}

impl FromRawValue<'_> for i64 {
    fn from_i8(v: i8) -> Result<Self> {
        Ok(v as i64)
    }

    fn from_i16(v: i16) -> Result<Self> {
        Ok(v as i64)
    }

    fn from_i32(v: i32) -> Result<Self> {
        Ok(v as i64)
    }

    fn from_i64(v: i64) -> Result<Self> {
        Ok(v)
    }
}

impl FromRawValue<'_> for bool {
    fn from_i8(v: i8) -> Result<Self> {
        Ok(v != 0)
    }

    fn from_u8(v: u8) -> Result<Self> {
        Ok(v != 0)
    }
}

impl FromRawValue<'_> for u8 {
    fn from_u8(v: u8) -> Result<Self> {
        Ok(v)
    }
}

impl FromRawValue<'_> for u16 {
    fn from_u8(v: u8) -> Result<Self> {
        Ok(v as u16)
    }

    fn from_u16(v: u16) -> Result<Self> {
        Ok(v)
    }
}

impl FromRawValue<'_> for u32 {
    fn from_u8(v: u8) -> Result<Self> {
        Ok(v as u32)
    }

    fn from_u16(v: u16) -> Result<Self> {
        Ok(v as u32)
    }

    fn from_u32(v: u32) -> Result<Self> {
        Ok(v)
    }
}

impl FromRawValue<'_> for u64 {
    fn from_u8(v: u8) -> Result<Self> {
        Ok(v as u64)
    }

    fn from_u16(v: u16) -> Result<Self> {
        Ok(v as u64)
    }

    fn from_u32(v: u32) -> Result<Self> {
        Ok(v as u64)
    }

    fn from_u64(v: u64) -> Result<Self> {
        Ok(v)
    }
}

impl FromRawValue<'_> for f32 {
    fn from_float(v: f32) -> Result<Self> {
        Ok(v)
    }
}

impl FromRawValue<'_> for f64 {
    fn from_double(v: f64) -> Result<Self> {
        Ok(v)
    }

    fn from_float(v: f32) -> Result<Self> {
        Ok(v as f64)
    }
}

impl<'a> FromRawValue<'a> for &'a [u8] {
    fn from_bytes(v: &'a [u8]) -> Result<Self> {
        Ok(v)
    }
}

impl FromRawValue<'_> for Vec<u8> {
    fn from_bytes(v: &[u8]) -> Result<Self> {
        Ok(v.to_vec())
    }
}

impl<'a> FromRawValue<'a> for &'a str {
    fn from_str(v: &'a [u8]) -> Result<Self> {
        from_utf8(v).map_err(|e| {
            Error::BadUsageError(format!("Cannot decode MySQL type STRING to &str: {}", e))
        })
    }
}

impl FromRawValue<'_> for String {
    fn from_str(v: &[u8]) -> Result<Self> {
        from_utf8(v).map(|s| s.to_owned()).map_err(|e| {
            Error::BadUsageError(format!("Cannot decode MySQL type STRING to String: {}", e))
        })
    }
}

impl<'a, T: FromRawValue<'a>> FromRawValue<'a> for Option<T> {
    fn from_null() -> Result<Self> {
        Ok(None)
    }

    fn from_i8(v: i8) -> Result<Self> {
        T::from_i8(v).map(Some)
    }

    fn from_i16(v: i16) -> Result<Self> {
        T::from_i16(v).map(Some)
    }

    fn from_i32(v: i32) -> Result<Self> {
        T::from_i32(v).map(Some)
    }

    fn from_i64(v: i64) -> Result<Self> {
        T::from_i64(v).map(Some)
    }

    fn from_u8(v: u8) -> Result<Self> {
        T::from_u8(v).map(Some)
    }

    fn from_u16(v: u16) -> Result<Self> {
        T::from_u16(v).map(Some)
    }

    fn from_u32(v: u32) -> Result<Self> {
        T::from_u32(v).map(Some)
    }

    fn from_u64(v: u64) -> Result<Self> {
        T::from_u64(v).map(Some)
    }

    fn from_float(v: f32) -> Result<Self> {
        T::from_float(v).map(Some)
    }

    fn from_double(v: f64) -> Result<Self> {
        T::from_double(v).map(Some)
    }

    fn from_bytes(v: &'a [u8]) -> Result<Self> {
        T::from_bytes(v).map(Some)
    }

    fn from_str(v: &'a [u8]) -> Result<Self> {
        T::from_str(v).map(Some)
    }

    fn from_decimal(v: &'a [u8]) -> Result<Self> {
        T::from_decimal(v).map(Some)
    }

    fn from_date0() -> Result<Self> {
        T::from_date0().map(Some)
    }

    fn from_date4(v: &'a Timestamp4) -> Result<Self> {
        T::from_date4(v).map(Some)
    }

    fn from_datetime0() -> Result<Self> {
        T::from_datetime0().map(Some)
    }

    fn from_datetime4(v: &'a Timestamp4) -> Result<Self> {
        T::from_datetime4(v).map(Some)
    }

    fn from_datetime7(v: &'a Timestamp7) -> Result<Self> {
        T::from_datetime7(v).map(Some)
    }

    fn from_datetime11(v: &'a Timestamp11) -> Result<Self> {
        T::from_datetime11(v).map(Some)
    }

    fn from_time0() -> Result<Self> {
        T::from_time0().map(Some)
    }

    fn from_time8(v: &'a Time8) -> Result<Self> {
        T::from_time8(v).map(Some)
    }

    fn from_time12(v: &'a Time12) -> Result<Self> {
        T::from_time12(v).map(Some)
    }
}

// ============================================================================
// FromRawRow implementations for tuples
// ============================================================================

macro_rules! impl_from_raw_row_tuple {
    ($($idx:tt: $T:ident),+) => {
        impl<'buf, 'value, $($T: FromRawValue<'buf>),+> FromRawRow<'buf> for ($($T,)+) {
            #[expect(non_snake_case)]
            fn from_raw_row(cols: &[ColumnDefinition<'_>], row: BinaryRowPayload<'buf>) -> Result<Self> {
                let mut data = row.values();
                let null_bitmap = row.null_bitmap();
                $(
                    let ($T, rest) = parse_value::<$T>(&cols[$idx].tail, null_bitmap.is_null($idx), data)?;
                    data = rest;
                )+
                let _ = data; // suppress unused warning for last element
                Ok(($($T,)+))
            }
        }
    };
}

impl_from_raw_row_tuple!(0: A);
impl_from_raw_row_tuple!(0: A, 1: B);
impl_from_raw_row_tuple!(0: A, 1: B, 2: C);
impl_from_raw_row_tuple!(0: A, 1: B, 2: C, 3: D);
impl_from_raw_row_tuple!(0: A, 1: B, 2: C, 3: D, 4: E);
impl_from_raw_row_tuple!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F);
impl_from_raw_row_tuple!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G);
impl_from_raw_row_tuple!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H);
impl_from_raw_row_tuple!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I);
impl_from_raw_row_tuple!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J);
impl_from_raw_row_tuple!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K);
impl_from_raw_row_tuple!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L);
