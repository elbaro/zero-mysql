use std::mem::align_of;

use crate::constant::{ColumnFlags, ColumnType};
use crate::protocol::command::ColumnDefinitionTail;
use crate::raw::parse_value;
use crate::test_macros::{check, check_eq};
use crate::value::{NullBitmap, Time8, Time12, Timestamp4, Timestamp7, Timestamp11, Value};
use zerocopy::FromBytes;

/// Helper to create a ColumnDefinitionTail for testing
fn make_col_tail(
    column_type: ColumnType,
    flags: ColumnFlags,
) -> crate::error::Result<ColumnDefinitionTail> {
    let mut bytes = [0u8; 12];
    bytes[0..2].copy_from_slice(&33u16.to_le_bytes()); // charset (utf8)
    bytes[2..6].copy_from_slice(&255u32.to_le_bytes()); // column_length
    bytes[6] = column_type as u8; // column_type
    bytes[7..9].copy_from_slice(&flags.bits().to_le_bytes()); // flags
    bytes[9] = 0; // decimals
    bytes[10..12].copy_from_slice(&0u16.to_le_bytes()); // reserved
    Ok(*ColumnDefinitionTail::ref_from_bytes(&bytes)?)
}

#[test]
fn value_parse_signed_integers() -> crate::error::Result<()> {
    // TINYINT (-42)
    let col = make_col_tail(ColumnType::MYSQL_TYPE_TINY, ColumnFlags::empty())?;
    let data = [214u8]; // -42 as i8
    let (value, rest) = parse_value::<Value>(&col, false, &data)?;
    check!(matches!(value, Value::SignedInt(-42)));
    check_eq!(rest.len(), 0);

    // SMALLINT (-1000)
    let col2 = make_col_tail(ColumnType::MYSQL_TYPE_SHORT, ColumnFlags::empty())?;
    let data2 = [0x18, 0xFC]; // -1000 as i16 LE
    let (value2, rest2) = parse_value::<Value>(&col2, false, &data2)?;
    check!(matches!(value2, Value::SignedInt(-1000)));
    check_eq!(rest2.len(), 0);

    // INT (-100000)
    let col3 = make_col_tail(ColumnType::MYSQL_TYPE_LONG, ColumnFlags::empty())?;
    let data3 = [0x60, 0x79, 0xFE, 0xFF]; // -100000 as i32 LE
    let (value3, rest3) = parse_value::<Value>(&col3, false, &data3)?;
    check!(matches!(value3, Value::SignedInt(-100000)));
    check_eq!(rest3.len(), 0);

    Ok(())
}

#[test]
fn value_parse_unsigned_integers() -> crate::error::Result<()> {
    // TINYINT UNSIGNED (200)
    let col = make_col_tail(ColumnType::MYSQL_TYPE_TINY, ColumnFlags::UNSIGNED_FLAG)?;
    let data = [200_u8];
    let (value, rest) = parse_value::<Value>(&col, false, &data)?;
    check!(matches!(value, Value::UnsignedInt(200)));
    check_eq!(rest.len(), 0);

    // BIGINT UNSIGNED (large number)
    let col2 = make_col_tail(ColumnType::MYSQL_TYPE_LONGLONG, ColumnFlags::UNSIGNED_FLAG)?;
    let data2 = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]; // i64::MAX
    let (value2, rest2) = parse_value::<Value>(&col2, false, &data2)?;
    check!(matches!(value2, Value::UnsignedInt(0x7FFF_FFFF_FFFF_FFFF)));
    check_eq!(rest2.len(), 0);

    Ok(())
}

#[test]
fn value_parse_float_double() -> crate::error::Result<()> {
    // FLOAT (3.12)
    let col = make_col_tail(ColumnType::MYSQL_TYPE_FLOAT, ColumnFlags::empty())?;
    let data = 3.12f32.to_le_bytes();
    let (value, rest) = parse_value::<Value>(&col, false, &data)?;
    let Value::Float(f) = value else {
        return Err(crate::error::Error::LibraryBug(crate::error::eyre!(
            "Expected Float value"
        )));
    };
    let close = (f - 3.12).abs() < 0.001;
    check!(close);
    check_eq!(rest.len(), 0);

    // DOUBLE (3.141592653589793)
    let col2 = make_col_tail(ColumnType::MYSQL_TYPE_DOUBLE, ColumnFlags::empty())?;
    let data2 = std::f64::consts::PI.to_le_bytes();
    let (value2, rest2) = parse_value::<Value>(&col2, false, &data2)?;
    let Value::Double(d) = value2 else {
        return Err(crate::error::Error::LibraryBug(crate::error::eyre!(
            "Expected Double value"
        )));
    };
    let close2 = (d - std::f64::consts::PI).abs() < 0.0000001;
    check!(close2);
    check_eq!(rest2.len(), 0);

    Ok(())
}

#[test]
fn value_parse_datetime() -> crate::error::Result<()> {
    let col = make_col_tail(ColumnType::MYSQL_TYPE_DATETIME, ColumnFlags::empty())?;

    // Datetime0 (0000-00-00 00:00:00)
    let data = [0_u8]; // length = 0
    let (value, rest) = parse_value::<Value>(&col, false, &data)?;
    check!(matches!(value, Value::Datetime0));
    check_eq!(rest.len(), 0);

    // Datetime4 (2024-12-25)
    let mut data2 = vec![4u8]; // length = 4
    data2.extend_from_slice(&2024u16.to_le_bytes()); // year
    data2.push(12); // month
    data2.push(25); // day
    let (value2, rest2) = parse_value::<Value>(&col, false, &data2)?;
    let Value::Datetime4(ts) = value2 else {
        return Err(crate::error::Error::LibraryBug(crate::error::eyre!(
            "Expected Datetime4 value"
        )));
    };
    check_eq!(ts.year(), 2024);
    check_eq!(ts.month, 12);
    check_eq!(ts.day, 25);
    check_eq!(rest2.len(), 0);

    // Datetime7 (2024-12-25 15:30:45)
    let mut data3 = vec![7u8]; // length = 7
    data3.extend_from_slice(&2024u16.to_le_bytes()); // year
    data3.push(12); // month
    data3.push(25); // day
    data3.push(15); // hour
    data3.push(30); // minute
    data3.push(45); // second
    let (value3, rest3) = parse_value::<Value>(&col, false, &data3)?;
    let Value::Datetime7(ts2) = value3 else {
        return Err(crate::error::Error::LibraryBug(crate::error::eyre!(
            "Expected Datetime7 value"
        )));
    };
    check_eq!(ts2.year(), 2024);
    check_eq!(ts2.month, 12);
    check_eq!(ts2.day, 25);
    check_eq!(ts2.hour, 15);
    check_eq!(ts2.minute, 30);
    check_eq!(ts2.second, 45);
    check_eq!(rest3.len(), 0);

    Ok(())
}

#[test]
fn value_parse_date() -> crate::error::Result<()> {
    let col = make_col_tail(ColumnType::MYSQL_TYPE_DATE, ColumnFlags::empty())?;

    // Date0 (0000-00-00)
    let data = [0_u8]; // length = 0
    let (value, rest) = parse_value::<Value>(&col, false, &data)?;
    check!(matches!(value, Value::Date0));
    check_eq!(rest.len(), 0);

    // Date4 (2024-12-25)
    let mut data2 = vec![4u8]; // length = 4
    data2.extend_from_slice(&2024u16.to_le_bytes()); // year
    data2.push(12); // month
    data2.push(25); // day
    let (value2, rest2) = parse_value::<Value>(&col, false, &data2)?;
    let Value::Date4(ts) = value2 else {
        return Err(crate::error::Error::LibraryBug(crate::error::eyre!(
            "Expected Date4 value, got {:?}",
            value2
        )));
    };
    check_eq!(ts.year(), 2024);
    check_eq!(ts.month, 12);
    check_eq!(ts.day, 25);
    check_eq!(rest2.len(), 0);

    Ok(())
}

#[test]
fn value_parse_time() -> crate::error::Result<()> {
    let col = make_col_tail(ColumnType::MYSQL_TYPE_TIME, ColumnFlags::empty())?;

    // Time0 (00:00:00)
    let data = [0_u8]; // length = 0
    let (value, rest) = parse_value::<Value>(&col, false, &data)?;
    check!(matches!(value, Value::Time0));
    check_eq!(rest.len(), 0);

    // Time8 (negative, 1 day 12:30:45)
    let mut data2 = vec![8u8]; // length = 8
    data2.push(1); // is_negative
    data2.extend_from_slice(&1u32.to_le_bytes()); // days
    data2.push(12); // hour
    data2.push(30); // minute
    data2.push(45); // second
    let (value2, rest2) = parse_value::<Value>(&col, false, &data2)?;
    let Value::Time8(time) = value2 else {
        return Err(crate::error::Error::LibraryBug(crate::error::eyre!(
            "Expected Time8 value"
        )));
    };
    check!(time.is_negative());
    check_eq!(time.days(), 1);
    check_eq!(time.hour, 12);
    check_eq!(time.minute, 30);
    check_eq!(time.second, 45);
    check_eq!(rest2.len(), 0);

    Ok(())
}

#[test]
fn value_parse_string() -> crate::error::Result<()> {
    let col = make_col_tail(ColumnType::MYSQL_TYPE_VAR_STRING, ColumnFlags::empty())?;

    // Length-encoded string "Hello"
    let mut data = vec![5u8]; // length = 5
    data.extend_from_slice(b"Hello");
    let (value, rest) = parse_value::<Value>(&col, false, &data)?;
    let Value::Byte(bytes) = value else {
        return Err(crate::error::Error::LibraryBug(crate::error::eyre!(
            "Expected Byte value"
        )));
    };
    check_eq!(bytes, b"Hello");
    check_eq!(rest.len(), 0);

    Ok(())
}

#[test]
fn value_parse_blob() -> crate::error::Result<()> {
    let col = make_col_tail(ColumnType::MYSQL_TYPE_BLOB, ColumnFlags::empty())?;

    // Length-encoded binary data
    let mut data = vec![4u8]; // length = 4
    data.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
    let (value, rest) = parse_value::<Value>(&col, false, &data)?;
    let Value::Byte(bytes) = value else {
        return Err(crate::error::Error::LibraryBug(crate::error::eyre!(
            "Expected Byte value"
        )));
    };
    check_eq!(bytes, &[0xDE, 0xAD, 0xBE, 0xEF]);
    check_eq!(rest.len(), 0);

    Ok(())
}

#[test]
fn value_parse_null() -> crate::error::Result<()> {
    let col = make_col_tail(ColumnType::MYSQL_TYPE_NULL, ColumnFlags::empty())?;

    let data = []; // NULL takes no bytes
    let (value, rest) = parse_value::<Value>(&col, false, &data)?;
    check!(matches!(value, Value::Null));
    check_eq!(rest.len(), 0);

    Ok(())
}

#[test]
fn value_parse_with_remaining_data() -> crate::error::Result<()> {
    let col = make_col_tail(ColumnType::MYSQL_TYPE_TINY, ColumnFlags::UNSIGNED_FLAG)?;

    let data = [42u8, 0xFF, 0xFF]; // 42 followed by extra data
    let (value, rest) = parse_value::<Value>(&col, false, &data)?;
    check!(matches!(value, Value::UnsignedInt(42)));
    check_eq!(rest, &[0xFF, 0xFF]);

    Ok(())
}

#[test]
fn null_bitmap_result_set() {
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
fn null_bitmap_parameters() {
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

#[test]
fn zerocopy_types_have_alignment_of_1() {
    assert_eq!(align_of::<Timestamp4>(), 1);
    assert_eq!(align_of::<Timestamp7>(), 1);
    assert_eq!(align_of::<Timestamp11>(), 1);
    assert_eq!(align_of::<Time8>(), 1);
    assert_eq!(align_of::<Time12>(), 1);
}
