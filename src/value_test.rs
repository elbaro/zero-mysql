use std::mem::align_of;

use crate::constant::{ColumnFlags, ColumnType};
use crate::protocol::command::ColumnDefinitionTail;
use crate::raw::parse_value;
use crate::value::{NullBitmap, Time8, Time12, Timestamp4, Timestamp7, Timestamp11, Value};
use zerocopy::FromBytes;

/// Helper to create a ColumnDefinitionTail for testing
fn make_col_tail(column_type: ColumnType, flags: ColumnFlags) -> ColumnDefinitionTail {
    let mut bytes = [0u8; 12];
    bytes[0..2].copy_from_slice(&33u16.to_le_bytes()); // charset (utf8)
    bytes[2..6].copy_from_slice(&255u32.to_le_bytes()); // column_length
    bytes[6] = column_type as u8; // column_type
    bytes[7..9].copy_from_slice(&flags.bits().to_le_bytes()); // flags
    bytes[9] = 0; // decimals
    bytes[10..12].copy_from_slice(&0u16.to_le_bytes()); // reserved
    *ColumnDefinitionTail::ref_from_bytes(&bytes).unwrap()
}

#[test]
fn test_value_parse_signed_integers() {
    // TINYINT (-42)
    let col = make_col_tail(ColumnType::MYSQL_TYPE_TINY, ColumnFlags::empty());
    let data = [214u8]; // -42 as i8
    let (value, rest) = parse_value::<Value>(&col, false, &data).unwrap();
    assert!(matches!(value, Value::SignedInt(-42)));
    assert_eq!(rest.len(), 0);

    // SMALLINT (-1000)
    let col = make_col_tail(ColumnType::MYSQL_TYPE_SHORT, ColumnFlags::empty());
    let data = [0x18, 0xFC]; // -1000 as i16 LE
    let (value, rest) = parse_value::<Value>(&col, false, &data).unwrap();
    assert!(matches!(value, Value::SignedInt(-1000)));
    assert_eq!(rest.len(), 0);

    // INT (-100000)
    let col = make_col_tail(ColumnType::MYSQL_TYPE_LONG, ColumnFlags::empty());
    let data = [0x60, 0x79, 0xFE, 0xFF]; // -100000 as i32 LE
    let (value, rest) = parse_value::<Value>(&col, false, &data).unwrap();
    assert!(matches!(value, Value::SignedInt(-100000)));
    assert_eq!(rest.len(), 0);
}

#[test]
fn test_value_parse_unsigned_integers() {
    // TINYINT UNSIGNED (200)
    let col = make_col_tail(ColumnType::MYSQL_TYPE_TINY, ColumnFlags::UNSIGNED_FLAG);
    let data = [200_u8];
    let (value, rest) = parse_value::<Value>(&col, false, &data).unwrap();
    assert!(matches!(value, Value::UnsignedInt(200)));
    assert_eq!(rest.len(), 0);

    // BIGINT UNSIGNED (large number)
    let col = make_col_tail(ColumnType::MYSQL_TYPE_LONGLONG, ColumnFlags::UNSIGNED_FLAG);
    let data = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]; // i64::MAX
    let (value, rest) = parse_value::<Value>(&col, false, &data).unwrap();
    assert!(matches!(value, Value::UnsignedInt(9223372036854775807)));
    assert_eq!(rest.len(), 0);
}

#[test]
fn test_value_parse_float_double() {
    // FLOAT (3.14)
    let col = make_col_tail(ColumnType::MYSQL_TYPE_FLOAT, ColumnFlags::empty());
    let data = 3.14f32.to_le_bytes();
    let (value, rest) = parse_value::<Value>(&col, false, &data).unwrap();
    if let Value::Float(f) = value {
        assert!((f - 3.14).abs() < 0.001);
    } else {
        panic!("Expected Float value");
    }
    assert_eq!(rest.len(), 0);

    // DOUBLE (3.141592653589793)
    let col = make_col_tail(ColumnType::MYSQL_TYPE_DOUBLE, ColumnFlags::empty());
    let data = std::f64::consts::PI.to_le_bytes();
    let (value, rest) = parse_value::<Value>(&col, false, &data).unwrap();
    if let Value::Double(d) = value {
        assert!((d - std::f64::consts::PI).abs() < 0.0000001);
    } else {
        panic!("Expected Double value");
    }
    assert_eq!(rest.len(), 0);
}

#[test]
fn test_value_parse_datetime() {
    let col = make_col_tail(ColumnType::MYSQL_TYPE_DATETIME, ColumnFlags::empty());

    // Datetime0 (0000-00-00 00:00:00)
    let data = [0_u8]; // length = 0
    let (value, rest) = parse_value::<Value>(&col, false, &data).unwrap();
    assert!(matches!(value, Value::Datetime0));
    assert_eq!(rest.len(), 0);

    // Datetime4 (2024-12-25)
    let mut data = vec![4u8]; // length = 4
    data.extend_from_slice(&2024u16.to_le_bytes()); // year
    data.push(12); // month
    data.push(25); // day
    let (value, rest) = parse_value::<Value>(&col, false, &data).unwrap();
    if let Value::Datetime4(ts) = value {
        assert_eq!(ts.year(), 2024);
        assert_eq!(ts.month, 12);
        assert_eq!(ts.day, 25);
    } else {
        panic!("Expected Datetime4 value");
    }
    assert_eq!(rest.len(), 0);

    // Datetime7 (2024-12-25 15:30:45)
    let mut data = vec![7u8]; // length = 7
    data.extend_from_slice(&2024u16.to_le_bytes()); // year
    data.push(12); // month
    data.push(25); // day
    data.push(15); // hour
    data.push(30); // minute
    data.push(45); // second
    let (value, rest) = parse_value::<Value>(&col, false, &data).unwrap();
    if let Value::Datetime7(ts) = value {
        assert_eq!(ts.year(), 2024);
        assert_eq!(ts.month, 12);
        assert_eq!(ts.day, 25);
        assert_eq!(ts.hour, 15);
        assert_eq!(ts.minute, 30);
        assert_eq!(ts.second, 45);
    } else {
        panic!("Expected Datetime7 value");
    }
    assert_eq!(rest.len(), 0);
}

#[test]
fn test_value_parse_date() {
    let col = make_col_tail(ColumnType::MYSQL_TYPE_DATE, ColumnFlags::empty());

    // Date0 (0000-00-00)
    let data = [0_u8]; // length = 0
    let (value, rest) = parse_value::<Value>(&col, false, &data).unwrap();
    assert!(matches!(value, Value::Date0));
    assert_eq!(rest.len(), 0);

    // Date4 (2024-12-25)
    let mut data = vec![4u8]; // length = 4
    data.extend_from_slice(&2024u16.to_le_bytes()); // year
    data.push(12); // month
    data.push(25); // day
    let (value, rest) = parse_value::<Value>(&col, false, &data).unwrap();
    if let Value::Date4(ts) = value {
        assert_eq!(ts.year(), 2024);
        assert_eq!(ts.month, 12);
        assert_eq!(ts.day, 25);
    } else {
        panic!("Expected Date4 value, got {:?}", value);
    }
    assert_eq!(rest.len(), 0);
}

#[test]
fn test_value_parse_time() {
    let col = make_col_tail(ColumnType::MYSQL_TYPE_TIME, ColumnFlags::empty());

    // Time0 (00:00:00)
    let data = [0_u8]; // length = 0
    let (value, rest) = parse_value::<Value>(&col, false, &data).unwrap();
    assert!(matches!(value, Value::Time0));
    assert_eq!(rest.len(), 0);

    // Time8 (negative, 1 day 12:30:45)
    let mut data = vec![8u8]; // length = 8
    data.push(1); // is_negative
    data.extend_from_slice(&1u32.to_le_bytes()); // days
    data.push(12); // hour
    data.push(30); // minute
    data.push(45); // second
    let (value, rest) = parse_value::<Value>(&col, false, &data).unwrap();
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
    let col = make_col_tail(ColumnType::MYSQL_TYPE_VAR_STRING, ColumnFlags::empty());

    // Length-encoded string "Hello"
    let mut data = vec![5u8]; // length = 5
    data.extend_from_slice(b"Hello");
    let (value, rest) = parse_value::<Value>(&col, false, &data).unwrap();
    if let Value::Byte(bytes) = value {
        assert_eq!(bytes, b"Hello");
    } else {
        panic!("Expected Byte value");
    }
    assert_eq!(rest.len(), 0);
}

#[test]
fn test_value_parse_blob() {
    let col = make_col_tail(ColumnType::MYSQL_TYPE_BLOB, ColumnFlags::empty());

    // Length-encoded binary data
    let mut data = vec![4u8]; // length = 4
    data.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
    let (value, rest) = parse_value::<Value>(&col, false, &data).unwrap();
    if let Value::Byte(bytes) = value {
        assert_eq!(bytes, &[0xDE, 0xAD, 0xBE, 0xEF]);
    } else {
        panic!("Expected Byte value");
    }
    assert_eq!(rest.len(), 0);
}

#[test]
fn test_value_parse_null() {
    let col = make_col_tail(ColumnType::MYSQL_TYPE_NULL, ColumnFlags::empty());

    let data = []; // NULL takes no bytes
    let (value, rest) = parse_value::<Value>(&col, false, &data).unwrap();
    assert!(matches!(value, Value::Null));
    assert_eq!(rest.len(), 0);
}

#[test]
fn test_value_parse_with_remaining_data() {
    let col = make_col_tail(ColumnType::MYSQL_TYPE_TINY, ColumnFlags::UNSIGNED_FLAG);

    let data = [42u8, 0xFF, 0xFF]; // 42 followed by extra data
    let (value, rest) = parse_value::<Value>(&col, false, &data).unwrap();
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

#[test]
fn zerocopy_types_have_alignment_of_1() {
    assert_eq!(align_of::<Timestamp4>(), 1);
    assert_eq!(align_of::<Timestamp7>(), 1);
    assert_eq!(align_of::<Timestamp11>(), 1);
    assert_eq!(align_of::<Time8>(), 1);
    assert_eq!(align_of::<Time12>(), 1);
}
