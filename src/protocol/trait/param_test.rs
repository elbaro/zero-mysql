use crate::constant::ColumnType;
use crate::protocol::r#trait::param::{Params, TypedParam};

#[test]
fn test_param_i32() {
    let param: i32 = -42;
    let mut types = Vec::new();
    let mut values = Vec::new();

    i32::encode_type(&mut types);
    param.encode_value(&mut values).unwrap();

    assert_eq!(types, vec![ColumnType::MYSQL_TYPE_LONG as u8, 0x00]);
    assert_eq!(values, (-42i32).to_le_bytes());
    assert!(!param.is_null());
}

#[test]
fn test_param_u64() {
    let param: u64 = 12345678901234;
    let mut types = Vec::new();
    let mut values = Vec::new();

    u64::encode_type(&mut types);
    param.encode_value(&mut values).unwrap();

    assert_eq!(types, vec![ColumnType::MYSQL_TYPE_LONGLONG as u8, 0x80]);
    assert_eq!(values, 12345678901234u64.to_le_bytes());
}

#[test]
fn test_param_f64() {
    let param: f64 = 3.14159;
    let mut types = Vec::new();
    let mut values = Vec::new();

    f64::encode_type(&mut types);
    param.encode_value(&mut values).unwrap();

    assert_eq!(types, vec![ColumnType::MYSQL_TYPE_DOUBLE as u8, 0x00]);
    assert_eq!(values, 3.14159f64.to_bits().to_le_bytes());
}

#[test]
fn test_param_str() {
    let param = "Hello, MySQL!";
    let mut types = Vec::new();
    let mut values = Vec::new();

    <&str>::encode_type(&mut types);
    param.encode_value(&mut values).unwrap();

    assert_eq!(types, vec![ColumnType::MYSQL_TYPE_VAR_STRING as u8, 0x00]);
    assert_eq!(values[0], 13);
    assert_eq!(&values[1..], b"Hello, MySQL!");
}

#[test]
fn test_param_string() {
    let param = String::from("Rust");
    let mut types = Vec::new();
    let mut values = Vec::new();

    String::encode_type(&mut types);
    param.encode_value(&mut values).unwrap();

    assert_eq!(types, vec![ColumnType::MYSQL_TYPE_VAR_STRING as u8, 0x00]);
    assert_eq!(values[0], 4);
    assert_eq!(&values[1..], b"Rust");
}

#[test]
fn test_param_bytes() {
    let param: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];
    let mut types = Vec::new();
    let mut values = Vec::new();

    <&[u8]>::encode_type(&mut types);
    param.encode_value(&mut values).unwrap();

    assert_eq!(types, vec![ColumnType::MYSQL_TYPE_BLOB as u8, 0x00]);
    assert_eq!(values[0], 4);
    assert_eq!(&values[1..], &[0xDE, 0xAD, 0xBE, 0xEF]);
}

#[test]
fn test_param_vec_u8() {
    let param = vec![1u8, 2, 3, 4, 5];
    let mut types = Vec::new();
    let mut values = Vec::new();

    Vec::<u8>::encode_type(&mut types);
    param.encode_value(&mut values).unwrap();

    assert_eq!(types, vec![ColumnType::MYSQL_TYPE_BLOB as u8, 0x00]);
    assert_eq!(values[0], 5);
    assert_eq!(&values[1..], &[1, 2, 3, 4, 5]);
}

#[test]
fn test_param_option_some() {
    let param = Some(42i32);
    let mut types = Vec::new();
    let mut values = Vec::new();

    assert!(!param.is_null());
    Option::<i32>::encode_type(&mut types);
    param.encode_value(&mut values).unwrap();

    assert_eq!(types, vec![ColumnType::MYSQL_TYPE_LONG as u8, 0x00]);
    assert_eq!(values, 42i32.to_le_bytes());
}

#[test]
fn test_param_option_none() {
    let param: Option<i32> = None;
    let mut types = Vec::new();
    let mut values = Vec::new();

    assert!(param.is_null());
    Option::<i32>::encode_type(&mut types);
    param.encode_value(&mut values).unwrap();

    assert_eq!(types, vec![ColumnType::MYSQL_TYPE_LONG as u8, 0x00]);
    assert_eq!(values, Vec::<u8>::new());
}

#[test]
fn test_param_option_string() {
    let param = Some("test".to_string());
    let mut types = Vec::new();
    let mut values = Vec::new();

    Option::<String>::encode_type(&mut types);
    param.encode_value(&mut values).unwrap();

    assert_eq!(types, vec![ColumnType::MYSQL_TYPE_VAR_STRING as u8, 0x00]);
    assert_eq!(values[0], 4);
    assert_eq!(&values[1..], b"test");
}

// ============================================================================
// Tests for Params trait
// ============================================================================

#[test]
fn test_params_tuple() {
    let params = (42i32, "hello", 3.14f64);
    assert_eq!(Params::len(&params), 3);

    let mut null_bitmap = Vec::new();
    Params::encode_null_bitmap(&params, &mut null_bitmap);
    assert_eq!(null_bitmap, vec![0]);

    let mut types = Vec::new();
    Params::encode_types(&params, &mut types);
    assert_eq!(types.len(), 6);

    let mut values = Vec::new();
    Params::encode_values(&params, &mut values).unwrap();
    assert!(values.len() > 12);
}

#[test]
fn test_params_tuple_with_option() {
    let params = (Some(42i32), None::<String>, Some("test"));
    assert_eq!(Params::len(&params), 3);

    let mut null_bitmap = Vec::new();
    Params::encode_null_bitmap(&params, &mut null_bitmap);
    assert_eq!(null_bitmap, vec![0b00000010]);

    let mut values = Vec::new();
    Params::encode_values(&params, &mut values).unwrap();
    assert_eq!(values.len(), 9);
}

#[test]
fn test_params_mixed_types() {
    let params = (
        1i8, 2i16, 3i32, 4i64, 5u8, 6u16, 7u32, 8u64, 1.5f32, 2.5f64, "hello",
    );
    assert_eq!(Params::len(&params), 11);

    let mut types = Vec::new();
    Params::encode_types(&params, &mut types);
    assert_eq!(types.len(), 22);

    let mut values = Vec::new();
    Params::encode_values(&params, &mut values).unwrap();
    assert_eq!(values.len(), 48);
}

#[test]
fn test_params_string_variants() {
    let s1 = "hello";
    let s2 = String::from("world");
    let s3 = &String::from("test");

    let params = (s1, s2, s3);
    assert_eq!(Params::len(&params), 3);

    let mut values = Vec::new();
    Params::encode_values(&params, &mut values).unwrap();
    assert_eq!(values.len(), 17);
}

#[test]
fn test_params_byte_variants() {
    let b1: &[u8] = &[1, 2, 3];
    let b2 = vec![4, 5, 6];
    let b3 = &vec![7, 8];

    let params = (b1, b2, b3);
    assert_eq!(Params::len(&params), 3);

    let mut out = Vec::new();
    Params::encode_values(&params, &mut out).unwrap();
    assert_eq!(out.len(), 11);
}
