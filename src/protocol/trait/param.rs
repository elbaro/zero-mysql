use crate::constant::ColumnType;
use crate::error::Result;
use crate::protocol::primitive::*;

/// Trait for encoding a single parameter in prepared statements
///
/// # Examples
/// - (42i32, 100u64)
/// - (1i32, 3.14f64, "hello")
/// - ("test", None::<String>)  // NULL string
/// - [1, 2, 3]
pub trait Param {
    /// Returns true if this parameter is NULL
    fn is_null(&self) -> bool {
        false
    }

    /// Encode parameter type
    ///
    /// Format:
    /// - Byte 0: MySQL type (MYSQL_TYPE_*)
    /// - Byte 1: Unsigned flag (0x80 if unsigned, 0x00 otherwise)
    fn encode_type(&self, out: &mut Vec<u8>);

    /// Encode parameter value (binary encoded)
    ///
    /// Only called if is_null() returns false.
    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()>;
}

// ============================================================================
// Signed integer implementations
// ============================================================================

impl Param for i8 {
    fn encode_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_TINY as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_1(out, *self as u8);
        Ok(())
    }
}

impl Param for i16 {
    fn encode_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_SHORT as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_2(out, *self as u16);
        Ok(())
    }
}

impl Param for i32 {
    fn encode_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_LONG as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_4(out, *self as u32);
        Ok(())
    }
}

impl Param for i64 {
    fn encode_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_LONGLONG as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_8(out, *self as u64);
        Ok(())
    }
}

// ============================================================================
// Unsigned integer implementations
// ============================================================================

impl Param for u8 {
    fn encode_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_TINY as u8);
        out.push(0x80);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_1(out, *self);
        Ok(())
    }
}

impl Param for u16 {
    fn encode_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_SHORT as u8);
        out.push(0x80);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_2(out, *self);
        Ok(())
    }
}

impl Param for u32 {
    fn encode_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_LONG as u8);
        out.push(0x80);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_4(out, *self);
        Ok(())
    }
}

impl Param for u64 {
    fn encode_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_LONGLONG as u8);
        out.push(0x80);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_8(out, *self);
        Ok(())
    }
}

// ============================================================================
// Floating point implementations
// ============================================================================

impl Param for f32 {
    fn encode_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_FLOAT as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_4(out, self.to_bits());
        Ok(())
    }
}

impl Param for f64 {
    fn encode_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_DOUBLE as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_8(out, self.to_bits());
        Ok(())
    }
}

// ============================================================================
// String implementations (&str and String work the same)
// ============================================================================

impl Param for &str {
    fn encode_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_VAR_STRING as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_string_lenenc(out, self);
        Ok(())
    }
}

impl Param for String {
    fn encode_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_VAR_STRING as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_string_lenenc(out, self);
        Ok(())
    }
}

impl Param for &String {
    fn encode_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_VAR_STRING as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_string_lenenc(out, self);
        Ok(())
    }
}

// ============================================================================
// Byte slice implementations (ergonomic - both &[u8] and Vec<u8> work)
// ============================================================================

impl Param for &[u8] {
    fn encode_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_BLOB as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_bytes_lenenc(out, self);
        Ok(())
    }
}

impl Param for Vec<u8> {
    fn encode_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_BLOB as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_bytes_lenenc(out, self);
        Ok(())
    }
}

impl Param for &Vec<u8> {
    fn encode_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_BLOB as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_bytes_lenenc(out, self);
        Ok(())
    }
}

// ============================================================================
// Option<T> implementation for NULL handling
// ============================================================================

impl<T: Param> Param for Option<T> {
    fn is_null(&self) -> bool {
        self.is_none()
    }

    fn encode_type(&self, out: &mut Vec<u8>) {
        match self {
            Some(value) => value.encode_type(out),
            None => {
                // For NULL, we still need to write a type
                // Use VARCHAR as a reasonable default
                out.push(ColumnType::MYSQL_TYPE_VAR_STRING as u8);
                out.push(0x00);
            }
        }
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Some(value) => value.encode_value(out),
            None => Ok(()), // NULL values don't write anything
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_param_i32() {
        let param: i32 = -42;
        let mut types = Vec::new();
        let mut values = Vec::new();

        param.encode_type(&mut types);
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

        param.encode_type(&mut types);
        param.encode_value(&mut values).unwrap();

        assert_eq!(types, vec![ColumnType::MYSQL_TYPE_LONGLONG as u8, 0x80]);
        assert_eq!(values, 12345678901234u64.to_le_bytes());
    }

    #[test]
    fn test_param_f64() {
        let param: f64 = 3.14159;
        let mut types = Vec::new();
        let mut values = Vec::new();

        param.encode_type(&mut types);
        param.encode_value(&mut values).unwrap();

        assert_eq!(types, vec![ColumnType::MYSQL_TYPE_DOUBLE as u8, 0x00]);
        assert_eq!(values, 3.14159f64.to_bits().to_le_bytes());
    }

    #[test]
    fn test_param_str() {
        let param = "Hello, MySQL!";
        let mut types = Vec::new();
        let mut values = Vec::new();

        param.encode_type(&mut types);
        param.encode_value(&mut values).unwrap();

        assert_eq!(types, vec![ColumnType::MYSQL_TYPE_VAR_STRING as u8, 0x00]);
        // Length-encoded: 13 (length) + "Hello, MySQL!"
        assert_eq!(values[0], 13); // length
        assert_eq!(&values[1..], b"Hello, MySQL!");
    }

    #[test]
    fn test_param_string() {
        let param = String::from("Rust");
        let mut types = Vec::new();
        let mut values = Vec::new();

        param.encode_type(&mut types);
        param.encode_value(&mut values).unwrap();

        assert_eq!(types, vec![ColumnType::MYSQL_TYPE_VAR_STRING as u8, 0x00]);
        assert_eq!(values[0], 4); // length
        assert_eq!(&values[1..], b"Rust");
    }

    #[test]
    fn test_param_bytes() {
        let param: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];
        let mut types = Vec::new();
        let mut values = Vec::new();

        param.encode_type(&mut types);
        param.encode_value(&mut values).unwrap();

        assert_eq!(types, vec![ColumnType::MYSQL_TYPE_BLOB as u8, 0x00]);
        assert_eq!(values[0], 4); // length
        assert_eq!(&values[1..], &[0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn test_param_vec_u8() {
        let param = vec![1u8, 2, 3, 4, 5];
        let mut types = Vec::new();
        let mut values = Vec::new();

        param.encode_type(&mut types);
        param.encode_value(&mut values).unwrap();

        assert_eq!(types, vec![ColumnType::MYSQL_TYPE_BLOB as u8, 0x00]);
        assert_eq!(values[0], 5); // length
        assert_eq!(&values[1..], &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_param_option_some() {
        let param = Some(42i32);
        let mut types = Vec::new();
        let mut values = Vec::new();

        assert!(!param.is_null());
        param.encode_type(&mut types);
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
        param.encode_type(&mut types);
        param.encode_value(&mut values).unwrap();

        assert_eq!(types, vec![ColumnType::MYSQL_TYPE_VAR_STRING as u8, 0x00]);
        assert_eq!(values, Vec::<u8>::new()); // NULL values don't write anything
    }

    #[test]
    fn test_param_option_string() {
        let param = Some("test".to_string());
        let mut types = Vec::new();
        let mut values = Vec::new();

        param.encode_type(&mut types);
        param.encode_value(&mut values).unwrap();

        assert_eq!(types, vec![ColumnType::MYSQL_TYPE_VAR_STRING as u8, 0x00]);
        assert_eq!(values[0], 4);
        assert_eq!(&values[1..], b"test");
    }
}
