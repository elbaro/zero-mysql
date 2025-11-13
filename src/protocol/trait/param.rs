//! # Param - Ergonomic Parameter Binding for MySQL
//!
//! This module provides the `Param` trait for individual parameters in MySQL prepared statements.
//! The implementation is designed to be ergonomic, allowing seamless use of both owned and borrowed types.
//!
//! ## Features
//!
//! - **Primitive types**: All signed and unsigned integers (i8, u8, i16, u16, i32, u32, i64, u64)
//! - **Floating point**: f32, f64
//! - **Strings**: Both `&str` and `String` work identically
//! - **Bytes**: Both `&[u8]` and `Vec<u8>` work identically
//! - **NULL handling**: Use `Option<T>` for nullable parameters
//!
//! ## Usage Examples
//!
//! ### Basic parameter types
//!
//! ```ignore
//! // Integer parameters
//! let params = (42i32, 100u64);
//!
//! // Mixed types
//! let params = (1i32, 3.14f64, "hello");
//!
//! // Strings work ergonomically - no need to worry about &str vs String
//! let s1 = "hello";
//! let s2 = String::from("world");
//! let params = (s1, s2);  // Both work!
//!
//! // Same with bytes
//! let b1: &[u8] = &[1, 2, 3];
//! let b2 = vec![4, 5, 6];
//! let params = (b1, b2);  // Both work!
//! ```
//!
//! ### NULL handling
//!
//! ```ignore
//! // Use Option<T> for nullable parameters
//! let params = (
//!     Some(42i32),
//!     None::<String>,  // NULL string
//!     Some("test"),
//! );
//! ```
//!
//! ### Using with Params trait
//!
//! ```ignore
//! // Arrays and slices automatically implement Params
//! let params = [1, 2, 3];
//! conn.exec(stmt_id, &params)?;
//!
//! // Tuples of Param types also implement Params
//! let params = (42i32, "hello", Some(3.14f64));
//! conn.exec(stmt_id, &params)?;
//! ```

use crate::constant::ColumnType;
use crate::error::Result;
use crate::protocol::primitive::*;

/// Trait for a single parameter in prepared statements
///
/// This trait represents a single parameter that can be bound to a prepared statement.
/// The implementation handles encoding the parameter according to MySQL binary protocol.
///
/// # Example
/// ```ignore
/// let param: i32 = 42;
/// let mut null_bit = 0u8;
/// let mut types = Vec::new();
/// let mut values = Vec::new();
///
/// if param.is_null() {
///     null_bit = 1;
/// }
/// param.write_type(&mut types);
/// param.write_value(&mut values)?;
/// ```
pub trait Param {
    /// Returns true if this parameter is NULL
    fn is_null(&self) -> bool {
        false
    }

    /// Write parameter type (2 bytes: MySQL type + unsigned flag)
    ///
    /// Format:
    /// - Byte 0: MySQL type (MYSQL_TYPE_*)
    /// - Byte 1: Unsigned flag (0x80 if unsigned, 0x00 otherwise)
    fn write_type(&self, out: &mut Vec<u8>);

    /// Write parameter value (binary encoded)
    ///
    /// Only called if is_null() returns false.
    /// Values are encoded according to MySQL binary protocol.
    fn write_value(&self, out: &mut Vec<u8>) -> Result<()>;
}

// ============================================================================
// Signed integer implementations
// ============================================================================

impl Param for i8 {
    fn write_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_TINY as u8);
        out.push(0x00);
    }

    fn write_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_1(out, *self as u8);
        Ok(())
    }
}

impl Param for i16 {
    fn write_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_SHORT as u8);
        out.push(0x00);
    }

    fn write_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_2(out, *self as u16);
        Ok(())
    }
}

impl Param for i32 {
    fn write_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_LONG as u8);
        out.push(0x00);
    }

    fn write_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_4(out, *self as u32);
        Ok(())
    }
}

impl Param for i64 {
    fn write_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_LONGLONG as u8);
        out.push(0x00);
    }

    fn write_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_8(out, *self as u64);
        Ok(())
    }
}

// ============================================================================
// Unsigned integer implementations
// ============================================================================

impl Param for u8 {
    fn write_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_TINY as u8);
        out.push(0x80);
    }

    fn write_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_1(out, *self);
        Ok(())
    }
}

impl Param for u16 {
    fn write_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_SHORT as u8);
        out.push(0x80);
    }

    fn write_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_2(out, *self);
        Ok(())
    }
}

impl Param for u32 {
    fn write_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_LONG as u8);
        out.push(0x80);
    }

    fn write_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_4(out, *self);
        Ok(())
    }
}

impl Param for u64 {
    fn write_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_LONGLONG as u8);
        out.push(0x80);
    }

    fn write_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_8(out, *self);
        Ok(())
    }
}

// ============================================================================
// Floating point implementations
// ============================================================================

impl Param for f32 {
    fn write_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_FLOAT as u8);
        out.push(0x00);
    }

    fn write_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_4(out, self.to_bits());
        Ok(())
    }
}

impl Param for f64 {
    fn write_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_DOUBLE as u8);
        out.push(0x00);
    }

    fn write_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_8(out, self.to_bits());
        Ok(())
    }
}

// ============================================================================
// String implementations (ergonomic - both &str and String work the same)
// ============================================================================

impl Param for &str {
    fn write_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_VAR_STRING as u8);
        out.push(0x00);
    }

    fn write_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_string_lenenc(out, self);
        Ok(())
    }
}

impl Param for String {
    fn write_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_VAR_STRING as u8);
        out.push(0x00);
    }

    fn write_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_string_lenenc(out, self);
        Ok(())
    }
}

impl Param for &String {
    fn write_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_VAR_STRING as u8);
        out.push(0x00);
    }

    fn write_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_string_lenenc(out, self);
        Ok(())
    }
}

// ============================================================================
// Byte slice implementations (ergonomic - both &[u8] and Vec<u8> work)
// ============================================================================

impl Param for &[u8] {
    fn write_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_BLOB as u8);
        out.push(0x00);
    }

    fn write_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_bytes_lenenc(out, self);
        Ok(())
    }
}

impl Param for Vec<u8> {
    fn write_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_BLOB as u8);
        out.push(0x00);
    }

    fn write_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_bytes_lenenc(out, self);
        Ok(())
    }
}

impl Param for &Vec<u8> {
    fn write_type(&self, out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_BLOB as u8);
        out.push(0x00);
    }

    fn write_value(&self, out: &mut Vec<u8>) -> Result<()> {
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

    fn write_type(&self, out: &mut Vec<u8>) {
        match self {
            Some(value) => value.write_type(out),
            None => {
                // For NULL, we still need to write a type
                // Use VARCHAR as a reasonable default
                out.push(ColumnType::MYSQL_TYPE_VAR_STRING as u8);
                out.push(0x00);
            }
        }
    }

    fn write_value(&self, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Some(value) => value.write_value(out),
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

        param.write_type(&mut types);
        param.write_value(&mut values).unwrap();

        assert_eq!(types, vec![ColumnType::MYSQL_TYPE_LONG as u8, 0x00]);
        assert_eq!(values, (-42i32).to_le_bytes());
        assert!(!param.is_null());
    }

    #[test]
    fn test_param_u64() {
        let param: u64 = 12345678901234;
        let mut types = Vec::new();
        let mut values = Vec::new();

        param.write_type(&mut types);
        param.write_value(&mut values).unwrap();

        assert_eq!(types, vec![ColumnType::MYSQL_TYPE_LONGLONG as u8, 0x80]);
        assert_eq!(values, 12345678901234u64.to_le_bytes());
    }

    #[test]
    fn test_param_f64() {
        let param: f64 = 3.14159;
        let mut types = Vec::new();
        let mut values = Vec::new();

        param.write_type(&mut types);
        param.write_value(&mut values).unwrap();

        assert_eq!(types, vec![ColumnType::MYSQL_TYPE_DOUBLE as u8, 0x00]);
        assert_eq!(values, 3.14159f64.to_bits().to_le_bytes());
    }

    #[test]
    fn test_param_str() {
        let param = "Hello, MySQL!";
        let mut types = Vec::new();
        let mut values = Vec::new();

        param.write_type(&mut types);
        param.write_value(&mut values).unwrap();

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

        param.write_type(&mut types);
        param.write_value(&mut values).unwrap();

        assert_eq!(types, vec![ColumnType::MYSQL_TYPE_VAR_STRING as u8, 0x00]);
        assert_eq!(values[0], 4); // length
        assert_eq!(&values[1..], b"Rust");
    }

    #[test]
    fn test_param_bytes() {
        let param: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];
        let mut types = Vec::new();
        let mut values = Vec::new();

        param.write_type(&mut types);
        param.write_value(&mut values).unwrap();

        assert_eq!(types, vec![ColumnType::MYSQL_TYPE_BLOB as u8, 0x00]);
        assert_eq!(values[0], 4); // length
        assert_eq!(&values[1..], &[0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn test_param_vec_u8() {
        let param = vec![1u8, 2, 3, 4, 5];
        let mut types = Vec::new();
        let mut values = Vec::new();

        param.write_type(&mut types);
        param.write_value(&mut values).unwrap();

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
        param.write_type(&mut types);
        param.write_value(&mut values).unwrap();

        assert_eq!(types, vec![ColumnType::MYSQL_TYPE_LONG as u8, 0x00]);
        assert_eq!(values, 42i32.to_le_bytes());
    }

    #[test]
    fn test_param_option_none() {
        let param: Option<i32> = None;
        let mut types = Vec::new();
        let mut values = Vec::new();

        assert!(param.is_null());
        param.write_type(&mut types);
        param.write_value(&mut values).unwrap();

        assert_eq!(types, vec![ColumnType::MYSQL_TYPE_VAR_STRING as u8, 0x00]);
        assert_eq!(values, Vec::<u8>::new()); // NULL values don't write anything
    }

    #[test]
    fn test_param_option_string() {
        let param = Some("test".to_string());
        let mut types = Vec::new();
        let mut values = Vec::new();

        param.write_type(&mut types);
        param.write_value(&mut values).unwrap();

        assert_eq!(types, vec![ColumnType::MYSQL_TYPE_VAR_STRING as u8, 0x00]);
        assert_eq!(values[0], 4);
        assert_eq!(&values[1..], b"test");
    }
}
