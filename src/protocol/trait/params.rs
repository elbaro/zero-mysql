use crate::error::Result;
use crate::protocol::r#trait::param::Param;

/// Trait for parameter binding in prepared statements
///
/// This trait is implemented by external libraries to provide parameter serialization
/// with minimal copying. The implementation is responsible for encoding parameters
/// according to MySQL binary protocol.
pub trait Params {
    /// Number of parameters
    fn len(&self) -> usize;

    /// Check if there are no parameters
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Write NULL bitmap
    ///
    /// The NULL bitmap is (num_params + 7) / 8 bytes long.
    /// Bit is set to 1 if the parameter is NULL.
    fn write_null_bitmap(&self, out: &mut Vec<u8>);

    /// Whether to send parameter types to server
    ///
    /// Typically true on first execute, false on subsequent executes
    /// with the same statement (optimization).
    fn send_types_to_server(&self) -> bool {
        true
    }

    /// Write parameter types
    ///
    /// Each parameter type is 2 bytes:
    /// - 1 byte: MySQL type (MYSQL_TYPE_*)
    /// - 1 byte: unsigned flag (0x80 if unsigned, 0x00 otherwise)
    ///
    /// Called only if send_types_to_server() returns true.
    fn write_types(&self, out: &mut Vec<u8>);

    /// Write parameter values (binary encoded)
    ///
    /// Values are encoded according to MySQL binary protocol.
    /// NULL parameters should be skipped (they're already in the NULL bitmap).
    ///
    /// The encoding is type-specific:
    /// - Integers: little-endian fixed-width
    /// - Floats/Doubles: IEEE 754 little-endian
    /// - Strings/Bytes: length-encoded
    /// - Date/Time: special encoding
    fn write_values(&self, out: &mut Vec<u8>) -> Result<()>;
}

/// Empty parameters (no parameters)
impl Params for () {
    fn len(&self) -> usize {
        0
    }

    fn write_null_bitmap(&self, _out: &mut Vec<u8>) {}

    fn write_types(&self, _out: &mut Vec<u8>) {}

    fn write_values(&self, _out: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }
}

// ============================================================================
// Implementations for slices and arrays of Param types
// ============================================================================

/// Implementation for slices of Param types
impl<T: Param> Params for &[T] {
    fn len(&self) -> usize {
        (*self).len()
    }

    fn write_null_bitmap(&self, out: &mut Vec<u8>) {
        // Calculate number of bytes needed for NULL bitmap
        let num_bytes = (self.len() + 7) / 8;
        let start_len = out.len();
        out.resize(start_len + num_bytes, 0);

        // Set bits for NULL parameters
        for (i, param) in self.iter().enumerate() {
            if param.is_null() {
                let byte_pos = start_len + (i / 8);
                let bit_offset = i % 8;
                out[byte_pos] |= 1 << bit_offset;
            }
        }
    }

    fn write_types(&self, out: &mut Vec<u8>) {
        for param in self.iter() {
            param.write_type(out);
        }
    }

    fn write_values(&self, out: &mut Vec<u8>) -> Result<()> {
        for param in self.iter() {
            if !param.is_null() {
                param.write_value(out)?;
            }
        }
        Ok(())
    }
}

/// Implementation for arrays of Param types
impl<T: Param, const N: usize> Params for [T; N] {
    fn len(&self) -> usize {
        N
    }

    fn write_null_bitmap(&self, out: &mut Vec<u8>) {
        self.as_slice().write_null_bitmap(out)
    }

    fn write_types(&self, out: &mut Vec<u8>) {
        self.as_slice().write_types(out)
    }

    fn write_values(&self, out: &mut Vec<u8>) -> Result<()> {
        self.as_slice().write_values(out)
    }
}

// ============================================================================
// Tuple implementations for common sizes
// ============================================================================

macro_rules! impl_params_for_tuple {
    ($($T:ident : $idx:tt),+) => {
        impl<$($T: Param),+> Params for ($($T,)+) {
            fn len(&self) -> usize {
                let mut count = 0;
                $(
                    let _ = &self.$idx;
                    count += 1;
                )+
                count
            }

            fn write_null_bitmap(&self, out: &mut Vec<u8>) {
                let num_bytes = (self.len() + 7) / 8;
                let start_len = out.len();
                out.resize(start_len + num_bytes, 0);

                $(
                    if self.$idx.is_null() {
                        let byte_pos = start_len + ($idx / 8);
                        let bit_offset = $idx % 8;
                        out[byte_pos] |= 1 << bit_offset;
                    }
                )+
            }

            fn write_types(&self, out: &mut Vec<u8>) {
                $(
                    self.$idx.write_type(out);
                )+
            }

            fn write_values(&self, out: &mut Vec<u8>) -> Result<()> {
                $(
                    if !self.$idx.is_null() {
                        self.$idx.write_value(out)?;
                    }
                )+
                Ok(())
            }
        }
    };
}

// Implement for tuples of size 1-12
impl_params_for_tuple!(T0: 0);
impl_params_for_tuple!(T0: 0, T1: 1);
impl_params_for_tuple!(T0: 0, T1: 1, T2: 2);
impl_params_for_tuple!(T0: 0, T1: 1, T2: 2, T3: 3);
impl_params_for_tuple!(T0: 0, T1: 1, T2: 2, T3: 3, T4: 4);
impl_params_for_tuple!(T0: 0, T1: 1, T2: 2, T3: 3, T4: 4, T5: 5);
impl_params_for_tuple!(T0: 0, T1: 1, T2: 2, T3: 3, T4: 4, T5: 5, T6: 6);
impl_params_for_tuple!(T0: 0, T1: 1, T2: 2, T3: 3, T4: 4, T5: 5, T6: 6, T7: 7);
impl_params_for_tuple!(T0: 0, T1: 1, T2: 2, T3: 3, T4: 4, T5: 5, T6: 6, T7: 7, T8: 8);
impl_params_for_tuple!(T0: 0, T1: 1, T2: 2, T3: 3, T4: 4, T5: 5, T6: 6, T7: 7, T8: 8, T9: 9);
impl_params_for_tuple!(T0: 0, T1: 1, T2: 2, T3: 3, T4: 4, T5: 5, T6: 6, T7: 7, T8: 8, T9: 9, T10: 10);
impl_params_for_tuple!(T0: 0, T1: 1, T2: 2, T3: 3, T4: 4, T5: 5, T6: 6, T7: 7, T8: 8, T9: 9, T10: 10, T11: 11);

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_params_slice() {
        let params: &[i32] = &[1, 2, 3];
        assert_eq!(params.len(), 3);

        let mut null_bitmap = Vec::new();
        params.write_null_bitmap(&mut null_bitmap);
        assert_eq!(null_bitmap, vec![0]); // No NULLs

        let mut types = Vec::new();
        params.write_types(&mut types);
        assert_eq!(types.len(), 6); // 2 bytes per parameter

        let mut values = Vec::new();
        params.write_values(&mut values).unwrap();
        assert_eq!(values.len(), 12); // 4 bytes per i32
    }

    #[test]
    fn test_params_vec() {
        let params = vec![42i32, -100, 200];
        let params_slice: &[i32] = &params;
        assert_eq!(params_slice.len(), 3);

        let mut values = Vec::new();
        params_slice.write_values(&mut values).unwrap();
        assert_eq!(values.len(), 12);
    }

    #[test]
    fn test_params_array() {
        let params = [1u8, 2, 3, 4, 5];
        assert_eq!(params.len(), 5);

        let mut types = Vec::new();
        params.write_types(&mut types);
        assert_eq!(types.len(), 10); // 2 bytes per parameter
    }

    #[test]
    fn test_params_tuple() {
        let params = (42i32, "hello", 3.14f64);
        assert_eq!(params.len(), 3);

        let mut null_bitmap = Vec::new();
        params.write_null_bitmap(&mut null_bitmap);
        assert_eq!(null_bitmap, vec![0]); // No NULLs

        let mut types = Vec::new();
        params.write_types(&mut types);
        assert_eq!(types.len(), 6); // 2 bytes per parameter

        let mut values = Vec::new();
        params.write_values(&mut values).unwrap();
        // i32 (4 bytes) + string length-encoded + f64 (8 bytes)
        assert!(values.len() > 12);
    }

    #[test]
    fn test_params_tuple_with_option() {
        let params = (Some(42i32), None::<String>, Some("test"));
        assert_eq!(params.len(), 3);

        let mut null_bitmap = Vec::new();
        params.write_null_bitmap(&mut null_bitmap);
        // Bitmap: bit 1 should be set (second param is NULL)
        assert_eq!(null_bitmap, vec![0b00000010]);

        let mut values = Vec::new();
        params.write_values(&mut values).unwrap();
        // Only non-NULL values are written
        // i32 (4 bytes) + "test" length-encoded (1 + 4 = 5 bytes)
        assert_eq!(values.len(), 9);
    }

    #[test]
    fn test_params_mixed_types() {
        let params = (
            1i8, 2i16, 3i32, 4i64, 5u8, 6u16, 7u32, 8u64, 1.5f32, 2.5f64, "hello",
        );
        assert_eq!(params.len(), 11);

        let mut types = Vec::new();
        params.write_types(&mut types);
        assert_eq!(types.len(), 22); // 2 bytes per parameter

        let mut values = Vec::new();
        params.write_values(&mut values).unwrap();
        // 1+2+4+8+1+2+4+8+4+8+6 = 48 bytes
        assert_eq!(values.len(), 48);
    }

    #[test]
    fn test_params_string_variants() {
        let s1 = "hello";
        let s2 = String::from("world");
        let s3 = &String::from("test");

        let params = (s1, s2, s3);
        assert_eq!(params.len(), 3);

        let mut values = Vec::new();
        params.write_values(&mut values).unwrap();
        // "hello" (1+5) + "world" (1+5) + "test" (1+4) = 17 bytes
        assert_eq!(values.len(), 17);
    }

    #[test]
    fn test_params_byte_variants() {
        let b1: &[u8] = &[1, 2, 3];
        let b2 = vec![4, 5, 6];
        let b3 = &vec![7, 8];

        let params = (b1, b2, b3);
        assert_eq!(params.len(), 3);

        let mut values = Vec::new();
        params.write_values(&mut values).unwrap();
        // [1,2,3] (1+3) + [4,5,6] (1+3) + [7,8] (1+2) = 11 bytes
        assert_eq!(values.len(), 11);
    }
}
