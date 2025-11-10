use crate::error::Result;

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

/// Implementation for vectors of parameter implementations
impl<T: Params> Params for Vec<T>
where
    T: Params,
{
    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn write_null_bitmap(&self, out: &mut Vec<u8>) {
        for param in self.iter() {
            param.write_null_bitmap(out);
        }
    }

    fn write_types(&self, out: &mut Vec<u8>) {
        for param in self.iter() {
            param.write_types(out);
        }
    }

    fn write_values(&self, out: &mut Vec<u8>) -> Result<()> {
        for param in self.iter() {
            param.write_values(out)?;
        }
        Ok(())
    }
}
