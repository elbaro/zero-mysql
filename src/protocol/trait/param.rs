use auto_impl::auto_impl;

use crate::constant::ColumnType;
use crate::error::Result;
use crate::protocol::primitive::*;

/// Parameter indicator for COM_STMT_BULK_EXECUTE
///
/// See: https://mariadb.com/docs/server/reference/clientserver-protocol/3-binary-protocol-prepared-statements/com_stmt_bulk_execute
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ParamIndicator {
    /// Value follows (0)
    None = 0,
    /// Value is null (1)
    Null = 1,
    /// For INSERT/UPDATE, value is default (2)
    Default = 2,
    /// Value is default for insert, ignored for update (3)
    Ignore = 3,
}

pub trait Param {
    fn is_null(&self) -> bool;
    fn encode_type(&self, out: &mut Vec<u8>);
    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()>;
}

pub trait TypedParam {
    fn is_null(&self) -> bool {
        false
    }
    fn encode_type(out: &mut Vec<u8>);
    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()>;
}

impl TypedParam for i8 {
    fn encode_type(out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_TINY as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_1(out, *self as u8);
        Ok(())
    }
}

impl TypedParam for i16 {
    fn encode_type(out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_SHORT as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_2(out, *self as u16);
        Ok(())
    }
}

impl TypedParam for i32 {
    fn encode_type(out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_LONG as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_4(out, *self as u32);
        Ok(())
    }
}

impl TypedParam for i64 {
    fn encode_type(out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_LONGLONG as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_8(out, *self as u64);
        Ok(())
    }
}

impl TypedParam for u8 {
    fn encode_type(out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_TINY as u8);
        out.push(0x80);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_1(out, *self);
        Ok(())
    }
}

impl TypedParam for u16 {
    fn encode_type(out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_SHORT as u8);
        out.push(0x80);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_2(out, *self);
        Ok(())
    }
}

impl TypedParam for u32 {
    fn encode_type(out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_LONG as u8);
        out.push(0x80);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_4(out, *self);
        Ok(())
    }
}

impl TypedParam for u64 {
    fn encode_type(out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_LONGLONG as u8);
        out.push(0x80);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_8(out, *self);
        Ok(())
    }
}

impl TypedParam for f32 {
    fn encode_type(out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_FLOAT as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_4(out, self.to_bits());
        Ok(())
    }
}

impl TypedParam for f64 {
    fn encode_type(out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_DOUBLE as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_int_8(out, self.to_bits());
        Ok(())
    }
}

impl TypedParam for &str {
    fn encode_type(out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_VAR_STRING as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_string_lenenc(out, self);
        Ok(())
    }
}

impl TypedParam for String {
    fn encode_type(out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_VAR_STRING as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_string_lenenc(out, self);
        Ok(())
    }
}

impl TypedParam for &String {
    fn encode_type(out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_VAR_STRING as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_string_lenenc(out, self);
        Ok(())
    }
}

impl TypedParam for &[u8] {
    fn encode_type(out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_BLOB as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_bytes_lenenc(out, self);
        Ok(())
    }
}

impl TypedParam for Vec<u8> {
    fn encode_type(out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_BLOB as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_bytes_lenenc(out, self);
        Ok(())
    }
}

impl TypedParam for &Vec<u8> {
    fn encode_type(out: &mut Vec<u8>) {
        out.push(ColumnType::MYSQL_TYPE_BLOB as u8);
        out.push(0x00);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        write_bytes_lenenc(out, self);
        Ok(())
    }
}

impl<T: TypedParam> TypedParam for Option<T> {
    fn is_null(&self) -> bool {
        self.is_none()
    }

    fn encode_type(out: &mut Vec<u8>) {
        T::encode_type(out);
    }

    fn encode_value(&self, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Some(value) => value.encode_value(out),
            None => Ok(()),
        }
    }
}

// ============================================================================
// Params trait - for collections of parameters
// ============================================================================

/// Trait for parameter binding in prepared statements
///
/// This trait is implemented by external libraries to provide a custom parameter serialization.
pub trait Params {
    /// Number of parameters
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Write NULL bitmap
    ///
    /// The NULL bitmap is (num_params + 7) / 8 bytes long.
    /// Bit is set to 1 if the parameter is NULL.
    fn encode_null_bitmap(&self, out: &mut Vec<u8>);

    /// Write parameter types
    ///
    /// Each parameter type is 2 bytes:
    /// - 1 byte: MySQL type (MYSQL_TYPE_*)
    /// - 1 byte: unsigned flag (0x80 if unsigned, 0x00 otherwise)
    fn encode_types(&self, out: &mut Vec<u8>);

    /// Write parameter values (binary encoded)
    ///
    /// Values are encoded according to MySQL binary protocol.
    /// NULL parameters should be skipped (they're already in the NULL bitmap).
    fn encode_values(&self, out: &mut Vec<u8>) -> Result<()>;

    /// Write parameter values for bulk execution (COM_STMT_BULK_EXECUTE)
    ///
    /// Format:
    /// - First: parameter indicators (1 byte per parameter)
    /// - Then: values (only for parameters with indicator None)
    ///
    /// See: https://mariadb.com/docs/server/reference/clientserver-protocol/3-binary-protocol-prepared-statements/com_stmt_bulk_execute
    fn encode_values_for_bulk(&self, out: &mut Vec<u8>) -> Result<()>;
}

#[auto_impl(&)]
pub trait TypedParams {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn encode_null_bitmap(&self, out: &mut Vec<u8>);
    fn encode_types(out: &mut Vec<u8>);
    fn encode_values(&self, out: &mut Vec<u8>) -> Result<()>;
    fn encode_values_for_bulk(&self, out: &mut Vec<u8>) -> Result<()>;
}

impl<T: TypedParams> Params for T {
    fn len(&self) -> usize {
        TypedParams::len(self)
    }
    fn encode_null_bitmap(&self, out: &mut Vec<u8>) {
        TypedParams::encode_null_bitmap(self, out)
    }
    fn encode_types(&self, out: &mut Vec<u8>) {
        T::encode_types(out)
    }
    fn encode_values(&self, out: &mut Vec<u8>) -> Result<()> {
        TypedParams::encode_values(self, out)
    }
    fn encode_values_for_bulk(&self, out: &mut Vec<u8>) -> Result<()> {
        TypedParams::encode_values_for_bulk(self, out)
    }
}

impl TypedParams for () {
    fn len(&self) -> usize {
        0
    }
    fn encode_null_bitmap(&self, _out: &mut Vec<u8>) {}
    fn encode_types(_out: &mut Vec<u8>) {}
    fn encode_values(&self, _out: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }
    fn encode_values_for_bulk(&self, _out: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }
}

// ============================================================================
// Tuple implementations for common sizes
// ============================================================================

macro_rules! impl_params_for_tuple {
    ($($T:ident : $idx:tt),+) => {
        impl<$($T: TypedParam),+> TypedParams for ($($T,)+) {
            fn len(&self) -> usize {
                let mut count = 0;
                $(
                    let _ = &self.$idx;
                    count += 1;
                )+
                count
            }

            fn encode_null_bitmap(&self, out: &mut Vec<u8>) {
                let num_bytes = TypedParams::len(self).div_ceil(8);
                let start_len = out.len();
                out.resize(start_len + num_bytes, 0);

                $(
                    if self.$idx.is_null() {
                        let byte_pos = start_len + ($idx >> 3);
                        let bit_offset = $idx & 7;
                        out[byte_pos] |= 1 << bit_offset;
                    }
                )+
            }

            fn encode_types(out: &mut Vec<u8>) {
                $(
                    $T::encode_type(out);
                )+
            }

            fn encode_values(&self, out: &mut Vec<u8>) -> Result<()> {
                $(
                    if !self.$idx.is_null() {
                        self.$idx.encode_value(out)?;
                    }
                )+
                Ok(())
            }

            fn encode_values_for_bulk(&self, out: &mut Vec<u8>) -> Result<()> {
                $(
                    if self.$idx.is_null() {
                        out.push(ParamIndicator::Null as u8);
                    } else {
                        out.push(ParamIndicator::None as u8);
                        self.$idx.encode_value(out)?;
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
