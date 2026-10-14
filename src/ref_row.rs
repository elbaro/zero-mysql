//! Zero-copy row decoding for fixed-size types.
//!
//! This module provides traits and types for zero-copy decoding of database rows
//! where all fields have fixed wire sizes. This is useful for high-performance
//! scenarios where avoiding allocations is critical.
//!
//! # Requirements
//!
//! - The struct derives `zerocopy::{FromBytes, KnownLayout, Immutable}` alongside `RefFromRow`
//! - All struct fields must implement `FixedWireSize`
//! - All columns must be `NOT NULL` (no `Option<T>` support)
//! - Struct must use `#[repr(C, packed)]` for predictable layout
//! - Fields must use endian-aware types (e.g., `I64LE` instead of `i64`)

use zerocopy::{FromBytes, Immutable, KnownLayout};

use crate::constant::ColumnType;
use crate::error::{Error, Result, eyre};
use crate::protocol::BinaryRowPayload;
use crate::protocol::command::ColumnDefinition;

/// Marker trait for types with a fixed wire size in MySQL binary protocol.
///
/// This trait is only implemented for types that have a guaranteed fixed size
/// on the wire. Native integer types like `i64` are NOT implemented because
/// MySQL uses little-endian encoding, which differs from native byte order on
/// big-endian platforms.
///
/// Use zerocopy's little-endian types instead:
/// - `zerocopy::little_endian::I16` instead of `i16`
/// - `zerocopy::little_endian::I32` instead of `i32`
/// - `zerocopy::little_endian::I64` instead of `i64`
/// - etc.
pub trait FixedWireSize {
    /// The fixed size in bytes on the wire.
    const WIRE_SIZE: usize;
}

// Single-byte types are endian-agnostic
impl FixedWireSize for i8 {
    const WIRE_SIZE: usize = 1;
}
impl FixedWireSize for u8 {
    const WIRE_SIZE: usize = 1;
}

// Little-endian integer types (MySQL wire format)
impl FixedWireSize for zerocopy::little_endian::I16 {
    const WIRE_SIZE: usize = 2;
}
impl FixedWireSize for zerocopy::little_endian::U16 {
    const WIRE_SIZE: usize = 2;
}
impl FixedWireSize for zerocopy::little_endian::I32 {
    const WIRE_SIZE: usize = 4;
}
impl FixedWireSize for zerocopy::little_endian::U32 {
    const WIRE_SIZE: usize = 4;
}
impl FixedWireSize for zerocopy::little_endian::I64 {
    const WIRE_SIZE: usize = 8;
}
impl FixedWireSize for zerocopy::little_endian::U64 {
    const WIRE_SIZE: usize = 8;
}

// Re-export little-endian types for convenience
pub use zerocopy::little_endian::{
    I16 as I16LE, I32 as I32LE, I64 as I64LE, U16 as U16LE, U32 as U32LE, U64 as U64LE,
};

/// Trait for zero-copy decoding of a row into a fixed-size struct.
///
/// Unlike `FromRow`, this trait returns a reference directly into the buffer
/// without any copying or allocation. This requires:
///
/// 1. All fields have fixed wire sizes (implement `FixedWireSize`)
/// 2. No NULL values (columns must be `NOT NULL`)
/// 3. Struct has `#[repr(C, packed)]` layout
/// 4. Struct derives `zerocopy::{FromBytes, KnownLayout, Immutable}`
///
/// # Example
///
/// ```
/// use zero_mysql::ref_row::{I32LE, I64LE};
/// use zero_mysql_derive::RefFromRow;
/// use zerocopy::{FromBytes, Immutable, KnownLayout};
///
/// #[derive(FromBytes, KnownLayout, Immutable, RefFromRow)]
/// #[repr(C, packed)]
/// struct UserStats {
///     user_id: I64LE,
///     login_count: I32LE,
/// }
/// ```
///
/// # Compile-fail tests
///
/// Missing zerocopy derives:
/// ```compile_fail,E0277
/// use zero_mysql::ref_row::I32LE;
/// use zero_mysql_derive::RefFromRow;
///
/// #[derive(RefFromRow)]
/// #[repr(C, packed)]
/// struct Invalid {
///     value: I32LE,
/// }
/// ```
///
/// Missing `#[repr(C, packed)]`:
/// ```compile_fail
/// use zero_mysql::ref_row::I32LE;
/// use zero_mysql_derive::RefFromRow;
/// use zerocopy::{FromBytes, Immutable, KnownLayout};
///
/// #[derive(FromBytes, KnownLayout, Immutable, RefFromRow)]
/// struct Invalid {
///     value: I32LE,
/// }
/// ```
///
/// Native integer types (must use little-endian wrappers):
/// ```compile_fail,E0277
/// use zero_mysql_derive::RefFromRow;
/// use zerocopy::{FromBytes, Immutable, KnownLayout};
///
/// #[derive(FromBytes, KnownLayout, Immutable, RefFromRow)]
/// #[repr(C, packed)]
/// struct Invalid {
///     value: i64,
/// }
/// ```
pub trait RefFromRow<'buf>: Sized {
    /// Decode a row as a zero-copy reference.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The column count doesn't match the field count
    /// - Any column is NULL (RefFromRow doesn't support NULL)
    /// - Any column's type is not an integer of its field's size
    fn ref_from_row(
        cols: &[ColumnDefinition<'_>],
        row: BinaryRowPayload<'buf>,
    ) -> Result<&'buf Self>;
}

/// Checks every column of `row` against `wire_sizes`, then casts the row to `T`.
#[doc(hidden)]
pub fn ref_from_binary_row<'buf, T>(
    cols: &[ColumnDefinition<'_>],
    row: BinaryRowPayload<'buf>,
    wire_sizes: &[usize],
) -> Result<&'buf T>
where
    T: FromBytes + KnownLayout + Immutable,
{
    if cols.len() != wire_sizes.len() {
        return Err(Error::BadUsageError(format!(
            "RefFromRow expects {} columns, got {}",
            wire_sizes.len(),
            cols.len()
        )));
    }

    let null_bitmap = row.null_bitmap();
    for (index, (col, &wire_size)) in cols.iter().zip(wire_sizes).enumerate() {
        if null_bitmap.is_null(index) {
            return Err(Error::BadUsageError(format!(
                "RefFromRow: column {index} is NULL"
            )));
        }
        let column_type = col.tail.column_type()?;
        if integer_wire_size(column_type) != Some(wire_size) {
            return Err(Error::BadUsageError(format!(
                "RefFromRow: column {index} has type {column_type:?}, expected a {wire_size}-byte integer"
            )));
        }
    }

    let data = row.values();
    let expected: usize = wire_sizes.iter().sum();
    if data.len() != expected {
        return Err(Error::BadUsageError(format!(
            "RefFromRow: row has {} bytes, expected {expected}",
            data.len()
        )));
    }

    T::ref_from_bytes(data).map_err(|e| Error::LibraryBug(eyre!("RefFromRow cast: {e}")))
}

/// Binary protocol size of an integer column type.
fn integer_wire_size(column_type: ColumnType) -> Option<usize> {
    match column_type {
        ColumnType::MYSQL_TYPE_TINY => Some(1),
        ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => Some(2),
        ColumnType::MYSQL_TYPE_INT24 | ColumnType::MYSQL_TYPE_LONG => Some(4),
        ColumnType::MYSQL_TYPE_LONGLONG => Some(8),
        _ => None,
    }
}
