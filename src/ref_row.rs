//! Zero-copy row decoding for fixed-size types.
//!
//! This module provides traits and types for zero-copy decoding of database rows
//! where all fields have fixed wire sizes. This is useful for high-performance
//! scenarios where avoiding allocations is critical.
//!
//! # Requirements
//!
//! - All struct fields must implement `FixedWireSize`
//! - All columns must be `NOT NULL` (no `Option<T>` support)
//! - Struct must use `#[repr(C, packed)]` for predictable layout
//! - Fields must use endian-aware types (e.g., `I64LE` instead of `i64`)
//!
//! # Example
//!
//! ```ignore
//! use zerocopy::little_endian::{I64 as I64LE, I32 as I32LE};
//! use zero_mysql::ref_row::RefFromRow;
//!
//! #[derive(RefFromRow)]
//! #[repr(C, packed)]
//! struct UserStats {
//!     user_id: I64LE,
//!     login_count: I32LE,
//! }
//! ```

use crate::error::Result;
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
///
/// The derive macro generates zerocopy trait implementations automatically.
pub trait RefFromRow<'buf>: Sized {
    /// Decode a row as a zero-copy reference.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The row data size doesn't match the struct size
    /// - Any column is NULL (RefFromRow doesn't support NULL)
    fn ref_from_row(
        cols: &[ColumnDefinition<'_>],
        row: BinaryRowPayload<'buf>,
    ) -> Result<&'buf Self>;
}
