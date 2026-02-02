# Zero-Copy Decoding

For maximum performance, `zero-mysql` provides zero-copy row decoding through the `RefFromRow` trait. This allows you to decode rows as references directly into the read buffer, avoiding any memory allocation or copying.

## When to Use

Zero-copy decoding is useful when:
- Processing large result sets where allocation overhead matters
- All columns are fixed-size types (integers, floats)
- All columns are `NOT NULL`
- You don't need to store the decoded rows (processing in a callback)

## Requirements

To use zero-copy decoding, your struct must:

1. Derive `RefFromRow`
2. Have `#[repr(C, packed)]` attribute
3. Use little-endian types from `zerocopy` (MySQL uses little-endian wire format)

## Example

```rust
use zero_mysql::ref_row::{RefFromRow, I64LE, I32LE};
use zero_mysql_derive::RefFromRow;

#[derive(RefFromRow)]
#[repr(C, packed)]
struct UserStats {
    user_id: I64LE,      // 8 bytes
    login_count: I32LE,  // 4 bytes
}

// Process rows without allocation
conn.exec_foreach_ref::<UserStats, _, _>(&mut stmt, (), |row| {
    // row is &UserStats - a reference into the buffer
    println!("user_id: {}", row.user_id.get());
    println!("login_count: {}", row.login_count.get());
    Ok(())
})?;
```

## Available Types

MySQL uses little-endian encoding on the wire. Use these types:

| Rust Type | Wire Size | Description |
|-----------|-----------|-------------|
| `i8` / `u8` | 1 byte | Single-byte (endian-agnostic) |
| `I16LE` / `U16LE` | 2 bytes | 16-bit little-endian |
| `I32LE` / `U32LE` | 4 bytes | 32-bit little-endian |
| `I64LE` / `U64LE` | 8 bytes | 64-bit little-endian |

These are re-exported from `zero_mysql::ref_row` for convenience.

## Accessing Values

The endian-aware types provide a `.get()` method to convert to native integers:

```rust
let user_id: i64 = row.user_id.get();
let count: i32 = row.login_count.get();
```

On little-endian platforms (x86, ARM), `.get()` is a no-op and compiles to zero instructions.

## Limitations

- **No NULL support**: All columns must be `NOT NULL`. Use `FromRow` for nullable columns.
- **Fixed-size types only**: Variable-length types like `VARCHAR`, `TEXT`, `BLOB` are not supported.
- **No column name matching**: Columns must match struct field order exactly.
- **Callback-based only**: Returns references into the buffer, so can only be used with `exec_foreach_ref`.

## Comparison with FromRow

| Feature | `FromRow` | `RefFromRow` |
|---------|-----------|--------------|
| Allocation | Yes (per row) | No |
| NULL support | Yes (`Option<T>`) | No |
| Variable-length types | Yes | No |
| Column name matching | Yes | No |
| Return type | Owned `T` | Reference `&T` |
| API | `exec_first`, `exec_collect`, `exec_foreach` | `exec_foreach_ref` |

## How It Works

1. The derive macro generates `zerocopy` trait implementations (`FromBytes`, `KnownLayout`, `Immutable`)
2. At compile time, it verifies all fields implement `FixedWireSize`
3. At runtime, the row buffer is cast directly to `&YourStruct` using zerocopy
4. No parsing, no allocation - just a pointer cast with size validation
