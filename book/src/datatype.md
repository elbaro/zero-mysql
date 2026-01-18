# Data Type

This section describes the mapping between Rust types and MySQL types.

## Design Philosophy

**No lossy conversions are allowed.** The library intentionally rejects conversions that could silently lose data. For example, reading a `BIGINT` column as `u8` will return an error rather than truncating the value. This ensures data integrity and makes bugs easier to catch.

Widening conversions (e.g., reading `TINYINT` as `i64`) are allowed because they never lose data.

## Parameter Types (Rust to MySQL)

| Rust Type | MySQL Type | Notes |
|-----------|------------|-------|
| `bool` | `TINYINT` | Encoded as 0 or 1 |
| `i8` | `TINYINT` | Signed |
| `i16` | `SMALLINT` | Signed |
| `i32` | `INT` | Signed |
| `i64` | `BIGINT` | Signed |
| `u8` | `TINYINT UNSIGNED` | |
| `u16` | `SMALLINT UNSIGNED` | |
| `u32` | `INT UNSIGNED` | |
| `u64` | `BIGINT UNSIGNED` | |
| `f32` | `FLOAT` | |
| `f64` | `DOUBLE` | |
| `&str` | `VARCHAR` | |
| `String` | `VARCHAR` | |
| `&[u8]` | `BLOB` | Binary data |
| `Vec<u8>` | `BLOB` | Binary data |
| `Option<T>` | Same as `T` | `None` encodes as `NULL` |

### Example

```rust,ignore
let mut stmt = conn.prepare("INSERT INTO users (name, age, active) VALUES (?, ?, ?)")?;
conn.exec_drop(&mut stmt, ("Alice", 30i32, true))?;

// Using Option for nullable columns
conn.exec_drop(&mut stmt, ("Bob", 25i32, None::<bool>))?;
```

## Result Types (MySQL to Rust)

Signed and unsigned types are strictly separated. You cannot decode a signed column (e.g., `TINYINT`) to an unsigned Rust type (e.g., `u8`), or vice versa.

| MySQL Type | Rust Types |
|------------|------------|
| `TINYINT` | `i8`, `i16`, `i32`, `i64`, `bool` |
| `SMALLINT` | `i16`, `i32`, `i64` |
| `MEDIUMINT`, `INT` | `i32`, `i64` |
| `BIGINT` | `i64` |
| `TINYINT UNSIGNED` | `u8`, `u16`, `u32`, `u64`, `bool` |
| `SMALLINT UNSIGNED` | `u16`, `u32`, `u64` |
| `MEDIUMINT UNSIGNED`, `INT UNSIGNED` | `u32`, `u64` |
| `BIGINT UNSIGNED` | `u64` |
| `FLOAT` | `f32`, `f64` |
| `DOUBLE` | `f64` |
| `VARCHAR`, `CHAR`, `TEXT`, etc. | `&str`, `String` |
| `BLOB`, `BINARY`, `VARBINARY`, etc. | `&[u8]`, `Vec<u8>` |
| `NULL` | `Option<T>` |

### Example

```rust,ignore
// Reading exact types
let (id, name): (i64, String) = conn.exec_first(&mut stmt, ())?.unwrap();

// Widening conversion: TINYINT -> i64 is allowed
let count: i64 = conn.exec_first(&mut stmt, ())?.unwrap();

// Using Option for nullable columns
let email: Option<String> = conn.exec_first(&mut stmt, ())?.unwrap();
```

## Conversion Errors

When a conversion is not allowed, you'll get a clear error message:

```rust,ignore
// This will fail with an error like:
// "Cannot decode MySQL type BIGINT (i64) to u8"
let value: u8 = conn.exec_first(&mut stmt, ())?;
```

The error message includes both the source MySQL type and the target Rust type, making it easy to diagnose the issue.

## Date and Time Types

MySQL date/time types are exposed through the `Value` enum for zero-copy access:

| MySQL Type | `Value` Variant | Description |
|------------|-----------------|-------------|
| `DATE` | `Date0`, `Date4` | Date without time |
| `DATETIME`, `TIMESTAMP` | `Datetime0`, `Datetime4`, `Datetime7`, `Datetime11` | Date with time |
| `TIME` | `Time0`, `Time8`, `Time12` | Time or duration |

The numeric suffix indicates the wire format byte length. Different lengths are used depending on whether the value includes sub-second precision.

### Example: Reading Date/Time

```rust,ignore
use zero_mysql::value::{Value, Timestamp7};

conn.exec_foreach(&mut stmt, (), |row: (Value,)| {
    match row.0 {
        Value::Datetime7(ts) => {
            println!("{}-{:02}-{:02} {:02}:{:02}:{:02}",
                ts.year(), ts.month, ts.day,
                ts.hour, ts.minute, ts.second);
        }
        Value::Null => println!("NULL"),
        _ => println!("Other type"),
    }
    Ok(())
})?;
```

## DECIMAL Type

`DECIMAL` and `NUMERIC` columns are returned as byte slices containing the string representation. This preserves full precision and allows you to use your preferred decimal library:

```rust,ignore
use std::str::from_utf8;

conn.exec_foreach(&mut stmt, (), |row: (Value,)| {
    if let Value::Byte(bytes) = row.0 {
        let decimal_str = from_utf8(bytes)?;
        // Parse with your preferred library
        // let d: rust_decimal::Decimal = decimal_str.parse()?;
    }
    Ok(())
})?;
```
