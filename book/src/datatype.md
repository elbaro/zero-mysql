# Data Type

The library intentionally rejects conversions that could silently lose data. For example, reading a `BIGINT` column as `i8` will return an error rather than truncating the value. This ensures data integrity and makes bugs easier to catch.

Widening conversions (e.g., reading `TINYINT` as `i64`) are allowed.

## Parameter Types (Rust to MySQL)

| Rust Type | MySQL Type | Notes |
|-----------|------------|-------|
| `bool` | `TINYINT` | Encoded as 0 or 1 |
| `i8` | `TINYINT` | |
| `i16` | `SMALLINT` | |
| `i32` | `INT` | |
| `i64` | `BIGINT` | |
| `u8` | `TINYINT UNSIGNED` | |
| `u16` | `SMALLINT UNSIGNED` | |
| `u32` | `INT UNSIGNED` | |
| `u64` | `BIGINT UNSIGNED` | |
| `f32` | `FLOAT` | |
| `f64` | `DOUBLE` | |
| `&str` | `VARCHAR` | |
| `String` | `VARCHAR` | |
| `&[u8]` | `BLOB` | |
| `Vec<u8>` | `BLOB` | |
| `Option<T>` | Same as `T` | `None` encodes as `NULL` |

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

## Date and Time Types

Date/time types are exposed through the `Value` enum:

| MySQL Type | `Value` Variants |
|------------|------------------|
| `DATE` | `Date0`, `Date4` |
| `DATETIME`, `TIMESTAMP` | `Datetime0`, `Datetime4`, `Datetime7`, `Datetime11` |
| `TIME` | `Time0`, `Time8`, `Time12` |

The numeric suffix indicates the wire format byte length.

## DECIMAL Type

`DECIMAL` and `NUMERIC` columns are returned as `Value::Byte` containing the string representation.

## Feature-Gated Types

Additional type support is available through feature flags.

### `with-uuid` (uuid crate)

| Rust Type | MySQL Type | Notes |
|-----------|------------|-------|
| `uuid::Uuid` | `VARCHAR(36)` | Encoded as hyphenated string |

| MySQL Type | Rust Type | Notes |
|------------|-----------|-------|
| `VARCHAR`, `CHAR` | `uuid::Uuid` | Parsed from hyphenated string |
| `BINARY(16)` | `uuid::Uuid` | Parsed from 16 bytes |

### `with-chrono` (chrono crate)

| Rust Type | MySQL Type |
|-----------|------------|
| `chrono::NaiveDate` | `DATE` |
| `chrono::NaiveTime` | `TIME` |
| `chrono::NaiveDateTime` | `DATETIME` |

| MySQL Type | Rust Type | Notes |
|------------|-----------|-------|
| `DATE` | `chrono::NaiveDate` | Zero dates (`0000-00-00`) return an error |
| `TIME` | `chrono::NaiveTime` | Negative times or times with days return an error |
| `DATETIME`, `TIMESTAMP` | `chrono::NaiveDateTime` | Zero datetimes return an error |

### `with-time` (time crate)

| Rust Type | MySQL Type |
|-----------|------------|
| `time::Date` | `DATE` |
| `time::Time` | `TIME` |
| `time::PrimitiveDateTime` | `DATETIME` |

| MySQL Type | Rust Type | Notes |
|------------|-----------|-------|
| `DATE` | `time::Date` | Zero dates return an error |
| `TIME` | `time::Time` | Negative times or times with days return an error |
| `DATETIME`, `TIMESTAMP` | `time::PrimitiveDateTime` | Zero datetimes return an error |

### `with-rust-decimal` (rust_decimal crate)

| Rust Type | MySQL Type |
|-----------|------------|
| `rust_decimal::Decimal` | `DECIMAL` |

| MySQL Type | Rust Type | Notes |
|------------|-----------|-------|
| `DECIMAL` | `rust_decimal::Decimal` | 96-bit precision, not arbitrary like MySQL |
