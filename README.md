 zero-mysql

A high-performance MySQL client library for Rust.

[API Reference (docs.rs)](https://docs.rs/zero-mysql) | [User Guide](https://elbaro.github.io/zero-mysql/)

Python binding: [pyro-mysql](https://github.com/elbaro/pyro-mysql/)

## Feature Flags
- `sync` (default): synchronous API
- `tokio` (default): asynchronous API
- `derive` (default): `#[derive(FromRow)]` and `#[derive(RefFromRow)]` macros
- `compio`: asynchronous API using compio (experimental)
- `sync-tls`: TLS support for synchronous API (experimental)
- `tokio-tls`: TLS support for tokio (experimental)
- `compio-tls`: TLS support for compio (experimental)
- `diesel`: Diesel support (experimental)

TLS flags use `native-tls`.

[External type supports](https://elbaro.github.io/zero-mysql/datatype.html#feature-gated-types):
- `with-chrono` - Support [chrono](https://crates.io/crates/chrono) date/time types
- `with-time` - Support [time](https://crates.io/crates/time) date/time types
- `with-uuid` - Support [uuid](https://crates.io/crates/uuid) types
- `with-rust-decimal` - Support [rust_decimal](https://crates.io/crates/rust_decimal) types

## Perf Notes
- Prefer MariaDB to MySQL
- Prefer UnixSocket to TCP
- Set `Opts.upgrade_to_unix_socket=false` and manually set the socket path
- Use Conn.exec_bulk_insert_or_update to group 2~1000 `INSERT`s or `UPDATE`s
