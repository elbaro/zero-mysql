# zero-mysql

A high-performance MySQL client library for Rust.

[API Reference (docs.rs)](https://docs.rs/zero-mysql) | [User Guide](https://elbaro.github.io/zero-mysql/)

Python binding: [pyro-mysql](https://github.com/elbaro/pyro-mysql/)

## Feature Flags
- `sync` (default): synchronous API
- `tokio` (default): asynchronous API
- `sync-tls`: TLS support for synchronous API (experimental)
- `tokio-tls`: TLS support for asynchronous API (experimental)
- `experimental-compio`: asynchronous API using compio (io_uring)
- `compio-tls`: TLS support for compio API
- `experimental-diesel`: Diesel ORM backend using zero-mysql as the underlying connection

## Perf Notes
- Prefer MariaDB to MySQL
- Prefer UnixSocket to TCP
- Set `Opts.upgrade_to_unix_socket=false` and manually set the socket path
- Use Conn.exec_bulk_insert_or_update to group 2~1000 `INSERT`s or `UPDATE`s
