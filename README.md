# zero-mysql

A high-performance MySQL client library for Rust.

**Requires Rust nightly.**

Python binding: [pyro-mysql](https://github.com/elbaro/pyro-mysql/)

## Feature Flags
- `sync` (default): synchronous API.
- `tokio` (default): asynchronous API.
- `sync-tls`: TLS support for synchronous API. (experimental)
- `tokio-tls`: TLS support for asynchronous API. (experimental)

## Features
- zero-copy whenever possible
- zero-allocation in hot paths
- customizable de/serialization
    - user can process a row and throw it away without collecting into Vec
    - user can deserialize bytes to PyString without intermediate objects
    - user can even choose not to parse the packets
    - for performance-critical query, you can define `#[repr(C)] struct Row` and transmute network packets into it

## Perf Notes
- Prefer MariaDB to MySQL
- Prefer UnixSocket to TCP
- Set `Opts.upgrade_to_unix_socket=false` and manually set the socket path
- Use Conn.exec_bulk_insert_or_update to group 2~1000 INSERTTs or UPDATEEs
