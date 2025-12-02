# zero-mysql

`zero-mysql` is a high-performance MySQL library.

- `zero_mysql::protocol`: implements sans I/O for MySQL protocol.
- `zero_mysql::sync`: implements a synchronous `Conn`.
- `zero_mysql::tokio`: implements an asynchronous `Conn`.

The test, benchmark and feature development is driven by the need of [pyro-mysql](https://github.com/elbaro/pyro-mysql/).

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
- Use Conn.exec_bulk to group 2~1000 INSERTs or UPDATEs
