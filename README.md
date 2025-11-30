# zero-mysql

`zero-mysql` is a high-performance MySQL library.

Each submodule is behind a feature gate.
- `zero_mysql::protocol`: implements sans I/O for MySQL protocol.
- `zero_mysql::sync`: implements a synchronous `Conn`.
- `zero_mysql::tokio`: implements an asynchronous `Conn`.

Its test, benchmark and feature development is driven by the need of [pyro-mysql](https://github.com/elbaro/pyro-mysql/).

## Features
- zero-copy whenever possible
- zero-allocation in hot paths
- customizable de/serialization
    - user can process a row and throw it away without collecting into Vec
    - user can deserialize bytes to PyString without intermediate objects
    - user can choose even not to parse the packets
    - for performance-critical query, you can define `#[repr(C)] struct Row` and transmute network packets into it

### Not Implemented (Yet)
- Custom max_allowed_packet
- Sequence ID verification
- Authentication plugins other than username/password

## Perf Notes
- Prefer MariaDB to MySQL
- Prefer UnixSocket to TCP
- Use BufferPool to reuse allocations between connections
- Use Conn.exec_bulk to group 2~1000 INSERTs or UPDATEs
