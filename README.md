# zero-mysql

`zero-mysql` is a high-performance MySQL library.

Each submodule is behind a feature gate.
- `zero_mysql::protocol`: implements sans I/O for MySQL protocol.
- `zero_mysql::sync`: implements a synchronous `Conn`.
- `zero_mysql::tokio`: implements an asynchronous `Conn`.
- `zero_mysql::compio`: implements an asynchronous `Conn` (the API returns `!Send` futures).

Its test, benchmark and feature development is driven by the need of [pyro-mysql](https://github.com/elbaro/pyro-mysql/).

## Features
- zero-copy whenever possible
- zero-allocation in hot paths
- customizable de/serialization
    - user can process a row and throw it away without collecting into Vec
    - user can deserialize bytes to PyString without intermediate objects
    - user can choose even not to parse the packets
    - for performance-critical location, you can define `#[repr(C)] struct Row` and transmute network packets into it

### Not Implemented (Yet)
- Sequence ID verification
- SSL/TLS
- Authentication plugins other than username/password
- Old protocol (ColumnDefinition320 or MySQL 5.x)
