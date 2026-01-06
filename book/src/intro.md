# Introduction

zero-mysql is a high-performance MySQL client library for Rust.

```toml
[dependencies]
zero-mysql = "0.2"
```

**Requires Rust nightly.**

## Quick Start

```rust,ignore
use zero_mysql::sync::Conn;

let mut conn = Conn::new("mysql://user:password@localhost/mydb")?;

// Text protocol query
conn.query_drop("INSERT INTO users (name) VALUES ('Alice')")?;

// Prepared statement
let mut stmt = conn.prepare("SELECT * FROM users WHERE id = ?")?;
conn.exec_drop(&mut stmt, (42,))?;

// Transaction
conn.run_transaction(|conn, tx| {
    conn.query_drop("INSERT INTO users (name) VALUES ('Bob')")?;
    tx.commit(conn)
})?;
```

## Features

- **Zero-Copy**: Minimal allocations and copies in hot paths
- **Zero-Allocation**: Reuse buffers across queries
- **Sync and Async**: Both `sync` and `tokio` modules available
- **Binary Protocol**: Prepared statements with automatic caching
- **MariaDB Bulk Execution**: Single round-trip bulk operations
- **Customizable Deserialization**: Process rows without intermediate allocations

## Feature Flags

- `sync` (default): Synchronous API
- `tokio` (default): Asynchronous API with Tokio
- `sync-tls`: TLS support for synchronous API (experimental)
- `tokio-tls`: TLS support for asynchronous API (experimental)

## Limitations

- **No Streaming**: All results are fetched into memory
- **Nightly Rust Required**: Uses unstable features for performance
