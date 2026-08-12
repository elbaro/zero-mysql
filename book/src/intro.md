# Introduction

zero-mysql is a high-performance MySQL client library for Rust.

```toml
[dependencies]
zero-mysql = "*"
```

See [Feature Flags in the README](https://github.com/elbaro/zero-mysql#feature-flags).

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
conn.transaction(|conn, _tx| {
    conn.query_drop("INSERT INTO users (name) VALUES ('Bob')")?;
    Ok(())
})?;
```
