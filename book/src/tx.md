# Transaction

Transactions ensure a group of operations either all succeed (commit) or all fail (rollback).

## Using Transactions

```rust,ignore
use zero_mysql::sync::Conn;

conn.run_transaction(|conn, tx| {
    conn.query_drop("INSERT INTO users (name) VALUES ('Alice')")?;
    conn.query_drop("INSERT INTO users (name) VALUES ('Bob')")?;
    tx.commit(conn)
})?;
```

## Automatic Rollback

If neither `commit()` nor `rollback()` is called, the transaction automatically rolls back when the closure returns:

```rust,ignore
conn.run_transaction(|conn, tx| {
    conn.query_drop("INSERT INTO users (name) VALUES ('Alice')")?;
    // Returns error - transaction will be rolled back
    Err(Error::BadUsageError("oops".to_string()))
})?;
// No data inserted
```

## Explicit Rollback

```rust,ignore
conn.run_transaction(|conn, tx| {
    conn.query_drop("INSERT INTO users (name) VALUES ('Alice')")?;

    if some_condition {
        tx.commit(conn)
    } else {
        tx.rollback(conn)
    }
})?;
```

## Nested Transactions

Nested transactions are not supported. Calling `run_transaction` while already in a transaction returns `Error::NestedTransaction`.

## Async Transactions

For async connections, use the same pattern:

```rust,ignore
use zero_mysql::tokio::Conn;

conn.run_transaction(|conn, tx| async move {
    conn.query_drop("INSERT INTO users (name) VALUES ('Alice')").await?;
    tx.commit(conn).await
}).await?;
```
