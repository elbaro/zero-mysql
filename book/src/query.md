# Query

There are two sets of query APIs: Text Protocol and Binary Protocol.

## Text Protocol

Text protocol is simple and supports multiple statements separated by `;`, but does not support parameter binding.
Use binary protocol if you need to send parameters or read typed results.

```rust,ignore
impl Conn {
    fn query<H: TextResultSetHandler>(&mut self, sql: &str, handler: &mut H) -> Result<()>;
    fn query_drop(&mut self, sql: &str) -> Result<()>;
}
```

- `query`: executes SQL and processes results with a handler
- `query_drop`: executes SQL and discards the result

### Example

```rust,ignore
conn.query_drop("INSERT INTO users (name) VALUES ('Alice')")?;
conn.query_drop("DELETE FROM users WHERE id = 1")?;
```

## Binary Protocol

Binary protocol uses prepared statements with parameter binding. Use `?` as the placeholder.

```rust,ignore
impl Conn {
    fn prepare(&mut self, sql: &str) -> Result<PreparedStatement>;
    fn exec<P, H>(&mut self, stmt: &mut PreparedStatement, params: P, handler: &mut H) -> Result<()>;
    fn exec_drop<P>(&mut self, stmt: &mut PreparedStatement, params: P) -> Result<()>;
    fn exec_first<Row, P>(&mut self, stmt: &mut PreparedStatement, params: P) -> Result<Option<Row>>;
    fn exec_collect<Row, P>(&mut self, stmt: &mut PreparedStatement, params: P) -> Result<Vec<Row>>;
    fn exec_foreach<Row, P, F>(&mut self, stmt: &mut PreparedStatement, params: P, f: F) -> Result<()>;
    fn exec_bulk_insert_or_update<P, I, H>(...) -> Result<()>;
}
```

- `prepare`: prepare a statement for execution
- `exec`: execute a prepared statement with a handler
- `exec_drop`: execute and discard all results
- `exec_first`: execute and return `Option<Row>` for the first row
- `exec_collect`: execute and collect all rows into a Vec
- `exec_foreach`: execute and call a closure for each row
- `exec_bulk_insert_or_update`: bulk execution (uses MariaDB bulk command extension; falls back to multiple `exec()` calls on Oracle MySQL)

### Example: Basic

```rust,ignore
// Prepare a statement
let mut stmt = conn.prepare("SELECT * FROM users WHERE id = ?")?;

// Execute with parameters
conn.exec_drop(&mut stmt, (42,))?;

// Execute with different parameters (statement is reused)
conn.exec_drop(&mut stmt, (100,))?;
```

### Example: Bulk Execution

On MariaDB, bulk execution sends all parameters in a single packet using the bulk command extension. On Oracle MySQL, it falls back to multiple `exec()` calls:

```rust,ignore
use zero_mysql::protocol::command::bulk_exec::BulkFlags;

let mut stmt = conn.prepare("INSERT INTO users (age, name) VALUES (?, ?)")?;

conn.exec_bulk_insert_or_update(
    &mut stmt,
    vec![
        (20, "Alice"),
        (21, "Bob"),
        (22, "Charlie"),
    ],
    BulkFlags::empty(),
    &mut handler,
)?;
```

## Statement Caching

Prepared statements are cached per connection. After calling `prepare()`, reuse the `PreparedStatement` for subsequent executions.

```rust,ignore
// Prepare once
let mut stmt = conn.prepare("SELECT * FROM users WHERE id = ?")?;

// Reuse multiple times
conn.exec_drop(&mut stmt, (1,))?;
conn.exec_drop(&mut stmt, (2,))?;
conn.exec_drop(&mut stmt, (3,))?;
```

## Result Handlers

zero-mysql uses a handler pattern for processing results. Implement `TextResultSetHandler` or `BinaryResultSetHandler` to customize how rows are processed.

Built-in handlers:
- `DropHandler`: Discards all results
- `FirstHandler<Row>`: Stores only the first row
- `CollectHandler<Row>`: Collects rows into a Vec
- `ForEachHandler<Row, F>`: Calls a closure for each row
