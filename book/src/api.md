# API Reference

The full API documentation is available on docs.rs:

**[docs.rs/zero-mysql](https://docs.rs/zero-mysql)**

## Module Overview

```
zero_mysql
├── Opts                    # Connection options
├── BufferPool              # Buffer reuse pool
├── BufferSet               # Set of buffers for a connection
├── PreparedStatement       # Prepared statement handle
├── sync/                   # Synchronous API
│   ├── Conn                # Synchronous connection
│   ├── Pool                # Connection pool
│   ├── PooledConn          # Pooled connection
│   └── Transaction         # Transaction handle
├── tokio/                  # Asynchronous API (Tokio)
│   ├── Conn                # Async connection
│   ├── Pool                # Async connection pool
│   ├── PooledConn          # Async pooled connection
│   └── Transaction         # Async transaction handle
├── protocol/               # MySQL protocol types
│   └── trait/              # Handler traits
├── handler/                # Built-in result handlers
├── error/                  # Error types
│   ├── Error               # Main error enum
│   └── Result<T>           # Result type alias
├── constant/               # Protocol constants
│   ├── CapabilityFlags     # Client/server capabilities
│   └── ServerStatusFlags   # Server status
├── value/                  # Value types
└── raw/                    # Raw packet access
```

## Key Traits

### TextResultSetHandler

Handle results from text protocol queries:

```rust,ignore
pub trait TextResultSetHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()>;
    fn resultset_start(&mut self, columns: &[ColumnDefinition<'_>]) -> Result<()>;
    fn resultset_end(&mut self, ok: OkPayloadBytes) -> Result<()>;
    fn row(&mut self, columns: &[ColumnDefinition<'_>], row: TextRowPayload<'_>) -> Result<()>;
}
```

### BinaryResultSetHandler

Handle results from binary protocol queries:

```rust,ignore
pub trait BinaryResultSetHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()>;
    fn resultset_start(&mut self, columns: &[ColumnDefinition<'_>]) -> Result<()>;
    fn resultset_end(&mut self, ok: OkPayloadBytes) -> Result<()>;
    fn row(&mut self, columns: &[ColumnDefinition<'_>], row: BinaryRowPayload<'_>) -> Result<()>;
}
```

### Params

Parameter binding for prepared statements:

```rust,ignore
pub trait Params {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<()>;
    fn num_params(&self) -> usize;
}
```

Implemented for tuples up to 16 elements.
