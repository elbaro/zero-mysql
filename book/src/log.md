# Logging

zero-mysql uses the `tracing` crate for logging and instrumentation.

## Setup

Add `tracing-subscriber` to your dependencies:

```toml
[dependencies]
tracing-subscriber = "0.3"
```

Initialize the subscriber:

```rust,ignore
tracing_subscriber::fmt::init();
```

## Log Levels

- `WARN`: Connection errors and protocol issues
- `DEBUG`: Query execution details
- `TRACE`: Low-level protocol packets

## Example

```rust,ignore
use tracing_subscriber;

fn main() {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let mut conn = Conn::new("mysql://localhost")?;
    conn.query_drop("SELECT 1")?;  // Will log query execution
}
```

## Performance Note

In release builds, `tracing` macros above `WARN` level are compiled out via the `release_max_level_warn` feature for minimal runtime overhead.
