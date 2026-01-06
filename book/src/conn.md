# Connection

A connection can be made with a URL string or `Opts`.

The URL format is:

```
mysql://[user[:password]@]host[:port][/database][?parameters]
```

## Example: Basic

```rust,ignore
use zero_mysql::sync::Conn;
use zero_mysql::Opts;

// URL
let mut conn = Conn::new("mysql://test:1234@localhost:3306/test_db")?;

// Opts struct
let mut opts = Opts::default();
opts.host = "localhost".to_string();
opts.port = 3306;
opts.user = "test".to_string();
opts.password = "1234".to_string();
opts.db = Some("test_db".to_string());
let mut conn = Conn::new(opts)?;
```

## Example: Async

```rust,ignore
use zero_mysql::tokio::Conn;

let mut conn = Conn::new("mysql://test:1234@localhost:3306/test_db").await?;
```

## Example: Unix Socket

```rust,ignore
use zero_mysql::Opts;
use zero_mysql::sync::Conn;

let mut opts = Opts::default();
opts.socket = Some("/var/run/mysqld/mysqld.sock".to_string());
opts.db = Some("test".to_string());
let mut conn = Conn::new(opts)?;
```

## Connection Options

See [`Opts`](https://docs.rs/zero-mysql/latest/zero_mysql/struct.Opts.html) for all available connection options and URL query parameters.

## Upgrade to Unix Socket

By default, `upgrade_to_unix_socket` is `true`.

If the connection is made via TCP to localhost, the driver queries `SELECT @@socket` to get the Unix socket path, then reconnects using the socket for better performance.

For production, disable this flag and manually specify the socket address:

```rust,ignore
let mut opts = Opts::try_from("mysql://test:1234@localhost")?;
opts.upgrade_to_unix_socket = false;
let mut conn = Conn::new(opts)?;
```
