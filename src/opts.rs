use std::sync::Arc;

use url::Url;

use crate::buffer_pool::{BufferPool, GLOBAL_BUFFER_POOL};
use crate::constant::{CAPABILITIES_ALWAYS_ENABLED, CapabilityFlags};
use crate::error::Error;

/// A configuration for connection
///
/// ```rs
/// let mut opts1 = Opts::default();
/// opts1.port = 5000;
///
/// let mut opts2 = Opts::try_from("mysql://root:password@localhost:3306");
/// opts2.compress = true;
/// ```
#[derive(Debug, Clone)]
pub struct Opts {
    /// Enable TCP_NODELAY socket option to disable Nagle's algorithm
    /// Unix socket is not affected
    pub tcp_nodelay: bool,

    /// The client capabilities are `CAPABILITIES_ALWAYS_ENABLED | (opts.capabilities & CAPABILITIES_CONFIGURABLE)`.
    /// The final negotiated capabilities are `SERVER_CAPABILITIES & CLIENT_CAPABILITIES`.
    pub capabilities: CapabilityFlags,

    /// Enable compression for the connection
    pub compress: bool,

    /// Database name to use
    pub db: Option<String>,

    /// Hostname or IP address
    pub host: Option<String>,

    /// Port number for the MySQL server
    pub port: u16,

    /// Unix socket path
    pub socket: Option<String>,

    /// Username for authentication (can be empty for anonymous connections)
    pub user: String,

    pub password: Option<String>,

    pub tls: bool,

    /// When connected via TCP, read `SELECT @@socket` and reconnect to the unix socket
    pub upgrade_to_unix_socket: bool,

    /// SQL command to execute after connection is established
    pub init_command: Option<String>,

    /// Reset connection state when returning to pool
    pub pool_reset_conn: bool,

    /// Maximum number of idle connections in the pool
    pub pool_max_idle_conn: usize,

    /// Maximum number of concurrent connections (active + idle).
    /// None means unlimited.
    pub pool_max_concurrency: Option<usize>,

    pub buffer_pool: Arc<BufferPool>,
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            tcp_nodelay: true,
            capabilities: CAPABILITIES_ALWAYS_ENABLED,
            compress: false,
            db: None,
            host: None,
            port: 3306,
            socket: None,
            user: String::new(),
            password: None,
            tls: false,
            upgrade_to_unix_socket: true,
            init_command: None,
            pool_reset_conn: true,
            pool_max_idle_conn: 100,
            pool_max_concurrency: None,
            buffer_pool: Arc::clone(&GLOBAL_BUFFER_POOL),
        }
    }
}

/// Parse a boolean value from a query parameter.
/// Accepts: "1", "0", "true", "false", "True", "False"
fn parse_bool(key: &str, value: &str) -> Result<bool, Error> {
    match value {
        "1" | "true" | "True" => Ok(true),
        "0" | "false" | "False" => Ok(false),
        _ => Err(Error::BadConfigError(format!(
            "Invalid boolean value '{}' for parameter '{}', expected 1, 0, true, false, True, or False",
            value, key
        ))),
    }
}

/// Parse a usize value from a query parameter.
fn parse_usize(key: &str, value: &str) -> Result<usize, Error> {
    value.parse().map_err(|_| {
        Error::BadConfigError(format!(
            "Invalid unsigned integer value '{}' for parameter '{}'",
            value, key
        ))
    })
}

impl TryFrom<&Url> for Opts {
    type Error = Error;

    fn try_from(url: &Url) -> Result<Self, Self::Error> {
        // Verify scheme
        if url.scheme() != "mysql" {
            return Err(Error::BadConfigError(format!(
                "Invalid URL scheme '{}', expected 'mysql'",
                url.scheme()
            )));
        }

        // Extract host (can be None for socket connections)
        let host = url.host_str().map(ToString::to_string);
        let port = url.port().unwrap_or(3306);

        // Extract username (default empty)
        let user = url.username().to_string();

        // Extract password (default None)
        let password = url.password().map(ToString::to_string);

        // Extract database from path
        let db = url
            .path()
            .strip_prefix('/')
            .filter(|db| !db.is_empty())
            .map(ToString::to_string);

        let mut opts = Self {
            host,
            port,
            user,
            password,
            db,
            ..Default::default()
        };

        // Parse query parameters
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "socket" => opts.socket = Some(value.into_owned()),
                "tls" | "ssl" => opts.tls = parse_bool(&key, &value)?,
                "compress" => opts.compress = parse_bool(&key, &value)?,
                "tcp_nodelay" => opts.tcp_nodelay = parse_bool(&key, &value)?,
                "upgrade_to_unix_socket" => opts.upgrade_to_unix_socket = parse_bool(&key, &value)?,
                "init_command" => opts.init_command = Some(value.into_owned()),
                "pool_reset_conn" => opts.pool_reset_conn = parse_bool(&key, &value)?,
                "pool_max_idle_conn" => opts.pool_max_idle_conn = parse_usize(&key, &value)?,
                "pool_max_concurrency" => {
                    opts.pool_max_concurrency = Some(parse_usize(&key, &value)?)
                }
                _ => {
                    return Err(Error::BadConfigError(format!(
                        "Unknown query parameter '{}'",
                        key
                    )));
                }
            }
        }

        Ok(opts)
    }
}

impl TryFrom<&str> for Opts {
    type Error = Error;

    fn try_from(url: &str) -> Result<Self, Self::Error> {
        let parsed = Url::parse(url)
            .map_err(|e| Error::BadConfigError(format!("Failed to parse MySQL URL: {}", e)))?;
        Self::try_from(&parsed)
    }
}
