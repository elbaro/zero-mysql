use crate::constant::{CapabilityFlags, CAPABILITIES_ALWAYS_ENABLED};
use crate::error::Error;

/// Connection options for MySQL connections.
/// This struct will be exposed as a PyClass for Python bindings.
#[derive(Debug, Clone)]
pub struct Opts {
    /// Enable TCP_NODELAY socket option to disable Nagle's algorithm
    /// Unix socket is not affected
    pub tcp_nodelay: bool,

    /// MySQL client capability flags
    pub capabilities: CapabilityFlags,

    /// Enable compression for the connection
    pub compress: bool,

    /// Database name to connect to
    pub db: Option<String>,

    /// Hostname or IP address of the MySQL server
    pub host: Option<String>,

    /// Port number for the MySQL server
    pub port: u16,

    /// Unix socket path for local connections
    pub socket: Option<String>,

    /// Username for authentication (can be empty for anonymous connections)
    pub user: String,

    /// Password for authentication
    pub password: Option<String>,
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
        }
    }
}

impl TryFrom<&str> for Opts {
    type Error = Error;

    /// Parse a MySQL connection URL into Opts
    ///
    /// # URL Format
    /// ```text
    /// mysql://[username[:password]@]host[:port][/database]
    /// ```
    ///
    /// # Examples
    /// - `mysql://localhost`
    /// - `mysql://root:password@localhost:3306`
    /// - `mysql://user:pass@127.0.0.1:3306/mydb`
    fn try_from(url: &str) -> Result<Self, Self::Error> {
        // Parse URL
        let parsed = url::Url::parse(url)
            .map_err(|e| Error::BadInputError(format!("Failed to parse MySQL URL: {}", e)))?;

        // Verify scheme
        if parsed.scheme() != "mysql" {
            return Err(Error::BadInputError(format!(
                "Invalid URL scheme '{}', expected 'mysql'",
                parsed.scheme()
            )));
        }

        // Extract host (can be None for socket connections)
        let host = parsed.host_str().map(ToString::to_string);

        // Extract port (default 3306)
        let port = parsed.port().unwrap_or(3306);

        // Extract username (default empty)
        let user = parsed.username().to_string();

        // Extract password (default None)
        let password = parsed.password().map(ToString::to_string);

        // Extract database from path
        let db = parsed
            .path()
            .strip_prefix('/')
            .filter(|db| !db.is_empty())
            .map(ToString::to_string);

        Ok(Self {
            tcp_nodelay: true,
            capabilities: CAPABILITIES_ALWAYS_ENABLED,
            compress: false,
            db,
            host,
            port,
            socket: None,
            user,
            password,
        })
    }
}
