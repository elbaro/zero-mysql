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
        }
    }
}

impl TryFrom<&str> for Opts {
    type Error = Error;

    fn try_from(url: &str) -> Result<Self, Self::Error> {
        // Parse URL
        let parsed = url::Url::parse(url)
            .map_err(|e| Error::BadConfigError(format!("Failed to parse MySQL URL: {}", e)))?;

        // Verify scheme
        if parsed.scheme() != "mysql" {
            return Err(Error::BadConfigError(format!(
                "Invalid URL scheme '{}', expected 'mysql'",
                parsed.scheme()
            )));
        }

        // Extract host (can be None for socket connections)
        let host = parsed.host_str().map(ToString::to_string);
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
            tls: false,
        })
    }
}
