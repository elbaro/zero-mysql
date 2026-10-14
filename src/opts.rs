use std::sync::Arc;

use url::Url;

use crate::buffer_pool::{BufferPool, GLOBAL_BUFFER_POOL};
use crate::constant::CapabilityFlags;
use crate::error::Error;

/// TLS connection mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TlsMode {
    /// Don't use TLS (Insecure)
    Disable,
    /// Try TLS, fall back to unencrypted if the server does not support TLS (Insecure)
    #[default]
    Prefer,
    /// Require TLS (Recommended)
    Require,
}

/// A configuration for connection
///
/// ```rs
/// let mut opts1 = Opts::default();
/// opts1.port = 5000;
///
/// let mut opts2 = Opts::try_from("mysql://root:password@localhost:3306?compress=true&tcp_nodelay=false");
/// opts2.compress = true;
/// ```
#[derive(Debug, Clone)]
pub struct Opts {
    /// Enable TCP_NODELAY socket option to disable Nagle's algorithm.
    /// Unix socket is not affected.
    ///
    /// Default: `true`
    pub tcp_nodelay: bool,

    /// The client capabilities are `CAPABILITIES_ALWAYS_ENABLED | (opts.capabilities & CAPABILITIES_CONFIGURABLE)`.
    /// The final negotiated capabilities are `SERVER_CAPABILITIES & CLIENT_CAPABILITIES`.
    ///
    /// Default: `CapabilityFlags::empty()`
    pub capabilities: CapabilityFlags,

    /// Enable compression for the connection.
    ///
    /// Default: `false`
    pub compress: bool,

    /// Database name to use.
    ///
    /// Default: `None`
    pub db: Option<String>,

    /// Hostname or IP address.
    ///
    /// Default: `""`
    pub host: String,

    /// Port number for the MySQL server.
    ///
    /// Default: `3306`
    pub port: u16,

    /// Unix socket path. Only supported on Unix platforms.
    ///
    /// Default: `None`
    pub socket: Option<String>,

    /// Username for authentication (can be empty for anonymous connections).
    ///
    /// Default: `""`
    pub user: String,

    /// Password for authentication.
    ///
    /// Default: `""`
    pub password: String,

    /// TLS connection mode.
    ///
    /// Default: `TlsMode::Prefer`
    pub tls_mode: TlsMode,

    /// rustls client config for TLS connections.
    ///
    /// Set this to trust a private CA or to present a client certificate.
    /// Build the config with [`crate::rustls`] so that the rustls versions match,
    /// and with the aws-lc-rs provider, which this crate uses:
    ///
    /// ```no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use std::sync::Arc;
    /// use zero_mysql::rustls::pki_types::{pem::PemObject, CertificateDer};
    /// use zero_mysql::rustls::{crypto::aws_lc_rs, ClientConfig, RootCertStore};
    ///
    /// let mut roots = RootCertStore::empty();
    /// for cert in CertificateDer::pem_file_iter("/etc/myapp/db-ca.pem")? {
    ///     roots.add(cert?)?;
    /// }
    /// let config = ClientConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
    ///     .with_safe_default_protocol_versions()?
    ///     .with_root_certificates(roots)
    ///     .with_no_client_auth();
    ///
    /// let mut opts = zero_mysql::Opts::default();
    /// opts.tls_config = Some(Arc::new(config));
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// `None` uses the platform's native root certificates, loaded once and
    /// shared by all connections.
    ///
    /// Default: `None`
    #[cfg(any(feature = "sync-tls", feature = "tokio-tls", feature = "compio-tls"))]
    pub tls_config: Option<Arc<rustls::ClientConfig>>,

    /// When connected via TCP, read `SELECT @@socket` and reconnect to the unix socket.
    /// Only has effect on Unix platforms.
    ///
    /// Default: `true`
    pub upgrade_to_unix_socket: bool,

    /// SQL command to execute after connection is established.
    ///
    /// Default: `None`
    pub init_command: Option<String>,

    /// Reset connection state when returning to pool.
    ///
    /// Default: `true`
    pub pool_reset_conn: bool,

    /// Maximum number of idle connections in the pool.
    ///
    /// Default: `100`
    pub pool_max_idle_conn: usize,

    /// Maximum number of concurrent connections (active + idle).
    /// `None` means unlimited.
    ///
    /// Default: `None`
    pub pool_max_concurrency: Option<usize>,

    /// `BufferPool` to reuse byte buffers (`Vec<u8>`).
    ///
    /// Default: `GLOBAL_BUFFER_POOL`
    pub buffer_pool: Arc<BufferPool>,
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            tcp_nodelay: true,
            capabilities: CapabilityFlags::empty(),
            compress: false,
            db: None,
            host: String::new(),
            port: 3306,
            socket: None,
            user: String::new(),
            password: String::new(),
            tls_mode: TlsMode::Prefer,
            #[cfg(any(feature = "sync-tls", feature = "tokio-tls", feature = "compio-tls"))]
            tls_config: None,
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
        _ => Err(Error::BadUsageError(format!(
            "Invalid boolean value '{}' for parameter '{}', expected 1, 0, true, false, True, or False",
            value, key
        ))),
    }
}

/// Parse a usize value from a query parameter.
fn parse_usize(key: &str, value: &str) -> Result<usize, Error> {
    value.parse().map_err(|_unhelpful_err| {
        Error::BadUsageError(format!(
            "Invalid unsigned integer value '{}' for parameter '{}'",
            value, key
        ))
    })
}

/// Decode a percent-encoded URL component such as the user or password.
fn percent_decode(s: &str) -> Result<String, Error> {
    percent_encoding::percent_decode_str(s)
        .decode_utf8()
        .map(|s| s.into_owned())
        .map_err(|e| Error::BadUsageError(format!("Invalid UTF-8 in URL: {}", e)))
}

/// Parse connection options from a MySQL URL.
///
/// # URL Format
///
/// ```text
/// mysql://[user[:password]@]host[:port][/database][?parameters]
/// ```
///
/// # Query Parameters
///
/// - `socket`
/// - `tls_mode`: `disable`, `prefer`, `require`
/// - `compress`
/// - `tcp_nodelay`
/// - `upgrade_to_unix_socket`
/// - `init_command`
/// - `pool_reset_conn`
/// - `pool_max_idle_conn`
/// - `pool_max_concurrency`
///
/// Boolean values accept: `1`, `0`, `true`, `false`, `True`, `False`
///
/// The user, password and database are percent-decoded, so characters such as
/// `/`, `#` and `?` can be written as `%2F`, `%23` and `%3F`. A literal `%`
/// followed by two hex digits must be written as `%25`.
///
/// # Examples
///
/// ```
/// use zero_mysql::Opts;
///
/// // Basic connection
/// let opts = Opts::try_from("mysql://localhost").unwrap();
///
/// // With credentials and database
/// let opts = Opts::try_from("mysql://root:password@localhost:3306/mydb").unwrap();
///
/// // With query parameters
/// let opts = Opts::try_from("mysql://localhost?tls_mode=require&compress=true").unwrap();
///
/// // Unix socket (hostname is ignored)
/// let opts = Opts::try_from("mysql://localhost?socket=/var/run/mysqld/mysqld.sock").unwrap();
/// ```
impl TryFrom<&Url> for Opts {
    type Error = Error;

    fn try_from(url: &Url) -> Result<Self, Self::Error> {
        if url.scheme() != "mysql" {
            return Err(Error::BadUsageError(format!(
                "Invalid URL scheme '{}', expected 'mysql'",
                url.scheme()
            )));
        }

        let host = url.host_str().unwrap_or_default().to_string();
        let port = url.port().unwrap_or(3306);
        let user = percent_decode(url.username())?;
        let password = percent_decode(url.password().unwrap_or_default())?;
        let db = url
            .path()
            .strip_prefix('/')
            .filter(|db| !db.is_empty())
            .map(percent_decode)
            .transpose()?;

        let mut opts = Self {
            host,
            port,
            user,
            password,
            db,
            ..Default::default()
        };

        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "socket" => opts.socket = Some(value.into_owned()),
                "tls_mode" => {
                    opts.tls_mode = match value.as_ref() {
                        "disable" => TlsMode::Disable,
                        "prefer" => TlsMode::Prefer,
                        "require" => TlsMode::Require,
                        _ => {
                            return Err(Error::BadUsageError(format!(
                                "Invalid tls_mode: expected one of ['disable', 'prefer', 'require'], got {}",
                                value
                            )));
                        }
                    };
                }
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
                    return Err(Error::BadUsageError(format!(
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
            .map_err(|e| Error::BadUsageError(format!("Failed to parse MySQL URL: {}", e)))?;
        Self::try_from(&parsed)
    }
}
