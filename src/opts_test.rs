use crate::Opts;

#[test]
fn default_opts() {
    let opts = Opts::default();
    assert!(opts.tcp_nodelay);
    assert!(!opts.compress);
    assert!(opts.db.is_none());
    assert!(opts.host.is_empty());
    assert_eq!(opts.port, 3306);
    assert!(opts.socket.is_none());
    assert!(opts.user.is_empty());
    assert!(opts.password.is_empty());
    assert!(!opts.tls);
    assert!(opts.upgrade_to_unix_socket);
    assert!(opts.init_command.is_none());
    assert!(opts.pool_reset_conn);
    assert_eq!(opts.pool_max_idle_conn, 100);
    assert!(opts.pool_max_concurrency.is_none());
}

#[test]
fn parse_basic_url() {
    let opts = Opts::try_from("mysql://localhost").unwrap();
    assert_eq!(opts.host, "localhost");
    assert_eq!(opts.port, 3306);
    assert!(opts.user.is_empty());
    assert!(opts.password.is_empty());
    assert!(opts.db.is_none());
}

#[test]
fn parse_url_with_port() {
    let opts = Opts::try_from("mysql://localhost:3307").unwrap();
    assert_eq!(opts.host, "localhost");
    assert_eq!(opts.port, 3307);
}

#[test]
fn parse_url_with_credentials() {
    let opts = Opts::try_from("mysql://root:password@localhost").unwrap();
    assert_eq!(opts.host, "localhost");
    assert_eq!(opts.user, "root");
    assert_eq!(opts.password.as_str(), "password");
}

#[test]
fn parse_url_with_database() {
    let opts = Opts::try_from("mysql://localhost/mydb").unwrap();
    assert_eq!(opts.host, "localhost");
    assert_eq!(opts.db.as_deref(), Some("mydb"));
}

#[test]
fn parse_url_with_empty_database() {
    let opts = Opts::try_from("mysql://localhost/").unwrap();
    assert!(opts.db.is_none());
}

#[test]
fn parse_full_url() {
    let opts = Opts::try_from("mysql://admin:secret@db.example.com:3308/production").unwrap();
    assert_eq!(opts.host, "db.example.com");
    assert_eq!(opts.port, 3308);
    assert_eq!(opts.user, "admin");
    assert_eq!(opts.password.as_str(), "secret");
    assert_eq!(opts.db.as_deref(), Some("production"));
}

#[test]
fn parse_socket_param() {
    let opts = Opts::try_from("mysql://localhost?socket=/var/run/mysqld/mysqld.sock").unwrap();
    assert_eq!(
        opts.socket.as_deref(),
        Some("/var/run/mysqld/mysqld.sock")
    );
}

#[test]
fn parse_tls_param() {
    let opts = Opts::try_from("mysql://localhost?tls=true").unwrap();
    assert!(opts.tls);

    let opts = Opts::try_from("mysql://localhost?tls=1").unwrap();
    assert!(opts.tls);

    let opts = Opts::try_from("mysql://localhost?tls=True").unwrap();
    assert!(opts.tls);

    let opts = Opts::try_from("mysql://localhost?tls=false").unwrap();
    assert!(!opts.tls);

    let opts = Opts::try_from("mysql://localhost?tls=0").unwrap();
    assert!(!opts.tls);

    let opts = Opts::try_from("mysql://localhost?tls=False").unwrap();
    assert!(!opts.tls);
}

#[test]
fn parse_ssl_param() {
    let opts = Opts::try_from("mysql://localhost?ssl=true").unwrap();
    assert!(opts.tls);

    let opts = Opts::try_from("mysql://localhost?ssl=false").unwrap();
    assert!(!opts.tls);
}

#[test]
fn parse_compress_param() {
    let opts = Opts::try_from("mysql://localhost?compress=true").unwrap();
    assert!(opts.compress);

    let opts = Opts::try_from("mysql://localhost?compress=false").unwrap();
    assert!(!opts.compress);
}

#[test]
fn parse_tcp_nodelay_param() {
    let opts = Opts::try_from("mysql://localhost?tcp_nodelay=false").unwrap();
    assert!(!opts.tcp_nodelay);

    let opts = Opts::try_from("mysql://localhost?tcp_nodelay=true").unwrap();
    assert!(opts.tcp_nodelay);
}

#[test]
fn parse_upgrade_to_unix_socket_param() {
    let opts = Opts::try_from("mysql://localhost?upgrade_to_unix_socket=false").unwrap();
    assert!(!opts.upgrade_to_unix_socket);

    let opts = Opts::try_from("mysql://localhost?upgrade_to_unix_socket=true").unwrap();
    assert!(opts.upgrade_to_unix_socket);
}

#[test]
fn parse_init_command_param() {
    let opts = Opts::try_from("mysql://localhost?init_command=SET%20NAMES%20utf8mb4").unwrap();
    assert_eq!(opts.init_command.as_deref(), Some("SET NAMES utf8mb4"));
}

#[test]
fn parse_pool_reset_conn_param() {
    let opts = Opts::try_from("mysql://localhost?pool_reset_conn=false").unwrap();
    assert!(!opts.pool_reset_conn);

    let opts = Opts::try_from("mysql://localhost?pool_reset_conn=true").unwrap();
    assert!(opts.pool_reset_conn);
}

#[test]
fn parse_pool_max_idle_conn_param() {
    let opts = Opts::try_from("mysql://localhost?pool_max_idle_conn=50").unwrap();
    assert_eq!(opts.pool_max_idle_conn, 50);

    let opts = Opts::try_from("mysql://localhost?pool_max_idle_conn=0").unwrap();
    assert_eq!(opts.pool_max_idle_conn, 0);
}

#[test]
fn parse_pool_max_concurrency_param() {
    let opts = Opts::try_from("mysql://localhost?pool_max_concurrency=10").unwrap();
    assert_eq!(opts.pool_max_concurrency, Some(10));
}

#[test]
fn parse_multiple_params() {
    let opts = Opts::try_from(
        "mysql://root:pass@localhost:3307/mydb?tls=true&compress=true&pool_max_idle_conn=50",
    )
    .unwrap();
    assert_eq!(opts.host, "localhost");
    assert_eq!(opts.port, 3307);
    assert_eq!(opts.user, "root");
    assert_eq!(opts.password.as_str(), "pass");
    assert_eq!(opts.db.as_deref(), Some("mydb"));
    assert!(opts.tls);
    assert!(opts.compress);
    assert_eq!(opts.pool_max_idle_conn, 50);
}

#[test]
fn error_invalid_scheme() {
    let result = Opts::try_from("postgres://localhost");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Invalid URL scheme"));
}

#[test]
fn error_invalid_url() {
    let result = Opts::try_from("not a valid url");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Failed to parse MySQL URL"));
}

#[test]
fn error_unknown_param() {
    let result = Opts::try_from("mysql://localhost?unknown_param=value");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Unknown query parameter"));
}

#[test]
fn error_invalid_bool_value() {
    let result = Opts::try_from("mysql://localhost?tls=yes");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Invalid boolean value"));
}

#[test]
fn error_invalid_usize_value() {
    let result = Opts::try_from("mysql://localhost?pool_max_idle_conn=abc");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Invalid unsigned integer value"));
}

#[test]
fn error_negative_usize_value() {
    let result = Opts::try_from("mysql://localhost?pool_max_idle_conn=-1");
    assert!(result.is_err());
}

#[test]
fn parse_ip_address() {
    let opts = Opts::try_from("mysql://127.0.0.1:3306").unwrap();
    assert_eq!(opts.host, "127.0.0.1");
    assert_eq!(opts.port, 3306);
}

#[test]
fn parse_ipv6_address() {
    let opts = Opts::try_from("mysql://[::1]:3306").unwrap();
    assert_eq!(opts.host, "[::1]");
    assert_eq!(opts.port, 3306);
}

#[test]
fn parse_url_encoded_password() {
    // URL library preserves percent-encoded characters in password
    let opts = Opts::try_from("mysql://root:p%40ssw%2Frd@localhost").unwrap();
    assert_eq!(opts.password.as_str(), "p%40ssw%2Frd");
}

#[test]
fn parse_empty_password() {
    // Empty password in URL is treated as None
    let opts = Opts::try_from("mysql://root:@localhost").unwrap();
    assert_eq!(opts.user, "root");
    assert!(opts.password.is_empty());
}

#[test]
fn parse_no_password() {
    let opts = Opts::try_from("mysql://root@localhost").unwrap();
    assert_eq!(opts.user, "root");
    assert!(opts.password.is_empty());
}
