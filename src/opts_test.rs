use crate::Opts;
use crate::test_macros::{check, check_eq, check_err};

#[test]
fn default_opts() -> crate::error::Result<()> {
    let opts = Opts::default();
    check!(opts.tcp_nodelay);
    check!(!opts.compress);
    check!(opts.db.is_none());
    check!(opts.host.is_empty());
    check_eq!(opts.port, 3306);
    check!(opts.socket.is_none());
    check!(opts.user.is_empty());
    check!(opts.password.is_empty());
    check!(!opts.tls);
    check!(opts.upgrade_to_unix_socket);
    check!(opts.init_command.is_none());
    check!(opts.pool_reset_conn);
    check_eq!(opts.pool_max_idle_conn, 100);
    check!(opts.pool_max_concurrency.is_none());
    Ok(())
}

#[test]
fn parse_basic_url() -> crate::error::Result<()> {
    let opts = Opts::try_from("mysql://localhost")?;
    check_eq!(opts.host, "localhost");
    check_eq!(opts.port, 3306);
    check!(opts.user.is_empty());
    check!(opts.password.is_empty());
    check!(opts.db.is_none());
    Ok(())
}

#[test]
fn parse_url_with_port() -> crate::error::Result<()> {
    let opts = Opts::try_from("mysql://localhost:3307")?;
    check_eq!(opts.host, "localhost");
    check_eq!(opts.port, 3307);
    Ok(())
}

#[test]
fn parse_url_with_credentials() -> crate::error::Result<()> {
    let opts = Opts::try_from("mysql://root:password@localhost")?;
    check_eq!(opts.host, "localhost");
    check_eq!(opts.user, "root");
    check_eq!(opts.password.as_str(), "password");
    Ok(())
}

#[test]
fn parse_url_with_database() -> crate::error::Result<()> {
    let opts = Opts::try_from("mysql://localhost/mydb")?;
    check_eq!(opts.host, "localhost");
    check_eq!(opts.db.as_deref(), Some("mydb"));
    Ok(())
}

#[test]
fn parse_url_with_empty_database() -> crate::error::Result<()> {
    let opts = Opts::try_from("mysql://localhost/")?;
    check!(opts.db.is_none());
    Ok(())
}

#[test]
fn parse_full_url() -> crate::error::Result<()> {
    let opts = Opts::try_from("mysql://admin:secret@db.example.com:3308/production")?;
    check_eq!(opts.host, "db.example.com");
    check_eq!(opts.port, 3308);
    check_eq!(opts.user, "admin");
    check_eq!(opts.password.as_str(), "secret");
    check_eq!(opts.db.as_deref(), Some("production"));
    Ok(())
}

#[test]
fn parse_socket_param() -> crate::error::Result<()> {
    let opts = Opts::try_from("mysql://localhost?socket=/var/run/mysqld/mysqld.sock")?;
    check_eq!(opts.socket.as_deref(), Some("/var/run/mysqld/mysqld.sock"));
    Ok(())
}

#[test]
fn parse_tls_param() -> crate::error::Result<()> {
    let opts1 = Opts::try_from("mysql://localhost?tls=true")?;
    check!(opts1.tls);

    let opts2 = Opts::try_from("mysql://localhost?tls=1")?;
    check!(opts2.tls);

    let opts3 = Opts::try_from("mysql://localhost?tls=True")?;
    check!(opts3.tls);

    let opts4 = Opts::try_from("mysql://localhost?tls=false")?;
    check!(!opts4.tls);

    let opts5 = Opts::try_from("mysql://localhost?tls=0")?;
    check!(!opts5.tls);

    let opts6 = Opts::try_from("mysql://localhost?tls=False")?;
    check!(!opts6.tls);
    Ok(())
}

#[test]
fn parse_ssl_param() -> crate::error::Result<()> {
    let opts1 = Opts::try_from("mysql://localhost?ssl=true")?;
    check!(opts1.tls);

    let opts2 = Opts::try_from("mysql://localhost?ssl=false")?;
    check!(!opts2.tls);
    Ok(())
}

#[test]
fn parse_compress_param() -> crate::error::Result<()> {
    let opts1 = Opts::try_from("mysql://localhost?compress=true")?;
    check!(opts1.compress);

    let opts2 = Opts::try_from("mysql://localhost?compress=false")?;
    check!(!opts2.compress);
    Ok(())
}

#[test]
fn parse_tcp_nodelay_param() -> crate::error::Result<()> {
    let opts1 = Opts::try_from("mysql://localhost?tcp_nodelay=false")?;
    check!(!opts1.tcp_nodelay);

    let opts2 = Opts::try_from("mysql://localhost?tcp_nodelay=true")?;
    check!(opts2.tcp_nodelay);
    Ok(())
}

#[test]
fn parse_upgrade_to_unix_socket_param() -> crate::error::Result<()> {
    let opts1 = Opts::try_from("mysql://localhost?upgrade_to_unix_socket=false")?;
    check!(!opts1.upgrade_to_unix_socket);

    let opts2 = Opts::try_from("mysql://localhost?upgrade_to_unix_socket=true")?;
    check!(opts2.upgrade_to_unix_socket);
    Ok(())
}

#[test]
fn parse_init_command_param() -> crate::error::Result<()> {
    let opts = Opts::try_from("mysql://localhost?init_command=SET%20NAMES%20utf8mb4")?;
    check_eq!(opts.init_command.as_deref(), Some("SET NAMES utf8mb4"));
    Ok(())
}

#[test]
fn parse_pool_reset_conn_param() -> crate::error::Result<()> {
    let opts1 = Opts::try_from("mysql://localhost?pool_reset_conn=false")?;
    check!(!opts1.pool_reset_conn);

    let opts2 = Opts::try_from("mysql://localhost?pool_reset_conn=true")?;
    check!(opts2.pool_reset_conn);
    Ok(())
}

#[test]
fn parse_pool_max_idle_conn_param() -> crate::error::Result<()> {
    let opts1 = Opts::try_from("mysql://localhost?pool_max_idle_conn=50")?;
    check_eq!(opts1.pool_max_idle_conn, 50);

    let opts2 = Opts::try_from("mysql://localhost?pool_max_idle_conn=0")?;
    check_eq!(opts2.pool_max_idle_conn, 0);
    Ok(())
}

#[test]
fn parse_pool_max_concurrency_param() -> crate::error::Result<()> {
    let opts = Opts::try_from("mysql://localhost?pool_max_concurrency=10")?;
    check_eq!(opts.pool_max_concurrency, Some(10));
    Ok(())
}

#[test]
fn parse_multiple_params() -> crate::error::Result<()> {
    let opts = Opts::try_from(
        "mysql://root:pass@localhost:3307/mydb?tls=true&compress=true&pool_max_idle_conn=50",
    )?;
    check_eq!(opts.host, "localhost");
    check_eq!(opts.port, 3307);
    check_eq!(opts.user, "root");
    check_eq!(opts.password.as_str(), "pass");
    check_eq!(opts.db.as_deref(), Some("mydb"));
    check!(opts.tls);
    check!(opts.compress);
    check_eq!(opts.pool_max_idle_conn, 50);
    Ok(())
}

#[test]
fn error_invalid_scheme() -> crate::error::Result<()> {
    let result = Opts::try_from("postgres://localhost");
    let err = check_err!(result);
    check!(err.to_string().contains("Invalid URL scheme"));
    Ok(())
}

#[test]
fn error_invalid_url() -> crate::error::Result<()> {
    let result = Opts::try_from("not a valid url");
    let err = check_err!(result);
    check!(err.to_string().contains("Failed to parse MySQL URL"));
    Ok(())
}

#[test]
fn error_unknown_param() -> crate::error::Result<()> {
    let result = Opts::try_from("mysql://localhost?unknown_param=value");
    let err = check_err!(result);
    check!(err.to_string().contains("Unknown query parameter"));
    Ok(())
}

#[test]
fn error_invalid_bool_value() -> crate::error::Result<()> {
    let result = Opts::try_from("mysql://localhost?tls=yes");
    let err = check_err!(result);
    check!(err.to_string().contains("Invalid boolean value"));
    Ok(())
}

#[test]
fn error_invalid_usize_value() -> crate::error::Result<()> {
    let result = Opts::try_from("mysql://localhost?pool_max_idle_conn=abc");
    let err = check_err!(result);
    check!(err.to_string().contains("Invalid unsigned integer value"));
    Ok(())
}

#[test]
fn error_negative_usize_value() -> crate::error::Result<()> {
    let result = Opts::try_from("mysql://localhost?pool_max_idle_conn=-1");
    check_err!(result);
    Ok(())
}

#[test]
fn parse_ip_address() -> crate::error::Result<()> {
    let opts = Opts::try_from("mysql://127.0.0.1:3306")?;
    check_eq!(opts.host, "127.0.0.1");
    check_eq!(opts.port, 3306);
    Ok(())
}

#[test]
fn parse_ipv6_address() -> crate::error::Result<()> {
    let opts = Opts::try_from("mysql://[::1]:3306")?;
    check_eq!(opts.host, "[::1]");
    check_eq!(opts.port, 3306);
    Ok(())
}

#[test]
fn parse_url_encoded_password() -> crate::error::Result<()> {
    // URL library preserves percent-encoded characters in password
    let opts = Opts::try_from("mysql://root:p%40ssw%2Frd@localhost")?;
    check_eq!(opts.password.as_str(), "p%40ssw%2Frd");
    Ok(())
}

#[test]
fn parse_empty_password() -> crate::error::Result<()> {
    // Empty password in URL is treated as None
    let opts = Opts::try_from("mysql://root:@localhost")?;
    check_eq!(opts.user, "root");
    check!(opts.password.is_empty());
    Ok(())
}

#[test]
fn parse_no_password() -> crate::error::Result<()> {
    let opts = Opts::try_from("mysql://root@localhost")?;
    check_eq!(opts.user, "root");
    check!(opts.password.is_empty());
    Ok(())
}
