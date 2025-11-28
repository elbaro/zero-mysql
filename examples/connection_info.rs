/// Example demonstrating how to access connection information
///
/// This example shows how to use the new connection_id() and status_flags() methods
/// to get information about the MySQL connection.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    use zero_mysql::constant::ServerStatusFlags;

    // Create a connection (adjust credentials as needed)
    let conn = zero_mysql::sync::Conn::new("mysql://test:1234@localhost:3306")?;

    // Get connection ID assigned by the server
    let conn_id = conn.connection_id();
    println!("Connection ID: {}", conn_id);

    // Get server status flags from the initial handshake
    let status_flags = conn.status_flags();
    println!("Server Status Flags: {:?}", status_flags);

    // Check specific status flags
    if status_flags.contains(ServerStatusFlags::SERVER_STATUS_AUTOCOMMIT) {
        println!("Autocommit is enabled");
    } else {
        println!("Autocommit is disabled");
    }

    if status_flags.contains(ServerStatusFlags::SERVER_STATUS_IN_TRANS) {
        println!("Currently in a transaction");
    } else {
        println!("Not in a transaction");
    }

    // Get other connection information
    let server_version = conn.server_version();
    println!(
        "Server version: {}",
        String::from_utf8_lossy(server_version)
    );

    let capability_flags = conn.capability_flags();
    println!("Capability flags: {:?}", capability_flags);

    Ok(())
}
