use zero_mysql::error::Result;
use zero_mysql::protocol::command::ColumnDefinitionBytes;
use zero_mysql::protocol::response::OkPayloadBytes;
use zero_mysql::protocol::TextRowPayload;
use zero_mysql::sync::Conn;

fn main() -> Result<()> {
    // Connect to MySQL server
    println!("Connecting to MySQL...");
    let mut conn = match Conn::new("mysql://test:1234@localhost/test") {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to connect: {:?}", e);
            return Err(e);
        }
    };

    println!("Connected to MySQL {:?}", conn.server_version());
    println!("Capability flags: {:?}", conn.capability_flags());

    // Create a simple handler using the new TextResultSetHandler trait
    struct TextHandler {
        column_count: usize,
    }

    impl TextHandler {
        fn new() -> Self {
            Self { column_count: 0 }
        }
    }

    impl zero_mysql::protocol::r#trait::TextResultSetHandler for TextHandler {
        fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
            let ok_payload = zero_mysql::protocol::response::OkPayload::try_from(ok)?;
            println!("Query OK, {} rows affected", ok_payload.affected_rows);
            Ok(())
        }

        fn resultset_start(&mut self, num_columns: usize) -> Result<()> {
            println!("Result set started with {} columns", num_columns);
            self.column_count = num_columns;
            Ok(())
        }

        fn col<'buffers>(&mut self, col: ColumnDefinitionBytes<'buffers>) -> Result<()> {
            // Parse the full column definition to get the name
            let col_def: zero_mysql::protocol::command::ColumnDefinition = col.try_into()?;
            println!("  Column: {:?}", str::from_utf8(col_def.name));
            Ok(())
        }

        fn row(&mut self, row: &TextRowPayload) -> Result<()> {
            println!("Row data (raw bytes): {} bytes", row.0.len());
            // Note: Text protocol row parsing would be done by an external library
            // For now we just show the raw data exists
            Ok(())
        }

        fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()> {
            println!(
                "Result set finished (EOF received): {:?}",
                zero_mysql::protocol::response::OkPayload::try_from(eof)?
            );
            Ok(())
        }
    }

    // Test 1: Simple SELECT query
    println!("\n--- Test 1: Simple SELECT ---");
    let mut handler = TextHandler::new();
    conn.query("SELECT 1 + 1 AS result", &mut handler)?;

    // Test 2: Create a temporary table
    println!("\n--- Test 2: CREATE TABLE ---");
    let mut handler = TextHandler::new();
    conn.query(
        "CREATE TEMPORARY TABLE IF NOT EXISTS test_text (
            id INT PRIMARY KEY,
            username VARCHAR(50),
            age INT,
            email VARCHAR(100)
        )",
        &mut handler,
    )?;

    // Test 3: Insert some data
    println!("\n--- Test 3: INSERT data ---");
    let mut handler = TextHandler::new();
    conn.query(
        "INSERT INTO test_text (id, username, age, email) VALUES
         (1, 'alice', 25, 'alice@example.com'),
         (2, 'bob', 30, 'bob@example.com'),
         (3, 'charlie', 35, 'charlie@example.com')",
        &mut handler,
    )?;

    // Test 4: SELECT the inserted data
    println!("\n--- Test 4: SELECT data ---");
    let mut handler = TextHandler::new();
    conn.query(
        "SELECT id, username, age, email FROM test_text ORDER BY id",
        &mut handler,
    )?;

    // Test 5: UPDATE data
    println!("\n--- Test 5: UPDATE data ---");
    let mut handler = TextHandler::new();
    conn.query(
        "UPDATE test_text SET age = age + 1 WHERE username = 'alice'",
        &mut handler,
    )?;

    // Test 6: DELETE data
    println!("\n--- Test 6: DELETE data ---");
    let mut handler = TextHandler::new();
    conn.query("DELETE FROM test_text WHERE id = 3", &mut handler)?;

    // Test 7: Verify final state
    println!("\n--- Test 7: Verify final state ---");
    let mut handler = TextHandler::new();
    conn.query(
        "SELECT id, username, age, email FROM test_text ORDER BY id",
        &mut handler,
    )?;

    println!("\nText protocol example completed successfully!");

    Ok(())
}
