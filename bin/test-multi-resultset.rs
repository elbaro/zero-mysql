use zero_mysql::error::Result;
use zero_mysql::protocol::TextRowPayload;
use zero_mysql::protocol::connection::ColumnDefinitionBytes;
use zero_mysql::protocol::response::OkPayloadBytes;
use zero_mysql::tokio::Conn;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Connecting to MySQL...");
    let mut conn = match Conn::new("mysql://test:1234@localhost/test").await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to connect: {:?}", e);
            return Err(e);
        }
    };

    println!("Connected to MySQL");
    println!("Capability flags: {:?}", conn.capability_flags());

    // Create a handler that counts resultsets
    struct MultiResultSetHandler {
        resultset_count: usize,
        current_columns: usize,
        row_count: usize,
    }

    impl MultiResultSetHandler {
        fn new() -> Self {
            Self {
                resultset_count: 0,
                current_columns: 0,
                row_count: 0,
            }
        }
    }

    impl<'a> zero_mysql::protocol::r#trait::TextResultSetHandler<'a> for MultiResultSetHandler {
        fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
            let ok_payload = zero_mysql::protocol::response::OkPayload::try_from(ok)?;
            println!(
                "  [ResultSet #{}] Query OK, {} rows affected",
                self.resultset_count, ok_payload.affected_rows
            );
            self.resultset_count += 1;
            self.row_count = 0;
            Ok(())
        }

        fn resultset_start(&mut self, num_columns: usize) -> Result<()> {
            println!(
                "  [ResultSet #{}] Started with {} columns",
                self.resultset_count, num_columns
            );
            self.current_columns = num_columns;
            self.row_count = 0;
            Ok(())
        }

        fn col(&mut self, col: ColumnDefinitionBytes) -> Result<()> {
            let col_def: zero_mysql::protocol::connection::ColumnDefinition = col.try_into()?;
            println!("    Column: {:?}", col_def.name);
            Ok(())
        }

        fn row(&mut self, _row: &TextRowPayload) -> Result<()> {
            self.row_count += 1;
            Ok(())
        }

        fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()> {
            let eof_payload = zero_mysql::protocol::response::OkPayload::try_from(eof)?;
            println!(
                "  [ResultSet #{}] Finished with {} rows (status_flags: {:?})",
                self.resultset_count, self.row_count, eof_payload.status_flags
            );
            self.resultset_count += 1;
            self.row_count = 0;
            Ok(())
        }
    }

    // Test 1: Multiple SELECT statements
    println!("\n--- Test 1: Multiple SELECT statements ---");
    let mut handler = MultiResultSetHandler::new();
    conn.query(
        "SELECT 1 AS first; SELECT 2 AS second; SELECT 3 AS third",
        &mut handler,
    )
    .await?;
    println!("Total resultsets processed: {}", handler.resultset_count);

    // Test 2: Mix of INSERT and SELECT
    println!("\n--- Test 2: CREATE TABLE, INSERT, and SELECT ---");
    conn.query_drop("DROP TEMPORARY TABLE IF EXISTS multi_test")
        .await?;

    let mut handler = MultiResultSetHandler::new();
    conn.query(
        "CREATE TEMPORARY TABLE multi_test (id INT, value VARCHAR(50)); \
         INSERT INTO multi_test VALUES (1, 'first'), (2, 'second'); \
         SELECT * FROM multi_test",
        &mut handler,
    )
    .await?;
    println!("Total resultsets processed: {}", handler.resultset_count);

    // Test 3: Multiple UPDATEs and a SELECT
    println!("\n--- Test 3: Multiple UPDATEs and a SELECT ---");
    let mut handler = MultiResultSetHandler::new();
    conn.query(
        "UPDATE multi_test SET value = 'updated_first' WHERE id = 1; \
         UPDATE multi_test SET value = 'updated_second' WHERE id = 2; \
         SELECT * FROM multi_test ORDER BY id",
        &mut handler,
    )
    .await?;
    println!("Total resultsets processed: {}", handler.resultset_count);

    println!("\nMultiple resultset test completed successfully!");

    Ok(())
}
