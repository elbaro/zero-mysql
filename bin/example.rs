use zero_mysql::error::Result;
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

    println!("Connected to MySQL {}", conn.server_version());
    println!("Capability flags: {:?}", conn.capability_flags());

    // Prepare a simple query
    println!("\nPreparing query: SELECT 1 + 1 AS result");
    let stmt_id = conn.prepare("SELECT 1 + 1 AS result")?;
    println!("Statement prepared successfully with ID: {}", stmt_id);

    // Execute the query with no parameters
    println!("\nExecuting query...");
    let mut buffer = Vec::new();

    // Create a simple row counter using the new ResultSetHandler trait
    struct RowCounter {
        count: usize,
    }

    impl RowCounter {
        fn new() -> Self {
            Self { count: 0 }
        }

        fn count(&self) -> usize {
            self.count
        }
    }

    impl<'a> zero_mysql::protocol::r#trait::ResultSetHandler<'a> for RowCounter {
        fn ok(&mut self, _ok: zero_mysql::protocol::packet::OkPayloadBytes) -> Result<()> {
            println!("Received OK packet");
            Ok(())
        }

        // fn err(_err: zero_mysql::protocol::packet::ErrPayload) {
        //     println!("Received ERR packet");
        // }

        fn start(
            &mut self,
            column_count: usize,
            _column_defs: &[zero_mysql::col::ColumnDefinition],
        ) -> Result<()> {
            println!("Result set started with {} columns", column_count);
            self.count = 0;
            Ok(())
        }

        fn row(&mut self, _row: &zero_mysql::row::RowPayload) -> Result<()> {
            self.count += 1;
            Ok(())
        }

        fn finish(&mut self, _eof: &zero_mysql::protocol::packet::OkPayloadBytes) -> Result<()> {
            println!("Result set finished (EOF received)");
            Ok(())
        }
    }

    let mut decoder = RowCounter::new();
    conn.exec_fold(
        stmt_id,
        &(), // No parameters
        &mut decoder,
        &mut buffer,
    )?;

    println!(
        "Query executed successfully! Rows returned: {}",
        decoder.count()
    );

    println!("\nExample completed successfully!");

    Ok(())
}
