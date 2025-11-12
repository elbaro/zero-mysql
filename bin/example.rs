use zero_mysql::col::ColumnTypeAndFlags;
use zero_mysql::error::Result;
use zero_mysql::protocol::value::Value;
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
    let stmt_id = conn.prepare("SELECT 1 + ?")?;
    println!("Statement prepared successfully with ID: {}", stmt_id);

    // Execute the query with no parameters
    let mut buffer = Vec::new();

    // Create a simple row counter using the new ResultSetHandler trait
    struct Handler {
        cols: Vec<ColumnTypeAndFlags>,
    }

    impl Handler {
        fn new() -> Self {
            Self { cols: Vec::new() }
        }
    }

    impl<'a> zero_mysql::protocol::r#trait::ResultSetHandler<'a> for Handler {
        fn no_result_set(
            &mut self,
            _ok: zero_mysql::protocol::packet::OkPayloadBytes,
        ) -> Result<()> {
            println!("Received no result set");
            Ok(())
        }

        // fn err(_err: zero_mysql::protocol::packet::ErrPayload) {
        //     println!("Received ERR packet");
        // }

        fn resultset_start(&mut self, num_columns: usize) -> Result<()> {
            self.cols.reserve(num_columns);
            Ok(())
        }

        fn col(&mut self, col: zero_mysql::col::ColumnDefinitionBytes) -> Result<()> {
            self.cols.push(col.tail()?.type_and_flags()?);
            Ok(())
        }

        fn row(&mut self, row: &zero_mysql::row::RowPayload) -> Result<()> {
            let mut values = vec![];
            let mut bytes = row.values();
            for i in 0..self.cols.len() {
                if row.null_bitmap().is_null(i) {
                    values.push(Value::Null);
                } else {
                    let value;
                    (value, bytes) = Value::parse(&self.cols[i], bytes)?;
                    values.push(value);
                }
            }
            println!("Row: {values:?}");
            Ok(())
        }

        fn resultset_end(
            &mut self,
            _eof: zero_mysql::protocol::packet::OkPayloadBytes,
        ) -> Result<()> {
            println!(
                "Result set finished (EOF received) : {:?}",
                zero_mysql::protocol::response::OkPayload::try_from(_eof)?
            );
            Ok(())
        }
    }

    let mut decoder = Handler::new();
    conn.exec_fold(stmt_id, vec![2], &mut decoder, &mut buffer)?;

    println!("\nExample completed successfully!");

    Ok(())
}
