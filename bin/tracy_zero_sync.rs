use zero_mysql::col::ColumnTypeAndFlags;
use zero_mysql::error::Result;
use zero_mysql::protocol::value::Value;
use zero_mysql::sync::Conn;

#[global_allocator]
static GLOBAL: tracy_client::ProfiledAllocator<std::alloc::System> =
    tracy_client::ProfiledAllocator::new(std::alloc::System, 100);

fn main() -> Result<()> {
    // Initialize tracy client and tracing
    tracy_client::Client::start();

    use tracing_subscriber::layer::SubscriberExt;
    let subscriber = tracing_subscriber::registry()
        .with(tracing_tracy::TracyLayer::default());
    tracing::subscriber::set_global_default(subscriber).unwrap();

    // Connect to MySQL server
    println!("Connecting to MySQL...");
    let mut conn = Conn::new("mysql://test:1234@localhost/test")?;
    println!("Connected to MySQL {}", conn.server_version());

    // Create a simple handler for result sets
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
            Ok(())
        }

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
            Ok(())
        }

        fn resultset_end(
            &mut self,
            _eof: zero_mysql::protocol::packet::OkPayloadBytes,
        ) -> Result<()> {
            Ok(())
        }
    }

    // Drop and recreate the test table using MEMORY engine
    {
        let _span = tracy_client::span!("create_table");
        println!("Creating test table...");

        // Drop existing table
        let drop_stmt = conn.prepare("DROP TABLE IF EXISTS test_bench")?;
        let mut decoder = Handler::new();
        let empty_params: [i32; 0] = [];
        conn.exec(drop_stmt, &empty_params, &mut decoder)?;

        // Create new table with MEMORY engine
        let create_stmt = conn.prepare(
            "CREATE TABLE test_bench (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100),
                age INT,
                email VARCHAR(100),
                score FLOAT,
                description VARCHAR(100)
            ) ENGINE=MEMORY",
        )?;
        let mut decoder = Handler::new();
        conn.exec(create_stmt, &empty_params, &mut decoder)?;
    }

    // Prepare statements
    let insert_stmt = conn.prepare(
        "INSERT INTO test_bench (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
    )?;
    let truncate_stmt = conn.prepare("TRUNCATE TABLE test_bench")?;

    // Pre-construct row data to avoid measuring string formatting overhead
    let _span = tracing::info_span!("prepare_data").entered();
    let mut rows = Vec::with_capacity(10_000);
    for i in 0..10_000 {
        rows.push((
            format!("user_{}", i),
            20 + (i % 50),
            format!("user{}@example.com", i),
            (i % 100) as f32 / 10.0,
            format!("Description for user {}", i),
        ));
    }
    drop(_span);

    println!("Starting infinite loop: inserting 10,000 rows and truncating...");
    let mut iteration = 0u64;

    loop {
        iteration += 1;
        let iteration_start = std::time::Instant::now();

        // Insert 10,000 rows
        {
            let _span = tracing::info_span!("insert_10000_rows").entered();
            for (username, age, email, score, description) in &rows {
                let _span = tracing::trace_span!("insert_row").entered();

                let mut insert_decoder = Handler::new();
                let insert_params = (
                    username.as_str(),
                    *age,
                    email.as_str(),
                    *score,
                    description.as_str(),
                );
                {
                    let _trace = tracing::trace_span!("conn_exec").entered();
                    conn.exec(insert_stmt, &insert_params, &mut insert_decoder)?;
                }
            }
        }

        println!("Iteration {}: Inserted 10,000 rows", iteration);

        // Truncate the table
        {
            let _span = tracing::info_span!("truncate_table").entered();
            let mut truncate_decoder = Handler::new();
            let empty_params: [i32; 0] = [];
            conn.exec(truncate_stmt, &empty_params, &mut truncate_decoder)?;
        }

        let elapsed = iteration_start.elapsed();
        println!("Iteration {}: Truncated table (took {:.2}ms)", iteration, elapsed.as_secs_f64() * 1000.0);
    }
}
