use mysql_async::{prelude::*, *};

#[global_allocator]
static GLOBAL: tracy_client::ProfiledAllocator<std::alloc::System> =
    tracy_client::ProfiledAllocator::new(std::alloc::System, 100);

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracy client and tracing
    tracy_client::Client::start();

    use tracing_subscriber::layer::SubscriberExt;
    let subscriber = tracing_subscriber::registry()
        .with(tracing_tracy::TracyLayer::default());
    tracing::subscriber::set_global_default(subscriber).unwrap();

    // Connect to MySQL server
    println!("Connecting to MySQL...");
    let pool = Pool::new("mysql://test:1234@localhost/test");
    let mut conn = pool.get_conn().await?;
    println!("Connected to MySQL");

    // Drop and recreate the test table using MEMORY engine
    {
        let _span = tracy_client::span!("create_table");
        println!("Creating test table...");

        // Drop existing table
        conn.query_drop("DROP TABLE IF EXISTS test_bench").await?;

        // Create new table with MEMORY engine
        conn.query_drop(
            r"CREATE TABLE test_bench (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100),
                age INT,
                email VARCHAR(100),
                score FLOAT,
                description VARCHAR(100)
            ) ENGINE=MEMORY"
        ).await?;
    }

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

                {
                    let _trace = tracing::trace_span!("exec_drop").entered();
                    conn.exec_drop(
                        r"INSERT INTO test_bench (name, age, email, score, description)
                          VALUES (?, ?, ?, ?, ?)",
                        (username.as_str(), *age, email.as_str(), *score, description.as_str()),
                    ).await?;
                }
            }
        }

        println!("Iteration {}: Inserted 10,000 rows", iteration);

        // Truncate the table
        {
            let _span = tracing::info_span!("truncate_table").entered();
            conn.query_drop("TRUNCATE TABLE test_bench").await?;
        }

        let elapsed = iteration_start.elapsed();
        println!("Iteration {}: Truncated table (took {:.2}ms)", iteration, elapsed.as_secs_f64() * 1000.0);
    }
}
