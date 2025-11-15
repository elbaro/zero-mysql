use zero_mysql::error::Result;
use zero_mysql::r#async::Conn;

// #[global_allocator]
// static GLOBAL: tracy_client::ProfiledAllocator<std::alloc::System> =
//     tracy_client::ProfiledAllocator::new(std::alloc::System, 100);

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracy client and tracing
    tracy_client::Client::start();

    use tracing_subscriber::layer::SubscriberExt;
    let subscriber = tracing_subscriber::registry().with(tracing_tracy::TracyLayer::default());
    tracing::subscriber::set_global_default(subscriber).unwrap();

    // Connect to MySQL server
    println!("Connecting to MySQL...");
    let mut conn = Conn::new("mysql://test:1234@127.0.0.1/test").await?;
    println!("Connected to MySQL {}", conn.server_version());

    // Drop and recreate the test table using MEMORY engine
    {
        println!("Creating test table...");

        // Drop existing table
        let drop_stmt = conn.prepare("DROP TABLE IF EXISTS test_bench").await?;
        conn.exec_drop(drop_stmt, ()).await?;

        // Create new table with MEMORY engine
        let create_stmt = conn
            .prepare(
                "CREATE TABLE test_bench (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100),
                age INT,
                email VARCHAR(100),
                score FLOAT,
                description VARCHAR(100)
            ) ENGINE=MEMORY",
            )
            .await?;
        conn.exec_drop(create_stmt, ()).await?;
    }

    // Prepare statements
    let insert_stmt = conn
        .prepare(
            "INSERT INTO test_bench (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
        )
        .await?;
    let truncate_stmt = conn.prepare("TRUNCATE TABLE test_bench").await?;

    // Pre-construct row data to avoid measuring string formatting overhead
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

    println!("Starting infinite loop: inserting 10,000 rows and truncating...");
    let mut iteration = 0u64;

    loop {
        iteration += 1;
        let iteration_start = std::time::Instant::now();

        // Insert 10,000 rows
        for (row_id, (username, age, email, score, description)) in rows.iter().enumerate() {
            let _row_span = tracing::trace_span!("row", row_id).entered();
            conn.exec_drop(insert_stmt, (username.as_str(), *age, email.as_str(), *score, description.as_str())).await?;
        }

        println!("Iteration {}: Inserted 10,000 rows (took {:.2}ms)", iteration, iteration_start.elapsed().as_secs_f64()*1000.0);
        conn.exec_drop(truncate_stmt, ()).await?;
    }
}
