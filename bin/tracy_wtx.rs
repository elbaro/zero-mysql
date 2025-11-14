use wtx::{
    database::{Executor, Database},
    misc::UriRef,
};

#[global_allocator]
static GLOBAL: tracy_client::ProfiledAllocator<std::alloc::System> =
    tracy_client::ProfiledAllocator::new(std::alloc::System, 100);

#[tokio::main]
async fn main() -> wtx::Result<()> {
    // Initialize tracy client
    tracy_client::Client::start();

    // Connect to MySQL server
    println!("Connecting to MySQL...");
    let uri = UriRef::new("mysql://test:1234@localhost/test");
    let mut db = Database::with_executor(&uri).await?;
    println!("Connected to MySQL");

    // Create the test table
    {
        let _span = tracy_client::span!("create_table");
        println!("Creating test table...");
        db.execute(
            "CREATE TABLE IF NOT EXISTS test_bench (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(50),
                age INT,
                email VARCHAR(100),
                score FLOAT,
                description TEXT
            )",
            |_| {},
        )
        .await?;
    }

    println!("Starting infinite loop: inserting 10,000 rows and truncating...");
    let mut iteration = 0u64;

    loop {
        iteration += 1;
        let iteration_start = std::time::Instant::now();

        // Insert 10,000 rows
        {
            let _span = tracy_client::span!("insert_10000_rows");
            for i in 0..10_000 {
                let _span = tracy_client::span!("insert_row");

                let username = format!("user_{}", i);
                let age = 20 + (i % 50);
                let email = format!("user{}@example.com", i);
                let score = (i % 100) as f32 / 10.0;
                let description = format!("Description for user {} in iteration {}", i, iteration);

                db.execute_with_stmt(
                    "INSERT INTO test_bench (username, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
                    (username.as_str(), age, email.as_str(), score, description.as_str()),
                    |_| {},
                )
                .await?;
            }
        }

        println!("Iteration {}: Inserted 10,000 rows", iteration);

        // Truncate the table
        {
            let _span = tracy_client::span!("truncate_table");
            db.execute("TRUNCATE TABLE test_bench", |_| {}).await?;
        }

        let elapsed = iteration_start.elapsed();
        println!("Iteration {}: Truncated table (took {:.2}ms)", iteration, elapsed.as_secs_f64() * 1000.0);
        tracy_client::frame_mark();
    }
}
