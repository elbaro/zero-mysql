use zero_mysql::error::Result;
use zero_mysql::sync::Conn;

// #[global_allocator]
// static GLOBAL: tracy_client::ProfiledAllocator<std::alloc::System> =
//     tracy_client::ProfiledAllocator::new(std::alloc::System, 100);

fn main() -> Result<()> {
    // tracy_client::Client::start();
    // use tracing_subscriber::layer::SubscriberExt;
    // let subscriber = tracing_subscriber::registry().with(tracing_tracy::TracyLayer::default());
    // tracing::subscriber::set_global_default(subscriber).unwrap();

    let mut conn = Conn::new("mysql://test:1234@localhost/test")?;
    {
        // Drop existing table
        let mut drop_stmt = conn.prepare("DROP TABLE IF EXISTS test_bench")?;
        conn.exec_drop(&mut drop_stmt, ())?;

        // Create new table with MEMORY engine
        let mut create_stmt = conn.prepare(
            "CREATE TABLE test_bench (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100),
                age INT,
                email VARCHAR(100),
                score FLOAT,
                description VARCHAR(100)
            ) ENGINE=MEMORY",
        )?;
        conn.exec_drop(&mut create_stmt, ())?;
    }

    // Prepare statements
    let mut insert_stmt = conn.prepare(
        "INSERT INTO test_bench (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
    )?;
    let mut truncate_stmt = conn.prepare("TRUNCATE TABLE test_bench")?;

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

    let mut iteration = 0u64;

    loop {
        iteration += 1;
        let iteration_start = std::time::Instant::now();

        for (username, age, email, score, description) in &rows {
            conn.exec_drop(
                &mut insert_stmt,
                (
                    username.as_str(),
                    *age,
                    email.as_str(),
                    *score,
                    description.as_str(),
                ),
            )?;
        }

        let elapsed = iteration_start.elapsed();
        println!(
            "Iteration {}: Inserted 10,000 rows (took {:.2}ms)",
            iteration,
            elapsed.as_secs_f64() * 1000.0
        );
        conn.exec_drop(&mut truncate_stmt, ())?;
    }
}
