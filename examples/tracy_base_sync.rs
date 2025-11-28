use mysql::prelude::*;
use mysql::*;

// #[global_allocator]
// static GLOBAL: tracy_client::ProfiledAllocator<std::alloc::System> =
//     tracy_client::ProfiledAllocator::new(std::alloc::System, 100);

fn main() -> Result<()> {
    // tracy_client::Client::start();
    // use tracing_subscriber::layer::SubscriberExt;
    // let subscriber = tracing_subscriber::registry().with(tracing_tracy::TracyLayer::default());
    // tracing::subscriber::set_global_default(subscriber).unwrap();

    let pool = Pool::new("mysql://test:1234@localhost/test?prefer_socket=false")?;
    let mut conn = pool.get_conn()?;

    {
        conn.query_drop("DROP TABLE IF EXISTS test_bench")?;
        conn.query_drop(
            r"CREATE TABLE test_bench (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100),
                age INT,
                email VARCHAR(100),
                score FLOAT,
                description VARCHAR(100)
            ) ENGINE=MEMORY",
        )?;
    }

    // Prepare statement
    let insert_stmt = conn.prep(
        r"INSERT INTO test_bench (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
    )?;

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
        {
            for (username, age, email, score, description) in &rows {
                conn.exec_drop(
                    &insert_stmt,
                    (
                        username.as_str(),
                        *age,
                        email.as_str(),
                        *score,
                        description.as_str(),
                    ),
                )?;
            }
        }
        let elapsed = iteration_start.elapsed();

        println!(
            "Iteration {}: Inserted 10,000 rows (took {:.2}ms)",
            iteration,
            elapsed.as_secs_f64() * 1000.0
        );
        conn.query_drop("TRUNCATE TABLE test_bench")?;
    }
}
