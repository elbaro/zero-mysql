use mysql_async::{prelude::*, *};

// #[global_allocator]
// static GLOBAL: tracy_client::ProfiledAllocator<std::alloc::System> =
//     tracy_client::ProfiledAllocator::new(std::alloc::System, 100);

fn main() -> Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async_main())
}

async fn async_main() -> Result<()> {
    let pool = Pool::new("mysql://test:1234@127.0.0.1/test");
    let mut conn = pool.get_conn().await?;

    {
        conn.query_drop("DROP TABLE IF EXISTS test_bench").await?;
        conn.query_drop(
            r"CREATE TABLE test_bench (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100),
                age INT,
                email VARCHAR(100),
                score FLOAT,
                description VARCHAR(100)
            ) ENGINE=MEMORY",
        )
        .await?;
    }

    const N: usize = 10000;
    let mut rows = Vec::with_capacity(N);
    for i in 0..N {
        rows.push((
            format!("user_{}", i),
            20 + (i % 50) as u64,
            format!("user{}@example.com", i),
            (i % 100) as f32 / 10.0,
            format!("Description for user {}", i),
        ));
    }

    for iteration in 0..1 {
        let iteration_start = std::time::Instant::now();

        for (username, age, email, score, description) in rows.iter() {
            // let _row_span = tracing::trace_span!("row", row_id).entered();
            conn.exec_drop(
                r"INSERT INTO test_bench (name, age, email, score, description)
                          VALUES (?, ?, ?, ?, ?)",
                (
                    username.as_str(),
                    *age,
                    email.as_str(),
                    *score,
                    description.as_str(),
                ),
            )
            .await?;
        }

        println!(
            "Iteration {}: Inserted 10,000 rows (took: {:.2}ms)",
            iteration,
            iteration_start.elapsed().as_secs_f64() * 1000.
        );

        // Truncate the table
        conn.query_drop("TRUNCATE TABLE test_bench").await?;
    }

    Ok(())
}
