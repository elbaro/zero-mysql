//! Benchmark: zero-mysql async (compio)
//!
//! Usage:
//!   DATABASE_URL=mysql://user:pass@localhost/test cargo run --example bench_zero_compio --release --features compio

use std::env;
use zero_mysql::compio::Conn;

#[compio::main]
async fn main() -> zero_mysql::error::Result<()> {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let mut conn = Conn::new(url.as_str()).await?;

    conn.query_drop("DROP TABLE IF EXISTS bench_compio").await?;
    conn.query_drop(
        "CREATE TABLE bench_compio (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            age INT,
            email VARCHAR(100),
            score FLOAT,
            description VARCHAR(100)
        )",
    )
    .await?;

    let mut insert_stmt = conn
        .prepare("INSERT INTO bench_compio (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)")
        .await?;

    const N: usize = 10000;
    let mut rows = Vec::with_capacity(N);
    for i in 0..N {
        rows.push((
            format!("user_{}", i),
            (20 + (i % 50)) as i32,
            format!("user{}@example.com", i),
            (i % 100) as f32 / 10.0,
            format!("Description for user {}", i),
        ));
    }

    for iteration in 0..10 {
        let iteration_start = std::time::Instant::now();

        for (username, age, email, score, description) in rows.iter() {
            conn.exec_drop(
                &mut insert_stmt,
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

        let elapsed = iteration_start.elapsed();
        let mut count_stmt = conn.prepare("SELECT COUNT(*) FROM bench_compio").await?;
        let count: Vec<(i64,)> = conn.exec_collect(&mut count_stmt, ()).await?;
        #[allow(clippy::print_stdout)]
        {
            println!(
                "Iteration {}: Inserted {} rows (took {:.2}ms)",
                iteration,
                count[0].0,
                elapsed.as_secs_f64() * 1000.0
            );
        }
        conn.query_drop("TRUNCATE TABLE bench_compio").await?;
    }

    conn.query_drop("DROP TABLE bench_compio").await?;

    Ok(())
}
