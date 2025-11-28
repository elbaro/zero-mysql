use zero_mysql::error::Result;
use zero_mysql::protocol::TextRowPayload;
use zero_mysql::protocol::command::ColumnDefinition;
use zero_mysql::protocol::response::OkPayloadBytes;
use zero_mysql::tokio::Conn;

// #[global_allocator]
// static GLOBAL: tracy_client::ProfiledAllocator<std::alloc::System> =
//     tracy_client::ProfiledAllocator::new(std::alloc::System, 100);

// Simple handler that ignores all output (for setup queries)
struct DropHandler;

impl zero_mysql::protocol::r#trait::TextResultSetHandler for DropHandler {
    fn no_result_set(&mut self, _ok: OkPayloadBytes) -> Result<()> {
        Ok(())
    }

    fn resultset_start(&mut self, _cols: &[ColumnDefinition<'_>]) -> Result<()> {
        Ok(())
    }

    fn row(&mut self, _cols: &[ColumnDefinition<'_>], _row: &TextRowPayload<'_>) -> Result<()> {
        Ok(())
    }

    fn resultset_end(&mut self, _eof: OkPayloadBytes) -> Result<()> {
        Ok(())
    }
}

fn main() -> Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async_main())
}

async fn async_main() -> Result<()> {
    let mut conn = Conn::new("mysql://test:1234@127.0.0.1/test").await?;

    {
        let mut handler = DropHandler;
        conn.query("DROP TABLE IF EXISTS test_bench", &mut handler)
            .await?;
        conn.query(
            "CREATE TABLE test_bench (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100),
                age INT,
                email VARCHAR(100),
                score FLOAT,
                description VARCHAR(100)
            ) ENGINE=MEMORY",
            &mut handler,
        )
        .await?;
    }

    let mut insert_stmt = conn
        .prepare(
            "INSERT INTO test_bench (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
        )
        .await?;

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

    for iteration in 0..10 {
        let iteration_start = std::time::Instant::now();

        for (username, age, email, score, description) in rows.iter() {
            // let _row_span = tracing::trace_span!("row", row_id).entered();
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

        println!(
            "Iteration {}: Inserted 10,000 rows (took {:.2}ms)",
            iteration,
            iteration_start.elapsed().as_secs_f64() * 1000.0
        );
        let mut handler = DropHandler;
        conn.query("TRUNCATE TABLE test_bench", &mut handler)
            .await?;
    }

    Ok(())
}
