use mysql::prelude::*;
use mysql::*;

// #[global_allocator]
// static GLOBAL: tracy_client::ProfiledAllocator<std::alloc::System> =
//     tracy_client::ProfiledAllocator::new(std::alloc::System, 100);

pub struct User {
    pub id: i32,
    pub name: String,
    pub hair_color: Option<String>,
}

fn main() -> Result<()> {
    tracy_client::Client::start();
    use tracing_subscriber::layer::SubscriberExt;
    let subscriber = tracing_subscriber::registry().with(tracing_tracy::TracyLayer::default());
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let connection_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set in order to run tests");
    let pool = Pool::new(connection_url.as_str())?;
    let mut conn = pool.get_conn()?;

    // Clear existing data
    // conn.query_drop("SET FOREIGN_KEY_CHECKS = 0;")?;
    // conn.query_drop("TRUNCATE TABLE comments")?;
    // conn.query_drop("TRUNCATE TABLE posts")?;
    // conn.query_drop("TRUNCATE TABLE users")?;

    // Prepare insert statement
    let insert_stmt = conn.prep(r"INSERT INTO users (name, hair_color) VALUES (?, ?)")?;

    // Prepare select statement
    let select_stmt = conn.prep(r"SELECT id, name, hair_color FROM users")?;

    // Insert initial data (10,000 users)
    // {
    //     for i in 0..1 {
    //         let name = format!("User {}", i);
    //         let hair_color = if i % 2 == 0 {
    //             Some("black")
    //         } else {
    //             Some("brown")
    //         };

    //         conn.exec_drop(&insert_stmt, (name.as_str(), hair_color.as_deref()))?;
    //     }
    // }

    let mut iteration = 0u64;
    loop {
        iteration += 1;
        let iteration_start = std::time::Instant::now();
        for _ in 0..1000 {
            let rows: Vec<User> = conn.exec_map(
                &select_stmt,
                (),
                |(id, name, hair_color): (i32, String, Option<String>)| User {
                    id,
                    name,
                    hair_color,
                },
            )?;
        }
        let elapsed = iteration_start.elapsed();

        println!(
            "Iteration {}: Selected (took {:.2}ms)",
            iteration,
            elapsed.as_secs_f64() * 1000.0
        );
    }
}
