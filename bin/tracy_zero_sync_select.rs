use zero_mysql::error::Result;
use zero_mysql::protocol::command::ColumnDefinition;
use zero_mysql::protocol::r#trait::BinaryResultSetHandler;
use zero_mysql::protocol::response::OkPayloadBytes;
use zero_mysql::protocol::value::Value;
use zero_mysql::protocol::BinaryRowPayload;
use zero_mysql::sync::Conn;

// #[global_allocator]
// static GLOBAL: tracy_client::ProfiledAllocator<std::alloc::System> =
//     tracy_client::ProfiledAllocator::new(std::alloc::System, 100);

pub struct User {
    pub id: i32,
    pub name: String,
    pub hair_color: Option<String>,
}

// Handler for collecting users
struct UsersHandler {
    users: Vec<User>,
}

impl UsersHandler {
    fn new() -> Self {
        Self { users: Vec::new() }
    }
}

impl BinaryResultSetHandler for UsersHandler {
    #[inline(always)]
    fn no_result_set(&mut self, _ok: OkPayloadBytes) -> zero_mysql::error::Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn resultset_start(&mut self, _num_columns: usize) -> zero_mysql::error::Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn row<'a>(
        &mut self,
        cols: &[ColumnDefinition<'a>],
        row: &'a BinaryRowPayload<'a>,
    ) -> zero_mysql::error::Result<()> {
        let mut bytes = row.values();
        let mut values: [std::mem::MaybeUninit<Value<'a>>; 3] =
            [const { std::mem::MaybeUninit::uninit() }; 3];

        for i in 0..cols.len() {
            if row.null_bitmap().is_null(i) {
                values[i].write(Value::Null);
            } else {
                let type_and_flags = cols[i].tail.type_and_flags()?;
                let (value, remaining) = Value::parse(&type_and_flags, bytes)?;
                values[i].write(value);
                bytes = remaining;
            }
        }

        let values = unsafe {
            [
                values[0].assume_init_read(),
                values[1].assume_init_read(),
                values[2].assume_init_read(),
            ]
        };

        let user = User {
            id: if let Value::SignedInt(id) = values[0] {
                id as i32
            } else {
                panic!("Expected SignedInt for id");
            },
            name: if let Value::Byte(name) = &values[1] {
                String::from_utf8_lossy(name).to_string()
            } else {
                panic!("Expected Byte for name");
            },
            hair_color: match &values[2] {
                Value::Null => None,
                Value::Byte(s) => Some(String::from_utf8_lossy(s).to_string()),
                _ => panic!("Expected Byte or Null for hair_color"),
            },
        };

        self.users.push(user);
        Ok(())
    }

    #[inline(always)]
    fn resultset_end(&mut self, _eof: OkPayloadBytes) -> zero_mysql::error::Result<()> {
        Ok(())
    }
}

fn main() -> Result<()> {
    tracy_client::Client::start();
    use tracing_subscriber::layer::SubscriberExt;
    let subscriber = tracing_subscriber::registry().with(tracing_tracy::TracyLayer::default());
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let connection_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set in order to run tests");
    let mut conn = Conn::new(connection_url.as_str())?;

    // conn.query_drop("SET FOREIGN_KEY_CHECKS = 0;")?;
    // conn.query_drop("TRUNCATE TABLE comments")?;
    // conn.query_drop("TRUNCATE TABLE posts")?;
    // conn.query_drop("TRUNCATE TABLE users")?;
    let mut _insert_stmt = conn.prepare("INSERT INTO users (name, hair_color) VALUES (?, ?)")?;
    let mut select_stmt = conn.prepare("SELECT id, name, hair_color FROM users")?;
    // for i in 0..1 {
    //     let name = format!("User {}", i);
    //     let hair_color = if i % 2 == 0 {
    //         Some("black")
    //     } else {
    //         Some("brown")
    //     };

    //     conn.exec_drop(insert_stmt, (name.as_str(), hair_color))?;
    // }

    let mut iteration = 0u64;
    let mut handler = UsersHandler::new();

    for iteration in 1..10 {
        let iteration_start = std::time::Instant::now();

        for _ in 0..1000 {
            handler.users.clear();
            conn.exec(&mut select_stmt, (), &mut handler)?;
        }
        let elapsed = iteration_start.elapsed();
        println!(
            "Iteration {}: Selected (took {:.2}ms)",
            iteration,
            elapsed.as_secs_f64() * 1000.0
        );
    }
    Ok(())
}
