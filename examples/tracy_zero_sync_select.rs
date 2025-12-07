use zero_mysql::error::Result;
use zero_mysql::protocol::BinaryRowPayload;
use zero_mysql::protocol::command::ColumnDefinition;
use zero_mysql::protocol::response::OkPayloadBytes;
use zero_mysql::protocol::r#trait::BinaryResultSetHandler;
use zero_mysql::raw::parse_value;
use zero_mysql::sync::Conn;
use zero_mysql::value::Value;

pub struct User {
    pub id: i32,
    pub name: String,
    pub hair_color: Option<String>,
}

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
    fn resultset_start(&mut self, _cols: &[ColumnDefinition<'_>]) -> zero_mysql::error::Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn row(
        &mut self,
        cols: &[ColumnDefinition<'_>],
        row: BinaryRowPayload<'_>,
    ) -> zero_mysql::error::Result<()> {
        let null_bitmap = row.null_bitmap();
        let mut bytes = row.values();

        // Parse id (column 0)
        let (id_value, rest): (Value<'_>, _) =
            parse_value(cols[0].tail, null_bitmap.is_null(0), bytes)?;
        bytes = rest;
        let id = if let Value::SignedInt(id) = id_value {
            id as i32
        } else {
            panic!("Expected SignedInt for id");
        };

        // Parse name (column 1)
        let (name_value, rest): (Value<'_>, _) =
            parse_value(cols[1].tail, null_bitmap.is_null(1), bytes)?;
        bytes = rest;
        let name = if let Value::Byte(name) = name_value {
            String::from_utf8_lossy(name).to_string()
        } else {
            panic!("Expected Byte for name");
        };

        // Parse hair_color (column 2)
        let (hair_color_value, _rest): (Value<'_>, _) =
            parse_value(cols[2].tail, null_bitmap.is_null(2), bytes)?;
        let hair_color = match hair_color_value {
            Value::Null => None,
            Value::Byte(s) => Some(String::from_utf8_lossy(s).to_string()),
            _ => panic!("Expected Byte or Null for hair_color"),
        };

        self.users.push(User {
            id,
            name,
            hair_color,
        });
        Ok(())
    }

    #[inline(always)]
    fn resultset_end(&mut self, _eof: OkPayloadBytes) -> zero_mysql::error::Result<()> {
        Ok(())
    }
}

fn main() -> Result<()> {
    let connection_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set in order to run tests");
    let mut conn = Conn::new(connection_url.as_str())?;

    conn.query_drop("SET FOREIGN_KEY_CHECKS = 0;")?;
    conn.query_drop("TRUNCATE TABLE comments")?;
    conn.query_drop("TRUNCATE TABLE posts")?;
    conn.query_drop("TRUNCATE TABLE users")?;
    let mut insert_stmt = conn.prepare("INSERT INTO users (name, hair_color) VALUES (?, ?)")?;
    let mut select_stmt = conn.prepare("SELECT id, name, hair_color FROM users")?;
    for i in 0..1 {
        let name = format!("User {}", i);
        let hair_color = if i % 2 == 0 {
            Some("black")
        } else {
            Some("brown")
        };

        conn.exec_drop(&mut insert_stmt, (name.as_str(), hair_color))?;
    }

    let mut handler = UsersHandler::new();

    for iteration in 1.. {
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
