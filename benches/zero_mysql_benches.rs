use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use zero_mysql::protocol::command::ColumnDefinition;
use zero_mysql::protocol::r#trait::BinaryResultSetHandler;
use zero_mysql::protocol::response::OkPayloadBytes;
use zero_mysql::protocol::value::Value;
use zero_mysql::protocol::BinaryRowPayload;
use zero_mysql::sync::Conn;

pub struct User {
    pub id: i32,
    pub name: String,
    pub hair_color: Option<String>,
}

pub struct Post {
    pub id: i32,
    pub user_id: i32,
    pub title: String,
    pub body: Option<String>,
}

pub struct Comment {
    pub id: i32,
    pub post_id: i32,
    pub text: String,
}

fn connection() -> Conn {
    let connection_url = std::env::var("MYSQL_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .expect("DATABASE_URL must be set in order to run tests");
    let mut conn = Conn::new(connection_url.as_str()).unwrap();

    conn.query_drop("SET FOREIGN_KEY_CHECKS = 0;").unwrap();
    conn.query_drop("TRUNCATE TABLE comments").unwrap();
    conn.query_drop("TRUNCATE TABLE posts").unwrap();
    conn.query_drop("TRUNCATE TABLE users").unwrap();

    conn
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

// Handler for collecting user-post tuples
struct UserPostHandler {
    results: Vec<(User, Option<Post>)>,
}

impl UserPostHandler {
    fn new() -> Self {
        Self {
            results: Vec::new(),
        }
    }
}

impl BinaryResultSetHandler for UserPostHandler {
    fn no_result_set(&mut self, _ok: OkPayloadBytes) -> zero_mysql::error::Result<()> {
        Ok(())
    }

    fn resultset_start(&mut self, _num_columns: usize) -> zero_mysql::error::Result<()> {
        Ok(())
    }

    fn row<'a>(
        &mut self,
        cols: &[ColumnDefinition<'a>],
        row: &'a BinaryRowPayload<'a>,
    ) -> zero_mysql::error::Result<()> {
        let mut values = Vec::with_capacity(cols.len());
        let mut bytes = row.values();

        for i in 0..cols.len() {
            if row.null_bitmap().is_null(i) {
                values.push(Value::Null);
            } else {
                let type_and_flags = cols[i].tail.type_and_flags()?;
                let (value, remaining) = Value::parse(&type_and_flags, bytes)?;
                values.push(value);
                bytes = remaining;
            }
        }

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

        let post = if let Value::SignedInt(id) = values[3] {
            Some(Post {
                id: id as i32,
                user_id: if let Value::SignedInt(uid) = values[4] {
                    uid as i32
                } else {
                    panic!("Expected SignedInt for user_id");
                },
                title: if let Value::Byte(title) = &values[5] {
                    String::from_utf8_lossy(title).to_string()
                } else {
                    panic!("Expected Byte for title");
                },
                body: match &values[6] {
                    Value::Null => None,
                    Value::Byte(s) => Some(String::from_utf8_lossy(s).to_string()),
                    _ => panic!("Expected Byte or Null for body"),
                },
            })
        } else {
            None
        };

        self.results.push((user, post));
        Ok(())
    }

    fn resultset_end(&mut self, _eof: OkPayloadBytes) -> zero_mysql::error::Result<()> {
        Ok(())
    }
}

fn insert_users(size: usize, conn: &mut Conn, hair_color_init: impl Fn(usize) -> Option<String>) {
    let mut stmt = conn
        .prepare("INSERT INTO users (name, hair_color) VALUES (?, ?)")
        .unwrap();

    for x in 0..size {
        let name = format!("User {}", x);
        let hair_color = hair_color_init(x);

        conn.exec_drop(&mut stmt, (name.as_str(), hair_color.as_deref()))
            .unwrap();
    }
}

fn bench_trivial_query_by_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("trivial_query_by_id");

    for size in [1, 10, 100].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut conn = connection();
            insert_users(size, &mut conn, |_| None);

            let mut stmt = conn
                .prepare("SELECT id, name, hair_color FROM users")
                .unwrap();

            let mut handler = UsersHandler::new();
            b.iter(|| {
                conn.exec(&mut stmt, (), &mut handler).unwrap();
                std::mem::take(&mut handler.users)
            })
        });
    }
    group.finish();
}

fn bench_medium_complex_query_by_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("medium_complex_query_by_id");

    for size in [1, 10, 100].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut conn = connection();
            insert_users(size, &mut conn, |i| {
                Some(if i % 2 == 0 {
                    String::from("black")
                } else {
                    String::from("brown")
                })
            });

            let mut stmt = conn
                .prepare(
                    "SELECT u.id, u.name, u.hair_color, p.id, p.user_id, p.title, p.body \
                     FROM users as u LEFT JOIN posts as p on u.id = p.user_id WHERE u.hair_color = ?",
                )
                .unwrap();

            b.iter(|| {
                let mut handler = UserPostHandler::new();
                conn.exec(&mut stmt, ("black",), &mut handler).unwrap();
                handler.results
            })
        });
    }
    group.finish();
}

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");

    for size in [1, 10, 100].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut conn = connection();

            b.iter(|| insert_users(size, &mut conn, |_| Some(String::from("hair_color"))))
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_trivial_query_by_id,
    bench_medium_complex_query_by_id,
    bench_insert
);
criterion_main!(benches);
