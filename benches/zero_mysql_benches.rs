use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
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

fn connection() -> zero_mysql::error::Result<Conn> {
    let connection_url = std::env::var("MYSQL_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .map_err(|_unhelpful_err| {
            zero_mysql::error::Error::BadUsageError(
                "DATABASE_URL must be set in order to run benchmarks".into(),
            )
        })?;
    let mut conn = Conn::new(connection_url.as_str())?;

    conn.query_drop("SET FOREIGN_KEY_CHECKS = 0;")?;
    conn.query_drop("TRUNCATE TABLE comments")?;
    conn.query_drop("TRUNCATE TABLE posts")?;
    conn.query_drop("TRUNCATE TABLE users")?;

    Ok(conn)
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
        let mut values = Vec::with_capacity(cols.len());

        for (i, col) in cols.iter().enumerate() {
            let (value, rest) = parse_value::<Value>(col.tail, null_bitmap.is_null(i), bytes)?;
            values.push(value);
            bytes = rest;
        }

        let user = User {
            id: if let Value::SignedInt(id) = values[0] {
                id as i32
            } else {
                return Err(zero_mysql::error::Error::LibraryBug(
                    zero_mysql::error::eyre!("Expected SignedInt for id"),
                ));
            },
            name: if let Value::Byte(name) = &values[1] {
                String::from_utf8_lossy(name).to_string()
            } else {
                return Err(zero_mysql::error::Error::LibraryBug(
                    zero_mysql::error::eyre!("Expected Byte for name"),
                ));
            },
            hair_color: match &values[2] {
                Value::Null => None,
                Value::Byte(s) => Some(String::from_utf8_lossy(s).to_string()),
                _ => {
                    return Err(zero_mysql::error::Error::LibraryBug(
                        zero_mysql::error::eyre!("Expected Byte or Null for hair_color"),
                    ));
                }
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

    fn resultset_start(&mut self, _cols: &[ColumnDefinition<'_>]) -> zero_mysql::error::Result<()> {
        Ok(())
    }

    fn row(
        &mut self,
        cols: &[ColumnDefinition<'_>],
        row: BinaryRowPayload<'_>,
    ) -> zero_mysql::error::Result<()> {
        let null_bitmap = row.null_bitmap();
        let mut bytes = row.values();
        let mut values = Vec::with_capacity(cols.len());

        for (i, col) in cols.iter().enumerate() {
            let (value, rest) = parse_value::<Value>(col.tail, null_bitmap.is_null(i), bytes)?;
            values.push(value);
            bytes = rest;
        }

        let user = User {
            id: if let Value::SignedInt(id) = values[0] {
                id as i32
            } else {
                return Err(zero_mysql::error::Error::LibraryBug(
                    zero_mysql::error::eyre!("Expected SignedInt for id"),
                ));
            },
            name: if let Value::Byte(name) = &values[1] {
                String::from_utf8_lossy(name).to_string()
            } else {
                return Err(zero_mysql::error::Error::LibraryBug(
                    zero_mysql::error::eyre!("Expected Byte for name"),
                ));
            },
            hair_color: match &values[2] {
                Value::Null => None,
                Value::Byte(s) => Some(String::from_utf8_lossy(s).to_string()),
                _ => {
                    return Err(zero_mysql::error::Error::LibraryBug(
                        zero_mysql::error::eyre!("Expected Byte or Null for hair_color"),
                    ));
                }
            },
        };

        let post = if let Value::SignedInt(id) = values[3] {
            Some(Post {
                id: id as i32,
                user_id: if let Value::SignedInt(uid) = values[4] {
                    uid as i32
                } else {
                    return Err(zero_mysql::error::Error::LibraryBug(
                        zero_mysql::error::eyre!("Expected SignedInt for user_id"),
                    ));
                },
                title: if let Value::Byte(title) = &values[5] {
                    String::from_utf8_lossy(title).to_string()
                } else {
                    return Err(zero_mysql::error::Error::LibraryBug(
                        zero_mysql::error::eyre!("Expected Byte for title"),
                    ));
                },
                body: match &values[6] {
                    Value::Null => None,
                    Value::Byte(s) => Some(String::from_utf8_lossy(s).to_string()),
                    _ => {
                        return Err(zero_mysql::error::Error::LibraryBug(
                            zero_mysql::error::eyre!("Expected Byte or Null for body"),
                        ));
                    }
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

fn insert_users(
    size: usize,
    conn: &mut Conn,
    hair_color_init: impl Fn(usize) -> Option<String>,
) -> zero_mysql::error::Result<()> {
    let mut stmt = conn.prepare("INSERT INTO users (name, hair_color) VALUES (?, ?)")?;

    for x in 0..size {
        let name = format!("User {}", x);
        let hair_color = hair_color_init(x);

        conn.exec_drop(&mut stmt, (name.as_str(), hair_color.as_deref()))?;
    }
    Ok(())
}

fn bench_trivial_query_by_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("trivial_query_by_id");

    for size in [1, 10, 100].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let Ok(mut conn) = connection() else { return };
            let Ok(()) = insert_users(size, &mut conn, |_| None) else {
                return;
            };

            let Ok(mut stmt) = conn.prepare("SELECT id, name, hair_color FROM users") else {
                return;
            };

            let mut handler = UsersHandler::new();
            b.iter(|| {
                let _result = conn.exec(&mut stmt, (), &mut handler);
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
            let Ok(mut conn) = connection() else { return };
            let Ok(()) = insert_users(size, &mut conn, |i| {
                Some(if i % 2 == 0 {
                    String::from("black")
                } else {
                    String::from("brown")
                })
            }) else { return };

            let Ok(mut stmt) = conn
                .prepare(
                    "SELECT u.id, u.name, u.hair_color, p.id, p.user_id, p.title, p.body \
                     FROM users as u LEFT JOIN posts as p on u.id = p.user_id WHERE u.hair_color = ?",
                ) else { return };

            b.iter(|| {
                let mut handler = UserPostHandler::new();
                let _result = conn.exec(&mut stmt, ("black",), &mut handler);
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
            let Ok(mut conn) = connection() else { return };

            b.iter(|| {
                let _result = insert_users(size, &mut conn, |_| Some(String::from("hair_color")));
            })
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
