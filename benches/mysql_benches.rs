use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mysql::prelude::*;
use mysql::*;

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

type UserPostRow = (
    i32,
    String,
    Option<String>,
    Option<i32>,
    Option<i32>,
    Option<String>,
    Option<String>,
);

fn connection() -> Result<PooledConn, Box<dyn std::error::Error>> {
    let connection_url =
        std::env::var("MYSQL_DATABASE_URL").or_else(|_| std::env::var("DATABASE_URL"))?;
    let pool = Pool::new(connection_url.as_str())?;
    let mut conn = pool.get_conn()?;

    conn.query_drop("SET FOREIGN_KEY_CHECKS = 0;")?;
    conn.query_drop("TRUNCATE TABLE comments")?;
    conn.query_drop("TRUNCATE TABLE posts")?;
    conn.query_drop("TRUNCATE TABLE users")?;

    Ok(conn)
}

fn insert_users(
    size: usize,
    conn: &mut PooledConn,
    hair_color_init: impl Fn(usize) -> Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let stmt = conn.prep("INSERT INTO users (name, hair_color) VALUES (?, ?)")?;

    for x in 0..size {
        let name = format!("User {}", x);
        let hair_color = hair_color_init(x);

        conn.exec_drop(&stmt, (name.as_str(), hair_color.as_deref()))?;
    }
    Ok(())
}

fn bench_trivial_query_by_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("trivial_query_by_id");

    {
        let size = 1;
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let Ok(mut conn) = connection() else { return };
            let Ok(()) = insert_users(size, &mut conn, |_| None) else {
                return;
            };

            let Ok(stmt) = conn.prep("SELECT id, name, hair_color FROM users") else {
                return;
            };

            b.iter(|| {
                conn.exec_map(
                    &stmt,
                    (),
                    |(id, name, hair_color): (i32, String, Option<String>)| User {
                        id,
                        name,
                        hair_color,
                    },
                )
                .unwrap_or_default()
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

            let Ok(stmt) = conn
                .prep(
                    "SELECT u.id, u.name, u.hair_color, p.id, p.user_id, p.title, p.body \
                     FROM users as u LEFT JOIN posts as p on u.id = p.user_id WHERE u.hair_color = ?",
                ) else { return };

            b.iter(|| {
                conn.exec_map(
                    &stmt,
                    ("black",),
                    |(user_id, name, hair_color, post_id, post_user_id, title, body): UserPostRow| {
                        let user = User {
                            id: user_id,
                            name,
                            hair_color,
                        };
                        let post = post_id.map(|id| Post {
                            id,
                            user_id: post_user_id.unwrap_or_default(),
                            title: title.unwrap_or_default(),
                            body,
                        });
                        (user, post)
                    },
                )
                .unwrap_or_default()
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
