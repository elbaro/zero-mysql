//! Tests for FromRow derive macro.
//!
//! Run with: cargo test --features derive --test derive

#![allow(dead_code)]

use zero_mysql::Opts;
use zero_mysql::r#macro::FromRow;
use zero_mysql::sync::Conn;

const TEST_URL: &str = "mysql://test:1234@localhost:3306/test";

fn get_conn() -> Conn {
    let opts = Opts::try_from(TEST_URL).expect("parse opts");
    Conn::new(opts).expect("connect")
}

// ============================================================================
// Struct definitions
// ============================================================================

#[derive(Debug, PartialEq, FromRow)]
struct User {
    id: i64,
    name: String,
    age: u8,
}

#[derive(Debug, PartialEq, FromRow)]
struct UserWithOptional {
    id: i64,
    name: String,
    email: Option<String>,
}

#[derive(Debug, PartialEq, FromRow)]
#[from_row(strict)]
struct StrictUser {
    id: i64,
    name: String,
}

#[derive(Debug, PartialEq, FromRow)]
struct IntTypes {
    tiny: i8,
    small: i16,
    medium: i32,
    big: i64,
}

#[derive(Debug, PartialEq, FromRow)]
struct FloatTypes {
    float_val: f32,
    double_val: f64,
}

#[derive(Debug, PartialEq, FromRow)]
struct PartialUser {
    name: String,
}

// ============================================================================
// Tests
// ============================================================================

#[test]
fn test_exec_collect_basic() {
    let mut conn = get_conn();

    conn.query_drop("DROP TABLE IF EXISTS test_derive_users")
        .expect("drop");
    conn.query_drop(
        "CREATE TEMPORARY TABLE test_derive_users (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age TINYINT UNSIGNED NOT NULL
        )",
    )
    .expect("create");

    let mut stmt = conn
        .prepare("INSERT INTO test_derive_users (id, name, age) VALUES (?, ?, ?)")
        .expect("prepare");
    conn.exec_drop(&mut stmt, (1i64, "Alice", 25u8))
        .expect("insert");
    conn.exec_drop(&mut stmt, (2i64, "Bob", 30u8))
        .expect("insert");

    let mut stmt = conn
        .prepare("SELECT id, name, age FROM test_derive_users ORDER BY id")
        .expect("prepare");
    let users: Vec<User> = conn.exec_collect(&mut stmt, ()).expect("select");

    assert_eq!(users.len(), 2);
    assert_eq!(
        users[0],
        User {
            id: 1,
            name: "Alice".to_string(),
            age: 25
        }
    );
    assert_eq!(
        users[1],
        User {
            id: 2,
            name: "Bob".to_string(),
            age: 30
        }
    );
}

#[test]
fn test_exec_first() {
    let mut conn = get_conn();

    conn.query_drop("DROP TABLE IF EXISTS test_derive_first")
        .expect("drop");
    conn.query_drop(
        "CREATE TEMPORARY TABLE test_derive_first (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age TINYINT UNSIGNED NOT NULL
        )",
    )
    .expect("create");

    let mut stmt = conn
        .prepare("INSERT INTO test_derive_first (id, name, age) VALUES (?, ?, ?)")
        .expect("prepare");
    conn.exec_drop(&mut stmt, (1i64, "Alice", 25u8))
        .expect("insert");

    let mut stmt = conn
        .prepare("SELECT id, name, age FROM test_derive_first WHERE id = ?")
        .expect("prepare");

    let user: Option<User> = conn.exec_first(&mut stmt, (1i64,)).expect("select");
    assert_eq!(
        user,
        Some(User {
            id: 1,
            name: "Alice".to_string(),
            age: 25
        })
    );

    let user: Option<User> = conn.exec_first(&mut stmt, (999i64,)).expect("select");
    assert_eq!(user, None);
}

#[test]
fn test_exec_foreach() {
    let mut conn = get_conn();

    conn.query_drop("DROP TABLE IF EXISTS test_derive_foreach")
        .expect("drop");
    conn.query_drop(
        "CREATE TEMPORARY TABLE test_derive_foreach (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age TINYINT UNSIGNED NOT NULL
        )",
    )
    .expect("create");

    let mut stmt = conn
        .prepare("INSERT INTO test_derive_foreach (id, name, age) VALUES (?, ?, ?)")
        .expect("prepare");
    conn.exec_drop(&mut stmt, (1i64, "Alice", 25u8))
        .expect("insert");
    conn.exec_drop(&mut stmt, (2i64, "Bob", 30u8))
        .expect("insert");

    let mut stmt = conn
        .prepare("SELECT id, name, age FROM test_derive_foreach ORDER BY id")
        .expect("prepare");

    let mut names = Vec::new();
    conn.exec_foreach(&mut stmt, (), |user: User| {
        names.push(user.name);
        Ok(())
    })
    .expect("foreach");

    assert_eq!(names, vec!["Alice", "Bob"]);
}

#[test]
fn test_optional_field() {
    let mut conn = get_conn();

    conn.query_drop("DROP TABLE IF EXISTS test_derive_optional")
        .expect("drop");
    conn.query_drop(
        "CREATE TEMPORARY TABLE test_derive_optional (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255)
        )",
    )
    .expect("create");

    let mut stmt = conn
        .prepare("INSERT INTO test_derive_optional (id, name, email) VALUES (?, ?, ?)")
        .expect("prepare");
    conn.exec_drop(&mut stmt, (1i64, "Alice", Some("alice@example.com")))
        .expect("insert");
    conn.exec_drop(&mut stmt, (2i64, "Bob", None::<String>))
        .expect("insert");

    let mut stmt = conn
        .prepare("SELECT id, name, email FROM test_derive_optional ORDER BY id")
        .expect("prepare");
    let users: Vec<UserWithOptional> = conn.exec_collect(&mut stmt, ()).expect("select");

    assert_eq!(users[0].email, Some("alice@example.com".to_string()));
    assert_eq!(users[1].email, None);
}

#[test]
fn test_skip_unknown_columns() {
    let mut conn = get_conn();

    conn.query_drop("DROP TABLE IF EXISTS test_derive_skip")
        .expect("drop");
    conn.query_drop(
        "CREATE TEMPORARY TABLE test_derive_skip (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age TINYINT UNSIGNED NOT NULL,
            extra_column VARCHAR(255)
        )",
    )
    .expect("create");

    let mut stmt = conn
        .prepare("INSERT INTO test_derive_skip (id, name, age, extra_column) VALUES (?, ?, ?, ?)")
        .expect("prepare");
    conn.exec_drop(&mut stmt, (1i64, "Alice", 25u8, "ignored"))
        .expect("insert");

    // Select all columns including extra_column, but PartialUser only has 'name'
    let mut stmt = conn
        .prepare("SELECT id, name, age, extra_column FROM test_derive_skip")
        .expect("prepare");
    let users: Vec<PartialUser> = conn.exec_collect(&mut stmt, ()).expect("select");

    assert_eq!(users.len(), 1);
    assert_eq!(users[0].name, "Alice");
}

#[test]
fn test_strict_mode_unknown_column() {
    let mut conn = get_conn();

    conn.query_drop("DROP TABLE IF EXISTS test_derive_strict")
        .expect("drop");
    conn.query_drop(
        "CREATE TEMPORARY TABLE test_derive_strict (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            extra VARCHAR(255)
        )",
    )
    .expect("create");

    let mut stmt = conn
        .prepare("INSERT INTO test_derive_strict (id, name, extra) VALUES (?, ?, ?)")
        .expect("prepare");
    conn.exec_drop(&mut stmt, (1i64, "Alice", "extra"))
        .expect("insert");

    // StrictUser expects only id and name, but we're selecting extra too
    let mut stmt = conn
        .prepare("SELECT id, name, extra FROM test_derive_strict")
        .expect("prepare");
    let result: Result<Vec<StrictUser>, _> = conn.exec_collect(&mut stmt, ());

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Unknown column"));
}

#[test]
fn test_missing_column() {
    let mut conn = get_conn();

    conn.query_drop("DROP TABLE IF EXISTS test_derive_missing")
        .expect("drop");
    conn.query_drop(
        "CREATE TEMPORARY TABLE test_derive_missing (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        )",
    )
    .expect("create");

    let mut stmt = conn
        .prepare("INSERT INTO test_derive_missing (id, name) VALUES (?, ?)")
        .expect("prepare");
    conn.exec_drop(&mut stmt, (1i64, "Alice")).expect("insert");

    // User expects id, name, age - but age is not in the result
    let mut stmt = conn
        .prepare("SELECT id, name FROM test_derive_missing")
        .expect("prepare");
    let result: Result<Vec<User>, _> = conn.exec_collect(&mut stmt, ());

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Missing column"));
}

#[test]
fn test_column_order_independence() {
    let mut conn = get_conn();

    conn.query_drop("DROP TABLE IF EXISTS test_derive_order")
        .expect("drop");
    conn.query_drop(
        "CREATE TEMPORARY TABLE test_derive_order (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age TINYINT UNSIGNED NOT NULL
        )",
    )
    .expect("create");

    let mut stmt = conn
        .prepare("INSERT INTO test_derive_order (id, name, age) VALUES (?, ?, ?)")
        .expect("prepare");
    conn.exec_drop(&mut stmt, (1i64, "Alice", 25u8))
        .expect("insert");

    // Select columns in different order than struct definition
    let mut stmt = conn
        .prepare("SELECT age, id, name FROM test_derive_order")
        .expect("prepare");
    let users: Vec<User> = conn.exec_collect(&mut stmt, ()).expect("select");

    assert_eq!(
        users[0],
        User {
            id: 1,
            name: "Alice".to_string(),
            age: 25
        }
    );
}

#[test]
fn test_int_types() {
    let mut conn = get_conn();

    conn.query_drop("DROP TABLE IF EXISTS test_derive_ints")
        .expect("drop");
    conn.query_drop(
        "CREATE TEMPORARY TABLE test_derive_ints (
            tiny TINYINT,
            small SMALLINT,
            medium INT,
            big BIGINT
        )",
    )
    .expect("create");

    let mut stmt = conn
        .prepare("INSERT INTO test_derive_ints (tiny, small, medium, big) VALUES (?, ?, ?, ?)")
        .expect("prepare");
    conn.exec_drop(&mut stmt, (-1i8, -100i16, -10000i32, -1000000i64))
        .expect("insert");

    let mut stmt = conn
        .prepare("SELECT tiny, small, medium, big FROM test_derive_ints")
        .expect("prepare");
    let rows: Vec<IntTypes> = conn.exec_collect(&mut stmt, ()).expect("select");

    assert_eq!(
        rows[0],
        IntTypes {
            tiny: -1,
            small: -100,
            medium: -10000,
            big: -1000000
        }
    );
}

#[test]
fn test_float_types() {
    let mut conn = get_conn();

    conn.query_drop("DROP TABLE IF EXISTS test_derive_floats")
        .expect("drop");
    conn.query_drop(
        "CREATE TEMPORARY TABLE test_derive_floats (
            float_val FLOAT,
            double_val DOUBLE
        )",
    )
    .expect("create");

    let mut stmt = conn
        .prepare("INSERT INTO test_derive_floats (float_val, double_val) VALUES (?, ?)")
        .expect("prepare");
    conn.exec_drop(&mut stmt, (3.14f32, 2.71828f64))
        .expect("insert");

    let mut stmt = conn
        .prepare("SELECT float_val, double_val FROM test_derive_floats")
        .expect("prepare");
    let rows: Vec<FloatTypes> = conn.exec_collect(&mut stmt, ()).expect("select");

    assert!((rows[0].float_val - 3.14).abs() < 0.001);
    assert!((rows[0].double_val - 2.71828).abs() < 0.00001);
}
