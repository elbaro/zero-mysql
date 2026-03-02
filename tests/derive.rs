//! Tests for FromRow derive macro.
//!
//! Run with: cargo test --features derive --test derive

#![allow(dead_code)]

use zero_mysql::Opts;
use zero_mysql::r#macro::FromRow;
use zero_mysql::sync::Conn;

include!("common/check.rs");
include!("common/check_eq.rs");
include!("common/check_err.rs");

const TEST_URL: &str = "mysql://test:1234@localhost:3306/test";

fn get_conn() -> Result<Conn, zero_mysql::error::Error> {
    let opts = Opts::try_from(TEST_URL)?;
    Conn::new(opts)
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
fn exec_collect_basic() -> Result<(), zero_mysql::error::Error> {
    let mut conn = get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_derive_users")?;
    conn.query_drop(
        "CREATE TEMPORARY TABLE test_derive_users (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age TINYINT UNSIGNED NOT NULL
        )",
    )?;

    let mut stmt1 =
        conn.prepare("INSERT INTO test_derive_users (id, name, age) VALUES (?, ?, ?)")?;
    conn.exec_drop(&mut stmt1, (1i64, "Alice", 25u8))?;
    conn.exec_drop(&mut stmt1, (2i64, "Bob", 30u8))?;

    let mut stmt2 = conn.prepare("SELECT id, name, age FROM test_derive_users ORDER BY id")?;
    let users: Vec<User> = conn.exec_collect(&mut stmt2, ())?;

    check_eq!(users.len(), 2);
    check_eq!(
        users[0],
        User {
            id: 1,
            name: "Alice".to_string(),
            age: 25
        }
    );
    check_eq!(
        users[1],
        User {
            id: 2,
            name: "Bob".to_string(),
            age: 30
        }
    );

    Ok(())
}

#[test]
fn exec_first() -> Result<(), zero_mysql::error::Error> {
    let mut conn = get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_derive_first")?;
    conn.query_drop(
        "CREATE TEMPORARY TABLE test_derive_first (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age TINYINT UNSIGNED NOT NULL
        )",
    )?;

    let mut stmt1 =
        conn.prepare("INSERT INTO test_derive_first (id, name, age) VALUES (?, ?, ?)")?;
    conn.exec_drop(&mut stmt1, (1i64, "Alice", 25u8))?;

    let mut stmt2 = conn.prepare("SELECT id, name, age FROM test_derive_first WHERE id = ?")?;

    let user: Option<User> = conn.exec_first(&mut stmt2, (1i64,))?;
    check_eq!(
        user,
        Some(User {
            id: 1,
            name: "Alice".to_string(),
            age: 25
        })
    );

    let user2: Option<User> = conn.exec_first(&mut stmt2, (999i64,))?;
    check_eq!(user2, None);

    Ok(())
}

#[test]
fn exec_foreach() -> Result<(), zero_mysql::error::Error> {
    let mut conn = get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_derive_foreach")?;
    conn.query_drop(
        "CREATE TEMPORARY TABLE test_derive_foreach (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age TINYINT UNSIGNED NOT NULL
        )",
    )?;

    let mut stmt1 =
        conn.prepare("INSERT INTO test_derive_foreach (id, name, age) VALUES (?, ?, ?)")?;
    conn.exec_drop(&mut stmt1, (1i64, "Alice", 25u8))?;
    conn.exec_drop(&mut stmt1, (2i64, "Bob", 30u8))?;

    let mut stmt2 = conn.prepare("SELECT id, name, age FROM test_derive_foreach ORDER BY id")?;

    let mut names = Vec::new();
    conn.exec_foreach(&mut stmt2, (), |user: User| {
        names.push(user.name);
        Ok(())
    })?;

    check_eq!(names, vec!["Alice", "Bob"]);

    Ok(())
}

#[test]
fn optional_field() -> Result<(), zero_mysql::error::Error> {
    let mut conn = get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_derive_optional")?;
    conn.query_drop(
        "CREATE TEMPORARY TABLE test_derive_optional (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255)
        )",
    )?;

    let mut stmt1 =
        conn.prepare("INSERT INTO test_derive_optional (id, name, email) VALUES (?, ?, ?)")?;
    conn.exec_drop(&mut stmt1, (1i64, "Alice", Some("alice@example.com")))?;
    conn.exec_drop(&mut stmt1, (2i64, "Bob", None::<String>))?;

    let mut stmt2 = conn.prepare("SELECT id, name, email FROM test_derive_optional ORDER BY id")?;
    let users: Vec<UserWithOptional> = conn.exec_collect(&mut stmt2, ())?;

    check_eq!(users[0].email, Some("alice@example.com".to_string()));
    check_eq!(users[1].email, None);

    Ok(())
}

#[test]
fn skip_unknown_columns() -> Result<(), zero_mysql::error::Error> {
    let mut conn = get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_derive_skip")?;
    conn.query_drop(
        "CREATE TEMPORARY TABLE test_derive_skip (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age TINYINT UNSIGNED NOT NULL,
            extra_column VARCHAR(255)
        )",
    )?;

    let mut stmt1 = conn.prepare(
        "INSERT INTO test_derive_skip (id, name, age, extra_column) VALUES (?, ?, ?, ?)",
    )?;
    conn.exec_drop(&mut stmt1, (1i64, "Alice", 25u8, "ignored"))?;

    // Select all columns including extra_column, but PartialUser only has 'name'
    let mut stmt2 = conn.prepare("SELECT id, name, age, extra_column FROM test_derive_skip")?;
    let users: Vec<PartialUser> = conn.exec_collect(&mut stmt2, ())?;

    check_eq!(users.len(), 1);
    check_eq!(users[0].name, "Alice");

    Ok(())
}

#[test]
fn strict_mode_unknown_column() -> Result<(), zero_mysql::error::Error> {
    let mut conn = get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_derive_strict")?;
    conn.query_drop(
        "CREATE TEMPORARY TABLE test_derive_strict (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            extra VARCHAR(255)
        )",
    )?;

    let mut stmt1 =
        conn.prepare("INSERT INTO test_derive_strict (id, name, extra) VALUES (?, ?, ?)")?;
    conn.exec_drop(&mut stmt1, (1i64, "Alice", "extra"))?;

    // StrictUser expects only id and name, but we're selecting extra too
    let mut stmt2 = conn.prepare("SELECT id, name, extra FROM test_derive_strict")?;
    let result: Result<Vec<StrictUser>, _> = conn.exec_collect(&mut stmt2, ());

    let err = check_err!(result);
    check!(err.to_string().contains("Unknown column"));

    Ok(())
}

#[test]
fn missing_column() -> Result<(), zero_mysql::error::Error> {
    let mut conn = get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_derive_missing")?;
    conn.query_drop(
        "CREATE TEMPORARY TABLE test_derive_missing (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        )",
    )?;

    let mut stmt1 = conn.prepare("INSERT INTO test_derive_missing (id, name) VALUES (?, ?)")?;
    conn.exec_drop(&mut stmt1, (1i64, "Alice"))?;

    // User expects id, name, age - but age is not in the result
    let mut stmt2 = conn.prepare("SELECT id, name FROM test_derive_missing")?;
    let result: Result<Vec<User>, _> = conn.exec_collect(&mut stmt2, ());

    let err = check_err!(result);
    check!(err.to_string().contains("Missing column"));

    Ok(())
}

#[test]
fn column_order_independence() -> Result<(), zero_mysql::error::Error> {
    let mut conn = get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_derive_order")?;
    conn.query_drop(
        "CREATE TEMPORARY TABLE test_derive_order (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age TINYINT UNSIGNED NOT NULL
        )",
    )?;

    let mut stmt1 =
        conn.prepare("INSERT INTO test_derive_order (id, name, age) VALUES (?, ?, ?)")?;
    conn.exec_drop(&mut stmt1, (1i64, "Alice", 25u8))?;

    // Select columns in different order than struct definition
    let mut stmt2 = conn.prepare("SELECT age, id, name FROM test_derive_order")?;
    let users: Vec<User> = conn.exec_collect(&mut stmt2, ())?;

    check_eq!(
        users[0],
        User {
            id: 1,
            name: "Alice".to_string(),
            age: 25
        }
    );

    Ok(())
}

#[test]
fn int_types() -> Result<(), zero_mysql::error::Error> {
    let mut conn = get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_derive_ints")?;
    conn.query_drop(
        "CREATE TEMPORARY TABLE test_derive_ints (
            tiny TINYINT,
            small SMALLINT,
            medium INT,
            big BIGINT
        )",
    )?;

    let mut stmt1 = conn
        .prepare("INSERT INTO test_derive_ints (tiny, small, medium, big) VALUES (?, ?, ?, ?)")?;
    conn.exec_drop(&mut stmt1, (-1i8, -100i16, -10000i32, -1000000i64))?;

    let mut stmt2 = conn.prepare("SELECT tiny, small, medium, big FROM test_derive_ints")?;
    let rows: Vec<IntTypes> = conn.exec_collect(&mut stmt2, ())?;

    check_eq!(
        rows[0],
        IntTypes {
            tiny: -1,
            small: -100,
            medium: -10000,
            big: -1000000
        }
    );

    Ok(())
}

#[test]
fn float_types() -> Result<(), zero_mysql::error::Error> {
    let mut conn = get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_derive_floats")?;
    conn.query_drop(
        "CREATE TEMPORARY TABLE test_derive_floats (
            float_val FLOAT,
            double_val DOUBLE
        )",
    )?;

    let mut stmt1 =
        conn.prepare("INSERT INTO test_derive_floats (float_val, double_val) VALUES (?, ?)")?;
    conn.exec_drop(&mut stmt1, (3.12f32, 2.81f64))?;

    let mut stmt2 = conn.prepare("SELECT float_val, double_val FROM test_derive_floats")?;
    let rows: Vec<FloatTypes> = conn.exec_collect(&mut stmt2, ())?;

    let close1 = (rows[0].float_val - 3.12).abs() < 0.001;
    check!(close1);
    let close2 = (rows[0].double_val - 2.81).abs() < 0.00001;
    check!(close2);

    Ok(())
}
