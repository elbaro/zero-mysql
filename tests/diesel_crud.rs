//! Tests for diesel MySQL backend

#![cfg(feature = "diesel")]

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{Integer, Text};
use std::env;

fn establish_connection() -> zero_mysql::diesel::Connection {
    let url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://test:1234@localhost:3306/test".to_string());
    <zero_mysql::diesel::Connection as diesel::Connection>::establish(&url)
        .expect("Failed to connect")
}

#[test]
fn test_simple_query() {
    let mut conn = establish_connection();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_simple")
        .unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_simple (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))",
    )
    .unwrap();

    conn.batch_execute("INSERT INTO diesel_test_simple (name) VALUES ('Alice'), ('Bob')")
        .unwrap();

    #[derive(QueryableByName, Debug, PartialEq)]
    struct Row {
        #[diesel(sql_type = Integer)]
        id: i32,
        #[diesel(sql_type = Text)]
        name: String,
    }

    let results: Vec<Row> = sql_query("SELECT id, name FROM diesel_test_simple ORDER BY id")
        .load(&mut conn)
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].name, "Alice");
    assert_eq!(results[1].name, "Bob");

    conn.batch_execute("DROP TABLE diesel_test_simple").unwrap();
}

#[test]
fn test_execute_returning_count() {
    let mut conn = establish_connection();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_count")
        .unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_count (id INT AUTO_INCREMENT PRIMARY KEY, value INT)",
    )
    .unwrap();

    let count = conn
        .batch_execute("INSERT INTO diesel_test_count (value) VALUES (1), (2), (3)");
    assert!(count.is_ok());

    #[derive(QueryableByName, Debug)]
    struct CountRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        cnt: i64,
    }

    let results: Vec<CountRow> =
        sql_query("SELECT COUNT(*) as cnt FROM diesel_test_count")
            .load(&mut conn)
            .unwrap();
    assert_eq!(results[0].cnt, 3);

    conn.batch_execute("DROP TABLE diesel_test_count").unwrap();
}

#[test]
fn test_transaction() {
    let mut conn = establish_connection();
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_tx")
        .unwrap();
    conn.batch_execute(
        "CREATE TABLE diesel_test_tx (id INT AUTO_INCREMENT PRIMARY KEY, value INT)",
    )
    .unwrap();

    // Successful transaction
    conn.transaction(|conn| {
        conn.batch_execute("INSERT INTO diesel_test_tx (value) VALUES (42)")?;
        Ok::<_, diesel::result::Error>(())
    })
    .unwrap();

    #[derive(QueryableByName, Debug)]
    struct CountRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        cnt: i64,
    }

    let results: Vec<CountRow> =
        sql_query("SELECT COUNT(*) as cnt FROM diesel_test_tx")
            .load(&mut conn)
            .unwrap();
    assert_eq!(results[0].cnt, 1);

    // Failed transaction (should rollback)
    let result = conn.transaction(|conn| {
        conn.batch_execute("INSERT INTO diesel_test_tx (value) VALUES (99)")?;
        Err::<(), _>(diesel::result::Error::RollbackTransaction)
    });
    assert!(result.is_err());

    let results: Vec<CountRow> =
        sql_query("SELECT COUNT(*) as cnt FROM diesel_test_tx")
            .load(&mut conn)
            .unwrap();
    assert_eq!(results[0].cnt, 1);

    conn.batch_execute("DROP TABLE diesel_test_tx").unwrap();
}
