//! Tests for diesel MySQL backend

#![cfg(feature = "diesel")]

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{Integer, Text};
use std::env;

include!("common/check.rs");
include!("common/check_eq.rs");

fn establish_connection() -> Result<zero_mysql::diesel::Connection, Box<dyn std::error::Error>> {
    let url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://test:1234@localhost:3306/test".to_string());
    Ok(<zero_mysql::diesel::Connection as diesel::Connection>::establish(&url)?)
}

#[test]
fn simple_query() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = establish_connection()?;
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_simple")?;
    conn.batch_execute(
        "CREATE TABLE diesel_test_simple (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))",
    )?;

    conn.batch_execute("INSERT INTO diesel_test_simple (name) VALUES ('Alice'), ('Bob')")?;

    #[derive(QueryableByName, Debug, PartialEq)]
    struct Row {
        #[diesel(sql_type = Integer)]
        id: i32,
        #[diesel(sql_type = Text)]
        name: String,
    }

    let results: Vec<Row> =
        sql_query("SELECT id, name FROM diesel_test_simple ORDER BY id").load(&mut conn)?;

    check_eq!(results.len(), 2);
    check_eq!(results[0].name, "Alice");
    check_eq!(results[1].name, "Bob");

    conn.batch_execute("DROP TABLE diesel_test_simple")?;

    Ok(())
}

#[test]
fn execute_returning_count() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = establish_connection()?;
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_count")?;
    conn.batch_execute(
        "CREATE TABLE diesel_test_count (id INT AUTO_INCREMENT PRIMARY KEY, value INT)",
    )?;

    conn.batch_execute("INSERT INTO diesel_test_count (value) VALUES (1), (2), (3)")?;

    #[derive(QueryableByName, Debug)]
    struct CountRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        cnt: i64,
    }

    let results: Vec<CountRow> =
        sql_query("SELECT COUNT(*) as cnt FROM diesel_test_count").load(&mut conn)?;
    check_eq!(results[0].cnt, 3);

    conn.batch_execute("DROP TABLE diesel_test_count")?;

    Ok(())
}

#[test]
fn transaction() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = establish_connection()?;
    conn.batch_execute("DROP TABLE IF EXISTS diesel_test_tx")?;
    conn.batch_execute(
        "CREATE TABLE diesel_test_tx (id INT AUTO_INCREMENT PRIMARY KEY, value INT)",
    )?;

    // Successful transaction
    conn.transaction(|conn| {
        conn.batch_execute("INSERT INTO diesel_test_tx (value) VALUES (42)")?;
        Ok::<_, diesel::result::Error>(())
    })?;

    #[derive(QueryableByName, Debug)]
    struct CountRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        cnt: i64,
    }

    let results: Vec<CountRow> =
        sql_query("SELECT COUNT(*) as cnt FROM diesel_test_tx").load(&mut conn)?;
    check_eq!(results[0].cnt, 1);

    // Failed transaction (should rollback)
    let result = conn.transaction(|conn| {
        conn.batch_execute("INSERT INTO diesel_test_tx (value) VALUES (99)")?;
        Err::<(), _>(diesel::result::Error::RollbackTransaction)
    });
    check!(result.is_err());

    let results2: Vec<CountRow> =
        sql_query("SELECT COUNT(*) as cnt FROM diesel_test_tx").load(&mut conn)?;
    check_eq!(results2[0].cnt, 1);

    conn.batch_execute("DROP TABLE diesel_test_tx")?;

    Ok(())
}
