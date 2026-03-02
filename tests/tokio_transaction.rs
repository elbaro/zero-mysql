//! Tests for async transaction behavior

use std::env;
use std::sync::atomic::{AtomicU32, Ordering};
use zero_mysql::error::Error;
use zero_mysql::tokio::Conn;

include!("common/check.rs");
include!("common/check_eq.rs");

static TABLE_COUNTER: AtomicU32 = AtomicU32::new(0);

async fn get_conn() -> Result<Conn, Error> {
    let url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://test:1234@localhost:3306/test".to_string());
    Conn::new(url.as_str()).await
}

fn unique_table_name() -> String {
    let id = TABLE_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("tx_test_async_{}", id)
}

async fn create_table(conn: &mut Conn, name: &str) -> Result<(), Error> {
    conn.query_drop(&format!("DROP TABLE IF EXISTS {}", name))
        .await?;
    conn.query_drop(&format!(
        "CREATE TABLE {} (id INT AUTO_INCREMENT PRIMARY KEY, value INT)",
        name
    ))
    .await?;
    Ok(())
}

async fn count_rows(conn: &mut Conn, table: &str) -> Result<i64, Error> {
    let mut stmt = conn
        .prepare(&format!("SELECT COUNT(*) FROM {}", table))
        .await?;
    let rows: Vec<(i64,)> = conn.exec_collect(&mut stmt, ()).await?;
    Ok(rows[0].0)
}

async fn cleanup_table(conn: &mut Conn, name: &str) {
    let _ = conn
        .query_drop(&format!("DROP TABLE IF EXISTS {}", name))
        .await;
}

#[tokio::test]
async fn transaction_explicit_commit() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = unique_table_name();
    create_table(&mut conn, &table).await?;

    let t = table.clone();
    conn.transaction(async |conn1, tx| {
        conn1
            .query_drop(&format!("INSERT INTO {} (value) VALUES (42)", t))
            .await?;
        tx.commit(conn1).await
    })
    .await?;

    check_eq!(count_rows(&mut conn, &table).await?, 1);
    cleanup_table(&mut conn, &table).await;
    Ok(())
}

#[tokio::test]
async fn transaction_explicit_rollback() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = unique_table_name();
    create_table(&mut conn, &table).await?;

    let t = table.clone();
    conn.transaction(async |conn1, tx| {
        conn1
            .query_drop(&format!("INSERT INTO {} (value) VALUES (42)", t))
            .await?;
        tx.rollback(conn1).await
    })
    .await?;

    check_eq!(count_rows(&mut conn, &table).await?, 0);
    cleanup_table(&mut conn, &table).await;
    Ok(())
}

#[tokio::test]
async fn transaction_implicit_commit_on_ok() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = unique_table_name();
    create_table(&mut conn, &table).await?;

    let t = table.clone();
    conn.transaction(async |conn1, _tx| {
        conn1
            .query_drop(&format!("INSERT INTO {} (value) VALUES (42)", t))
            .await?;
        Ok(())
    })
    .await?;

    check_eq!(count_rows(&mut conn, &table).await?, 1);
    cleanup_table(&mut conn, &table).await;
    Ok(())
}

#[tokio::test]
async fn transaction_implicit_rollback_on_err() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = unique_table_name();
    create_table(&mut conn, &table).await?;

    let t = table.clone();
    let result: Result<(), Error> = conn
        .transaction(async |conn1, _tx| {
            conn1
                .query_drop(&format!("INSERT INTO {} (value) VALUES (42)", t))
                .await?;
            Err(Error::BadUsageError("intentional error".into()))
        })
        .await;

    check!(result.is_err());
    check_eq!(count_rows(&mut conn, &table).await?, 0);
    cleanup_table(&mut conn, &table).await;
    Ok(())
}

#[tokio::test]
async fn transaction_implicit_commit_with_return_value() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = unique_table_name();
    create_table(&mut conn, &table).await?;

    let t = table.clone();
    let result: i32 = conn
        .transaction(async |conn1, _tx| {
            conn1
                .query_drop(&format!("INSERT INTO {} (value) VALUES (42)", t))
                .await?;
            Ok(123)
        })
        .await?;

    check_eq!(result, 123);
    check_eq!(count_rows(&mut conn, &table).await?, 1);
    cleanup_table(&mut conn, &table).await;
    Ok(())
}

#[tokio::test]
async fn transaction_implicit_commit_multiple_inserts() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = unique_table_name();
    create_table(&mut conn, &table).await?;

    let t = table.clone();
    conn.transaction(async |conn1, _tx| {
        for i in 1..=5 {
            conn1
                .query_drop(&format!("INSERT INTO {} (value) VALUES ({})", t, i))
                .await?;
        }
        Ok(())
    })
    .await?;

    check_eq!(count_rows(&mut conn, &table).await?, 5);
    cleanup_table(&mut conn, &table).await;
    Ok(())
}

#[tokio::test]
async fn transaction_implicit_rollback_partial_work() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = unique_table_name();
    create_table(&mut conn, &table).await?;

    let t = table.clone();
    let result: Result<(), Error> = conn
        .transaction(async |conn1, _tx| {
            conn1
                .query_drop(&format!("INSERT INTO {} (value) VALUES (1)", t))
                .await?;
            conn1
                .query_drop(&format!("INSERT INTO {} (value) VALUES (2)", t))
                .await?;
            Err(Error::BadUsageError("intentional error".into()))
        })
        .await;

    check!(result.is_err());
    check_eq!(count_rows(&mut conn, &table).await?, 0);
    cleanup_table(&mut conn, &table).await;
    Ok(())
}

#[tokio::test]
async fn transaction_connection_usable_after_implicit_commit() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = unique_table_name();
    create_table(&mut conn, &table).await?;

    let t1 = table.clone();
    conn.transaction(async |conn1, _tx| {
        conn1
            .query_drop(&format!("INSERT INTO {} (value) VALUES (1)", t1))
            .await?;
        Ok(())
    })
    .await?;

    let t2 = table.clone();
    conn.transaction(async |conn2, _tx| {
        conn2
            .query_drop(&format!("INSERT INTO {} (value) VALUES (2)", t2))
            .await?;
        Ok(())
    })
    .await?;

    check_eq!(count_rows(&mut conn, &table).await?, 2);
    cleanup_table(&mut conn, &table).await;
    Ok(())
}

#[tokio::test]
async fn transaction_connection_usable_after_implicit_rollback() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = unique_table_name();
    create_table(&mut conn, &table).await?;

    let t1 = table.clone();
    let _: Result<(), Error> = conn
        .transaction(async |conn1, _tx| {
            conn1
                .query_drop(&format!("INSERT INTO {} (value) VALUES (1)", t1))
                .await?;
            Err(Error::BadUsageError("intentional error".into()))
        })
        .await;

    let t2 = table.clone();
    conn.transaction(async |conn2, _tx| {
        conn2
            .query_drop(&format!("INSERT INTO {} (value) VALUES (2)", t2))
            .await?;
        Ok(())
    })
    .await?;

    check_eq!(count_rows(&mut conn, &table).await?, 1);
    cleanup_table(&mut conn, &table).await;
    Ok(())
}

#[tokio::test]
async fn transaction_not_in_transaction_after_implicit_commit() -> Result<(), Error> {
    let mut conn = get_conn().await?;

    conn.transaction(async |_conn1, _tx| Ok(())).await?;

    check!(!conn.in_transaction());
    Ok(())
}

#[tokio::test]
async fn transaction_not_in_transaction_after_implicit_rollback() -> Result<(), Error> {
    let mut conn = get_conn().await?;

    let _: Result<(), Error> = conn
        .transaction(async |_conn1, _tx| Err(Error::BadUsageError("intentional error".into())))
        .await;

    check!(!conn.in_transaction());
    Ok(())
}
