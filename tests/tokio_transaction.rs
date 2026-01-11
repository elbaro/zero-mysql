//! Tests for async transaction behavior

use std::env;
use std::sync::atomic::{AtomicU32, Ordering};
use zero_mysql::error::{Error, Result};
use zero_mysql::tokio::Conn;

static TABLE_COUNTER: AtomicU32 = AtomicU32::new(0);

async fn get_conn() -> Conn {
    let url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://test:1234@localhost:3306/test".to_string());
    Conn::new(url.as_str()).await.expect("Failed to connect")
}

fn unique_table_name() -> String {
    let id = TABLE_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("tx_test_async_{}", id)
}

async fn create_table(conn: &mut Conn, name: &str) {
    conn.query_drop(&format!("DROP TABLE IF EXISTS {}", name))
        .await
        .unwrap();
    conn.query_drop(&format!(
        "CREATE TABLE {} (id INT AUTO_INCREMENT PRIMARY KEY, value INT)",
        name
    ))
    .await
    .unwrap();
}

async fn count_rows(conn: &mut Conn, table: &str) -> i64 {
    let mut stmt = conn
        .prepare(&format!("SELECT COUNT(*) FROM {}", table))
        .await
        .unwrap();
    let rows: Vec<(i64,)> = conn.exec_collect(&mut stmt, ()).await.unwrap();
    rows[0].0
}

async fn cleanup_table(conn: &mut Conn, name: &str) {
    let _ = conn
        .query_drop(&format!("DROP TABLE IF EXISTS {}", name))
        .await;
}

#[tokio::test]
async fn test_transaction_explicit_commit() {
    let mut conn = get_conn().await;
    let table = unique_table_name();
    create_table(&mut conn, &table).await;

    let t = table.clone();
    conn.transaction(async |conn, tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (42)", t))
            .await?;
        tx.commit(conn).await
    })
    .await
    .unwrap();

    assert_eq!(count_rows(&mut conn, &table).await, 1);
    cleanup_table(&mut conn, &table).await;
}

#[tokio::test]
async fn test_transaction_explicit_rollback() {
    let mut conn = get_conn().await;
    let table = unique_table_name();
    create_table(&mut conn, &table).await;

    let t = table.clone();
    conn.transaction(async |conn, tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (42)", t))
            .await?;
        tx.rollback(conn).await
    })
    .await
    .unwrap();

    assert_eq!(count_rows(&mut conn, &table).await, 0);
    cleanup_table(&mut conn, &table).await;
}

#[tokio::test]
async fn test_transaction_implicit_commit_on_ok() {
    let mut conn = get_conn().await;
    let table = unique_table_name();
    create_table(&mut conn, &table).await;

    let t = table.clone();
    conn.transaction(async |conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (42)", t))
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(count_rows(&mut conn, &table).await, 1);
    cleanup_table(&mut conn, &table).await;
}

#[tokio::test]
async fn test_transaction_implicit_rollback_on_err() {
    let mut conn = get_conn().await;
    let table = unique_table_name();
    create_table(&mut conn, &table).await;

    let t = table.clone();
    let result: Result<()> = conn
        .transaction(async |conn, _tx| {
            conn.query_drop(&format!("INSERT INTO {} (value) VALUES (42)", t))
                .await?;
            Err(Error::BadUsageError("intentional error".into()))
        })
        .await;

    assert!(result.is_err());
    assert_eq!(count_rows(&mut conn, &table).await, 0);
    cleanup_table(&mut conn, &table).await;
}

#[tokio::test]
async fn test_transaction_implicit_commit_with_return_value() {
    let mut conn = get_conn().await;
    let table = unique_table_name();
    create_table(&mut conn, &table).await;

    let t = table.clone();
    let result: i32 = conn
        .transaction(async |conn, _tx| {
            conn.query_drop(&format!("INSERT INTO {} (value) VALUES (42)", t))
                .await?;
            Ok(123)
        })
        .await
        .unwrap();

    assert_eq!(result, 123);
    assert_eq!(count_rows(&mut conn, &table).await, 1);
    cleanup_table(&mut conn, &table).await;
}

#[tokio::test]
async fn test_transaction_implicit_commit_multiple_inserts() {
    let mut conn = get_conn().await;
    let table = unique_table_name();
    create_table(&mut conn, &table).await;

    let t = table.clone();
    conn.transaction(async |conn, _tx| {
        for i in 1..=5 {
            conn.query_drop(&format!("INSERT INTO {} (value) VALUES ({})", t, i))
                .await?;
        }
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(count_rows(&mut conn, &table).await, 5);
    cleanup_table(&mut conn, &table).await;
}

#[tokio::test]
async fn test_transaction_implicit_rollback_partial_work() {
    let mut conn = get_conn().await;
    let table = unique_table_name();
    create_table(&mut conn, &table).await;

    let t = table.clone();
    let result: Result<()> = conn
        .transaction(async |conn, _tx| {
            conn.query_drop(&format!("INSERT INTO {} (value) VALUES (1)", t))
                .await?;
            conn.query_drop(&format!("INSERT INTO {} (value) VALUES (2)", t))
                .await?;
            Err(Error::BadUsageError("intentional error".into()))
        })
        .await;

    assert!(result.is_err());
    assert_eq!(count_rows(&mut conn, &table).await, 0);
    cleanup_table(&mut conn, &table).await;
}

#[tokio::test]
async fn test_transaction_connection_usable_after_implicit_commit() {
    let mut conn = get_conn().await;
    let table = unique_table_name();
    create_table(&mut conn, &table).await;

    let t = table.clone();
    conn.transaction(async |conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (1)", t))
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    let t = table.clone();
    conn.transaction(async |conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (2)", t))
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(count_rows(&mut conn, &table).await, 2);
    cleanup_table(&mut conn, &table).await;
}

#[tokio::test]
async fn test_transaction_connection_usable_after_implicit_rollback() {
    let mut conn = get_conn().await;
    let table = unique_table_name();
    create_table(&mut conn, &table).await;

    let t = table.clone();
    let _: Result<()> = conn
        .transaction(async |conn, _tx| {
            conn.query_drop(&format!("INSERT INTO {} (value) VALUES (1)", t))
                .await?;
            Err(Error::BadUsageError("intentional error".into()))
        })
        .await;

    let t = table.clone();
    conn.transaction(async |conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (2)", t))
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(count_rows(&mut conn, &table).await, 1);
    cleanup_table(&mut conn, &table).await;
}

#[tokio::test]
async fn test_transaction_not_in_transaction_after_implicit_commit() {
    let mut conn = get_conn().await;

    conn.transaction(async |_conn, _tx| Ok(())).await.unwrap();

    assert!(!conn.in_transaction());
}

#[tokio::test]
async fn test_transaction_not_in_transaction_after_implicit_rollback() {
    let mut conn = get_conn().await;

    let _: Result<()> = conn
        .transaction(async |_conn, _tx| Err(Error::BadUsageError("intentional error".into())))
        .await;

    assert!(!conn.in_transaction());
}
