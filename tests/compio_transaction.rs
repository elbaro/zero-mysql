//! Tests for async transaction behavior (compio)

#![cfg(feature = "experimental-compio")]

use std::env;
use std::sync::atomic::{AtomicU32, Ordering};
use zero_mysql::compio::Conn;
use zero_mysql::error::Error;

static TABLE_COUNTER: AtomicU32 = AtomicU32::new(0);

async fn get_conn() -> Conn {
    let url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://test:1234@localhost:3306/test".to_string());
    Conn::new(url.as_str()).await.expect("Failed to connect")
}

struct TestTable {
    name: String,
}

impl TestTable {
    async fn new(conn: &mut Conn) -> Self {
        let id = TABLE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let name = format!("compio_tx_test_{}", id);
        conn.query_drop(&format!("DROP TABLE IF EXISTS {}", name))
            .await
            .unwrap();
        conn.query_drop(&format!(
            "CREATE TABLE {} (id INT AUTO_INCREMENT PRIMARY KEY, value INT)",
            name
        ))
        .await
        .unwrap();
        Self { name }
    }

    async fn count(&self, conn: &mut Conn) -> i64 {
        let mut stmt = conn
            .prepare(&format!("SELECT COUNT(*) FROM {}", self.name))
            .await
            .unwrap();
        let rows: Vec<(i64,)> = conn.exec_collect(&mut stmt, ()).await.unwrap();
        rows[0].0
    }

    async fn cleanup(&self, conn: &mut Conn) {
        let _ = conn
            .query_drop(&format!("DROP TABLE IF EXISTS {}", self.name))
            .await;
    }
}

#[compio::test]
async fn test_transaction_explicit_commit() {
    let mut conn = get_conn().await;
    let table = TestTable::new(&mut conn).await;

    conn.transaction(async |conn, tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (42)", table.name))
            .await?;
        tx.commit(conn).await
    })
    .await
    .unwrap();

    assert_eq!(table.count(&mut conn).await, 1);
    table.cleanup(&mut conn).await;
}

#[compio::test]
async fn test_transaction_explicit_rollback() {
    let mut conn = get_conn().await;
    let table = TestTable::new(&mut conn).await;

    conn.transaction(async |conn, tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (42)", table.name))
            .await?;
        tx.rollback(conn).await
    })
    .await
    .unwrap();

    assert_eq!(table.count(&mut conn).await, 0);
    table.cleanup(&mut conn).await;
}

#[compio::test]
async fn test_transaction_implicit_commit_on_ok() {
    let mut conn = get_conn().await;
    let table = TestTable::new(&mut conn).await;

    conn.transaction(async |conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (42)", table.name))
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(table.count(&mut conn).await, 1);
    table.cleanup(&mut conn).await;
}

#[compio::test]
async fn test_transaction_implicit_rollback_on_err() {
    let mut conn = get_conn().await;
    let table = TestTable::new(&mut conn).await;

    let result: Result<(), Error> = conn
        .transaction(async |conn, _tx| {
            conn.query_drop(&format!("INSERT INTO {} (value) VALUES (42)", table.name))
                .await?;
            Err(Error::BadUsageError("intentional error".into()))
        })
        .await;

    assert!(result.is_err());
    assert_eq!(table.count(&mut conn).await, 0);
    table.cleanup(&mut conn).await;
}

#[compio::test]
async fn test_transaction_implicit_commit_with_return_value() {
    let mut conn = get_conn().await;
    let table = TestTable::new(&mut conn).await;

    let result: i32 = conn
        .transaction(async |conn, _tx| {
            conn.query_drop(&format!("INSERT INTO {} (value) VALUES (42)", table.name))
                .await?;
            Ok(123)
        })
        .await
        .unwrap();

    assert_eq!(result, 123);
    assert_eq!(table.count(&mut conn).await, 1);
    table.cleanup(&mut conn).await;
}

#[compio::test]
async fn test_transaction_implicit_commit_multiple_inserts() {
    let mut conn = get_conn().await;
    let table = TestTable::new(&mut conn).await;

    conn.transaction(async |conn, _tx| {
        for i in 1..=5 {
            conn.query_drop(&format!(
                "INSERT INTO {} (value) VALUES ({})",
                table.name, i
            ))
            .await?;
        }
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(table.count(&mut conn).await, 5);
    table.cleanup(&mut conn).await;
}

#[compio::test]
async fn test_transaction_implicit_rollback_partial_work() {
    let mut conn = get_conn().await;
    let table = TestTable::new(&mut conn).await;

    let result: Result<(), Error> = conn
        .transaction(async |conn, _tx| {
            conn.query_drop(&format!("INSERT INTO {} (value) VALUES (1)", table.name))
                .await?;
            conn.query_drop(&format!("INSERT INTO {} (value) VALUES (2)", table.name))
                .await?;
            Err(Error::BadUsageError("intentional error".into()))
        })
        .await;

    assert!(result.is_err());
    assert_eq!(table.count(&mut conn).await, 0);
    table.cleanup(&mut conn).await;
}

#[compio::test]
async fn test_transaction_connection_usable_after_implicit_commit() {
    let mut conn = get_conn().await;
    let table = TestTable::new(&mut conn).await;

    conn.transaction(async |conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (1)", table.name))
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    conn.transaction(async |conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (2)", table.name))
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(table.count(&mut conn).await, 2);
    table.cleanup(&mut conn).await;
}

#[compio::test]
async fn test_transaction_connection_usable_after_implicit_rollback() {
    let mut conn = get_conn().await;
    let table = TestTable::new(&mut conn).await;

    let _: Result<(), Error> = conn
        .transaction(async |conn, _tx| {
            conn.query_drop(&format!("INSERT INTO {} (value) VALUES (1)", table.name))
                .await?;
            Err(Error::BadUsageError("intentional error".into()))
        })
        .await;

    conn.transaction(async |conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (2)", table.name))
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(table.count(&mut conn).await, 1);
    table.cleanup(&mut conn).await;
}

#[compio::test]
async fn test_transaction_not_in_transaction_after_implicit_commit() {
    let mut conn = get_conn().await;

    conn.transaction(async |conn, _tx| {
        assert!(conn.in_transaction());
        Ok(())
    })
    .await
    .unwrap();

    assert!(!conn.in_transaction());
}

#[compio::test]
async fn test_transaction_not_in_transaction_after_implicit_rollback() {
    let mut conn = get_conn().await;

    let _: Result<(), Error> = conn
        .transaction(async |_conn, _tx| Err(Error::BadUsageError("intentional error".into())))
        .await;

    assert!(!conn.in_transaction());
}
