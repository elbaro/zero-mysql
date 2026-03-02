//! Tests for async transaction behavior (compio)

#![cfg(feature = "compio")]

use std::env;
use std::sync::atomic::{AtomicU32, Ordering};
use zero_mysql::compio::Conn;
use zero_mysql::error::Error;

include!("common/check.rs");
include!("common/check_eq.rs");

static TABLE_COUNTER: AtomicU32 = AtomicU32::new(0);

async fn get_conn() -> Result<Conn, Error> {
    let url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://test:1234@localhost:3306/test".to_string());
    Conn::new(url.as_str()).await
}

struct TestTable {
    name: String,
}

impl TestTable {
    async fn new(conn: &mut Conn) -> Result<Self, Error> {
        let id = TABLE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let name = format!("compio_tx_test_{}", id);
        conn.query_drop(&format!("DROP TABLE IF EXISTS {}", name))
            .await?;
        conn.query_drop(&format!(
            "CREATE TABLE {} (id INT AUTO_INCREMENT PRIMARY KEY, value INT)",
            name
        ))
        .await?;
        Ok(Self { name })
    }

    async fn count(&self, conn: &mut Conn) -> Result<i64, Error> {
        let mut stmt = conn
            .prepare(&format!("SELECT COUNT(*) FROM {}", self.name))
            .await?;
        let rows: Vec<(i64,)> = conn.exec_collect(&mut stmt, ()).await?;
        Ok(rows[0].0)
    }

    async fn cleanup(&self, conn: &mut Conn) {
        let _ = conn
            .query_drop(&format!("DROP TABLE IF EXISTS {}", self.name))
            .await;
    }
}

#[compio::test]
async fn transaction_explicit_commit() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = TestTable::new(&mut conn).await?;

    conn.transaction(async |conn1, tx| {
        conn1
            .query_drop(&format!("INSERT INTO {} (value) VALUES (42)", table.name))
            .await?;
        tx.commit(conn1).await
    })
    .await?;

    check_eq!(table.count(&mut conn).await?, 1);
    table.cleanup(&mut conn).await;
    Ok(())
}

#[compio::test]
async fn transaction_explicit_rollback() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = TestTable::new(&mut conn).await?;

    conn.transaction(async |conn1, tx| {
        conn1
            .query_drop(&format!("INSERT INTO {} (value) VALUES (42)", table.name))
            .await?;
        tx.rollback(conn1).await
    })
    .await?;

    check_eq!(table.count(&mut conn).await?, 0);
    table.cleanup(&mut conn).await;
    Ok(())
}

#[compio::test]
async fn transaction_implicit_commit_on_ok() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = TestTable::new(&mut conn).await?;

    conn.transaction(async |conn1, _tx| {
        conn1
            .query_drop(&format!("INSERT INTO {} (value) VALUES (42)", table.name))
            .await?;
        Ok(())
    })
    .await?;

    check_eq!(table.count(&mut conn).await?, 1);
    table.cleanup(&mut conn).await;
    Ok(())
}

#[compio::test]
async fn transaction_implicit_rollback_on_err() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = TestTable::new(&mut conn).await?;

    let result: Result<(), Error> = conn
        .transaction(async |conn1, _tx| {
            conn1
                .query_drop(&format!("INSERT INTO {} (value) VALUES (42)", table.name))
                .await?;
            Err(Error::BadUsageError("intentional error".into()))
        })
        .await;

    check!(result.is_err());
    check_eq!(table.count(&mut conn).await?, 0);
    table.cleanup(&mut conn).await;
    Ok(())
}

#[compio::test]
async fn transaction_implicit_commit_with_return_value() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = TestTable::new(&mut conn).await?;

    let result: i32 = conn
        .transaction(async |conn1, _tx| {
            conn1
                .query_drop(&format!("INSERT INTO {} (value) VALUES (42)", table.name))
                .await?;
            Ok(123)
        })
        .await?;

    check_eq!(result, 123);
    check_eq!(table.count(&mut conn).await?, 1);
    table.cleanup(&mut conn).await;
    Ok(())
}

#[compio::test]
async fn transaction_implicit_commit_multiple_inserts() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = TestTable::new(&mut conn).await?;

    conn.transaction(async |conn1, _tx| {
        for i in 1..=5 {
            conn1
                .query_drop(&format!(
                    "INSERT INTO {} (value) VALUES ({})",
                    table.name, i
                ))
                .await?;
        }
        Ok(())
    })
    .await?;

    check_eq!(table.count(&mut conn).await?, 5);
    table.cleanup(&mut conn).await;
    Ok(())
}

#[compio::test]
async fn transaction_implicit_rollback_partial_work() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = TestTable::new(&mut conn).await?;

    let result: Result<(), Error> = conn
        .transaction(async |conn1, _tx| {
            conn1
                .query_drop(&format!("INSERT INTO {} (value) VALUES (1)", table.name))
                .await?;
            conn1
                .query_drop(&format!("INSERT INTO {} (value) VALUES (2)", table.name))
                .await?;
            Err(Error::BadUsageError("intentional error".into()))
        })
        .await;

    check!(result.is_err());
    check_eq!(table.count(&mut conn).await?, 0);
    table.cleanup(&mut conn).await;
    Ok(())
}

#[compio::test]
async fn transaction_connection_usable_after_implicit_commit() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = TestTable::new(&mut conn).await?;

    conn.transaction(async |conn1, _tx| {
        conn1
            .query_drop(&format!("INSERT INTO {} (value) VALUES (1)", table.name))
            .await?;
        Ok(())
    })
    .await?;

    conn.transaction(async |conn2, _tx| {
        conn2
            .query_drop(&format!("INSERT INTO {} (value) VALUES (2)", table.name))
            .await?;
        Ok(())
    })
    .await?;

    check_eq!(table.count(&mut conn).await?, 2);
    table.cleanup(&mut conn).await;
    Ok(())
}

#[compio::test]
async fn transaction_connection_usable_after_implicit_rollback() -> Result<(), Error> {
    let mut conn = get_conn().await?;
    let table = TestTable::new(&mut conn).await?;

    let _: Result<(), Error> = conn
        .transaction(async |conn1, _tx| {
            conn1
                .query_drop(&format!("INSERT INTO {} (value) VALUES (1)", table.name))
                .await?;
            Err(Error::BadUsageError("intentional error".into()))
        })
        .await;

    conn.transaction(async |conn2, _tx| {
        conn2
            .query_drop(&format!("INSERT INTO {} (value) VALUES (2)", table.name))
            .await?;
        Ok(())
    })
    .await?;

    check_eq!(table.count(&mut conn).await?, 1);
    table.cleanup(&mut conn).await;
    Ok(())
}

#[compio::test]
async fn transaction_not_in_transaction_after_implicit_commit() -> Result<(), Error> {
    let mut conn = get_conn().await?;

    conn.transaction(async |conn1, _tx| {
        check!(conn1.in_transaction());
        Ok(())
    })
    .await?;

    check!(!conn.in_transaction());
    Ok(())
}

#[compio::test]
async fn transaction_not_in_transaction_after_implicit_rollback() -> Result<(), Error> {
    let mut conn = get_conn().await?;

    let _: Result<(), Error> = conn
        .transaction(async |_conn1, _tx| Err(Error::BadUsageError("intentional error".into())))
        .await;

    check!(!conn.in_transaction());
    Ok(())
}
