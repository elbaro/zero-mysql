//! Tests for sync transaction behavior

use std::env;
use std::sync::atomic::{AtomicU32, Ordering};
use zero_mysql::error::Error;
use zero_mysql::sync::Conn;

static TABLE_COUNTER: AtomicU32 = AtomicU32::new(0);

fn get_conn() -> Conn {
    let url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://test:1234@localhost:3306/test".to_string());
    Conn::new(url.as_str()).expect("Failed to connect")
}

struct TestTable {
    name: String,
}

impl TestTable {
    fn new(conn: &mut Conn) -> Self {
        let id = TABLE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let name = format!("tx_test_{}", id);
        conn.query_drop(&format!("DROP TABLE IF EXISTS {}", name))
            .unwrap();
        conn.query_drop(&format!(
            "CREATE TABLE {} (id INT AUTO_INCREMENT PRIMARY KEY, value INT)",
            name
        ))
        .unwrap();
        Self { name }
    }

    fn count(&self, conn: &mut Conn) -> i64 {
        let mut stmt = conn
            .prepare(&format!("SELECT COUNT(*) FROM {}", self.name))
            .unwrap();
        let rows: Vec<(i64,)> = conn.exec_collect(&mut stmt, ()).unwrap();
        rows[0].0
    }

    fn cleanup(&self, conn: &mut Conn) {
        let _ = conn.query_drop(&format!("DROP TABLE IF EXISTS {}", self.name));
    }
}

#[test]
fn test_transaction_explicit_commit() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    conn.transaction(|conn, tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (42)", table.name))?;
        tx.commit(conn)
    })
    .unwrap();

    assert_eq!(table.count(&mut conn), 1);
    table.cleanup(&mut conn);
}

#[test]
fn test_transaction_explicit_rollback() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    conn.transaction(|conn, tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (42)", table.name))?;
        tx.rollback(conn)
    })
    .unwrap();

    assert_eq!(table.count(&mut conn), 0);
    table.cleanup(&mut conn);
}

#[test]
fn test_transaction_implicit_commit_on_ok() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    // Return Ok without explicit commit - should auto-commit
    conn.transaction(|conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (42)", table.name))?;
        Ok(())
    })
    .unwrap();

    // Data should be committed
    assert_eq!(table.count(&mut conn), 1);
    table.cleanup(&mut conn);
}

#[test]
fn test_transaction_implicit_rollback_on_err() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    // Return Err without explicit rollback - should auto-rollback
    let result: Result<(), Error> = conn.transaction(|conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (42)", table.name))?;
        Err(Error::BadUsageError("intentional error".into()))
    });

    assert!(result.is_err());
    // Data should be rolled back
    assert_eq!(table.count(&mut conn), 0);
    table.cleanup(&mut conn);
}

#[test]
fn test_transaction_implicit_commit_with_return_value() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    // Return Ok with a value without explicit commit
    let result: i32 = conn
        .transaction(|conn, _tx| {
            conn.query_drop(&format!("INSERT INTO {} (value) VALUES (42)", table.name))?;
            Ok(123)
        })
        .unwrap();

    assert_eq!(result, 123);
    assert_eq!(table.count(&mut conn), 1);
    table.cleanup(&mut conn);
}

#[test]
fn test_transaction_implicit_commit_multiple_inserts() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    conn.transaction(|conn, _tx| {
        for i in 1..=5 {
            conn.query_drop(&format!(
                "INSERT INTO {} (value) VALUES ({})",
                table.name, i
            ))?;
        }
        Ok(())
    })
    .unwrap();

    assert_eq!(table.count(&mut conn), 5);
    table.cleanup(&mut conn);
}

#[test]
fn test_transaction_implicit_rollback_partial_work() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    let result: Result<(), Error> = conn.transaction(|conn, _tx| {
        // Do some work
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (1)", table.name))?;
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (2)", table.name))?;
        // Then fail
        Err(Error::BadUsageError("intentional error".into()))
    });

    assert!(result.is_err());
    // All work should be rolled back
    assert_eq!(table.count(&mut conn), 0);
    table.cleanup(&mut conn);
}

#[test]
fn test_transaction_connection_usable_after_implicit_commit() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    // First transaction with implicit commit
    conn.transaction(|conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (1)", table.name))?;
        Ok(())
    })
    .unwrap();

    // Connection should be usable for another transaction
    conn.transaction(|conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (2)", table.name))?;
        Ok(())
    })
    .unwrap();

    assert_eq!(table.count(&mut conn), 2);
    table.cleanup(&mut conn);
}

#[test]
fn test_transaction_connection_usable_after_implicit_rollback() {
    let mut conn = get_conn();
    let table = TestTable::new(&mut conn);

    // First transaction with implicit rollback
    let _: Result<(), Error> = conn.transaction(|conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (1)", table.name))?;
        Err(Error::BadUsageError("intentional error".into()))
    });

    // Connection should be usable for another transaction
    conn.transaction(|conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (2)", table.name))?;
        Ok(())
    })
    .unwrap();

    assert_eq!(table.count(&mut conn), 1);
    table.cleanup(&mut conn);
}

#[test]
fn test_transaction_not_in_transaction_after_implicit_commit() {
    let mut conn = get_conn();

    conn.transaction(|conn, _tx| {
        assert!(conn.in_transaction());
        Ok(())
    })
    .unwrap();

    assert!(!conn.in_transaction());
}

#[test]
fn test_transaction_not_in_transaction_after_implicit_rollback() {
    let mut conn = get_conn();

    let _: Result<(), Error> =
        conn.transaction(|_conn, _tx| Err(Error::BadUsageError("intentional error".into())));

    assert!(!conn.in_transaction());
}
