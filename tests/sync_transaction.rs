//! Tests for sync transaction behavior

use std::env;
use std::sync::atomic::{AtomicU32, Ordering};
use zero_mysql::error::Error;
use zero_mysql::sync::Conn;

include!("common/check.rs");
include!("common/check_eq.rs");

static TABLE_COUNTER: AtomicU32 = AtomicU32::new(0);

fn get_conn() -> Result<Conn, Error> {
    let url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://test:1234@localhost:3306/test".to_string());
    Conn::new(url.as_str())
}

struct TestTable {
    name: String,
}

impl TestTable {
    fn new(conn: &mut Conn) -> Result<Self, Error> {
        let id = TABLE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let name = format!("tx_test_{}", id);
        conn.query_drop(&format!("DROP TABLE IF EXISTS {}", name))?;
        conn.query_drop(&format!(
            "CREATE TABLE {} (id INT AUTO_INCREMENT PRIMARY KEY, value INT)",
            name
        ))?;
        Ok(Self { name })
    }

    fn count(&self, conn: &mut Conn) -> Result<i64, Error> {
        let mut stmt = conn.prepare(&format!("SELECT COUNT(*) FROM {}", self.name))?;
        let rows: Vec<(i64,)> = conn.exec_collect(&mut stmt, ())?;
        Ok(rows[0].0)
    }

    fn cleanup(&self, conn: &mut Conn) {
        let _ = conn.query_drop(&format!("DROP TABLE IF EXISTS {}", self.name));
    }
}

#[test]
fn transaction_explicit_commit() -> Result<(), Error> {
    let mut conn = get_conn()?;
    let table = TestTable::new(&mut conn)?;

    conn.transaction(|conn, tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (42)", table.name))?;
        tx.commit(conn)
    })?;

    check_eq!(table.count(&mut conn)?, 1);
    table.cleanup(&mut conn);
    Ok(())
}

#[test]
fn transaction_explicit_rollback() -> Result<(), Error> {
    let mut conn = get_conn()?;
    let table = TestTable::new(&mut conn)?;

    conn.transaction(|conn, tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (42)", table.name))?;
        tx.rollback(conn)
    })?;

    check_eq!(table.count(&mut conn)?, 0);
    table.cleanup(&mut conn);
    Ok(())
}

#[test]
fn transaction_implicit_commit_on_ok() -> Result<(), Error> {
    let mut conn = get_conn()?;
    let table = TestTable::new(&mut conn)?;

    // Return Ok without explicit commit - should auto-commit
    conn.transaction(|conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (42)", table.name))?;
        Ok(())
    })?;

    // Data should be committed
    check_eq!(table.count(&mut conn)?, 1);
    table.cleanup(&mut conn);
    Ok(())
}

#[test]
fn transaction_implicit_rollback_on_err() -> Result<(), Error> {
    let mut conn = get_conn()?;
    let table = TestTable::new(&mut conn)?;

    // Return Err without explicit rollback - should auto-rollback
    let result: Result<(), Error> = conn.transaction(|conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (42)", table.name))?;
        Err(Error::BadUsageError("intentional error".into()))
    });

    check!(result.is_err());
    // Data should be rolled back
    check_eq!(table.count(&mut conn)?, 0);
    table.cleanup(&mut conn);
    Ok(())
}

#[test]
fn transaction_implicit_commit_with_return_value() -> Result<(), Error> {
    let mut conn = get_conn()?;
    let table = TestTable::new(&mut conn)?;

    // Return Ok with a value without explicit commit
    let result: i32 = conn.transaction(|conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (42)", table.name))?;
        Ok(123)
    })?;

    check_eq!(result, 123);
    check_eq!(table.count(&mut conn)?, 1);
    table.cleanup(&mut conn);
    Ok(())
}

#[test]
fn transaction_implicit_commit_multiple_inserts() -> Result<(), Error> {
    let mut conn = get_conn()?;
    let table = TestTable::new(&mut conn)?;

    conn.transaction(|conn, _tx| {
        for i in 1..=5 {
            conn.query_drop(&format!(
                "INSERT INTO {} (value) VALUES ({})",
                table.name, i
            ))?;
        }
        Ok(())
    })?;

    check_eq!(table.count(&mut conn)?, 5);
    table.cleanup(&mut conn);
    Ok(())
}

#[test]
fn transaction_implicit_rollback_partial_work() -> Result<(), Error> {
    let mut conn = get_conn()?;
    let table = TestTable::new(&mut conn)?;

    let result: Result<(), Error> = conn.transaction(|conn, _tx| {
        // Do some work
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (1)", table.name))?;
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (2)", table.name))?;
        // Then fail
        Err(Error::BadUsageError("intentional error".into()))
    });

    check!(result.is_err());
    // All work should be rolled back
    check_eq!(table.count(&mut conn)?, 0);
    table.cleanup(&mut conn);
    Ok(())
}

#[test]
fn transaction_connection_usable_after_implicit_commit() -> Result<(), Error> {
    let mut conn = get_conn()?;
    let table = TestTable::new(&mut conn)?;

    // First transaction with implicit commit
    conn.transaction(|conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (1)", table.name))?;
        Ok(())
    })?;

    // Connection should be usable for another transaction
    conn.transaction(|conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (2)", table.name))?;
        Ok(())
    })?;

    check_eq!(table.count(&mut conn)?, 2);
    table.cleanup(&mut conn);
    Ok(())
}

#[test]
fn transaction_connection_usable_after_implicit_rollback() -> Result<(), Error> {
    let mut conn = get_conn()?;
    let table = TestTable::new(&mut conn)?;

    // First transaction with implicit rollback
    let _: Result<(), Error> = conn.transaction(|conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (1)", table.name))?;
        Err(Error::BadUsageError("intentional error".into()))
    });

    // Connection should be usable for another transaction
    conn.transaction(|conn, _tx| {
        conn.query_drop(&format!("INSERT INTO {} (value) VALUES (2)", table.name))?;
        Ok(())
    })?;

    check_eq!(table.count(&mut conn)?, 1);
    table.cleanup(&mut conn);
    Ok(())
}

#[test]
fn transaction_not_in_transaction_after_implicit_commit() -> Result<(), Error> {
    let mut conn = get_conn()?;

    conn.transaction(|conn, _tx| {
        check!(conn.in_transaction());
        Ok(())
    })?;

    check!(!conn.in_transaction());
    Ok(())
}

#[test]
fn transaction_not_in_transaction_after_implicit_rollback() -> Result<(), Error> {
    let mut conn = get_conn()?;

    let _: Result<(), Error> =
        conn.transaction(|_conn, _tx| Err(Error::BadUsageError("intentional error".into())));

    check!(!conn.in_transaction());
    Ok(())
}
