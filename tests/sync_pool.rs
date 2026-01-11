//! Integration tests for sync connection pool

use std::sync::Arc;
use std::thread;

use zero_mysql::Opts;
use zero_mysql::sync::Pool;

const TEST_URL: &str = "mysql://test:1234@localhost:3306/test";

#[test]
fn test_pool_basic() {
    let opts = Opts::try_from(TEST_URL).expect("parse opts");
    let pool = Arc::new(Pool::new(opts));

    let mut conn = pool.get().expect("get connection");
    conn.query_drop("SELECT 1").expect("query");
}

#[test]
fn test_pool_connection_reuse() {
    let mut opts = Opts::try_from(TEST_URL).expect("parse opts");
    opts.pool_max_idle_conn = 1;
    opts.pool_reset_conn = false;
    let pool = Arc::new(Pool::new(opts));

    // Get first connection and remember its ID
    let conn1 = pool.get().expect("get connection 1");
    let conn_id1 = conn1.connection_id();
    drop(conn1);

    // Get another connection - should reuse the same one
    let conn2 = pool.get().expect("get connection 2");
    let conn_id2 = conn2.connection_id();

    assert_eq!(conn_id1, conn_id2, "connection should be reused from pool");
}

#[test]
fn test_pool_max_idle_conn() {
    let mut opts = Opts::try_from(TEST_URL).expect("parse opts");
    opts.pool_max_idle_conn = 2;
    opts.pool_reset_conn = false;
    let pool = Arc::new(Pool::new(opts));

    // Get 3 connections
    let conn1 = pool.get().expect("get connection 1");
    let conn2 = pool.get().expect("get connection 2");
    let conn3 = pool.get().expect("get connection 3");

    let id1 = conn1.connection_id();
    let id2 = conn2.connection_id();
    let id3 = conn3.connection_id();

    // Return all connections - pool can only hold 2, so the last one (conn3) will be dropped
    drop(conn1);
    drop(conn2);
    drop(conn3); // This one gets rejected since pool is full

    // Get 2 connections - they should be from the pool (id1 and id2)
    let conn_a = pool.get().expect("get connection a");
    let conn_b = pool.get().expect("get connection b");

    let id_a = conn_a.connection_id();
    let id_b = conn_b.connection_id();

    // The pool can only hold 2 connections, so conn3 was dropped
    // conn1 and conn2 should be reused
    assert!(
        (id_a == id1 || id_a == id2) && (id_b == id1 || id_b == id2),
        "connections should be reused from pool (got {id_a} and {id_b}, expected {id1} and {id2}), conn3 was dropped (id3={id3})"
    );
}

#[test]
fn test_pool_max_concurrency() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    let mut opts = Opts::try_from(TEST_URL).expect("parse opts");
    opts.pool_max_concurrency = Some(2);
    opts.pool_reset_conn = false;
    let pool = Arc::new(Pool::new(opts));

    let active_count = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];

    for _ in 0..4 {
        let pool = Arc::clone(&pool);
        let active_count = Arc::clone(&active_count);
        let max_active = Arc::clone(&max_active);

        handles.push(thread::spawn(move || {
            let _conn = pool.get().expect("get connection");
            let current = active_count.fetch_add(1, Ordering::SeqCst) + 1;

            // Update max_active if current is higher
            loop {
                let old_max = max_active.load(Ordering::SeqCst);
                if current <= old_max {
                    break;
                }
                if max_active
                    .compare_exchange(old_max, current, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    break;
                }
            }

            thread::sleep(Duration::from_millis(50));
            active_count.fetch_sub(1, Ordering::SeqCst);
        }));
    }

    for handle in handles {
        handle.join().expect("thread join");
    }

    let observed_max = max_active.load(Ordering::SeqCst);
    assert!(
        observed_max <= 2,
        "max concurrent connections should be limited to 2, but observed {observed_max}"
    );
}

#[test]
fn test_pool_reset_conn() {
    let mut opts = Opts::try_from(TEST_URL).expect("parse opts");
    opts.pool_max_idle_conn = 1;
    opts.pool_reset_conn = true;
    let pool = Arc::new(Pool::new(opts));

    // Get a connection and set a session variable
    {
        let mut conn = pool.get().expect("get connection");
        conn.query_drop("SET @test_var = 42").expect("set variable");
    }

    // Get another connection (should be reset)
    {
        let mut conn = pool.get().expect("get connection");

        // Query the variable - it should be NULL after reset
        struct VarHandler {
            value: Option<String>,
        }

        impl zero_mysql::protocol::r#trait::TextResultSetHandler for VarHandler {
            fn no_result_set(
                &mut self,
                _: zero_mysql::protocol::response::OkPayloadBytes,
            ) -> zero_mysql::error::Result<()> {
                Ok(())
            }
            fn resultset_start(
                &mut self,
                _: &[zero_mysql::protocol::command::ColumnDefinition<'_>],
            ) -> zero_mysql::error::Result<()> {
                Ok(())
            }
            fn resultset_end(
                &mut self,
                _: zero_mysql::protocol::response::OkPayloadBytes,
            ) -> zero_mysql::error::Result<()> {
                Ok(())
            }
            fn row(
                &mut self,
                _: &[zero_mysql::protocol::command::ColumnDefinition<'_>],
                row: zero_mysql::protocol::TextRowPayload<'_>,
            ) -> zero_mysql::error::Result<()> {
                // 0xFB indicates NULL
                if row.0.first() == Some(&0xFB) {
                    self.value = None;
                } else {
                    let (value, _) = zero_mysql::protocol::primitive::read_string_lenenc(row.0)?;
                    self.value = Some(String::from_utf8_lossy(value).into_owned());
                }
                Ok(())
            }
        }

        let mut handler = VarHandler {
            value: Some("not_null".to_string()),
        };
        conn.query("SELECT @test_var", &mut handler)
            .expect("query variable");

        assert!(
            handler.value.is_none(),
            "session variable should be NULL after connection reset, got {:?}",
            handler.value
        );
    }
}

#[test]
fn test_pool_multi_threaded() {
    let mut opts = Opts::try_from(TEST_URL).expect("parse opts");
    opts.pool_max_idle_conn = 5;
    opts.pool_reset_conn = false;
    let pool = Arc::new(Pool::new(opts));

    let mut handles = vec![];

    for i in 0..10 {
        let pool = Arc::clone(&pool);
        handles.push(thread::spawn(move || {
            let mut conn = pool.get().expect("get connection");
            conn.query_drop(&format!("SELECT {i}")).expect("query");
        }));
    }

    for handle in handles {
        handle.join().expect("thread join");
    }
}
