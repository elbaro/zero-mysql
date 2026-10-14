//! Tests for the compio connection pool

#![cfg(feature = "compio")]

use std::cell::Cell;
use std::rc::Rc;

use zero_mysql::Opts;
use zero_mysql::compio::Pool;

include!("common/check.rs");

fn test_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://test:1234@localhost:3306/test?tls_mode=disable".to_string())
}

#[compio::test]
async fn pool_max_concurrency() -> Result<(), Box<dyn std::error::Error>> {
    let mut opts = Opts::try_from(test_url().as_str())?;
    opts.pool_max_concurrency = Some(1);
    let pool = Pool::new(opts);

    let mut first = pool.get().await?;
    let acquired = Rc::new(Cell::new(false));
    let waiter = compio::runtime::spawn({
        let pool = Rc::clone(&pool);
        let acquired = Rc::clone(&acquired);
        async move {
            let conn = pool.get().await;
            acquired.set(true);
            conn.map(drop)
        }
    });

    // Give the waiter a chance to run while `first` is checked out.
    first.query_drop("DO SLEEP(0.1)").await?;
    check!(!acquired.get(), "second get() must wait for the permit");

    drop(first);
    waiter.await.map_err(|err| err.to_string())??;
    check!(acquired.get());
    Ok(())
}

#[test]
fn pool_drop_outside_runtime() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = compio::runtime::Runtime::new()?;
    let pool = Pool::new(Opts::try_from(test_url().as_str())?);
    let conn = runtime.block_on(pool.get())?;
    // Must not panic even though no runtime is current here.
    drop(conn);
    Ok(())
}
