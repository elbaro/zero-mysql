//! Asynchronous connection pool for compio (single-threaded, Rc-based).

use std::cell::RefCell;
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::sync::Arc;

use async_lock::{Semaphore, SemaphoreGuardArc};

use crate::error::Result;
use crate::opts::Opts;

use super::Conn;

pub struct Pool {
    opts: Opts,
    conns: RefCell<Vec<Conn>>,
    max_idle: usize,
    semaphore: Option<Arc<Semaphore>>,
}

impl Pool {
    pub fn new(opts: Opts) -> Rc<Self> {
        let max_idle = opts.pool_max_idle_conn;
        let semaphore = opts
            .pool_max_concurrency
            .map(|n| Arc::new(Semaphore::new(n)));
        Rc::new(Self {
            opts,
            conns: RefCell::new(Vec::new()),
            max_idle,
            semaphore,
        })
    }

    pub async fn get(self: &Rc<Self>) -> Result<PooledConn> {
        let permit = match &self.semaphore {
            Some(sem) => Some(sem.acquire_arc().await),
            None => None,
        };
        let conn = loop {
            let candidate = self.conns.borrow_mut().pop();
            match candidate {
                Some(mut c) => {
                    if c.ping().await.is_ok() {
                        break c;
                    }
                }
                None => break Conn::new(self.opts.clone()).await?,
            }
        };
        Ok(PooledConn {
            conn: ManuallyDrop::new(conn),
            pool: Rc::clone(self),
            permit,
        })
    }

    fn check_in(self: &Rc<Self>, mut conn: Conn, permit: Option<SemaphoreGuardArc>) {
        if conn.is_broken() {
            return;
        }
        let pool = Rc::clone(self);
        let reset = async move {
            if conn.reset().await.is_ok() {
                let mut conns = pool.conns.borrow_mut();
                if conns.len() < pool.max_idle {
                    conns.push(conn);
                }
            }
            drop(permit);
        };
        // Without a runtime (e.g. during shutdown) the connection cannot be
        // reset, so it is dropped instead of returned to the pool.
        let _ = compio::runtime::Runtime::try_with_current(|rt| rt.spawn(reset).detach());
    }
}

pub struct PooledConn {
    pool: Rc<Pool>,
    conn: ManuallyDrop<Conn>,
    permit: Option<SemaphoreGuardArc>,
}

impl Deref for PooledConn {
    type Target = Conn;
    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl DerefMut for PooledConn {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

impl Drop for PooledConn {
    fn drop(&mut self) {
        // SAFETY: conn is never accessed after this
        let conn = unsafe { ManuallyDrop::take(&mut self.conn) };
        self.pool.check_in(conn, self.permit.take());
    }
}
