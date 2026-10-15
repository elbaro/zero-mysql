use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crossbeam_queue::ArrayQueue;

use crate::error::Result;
use crate::opts::Opts;

use super::Conn;
use super::semaphore::{Permit, Semaphore};

pub struct Pool {
    opts: Opts,
    conns: ArrayQueue<Conn>,
    semaphore: Option<Arc<Semaphore>>,
}

impl Pool {
    pub fn new(opts: Opts) -> Self {
        let semaphore = opts
            .pool_max_concurrency
            .map(|n| Arc::new(Semaphore::new(n)));
        Self {
            conns: ArrayQueue::new(opts.pool_max_idle_conn),
            opts,
            semaphore,
        }
    }

    pub fn get(self: &Arc<Self>) -> Result<PooledConn> {
        let permit = self.semaphore.as_ref().map(Semaphore::acquire);
        let mut conn = match self.conns.pop() {
            Some(c) => c,
            None => Conn::new(self.opts.clone())?,
        };
        conn.ping()?;
        Ok(PooledConn {
            conn: ManuallyDrop::new(conn),
            pool: Arc::clone(self),
            permit,
        })
    }

    fn check_in(&self, mut conn: Conn) {
        if conn.is_broken() {
            return;
        }
        if self.opts.pool_reset_conn && conn.reset().is_err() {
            return;
        }
        let _ = self.conns.push(conn);
    }
}

pub struct PooledConn {
    pool: Arc<Pool>,
    conn: ManuallyDrop<Conn>,
    permit: Option<Permit>,
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
        self.pool.check_in(conn);
        // Release after check-in so the next permit holder reuses this connection.
        drop(self.permit.take());
    }
}
