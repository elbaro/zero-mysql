use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crossbeam_queue::ArrayQueue;

use crate::error::Result;
use crate::opts::Opts;

use super::Conn;

pub struct Pool {
    opts: Opts,
    conns: ArrayQueue<Conn>,
}

impl Pool {
    pub fn new(opts: Opts, max_size: usize) -> Self {
        Self {
            opts,
            conns: ArrayQueue::new(max_size),
        }
    }

    pub async fn get(self: &Arc<Self>) -> Result<PooledConn> {
        let mut conn = match self.conns.pop() {
            Some(c) => c,
            None => Conn::new(self.opts.clone()).await?,
        };
        conn.ping().await?;
        Ok(PooledConn {
            conn: ManuallyDrop::new(conn),
            pool: Arc::clone(self),
        })
    }

    fn check_in(self: &Arc<Self>, mut conn: Conn) {
        if conn.is_broken() {
            return;
        }
        if self.opts.pool_reset_conn {
            let Ok(handle) = tokio::runtime::Handle::try_current() else {
                return;
            };
            let pool = Arc::clone(self);
            handle.spawn(async move {
                if conn.reset().await.is_ok() {
                    let _ = pool.conns.push(conn);
                }
            });
        } else {
            let _ = self.conns.push(conn);
        }
    }
}

pub struct PooledConn {
    pool: Arc<Pool>,
    conn: ManuallyDrop<Conn>,
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
    }
}
