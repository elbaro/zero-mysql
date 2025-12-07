use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crossbeam_queue::ArrayQueue;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::Result;
use crate::opts::Opts;

use super::Conn;

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

    pub async fn get(self: &Arc<Self>) -> Result<PooledConn> {
        let permit =
            match &self.semaphore {
                Some(sem) => Some(Arc::clone(sem).acquire_owned().await.map_err(
                    |_acquire_err| {
                        crate::error::Error::LibraryBug(color_eyre::eyre::eyre!("semaphore closed"))
                    },
                )?),
                None => None,
            };
        let mut conn = match self.conns.pop() {
            Some(c) => c,
            None => Conn::new(self.opts.clone()).await?,
        };
        conn.ping().await?;
        Ok(PooledConn {
            conn: ManuallyDrop::new(conn),
            pool: Arc::clone(self),
            _permit: permit,
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
    _permit: Option<OwnedSemaphorePermit>,
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
