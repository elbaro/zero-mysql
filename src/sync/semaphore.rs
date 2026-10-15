//! A blocking counting semaphore that grants permits in FIFO order.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::{self, Thread};

pub struct Semaphore {
    state: Mutex<State>,
}

struct State {
    /// Only nonzero while `waiters` is empty.
    permits: usize,
    waiters: VecDeque<Arc<Waiter>>,
}

struct Waiter {
    thread: Thread,
    /// Set when `release` hands this waiter a permit.
    granted: AtomicBool,
}

pub struct Permit {
    semaphore: Arc<Semaphore>,
}

impl Semaphore {
    pub fn new(permits: usize) -> Self {
        Self {
            state: Mutex::new(State {
                permits,
                waiters: VecDeque::new(),
            }),
        }
    }

    pub fn acquire(self: &Arc<Self>) -> Permit {
        let waiter = {
            let mut state = self.lock_state();
            if state.permits > 0 {
                state.permits -= 1;
                return Permit {
                    semaphore: Arc::clone(self),
                };
            }
            let waiter = Arc::new(Waiter {
                thread: thread::current(),
                granted: AtomicBool::new(false),
            });
            state.waiters.push_back(Arc::clone(&waiter));
            waiter
        };

        // `park` can return spuriously, so it is only a hint to check `granted`.
        while !waiter.granted.load(Ordering::Acquire) {
            thread::park();
        }
        Permit {
            semaphore: Arc::clone(self),
        }
    }

    fn release(&self) {
        let mut state = self.lock_state();
        let Some(waiter) = state.waiters.pop_front() else {
            state.permits += 1;
            return;
        };
        waiter.granted.store(true, Ordering::Release);
        drop(state);
        waiter.thread.unpark();
    }

    fn lock_state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.semaphore.release();
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc;

    use super::*;

    fn wait_for_waiters(semaphore: &Semaphore, count: usize) {
        while semaphore.lock_state().waiters.len() < count {
            thread::yield_now();
        }
    }

    fn try_acquire(semaphore: &Arc<Semaphore>) -> Option<Permit> {
        let mut state = semaphore.lock_state();
        if state.permits == 0 {
            return None;
        }
        state.permits -= 1;
        Some(Permit {
            semaphore: Arc::clone(semaphore),
        })
    }

    #[test]
    fn never_exceeds_capacity() {
        let semaphore = Arc::new(Semaphore::new(4));
        let active = AtomicUsize::new(0);
        thread::scope(|scope| {
            for _ in 0..16 {
                scope.spawn(|| {
                    for _ in 0..20_000 {
                        let _permit = semaphore.acquire();
                        let now_active = active.fetch_add(1, Ordering::SeqCst) + 1;
                        assert!(now_active <= 4, "{now_active} permits held at once");
                        active.fetch_sub(1, Ordering::SeqCst);
                    }
                });
            }
        });
        let state = semaphore.lock_state();
        assert_eq!(state.permits, 4);
        assert!(state.waiters.is_empty());
    }

    #[test]
    fn grants_in_arrival_order() {
        let semaphore = Arc::new(Semaphore::new(1));
        let order = Mutex::new(Vec::new());
        let held = semaphore.acquire();
        thread::scope(|scope| {
            for id in 0..8 {
                let (semaphore, order) = (&semaphore, &order);
                scope.spawn(move || {
                    let _permit = semaphore.acquire();
                    order.lock().unwrap().push(id);
                });
                // Wait until this thread is queued before starting the next.
                wait_for_waiters(semaphore, id + 1);
            }
            drop(held);
        });
        assert_eq!(*order.lock().unwrap(), (0..8).collect::<Vec<_>>());
    }

    #[test]
    fn released_permit_cannot_be_taken_by_newcomer() {
        let semaphore = Arc::new(Semaphore::new(1));
        let held = semaphore.acquire();
        let (checked_sender, checked_receiver) = mpsc::channel::<()>();
        thread::scope(|scope| {
            let waiter_semaphore = &semaphore;
            scope.spawn(move || {
                let _permit = waiter_semaphore.acquire();
                // Hold the permit so it cannot return to the count before the
                // main thread checks.
                checked_receiver.recv().unwrap();
            });
            wait_for_waiters(&semaphore, 1);
            drop(held);
            assert!(try_acquire(&semaphore).is_none());
            checked_sender.send(()).unwrap();
        });
    }
}
