use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, LazyLock};

use crossbeam_queue::ArrayQueue;

use crate::BufferSet;

const POOL_CAPACITY: usize = 128;

pub static GLOBAL_BUFFER_POOL: LazyLock<Arc<BufferPool>> =
    LazyLock::new(|| Arc::new(BufferPool::default()));

/// A pooled `Vec<u8>` that returns itself to the pool on drop.
pub struct PooledColumnDefinitionVec {
    pool: Arc<BufferPool>,
    inner: ManuallyDrop<Vec<u8>>,
}

impl PooledColumnDefinitionVec {
    fn new(pool: Arc<BufferPool>, vec: Vec<u8>) -> Self {
        Self {
            pool,
            inner: ManuallyDrop::new(vec),
        }
    }
}

impl Deref for PooledColumnDefinitionVec {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for PooledColumnDefinitionVec {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Drop for PooledColumnDefinitionVec {
    fn drop(&mut self) {
        // SAFETY: inner is never accessed after this
        let vec = unsafe { ManuallyDrop::take(&mut self.inner) };
        self.pool.return_column_definition(vec);
    }
}

/// A pooled `BufferSet` that returns itself to the pool on drop.
pub struct PooledBufferSet {
    pool: Arc<BufferPool>,
    inner: ManuallyDrop<BufferSet>,
}

impl PooledBufferSet {
    fn new(pool: Arc<BufferPool>, buffer_set: BufferSet) -> Self {
        Self {
            pool,
            inner: ManuallyDrop::new(buffer_set),
        }
    }
}

impl Deref for PooledBufferSet {
    type Target = BufferSet;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for PooledBufferSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Drop for PooledBufferSet {
    fn drop(&mut self) {
        // SAFETY: inner is never accessed after this
        let buffer_set = unsafe { ManuallyDrop::take(&mut self.inner) };
        self.pool.return_buffer_set(buffer_set);
    }
}

#[derive(Debug)]
pub struct BufferPool {
    buffer_sets: ArrayQueue<BufferSet>,
    column_definition_buffers: ArrayQueue<Vec<u8>>,
}

impl BufferPool {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer_sets: ArrayQueue::new(capacity),
            column_definition_buffers: ArrayQueue::new(capacity),
        }
    }

    pub fn get_buffer_set(self: &Arc<Self>) -> PooledBufferSet {
        let buffer_set = self.buffer_sets.pop().unwrap_or_default();
        PooledBufferSet::new(Arc::clone(self), buffer_set)
    }

    pub fn return_buffer_set(&self, mut buffer_set: BufferSet) {
        // Clear buffers but preserve capacity
        buffer_set.initial_handshake.clear();
        buffer_set.read_buffer.clear();
        buffer_set.column_definition_buffer.clear();
        // write_buffer is handled by new_write_buffer()

        // Ignore if pool is full
        let _ = self.buffer_sets.push(buffer_set);
    }

    pub fn get_column_definition(self: &Arc<Self>) -> PooledColumnDefinitionVec {
        let vec = self.column_definition_buffers.pop().unwrap_or_default();
        PooledColumnDefinitionVec::new(Arc::clone(self), vec)
    }

    pub fn return_column_definition(&self, mut vec: Vec<u8>) {
        vec.clear();
        // Ignore if pool is full
        let _ = self.column_definition_buffers.push(vec);
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new(POOL_CAPACITY)
    }
}
