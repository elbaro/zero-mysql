use crate::protocol::packet::PacketHeader;
use std::io::IoSlice;

/// A set of reusable buffers for MySQL protocol communication
///
/// This struct consolidates various buffers used throughout the connection lifecycle
/// to reduce heap allocations and improve performance.
#[derive(Debug)]
pub struct BufferSet {
    /// Buffer that stores the initial handshake packet from the server
    ///
    /// This is preserved for the lifetime of the connection and may be used
    /// for connection pooling, debugging, or re-authentication scenarios.
    pub initial_handshake: Vec<u8>,

    /// Reusable buffer for reading payloads from the network
    pub read_buffer: Vec<u8>,

    /// Reusable buffer for building outgoing commands
    pub write_buffer: Vec<u8>,

    /// Reusable buffer for packet headers when writing payloads
    ///
    /// Used in sync implementation with vectored I/O
    pub write_headers_buffer: Vec<PacketHeader>,

    /// Reusable buffer for IoSlice when writing payloads
    ///
    /// Used in sync implementation with vectored I/O
    pub ioslice_buffer: Vec<IoSlice<'static>>,

    /// Reusable buffer for assembling complete packets with headers
    ///
    /// Used in async implementations (tokio/compio) where vectored I/O
    /// is less efficient than building a single buffer
    pub packet_buf: Vec<u8>,

    /// Pool of buffers for concurrent operations
    ///
    /// Used in compio implementation to avoid allocations during
    /// async operations that transfer buffer ownership
    pub buffer_pool: Vec<Vec<u8>>,
}

impl BufferSet {
    /// Create a new empty buffer set
    pub fn new() -> Self {
        Self {
            initial_handshake: Vec::new(),
            read_buffer: Vec::new(),
            write_buffer: Vec::new(),
            write_headers_buffer: Vec::new(),
            ioslice_buffer: Vec::new(),
            packet_buf: Vec::new(),
            buffer_pool: Vec::new(),
        }
    }

    /// Create a new buffer set with the initial handshake packet
    pub fn with_initial_handshake(initial_handshake: Vec<u8>) -> Self {
        Self {
            initial_handshake,
            read_buffer: Vec::new(),
            write_buffer: Vec::new(),
            write_headers_buffer: Vec::new(),
            ioslice_buffer: Vec::new(),
            packet_buf: Vec::new(),
            buffer_pool: Vec::new(),
        }
    }

    /// Get a buffer from the pool or create a new one
    ///
    /// Used in compio implementation
    pub fn get_pooled_buffer(&mut self) -> Vec<u8> {
        self.buffer_pool.pop().unwrap_or_default()
    }

    /// Return a buffer to the pool
    ///
    /// Used in compio implementation. Keeps pool size reasonable (max 8 buffers).
    pub fn return_pooled_buffer(&mut self, buffer: Vec<u8>) {
        if self.buffer_pool.len() < 8 {
            self.buffer_pool.push(buffer);
        }
    }
}

impl Default for BufferSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_set_new() {
        let buffers = BufferSet::new();
        assert!(buffers.initial_handshake.is_empty());
        assert!(buffers.read_buffer.is_empty());
        assert!(buffers.write_buffer.is_empty());
        assert!(buffers.write_headers_buffer.is_empty());
        assert!(buffers.ioslice_buffer.is_empty());
        assert!(buffers.packet_buf.is_empty());
        assert!(buffers.buffer_pool.is_empty());
    }

    #[test]
    fn test_buffer_set_default() {
        let buffers = BufferSet::default();
        assert!(buffers.initial_handshake.is_empty());
    }

    #[test]
    fn test_buffer_set_with_initial_handshake() {
        let handshake = vec![0x0a, 0x35, 0x2e, 0x37];
        let buffers = BufferSet::with_initial_handshake(handshake.clone());
        assert_eq!(buffers.initial_handshake, handshake);
        assert!(buffers.read_buffer.is_empty());
    }

    #[test]
    fn test_get_pooled_buffer_empty_pool() {
        let mut buffers = BufferSet::new();
        let buf = buffers.get_pooled_buffer();
        assert!(buf.is_empty());
        assert_eq!(buf.capacity(), 0);
    }

    #[test]
    fn test_get_pooled_buffer_from_pool() {
        let mut buffers = BufferSet::new();
        let test_buf = vec![1, 2, 3, 4];
        buffers.buffer_pool.push(test_buf);

        let buf = buffers.get_pooled_buffer();
        assert_eq!(buf, vec![1, 2, 3, 4]);
        assert!(buffers.buffer_pool.is_empty());
    }

    #[test]
    fn test_return_pooled_buffer() {
        let mut buffers = BufferSet::new();
        let test_buf = vec![1, 2, 3, 4];

        buffers.return_pooled_buffer(test_buf.clone());
        assert_eq!(buffers.buffer_pool.len(), 1);
        assert_eq!(buffers.buffer_pool[0], test_buf);
    }

    #[test]
    fn test_return_pooled_buffer_max_pool_size() {
        let mut buffers = BufferSet::new();

        // Fill pool to max size (8)
        for i in 0..8 {
            buffers.return_pooled_buffer(vec![i]);
        }
        assert_eq!(buffers.buffer_pool.len(), 8);

        // Try to add one more - should be dropped
        buffers.return_pooled_buffer(vec![99]);
        assert_eq!(buffers.buffer_pool.len(), 8);
        assert!(!buffers.buffer_pool.contains(&vec![99]));
    }

    #[test]
    fn test_buffer_reuse() {
        let mut buffers = BufferSet::new();

        // Write to buffers
        buffers.read_buffer.extend_from_slice(b"test data");
        buffers.write_buffer.extend_from_slice(b"query");

        assert_eq!(buffers.read_buffer.len(), 9);
        assert_eq!(buffers.write_buffer.len(), 5);

        // Clear and reuse
        buffers.read_buffer.clear();
        buffers.write_buffer.clear();

        assert_eq!(buffers.read_buffer.len(), 0);
        assert_eq!(buffers.write_buffer.len(), 0);

        // Capacity should be preserved
        assert!(buffers.read_buffer.capacity() >= 9);
        assert!(buffers.write_buffer.capacity() >= 5);
    }
}
