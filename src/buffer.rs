/// A set of reusable buffers for MySQL protocol communication
///
/// `Conn` uses a single `BufferSet` for all its operations.
#[derive(Debug)]
pub struct BufferSet {
    /// Bytes are valid during Conn.
    pub initial_handshake: Vec<u8>,

    /// General-purpose read buffer
    /// Bytes are valid during an operation.
    pub read_buffer: Vec<u8>,

    /// General-purpose write buffer
    /// It always has at least 4 bytes which is reserved for the first packet header.
    /// It is followed by payload bytes without considering 16MB split.
    /// Layout: [4-byte header space][payload that is possibly larger than 16MB]
    /// Bytes are valid during an operation.
    write_buffer: Vec<u8>,

    /// ColumnDefinition packets in one buffer
    /// Bytes are valid during an operation.
    pub column_definition_buffer: Vec<u8>,
}

impl BufferSet {
    /// Create a new empty buffer set
    pub fn new() -> Self {
        Self {
            initial_handshake: Vec::new(),
            read_buffer: Vec::new(),
            write_buffer: vec![0; 4],
            column_definition_buffer: Vec::new(),
        }
    }

    /// Create a new buffer set with the initial handshake packet
    pub fn with_initial_handshake(initial_handshake: Vec<u8>) -> Self {
        Self {
            initial_handshake,
            read_buffer: Vec::new(),
            write_buffer: vec![0; 4],
            column_definition_buffer: Vec::new(),
        }
    }

    /// Clear the write buffer, reserve 4 bytes for the header, and return mutable access.
    #[inline]
    pub fn new_write_buffer(&mut self) -> &mut Vec<u8> {
        self.write_buffer.clear();
        self.write_buffer.extend_from_slice(&[0u8; 4]);
        &mut self.write_buffer
    }

    /// Get mutable access to the write buffer.
    #[inline]
    pub fn write_buffer_mut(&mut self) -> &mut Vec<u8> {
        &mut self.write_buffer
    }

    /// Get the write buffer for reading.
    #[inline]
    pub fn write_buffer(&self) -> &[u8] {
        &self.write_buffer
    }

    /// Get the payload length (total buffer length minus 4-byte header).
    #[inline]
    pub fn payload_len(&self) -> usize {
        self.write_buffer.len().saturating_sub(4)
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
        assert_eq!(buffers.write_buffer().len(), 4); // pre-allocated header space
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
    fn test_new_write_buffer() {
        let mut buffers = BufferSet::new();
        let buf = buffers.new_write_buffer();
        // Should have 4 bytes reserved for header
        assert_eq!(buf.len(), 4);
        assert_eq!(buffers.payload_len(), 0);
    }

    #[test]
    fn test_write_buffer_mut() {
        let mut buffers = BufferSet::new();
        buffers.new_write_buffer().extend_from_slice(b"SELECT 1");
        // 4 header bytes + 8 payload bytes
        assert_eq!(buffers.write_buffer().len(), 12);
        assert_eq!(buffers.payload_len(), 8);
    }

    #[test]
    fn test_write_buffer() {
        let mut buffers = BufferSet::new();
        buffers.new_write_buffer().extend_from_slice(b"test");
        let packet = buffers.write_buffer();
        assert_eq!(packet.len(), 8); // 4 header + 4 payload
        assert_eq!(&packet[4..], b"test");
    }

    #[test]
    fn test_buffer_reuse() {
        let mut buffers = BufferSet::new();

        // Write to buffers
        buffers.read_buffer.extend_from_slice(b"test data");
        buffers.new_write_buffer().extend_from_slice(b"query");

        assert_eq!(buffers.read_buffer.len(), 9);
        assert_eq!(buffers.payload_len(), 5);

        // Clear and reuse
        buffers.read_buffer.clear();
        buffers.new_write_buffer();

        assert_eq!(buffers.read_buffer.len(), 0);
        assert_eq!(buffers.payload_len(), 0);

        // Capacity should be preserved
        assert!(buffers.read_buffer.capacity() >= 9);
    }
}
