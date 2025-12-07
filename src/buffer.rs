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
    pub write_buffer: Vec<u8>,

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
        self.write_buffer.extend_from_slice(&[0_u8; 4]);
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
