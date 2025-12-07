use crate::BufferSet;

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
