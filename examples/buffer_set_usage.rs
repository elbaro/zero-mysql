use zero_mysql::BufferSet;

fn main() {
    // Create a new empty buffer set
    let mut buffers = BufferSet::new();

    println!("Created empty BufferSet");
    println!("Initial handshake size: {}", buffers.initial_handshake.len());

    // Simulate storing an initial handshake packet
    buffers.initial_handshake = vec![0x0a, 0x38, 0x2e, 0x30]; // Mock handshake data
    println!("\nStored initial handshake: {} bytes", buffers.initial_handshake.len());

    // Use read buffer
    buffers.read_buffer.extend_from_slice(b"Some payload data");
    println!("Read buffer size: {}", buffers.read_buffer.len());

    // Use write buffer with the new API
    buffers.new_write_buffer().extend_from_slice(b"COM_QUERY SELECT 1");
    println!("Payload size: {}", buffers.payload_len());
    println!("Packet size (with header): {}", buffers.write_buffer().len());

    // Create a buffer set with initial handshake
    let initial_handshake = vec![0x0a, 0x35, 0x2e, 0x37, 0x2e, 0x33, 0x33];
    let buffers_with_handshake = BufferSet::with_initial_handshake(initial_handshake);
    println!("\nCreated BufferSet with handshake: {} bytes",
             buffers_with_handshake.initial_handshake.len());
}
