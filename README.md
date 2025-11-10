# zero-mysql

A high-performance sans-IO MySQL protocol implementation in Rust with minimal copying and syscalls.

## Architecture

This library implements the MySQL wire protocol as a pure sans-IO layer, allowing external libraries to handle I/O and provide their own async/sync implementations.

### Key Design Principles

- **Sans-IO**: Protocol logic separated from I/O operations
- **Zero-copy with zerocopy crate**: Fixed-size protocol structures (`PrepareOk`, `EofPacket`, `PacketHeader`) use the `zerocopy` crate for direct byte slice reinterpretation without parsing overhead
- **Minimal allocations**: Reusable buffers and careful memory management
- **Generic parameters**: `Params` trait allows external libraries to implement custom parameter serialization
- **Raw row data**: Returns `Row<'a>` with references to raw bytes for external parsing

### Module Structure

```
src/
├── lib.rs
├── error.rs              # Error types (ServerError, IoError, etc.)
├── constant.rs           # CommandByte, CapabilityFlags, StatusFlags, ColumnFlags, ColumnType
├── row.rs               # Row<'a> - zero-copy row wrapper
├── col.rs               # ColumnDefinition struct
└── protocol/
    ├── packet.rs         # PacketDecoder (implements tokio_util::Decoder)
    ├── primitive.rs      # Low-level read/write functions (read_int_1, read_lenenc, etc.)
    ├── response.rs       # OK/ERR/EOF packet parsing
    ├── connection/
    │   └── handshake.rs  # Connection handshake protocol
    ├── command/
    │   ├── text.rs       # Text protocol (COM_QUERY)
    │   ├── prepared.rs   # Prepared statements (COM_STMT_PREPARE, COM_STMT_EXECUTE)
    │   ├── resultset.rs  # Result set and column definition parsing
    │   └── utility.rs    # Simple commands (PING, QUIT, etc.)
    └── trait/
        └── params.rs     # Params trait for parameter binding
```

## Usage Example

```rust
use zero_mysql::protocol::command::prepared;

fn execute_prepared_statement<P: Params>(stmt_id: u32, params: P) {
    let mut out = Vec::new();

    // Write execute command
    prepared::write_execute(&mut out, stmt_id, &params);

    // Send to server (external responsibility)
    send_to_server(&out);

    // Read response
    let payload = read_payload_from_server();
    let response = prepared::read_execute_response(&payload)?;

    match response {
        ExecuteResponse::Ok(ok) => {
            println!("Affected rows: {}", ok.affected_rows);
        }
        ExecuteResponse::ResultSet { column_count } => {
            // Read column definitions
            for _ in 0..column_count {
                let col_payload = read_payload_from_server();
                let col_def = prepared::read_column_definition(&col_payload)?;
            }

            // Read rows
            loop {
                let row_payload = read_payload_from_server();
                if let Some(row) = prepared::read_binary_row(&row_payload, column_count)? {
                    // Process row (external library parses values)
                    process_row(row);
                } else {
                    break; // EOF
                }
            }
        }
    }
}
```

## Features

### Implemented

- ✅ Packet framing (PacketDecoder with tokio_util::Decoder)
- ✅ Primitive read/write operations
- ✅ OK/ERR/EOF packet parsing (EOF is zero-copy with `zerocopy` crate)
- ✅ Connection handshake
- ✅ Text protocol (COM_QUERY)
- ✅ Prepared statements (COM_STMT_PREPARE, COM_STMT_EXECUTE, COM_STMT_CLOSE) - PrepareOk is zero-copy
- ✅ Result set parsing (both text and binary)
- ✅ Column definition parsing
- ✅ Utility commands (PING, QUIT, INIT_DB, RESET_CONNECTION)
- ✅ Generic Params trait for parameter binding
- ✅ Zero-copy packet header (PacketHeader)

### Not Implemented (Non-Priority)

- Sequence ID verification
- SSL/TLS
- Authentication plugins (basic handshake only)
- Old protocol formats (ColumnDefinition320)
- Transaction state tracking
- Global state machine
- Sync/async wrappers

## External Responsibilities

The following are intentionally left to external libraries:

1. **I/O operations** - Reading from and writing to sockets
2. **16MB packet splitting** - Breaking large payloads into multiple packets
3. **16MB packet concatenation** - Combining multi-packet payloads
4. **Parameter value encoding** - Implementing the `Params` trait
5. **Row value decoding** - Parsing raw bytes from `Row<'a>`
6. **Connection pooling** - Managing multiple connections
7. **Async runtime** - Tokio, async-std, or sync

## Performance Characteristics

- **Minimal syscalls**: External code controls buffering strategy
- **True zero-copy parsing**: Fixed-size structs use `zerocopy` crate for direct memory reinterpretation
- **No allocations in hot paths**: Reuses buffers where possible
- **Predictable performance**: No hidden allocations or locks
- **Type-safe endianness**: `zerocopy` handles little-endian conversions at compile time

## Dependencies

- `thiserror` - Error handling
- `bytes` - Zero-copy buffer management
- `tokio-util` - Codec trait (no runtime dependency)
- `zerocopy` - Zero-copy byte slice reinterpretation for protocol structures

## License

(Add your license here)
