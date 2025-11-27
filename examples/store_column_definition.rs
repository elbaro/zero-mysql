/// Example demonstrating that handlers can borrow ColumnDefinitionBytes
/// and extract data from them without needing to clone the raw bytes,
/// thanks to the column_definition_buffer in BufferSet
use zero_mysql::error::Result;
use zero_mysql::protocol::BinaryRowPayload;
use zero_mysql::protocol::command::ColumnDefinitionBytes;
use zero_mysql::protocol::response::OkPayloadBytes;

/// Handler that extracts and stores column metadata

impl zero_mysql::protocol::r#trait::BinaryResultSetHandler for HandlerWithColumnMetadata {
    fn no_result_set(&mut self, _ok: OkPayloadBytes) -> Result<()> {
        Ok(())
    }

    fn resultset_start(&mut self, num_columns: usize) -> Result<()> {
        self.column_types.reserve(num_columns);
        Ok(())
    }

    fn col<'buffers>(&mut self, col: ColumnDefinitionBytes<'buffers>) -> Result<()> {
        // Extract type information from ColumnDefinitionBytes without cloning!
        // The key improvement: ColumnDefinitionBytes now references
        // buffer_set.column_definition_buffer with a clean lifetime,
        // so we can borrow it safely during this callback.
        let tail = col.tail()?;
        let type_and_flags = tail.type_and_flags()?;

        println!(
            "Column {}: type={:?}, flags={:?}",
            self.column_types.len(),
            type_and_flags.column_type,
            type_and_flags.flags
        );

        // Store the extracted metadata
        self.column_types.push(type_and_flags);
        Ok(())
    }

    fn row(&mut self, _row: &BinaryRowPayload) -> Result<()> {
        println!("Processing row with {} columns", self.column_types.len());
        Ok(())
    }

    fn resultset_end(&mut self, _eof: OkPayloadBytes) -> Result<()> {
        println!("Result set ended with {} columns", self.column_types.len());
        Ok(())
    }
}

fn main() {
    println!("This example demonstrates that handlers can store ColumnDefinitionBytes");
    println!("without cloning, thanks to buffer_set.column_definition_buffer.");
    println!();
    println!("The key insight:");
    println!("- Previously: ColumnDefinitionBytes referenced read_buffer (cleared each iteration)");
    println!("- Now: ColumnDefinitionBytes references column_definition_buffer (stable location)");
    println!("- Result: Handlers can store the bytes with lifetime 'a bound to BufferSet");
}
