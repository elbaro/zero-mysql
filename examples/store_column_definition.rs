/// Example demonstrating that handlers receive ColumnDefinition data
/// along with each row, allowing access to column metadata without
/// needing separate storage.
use zero_mysql::error::Result;
use zero_mysql::protocol::BinaryRowPayload;
use zero_mysql::protocol::command::ColumnDefinition;
use zero_mysql::protocol::response::OkPayloadBytes;

/// Handler that processes rows with column metadata
struct RowProcessor;

impl zero_mysql::protocol::r#trait::BinaryResultSetHandler for RowProcessor {
    fn no_result_set(&mut self, _ok: OkPayloadBytes) -> Result<()> {
        Ok(())
    }

    fn resultset_start(&mut self, num_columns: usize) -> Result<()> {
        println!("Result set starting with {} columns", num_columns);
        Ok(())
    }

    fn row<'a>(
        &mut self,
        cols: &[ColumnDefinition<'a>],
        _row: &'a BinaryRowPayload<'a>,
    ) -> Result<()> {
        // Column definitions are provided with each row
        for (i, col) in cols.iter().enumerate() {
            let type_and_flags = col.tail.type_and_flags()?;
            println!(
                "Column {}: type={:?}, flags={:?}",
                i, type_and_flags.column_type, type_and_flags.flags
            );
        }
        println!("Processing row with {} columns", cols.len());
        Ok(())
    }

    fn resultset_end(&mut self, _eof: OkPayloadBytes) -> Result<()> {
        println!("Result set ended");
        Ok(())
    }
}

fn main() {
    println!("This example demonstrates that handlers receive ColumnDefinition");
    println!("data along with each row in the row() callback.");
    println!();
    println!("The key insight:");
    println!("- Column definitions are passed to row() along with the row data");
    println!("- This allows handlers to decode values using type information");
    println!("- No need to store column metadata separately in the handler");
}
