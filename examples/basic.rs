//! Basic example demonstrating table creation, insertion, and querying with CollectHandler
//!
//! This example shows:
//! - Creating a table with various column types
//! - Inserting rows using prepared statements
//! - Querying rows with CollectHandler to collect results into a Vec

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to MySQL/MariaDB (adjust credentials as needed)
    let mut conn = zero_mysql::sync::Conn::new("mysql://test:1234@localhost:3306/test")?;

    // Create a table with various column types
    conn.query_drop(
        "CREATE TABLE IF NOT EXISTS example_types (
            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            tiny_val TINYINT,
            small_val SMALLINT,
            int_val INT,
            big_val BIGINT,
            float_val FLOAT,
            double_val DOUBLE,
            varchar_val VARCHAR(255),
            text_val TEXT,
            blob_val BLOB,
            bool_val BOOLEAN,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )",
    )?;

    // Clear any existing data
    conn.query_drop("TRUNCATE TABLE example_types")?;

    // Prepare an insert statement
    let mut insert_stmt = conn.prepare(
        "INSERT INTO example_types (tiny_val, small_val, int_val, big_val, float_val, double_val, varchar_val, text_val, blob_val, bool_val)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )?;

    // Insert a few rows with different values
    conn.exec_drop(
        &mut insert_stmt,
        (
            1_i8,
            100_i16,
            1000_i32,
            10000_i64,
            3.14_f32,
            2.71828_f64,
            "hello",
            "This is some text",
            b"binary data".as_slice(),
            true,
        ),
    )?;

    conn.exec_drop(
        &mut insert_stmt,
        (
            -1_i8,
            -100_i16,
            -1000_i32,
            -10000_i64,
            -3.14_f32,
            -2.71828_f64,
            "world",
            "More text content",
            b"\x00\x01\x02\x03".as_slice(),
            false,
        ),
    )?;

    conn.exec_drop(
        &mut insert_stmt,
        (
            127_i8,
            32767_i16,
            2147483647_i32,
            9223372036854775807_i64,
            1.0e38_f32,
            1.0e308_f64,
            "max values",
            "Testing maximum values",
            b"".as_slice(),
            true,
        ),
    )?;

    // Prepare a select statement
    let mut select_stmt = conn.prepare(
        "SELECT id, tiny_val, small_val, int_val, big_val, float_val, double_val, varchar_val FROM example_types",
    )?;

    // Use CollectHandler to fetch all rows
    let mut handler: zero_mysql::handler::CollectHandler<(
        u32,
        i8,
        i16,
        i32,
        i64,
        f32,
        f64,
        String,
    )> = zero_mysql::handler::CollectHandler::default();

    conn.exec(&mut select_stmt, (), &mut handler)?;

    let rows = handler.into_rows();

    println!("Fetched {} rows:", rows.len());
    for (id, tiny, small, int, big, float, double, varchar) in &rows {
        println!(
            "  id={}, tiny={}, small={}, int={}, big={}, float={:.2}, double={:.5}, varchar={}",
            id, tiny, small, int, big, float, double, varchar
        );
    }

    // Query with a WHERE clause
    let mut select_positive_stmt =
        conn.prepare("SELECT id, varchar_val FROM example_types WHERE int_val > ?")?;

    let mut handler2: zero_mysql::handler::CollectHandler<(u32, String)> =
        zero_mysql::handler::CollectHandler::default();

    conn.exec(&mut select_positive_stmt, (0_i32,), &mut handler2)?;

    let positive_rows = handler2.into_rows();
    println!("\nRows with positive int_val ({} rows):", positive_rows.len());
    for (id, varchar) in &positive_rows {
        println!("  id={}, varchar={}", id, varchar);
    }

    // Demonstrate exec_first to get only the first row
    let mut first_handler: zero_mysql::handler::CollectHandler<(u32, String)> =
        zero_mysql::handler::CollectHandler::default();
    let found = conn.exec_first(&mut select_positive_stmt, (0_i32,), &mut first_handler)?;

    if found {
        let first_rows = first_handler.into_rows();
        if let Some((id, varchar)) = first_rows.first() {
            println!("\nFirst positive row: id={}, varchar={}", id, varchar);
        }
    }

    println!("\nExample completed successfully!");

    Ok(())
}
