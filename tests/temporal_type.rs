//! Integration tests for TIMESTAMP column types
//!
//! Tests how DATETIME(6) column handles different input formats
//! and what binary representations (Timestamp4/7/11) are returned.

use zero_mysql::error::Result;
use zero_mysql::protocol::command::ColumnDefinition;
use zero_mysql::protocol::response::OkPayloadBytes;
use zero_mysql::protocol::r#trait::BinaryResultSetHandler;
use zero_mysql::protocol::BinaryRowPayload;
use zero_mysql::raw::parse_value;
use zero_mysql::value::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimestampType {
    Timestamp0,
    Timestamp4,
    Timestamp7,
    Timestamp11,
    Other,
}

struct TimestampTypeCollector {
    types: Vec<TimestampType>,
}

impl TimestampTypeCollector {
    fn new() -> Self {
        Self { types: Vec::new() }
    }
}

impl BinaryResultSetHandler for TimestampTypeCollector {
    fn no_result_set(&mut self, _ok: OkPayloadBytes) -> Result<()> {
        Ok(())
    }

    fn resultset_start(&mut self, _cols: &[ColumnDefinition<'_>]) -> Result<()> {
        Ok(())
    }

    fn row(&mut self, cols: &[ColumnDefinition<'_>], row: BinaryRowPayload<'_>) -> Result<()> {
        let null_bitmap = row.null_bitmap();
        let data = row.values();

        // Parse the first column (id INT) to skip it
        let (_id, rest): (i32, _) = parse_value(&cols[0].tail, null_bitmap.is_null(0), data)?;

        // Parse the timestamp column (second column, index 1)
        let (value, _): (Value<'_>, _) = parse_value(&cols[1].tail, null_bitmap.is_null(1), rest)?;

        let ts_type = match value {
            Value::Timestamp0 => TimestampType::Timestamp0,
            Value::Timestamp4(_) => TimestampType::Timestamp4,
            Value::Timestamp7(_) => TimestampType::Timestamp7,
            Value::Timestamp11(_) => TimestampType::Timestamp11,
            _ => TimestampType::Other,
        };
        self.types.push(ts_type);
        Ok(())
    }

    fn resultset_end(&mut self, _eof: OkPayloadBytes) -> Result<()> {
        Ok(())
    }
}

#[test]
fn test_datetime6_with_different_inputs() {
    // Test how DATETIME(6) column handles ymd, ymd-hms, and ymd-hms-micro inputs
    let mut conn =
        zero_mysql::sync::Conn::new("mysql://test:1234@localhost:3306/test").expect("connect");

    conn.query_drop(
        "CREATE TEMPORARY TABLE test_datetime6 (
            id INT AUTO_INCREMENT PRIMARY KEY,
            dt DATETIME(6)
        )",
    )
    .expect("create table");

    // Disable strict mode to allow zero dates
    conn.query_drop("SET SESSION sql_mode = ''")
        .expect("set sql_mode");

    // Insert with different formats
    conn.query_drop("INSERT INTO test_datetime6 (dt) VALUES ('0000-00-00 00:00:00')")
        .expect("insert zero");
    conn.query_drop("INSERT INTO test_datetime6 (dt) VALUES ('2024-01-15')")
        .expect("insert ymd");
    conn.query_drop("INSERT INTO test_datetime6 (dt) VALUES ('2024-01-15 12:30:45')")
        .expect("insert ymd-hms");
    conn.query_drop("INSERT INTO test_datetime6 (dt) VALUES ('2024-01-15 12:30:45.123456')")
        .expect("insert ymd-hms-micro");

    let mut stmt = conn
        .prepare("SELECT id, dt FROM test_datetime6 ORDER BY id")
        .expect("prepare");

    let mut handler = TimestampTypeCollector::new();
    conn.exec(&mut stmt, (), &mut handler).expect("exec");

    assert_eq!(handler.types.len(), 4);

    // Row 1: zero value '0000-00-00 00:00:00' -> returns Timestamp0 (0 bytes)
    eprintln!(
        "Row 1 (zero input '0000-00-00 00:00:00'): {:?}",
        handler.types[0]
    );
    assert_eq!(
        handler.types[0],
        TimestampType::Timestamp0,
        "zero input returns Timestamp0"
    );

    // Row 2: ymd only input '2024-01-15' -> returns Timestamp4 (date only, no time)
    eprintln!("Row 2 (ymd input '2024-01-15'): {:?}", handler.types[1]);
    assert_eq!(
        handler.types[1],
        TimestampType::Timestamp4,
        "ymd input returns Timestamp4"
    );

    // Row 3: ymd-hms input '2024-01-15 12:30:45' -> returns Timestamp7 (date + time)
    eprintln!(
        "Row 3 (ymd-hms input '2024-01-15 12:30:45'): {:?}",
        handler.types[2]
    );
    assert_eq!(
        handler.types[2],
        TimestampType::Timestamp7,
        "ymd-hms input returns Timestamp7"
    );

    // Row 4: ymd-hms-micro input '2024-01-15 12:30:45.123456' -> returns Timestamp11 (date + time + microseconds)
    eprintln!(
        "Row 4 (ymd-hms-micro input '2024-01-15 12:30:45.123456'): {:?}",
        handler.types[3]
    );
    assert_eq!(
        handler.types[3],
        TimestampType::Timestamp11,
        "ymd-hms-micro input returns Timestamp11"
    );
}

#[test]
fn test_datetime6_binary_protocol_insert() {
    // Test binary protocol INSERT with prepared statements
    let mut conn =
        zero_mysql::sync::Conn::new("mysql://test:1234@localhost:3306/test").expect("connect");

    conn.query_drop(
        "CREATE TEMPORARY TABLE test_datetime6_binary (
            id INT AUTO_INCREMENT PRIMARY KEY,
            dt DATETIME(6)
        )",
    )
    .expect("create table");

    // Disable strict mode to allow zero dates
    conn.query_drop("SET SESSION sql_mode = ''")
        .expect("set sql_mode");

    // Prepare INSERT statement (binary protocol)
    let mut insert_stmt = conn
        .prepare("INSERT INTO test_datetime6_binary (dt) VALUES (?)")
        .expect("prepare insert");

    // Insert using binary protocol with string parameters
    conn.exec_drop(&mut insert_stmt, ("0000-00-00 00:00:00",))
        .expect("insert zero");
    conn.exec_drop(&mut insert_stmt, ("2024-01-15",))
        .expect("insert ymd");
    conn.exec_drop(&mut insert_stmt, ("2024-01-15 12:30:45",))
        .expect("insert ymd-hms");
    conn.exec_drop(&mut insert_stmt, ("2024-01-15 12:30:45.123456",))
        .expect("insert ymd-hms-micro");

    // SELECT using binary protocol
    let mut select_stmt = conn
        .prepare("SELECT id, dt FROM test_datetime6_binary ORDER BY id")
        .expect("prepare select");

    let mut handler = TimestampTypeCollector::new();
    conn.exec(&mut select_stmt, (), &mut handler).expect("exec");

    assert_eq!(handler.types.len(), 4);

    // Row 1: zero value -> Timestamp0
    eprintln!(
        "Binary Row 1 (zero input '0000-00-00 00:00:00'): {:?}",
        handler.types[0]
    );
    assert_eq!(
        handler.types[0],
        TimestampType::Timestamp0,
        "zero input returns Timestamp0"
    );

    // Row 2: ymd only -> Timestamp4
    eprintln!(
        "Binary Row 2 (ymd input '2024-01-15'): {:?}",
        handler.types[1]
    );
    assert_eq!(
        handler.types[1],
        TimestampType::Timestamp4,
        "ymd input returns Timestamp4"
    );

    // Row 3: ymd-hms -> Timestamp7
    eprintln!(
        "Binary Row 3 (ymd-hms input '2024-01-15 12:30:45'): {:?}",
        handler.types[2]
    );
    assert_eq!(
        handler.types[2],
        TimestampType::Timestamp7,
        "ymd-hms input returns Timestamp7"
    );

    // Row 4: ymd-hms-micro -> Timestamp11
    eprintln!(
        "Binary Row 4 (ymd-hms-micro input '2024-01-15 12:30:45.123456'): {:?}",
        handler.types[3]
    );
    assert_eq!(
        handler.types[3],
        TimestampType::Timestamp11,
        "ymd-hms-micro input returns Timestamp11"
    );
}
