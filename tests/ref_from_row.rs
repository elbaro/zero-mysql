//! Tests for RefFromRow zero-copy row decoding.

use zero_mysql::ref_row::{FixedWireSize, I16LE, I32LE, I64LE, U16LE, U32LE, U64LE};
use zerocopy::{FromBytes, Immutable, KnownLayout};

include!("common/check.rs");
include!("common/check_eq.rs");
include!("common/check_err.rs");

/// Test that FixedWireSize is implemented for all expected types.
#[test]
fn fixed_wire_size_primitives() -> Result<(), Box<dyn std::error::Error>> {
    check_eq!(<i8 as FixedWireSize>::WIRE_SIZE, 1);
    check_eq!(<u8 as FixedWireSize>::WIRE_SIZE, 1);
    check_eq!(<I16LE as FixedWireSize>::WIRE_SIZE, 2);
    check_eq!(<U16LE as FixedWireSize>::WIRE_SIZE, 2);
    check_eq!(<I32LE as FixedWireSize>::WIRE_SIZE, 4);
    check_eq!(<U32LE as FixedWireSize>::WIRE_SIZE, 4);
    check_eq!(<I64LE as FixedWireSize>::WIRE_SIZE, 8);
    check_eq!(<U64LE as FixedWireSize>::WIRE_SIZE, 8);
    Ok(())
}

/// Test zerocopy parsing of little-endian integers.
#[test]
fn little_endian_parsing() -> Result<(), Box<dyn std::error::Error>> {
    // i32 value 0x12345678 in little-endian
    let data1: [u8; 4] = [0x78, 0x56, 0x34, 0x12];
    let value1: &I32LE = FromBytes::ref_from_bytes(&data1).unwrap();
    check_eq!(value1.get(), 0x12345678);

    // i64 value
    let data2: [u8; 8] = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
    let value2: &I64LE = FromBytes::ref_from_bytes(&data2).unwrap();
    check_eq!(value2.get(), 0x0807060504030201);
    Ok(())
}

/// Test a packed struct with multiple fields.
#[test]
fn packed_struct() -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, FromBytes, KnownLayout, Immutable)]
    #[repr(C, packed)]
    struct TestRow {
        a: I32LE,
        b: I64LE,
        c: I16LE,
    }

    // Total size: 4 + 8 + 2 = 14 bytes
    check_eq!(std::mem::size_of::<TestRow>(), 14);

    // Create test data
    let mut data = [0u8; 14];
    data[0..4].copy_from_slice(&42_i32.to_le_bytes());
    data[4..12].copy_from_slice(&12345_i64.to_le_bytes());
    data[12..14].copy_from_slice(&(-100_i16).to_le_bytes());

    let row: &TestRow = FromBytes::ref_from_bytes(&data).unwrap();
    check_eq!(row.a.get(), 42);
    check_eq!(row.b.get(), 12345);
    check_eq!(row.c.get(), -100);
    Ok(())
}

/// Test that packed structs have correct alignment (1 byte).
#[test]
fn packed_alignment() -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, FromBytes, KnownLayout, Immutable)]
    #[repr(C, packed)]
    struct MixedRow {
        a: u8,
        b: I64LE,
        c: u8,
    }

    // Without packed, this would be 24 bytes (1 + 7 padding + 8 + 1 + 7 padding)
    // With packed, it's 1 + 8 + 1 = 10 bytes
    check_eq!(std::mem::size_of::<MixedRow>(), 10);
    check_eq!(std::mem::align_of::<MixedRow>(), 1);
    Ok(())
}

/// Test unsigned integers.
#[test]
fn unsigned_integers() -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, FromBytes, KnownLayout, Immutable)]
    #[repr(C, packed)]
    struct UnsignedRow {
        a: U16LE,
        b: U32LE,
        c: U64LE,
    }

    let mut data = [0u8; 14];
    data[0..2].copy_from_slice(&0xFFFF_u16.to_le_bytes());
    data[2..6].copy_from_slice(&0xDEADBEEF_u32.to_le_bytes());
    data[6..14].copy_from_slice(&0xCAFEBABEDEADC0DE_u64.to_le_bytes());

    let row: &UnsignedRow = FromBytes::ref_from_bytes(&data).unwrap();
    check_eq!(row.a.get(), 0xFFFF);
    check_eq!(row.b.get(), 0xDEADBEEF);
    check_eq!(row.c.get(), 0xCAFEBABEDEADC0DE);
    Ok(())
}

/// Test single-byte types (endian-agnostic).
#[test]
fn single_byte_types() -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, FromBytes, KnownLayout, Immutable)]
    #[repr(C, packed)]
    struct ByteRow {
        signed: i8,
        unsigned: u8,
    }

    let data: [u8; 2] = [0xFF, 0xFF]; // -1 for i8, 255 for u8

    let row: &ByteRow = FromBytes::ref_from_bytes(&data).unwrap();
    check_eq!(row.signed, -1);
    check_eq!(row.unsigned, 255);
    Ok(())
}

/// Test that zerocopy correctly rejects misaligned/wrong-sized data.
#[test]
fn size_validation() -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, FromBytes, KnownLayout, Immutable)]
    #[repr(C, packed)]
    struct TestRow {
        a: I32LE,
        b: I64LE,
    }

    // Too small
    let data1 = [0u8; 11];
    let _err = check_err!(I32LE::ref_from_bytes(&data1[..3]));

    // Correct size
    let data2 = [0u8; 12];
    <TestRow as FromBytes>::ref_from_bytes(&data2).unwrap();

    // Too large is OK - zerocopy allows prefix
    let data3 = [0u8; 20];
    <TestRow as FromBytes>::ref_from_bytes(&data3[..12]).unwrap();
    Ok(())
}

// ─── #[derive(RefFromRow)] ───────────────────────────────────────────────────

use zero_mysql::error::Error;
use zero_mysql::sync::Conn;
use zero_mysql_derive::RefFromRow;

#[derive(FromBytes, KnownLayout, Immutable, RefFromRow)]
#[repr(C, packed)]
struct Stats {
    id: I64LE,
    count: I32LE,
    flag: i8,
}

/// Connects and fills a temporary `ref_rows` table with three rows.
fn conn_with_rows() -> Result<Conn, Error> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://test:1234@localhost:3306/test?tls_mode=disable".to_string());
    let mut conn = Conn::new(url.as_str())?;
    conn.query_drop(
        "CREATE TEMPORARY TABLE ref_rows (
            id BIGINT NOT NULL,
            count INT NOT NULL,
            maybe INT NULL,
            small SMALLINT NOT NULL,
            flag TINYINT NOT NULL,
            name VARCHAR(10) NOT NULL
        )",
    )?;
    conn.query_drop(
        "INSERT INTO ref_rows VALUES
            (1, -10, NULL, 1, 65, 'abcd'),
            (2, -20, NULL, 2, 66, 'abcd'),
            (3, -30, NULL, 3, 67, 'abcd')",
    )?;
    Ok(conn)
}

fn exec_stats(conn: &mut Conn, sql: &str) -> Result<Vec<(i64, i32, i8)>, Error> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = Vec::new();
    conn.exec_foreach_ref::<Stats, _, _>(&mut stmt, (), |row| {
        rows.push((row.id.get(), row.count.get(), row.flag));
        Ok(())
    })?;
    Ok(rows)
}

#[test]
fn exec_foreach_ref_decodes_rows() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = conn_with_rows()?;
    let rows = exec_stats(
        &mut conn,
        "SELECT id, count, flag FROM ref_rows ORDER BY id",
    )?;
    check_eq!(rows, vec![(1, -10, 65), (2, -20, 66), (3, -30, 67)]);
    Ok(())
}

#[test]
fn exec_foreach_ref_rejects_null() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = conn_with_rows()?;
    let err = check_err!(exec_stats(
        &mut conn,
        "SELECT id, maybe, flag FROM ref_rows"
    ));
    check!(err.to_string().contains("column 1 is NULL"));
    Ok(())
}

#[test]
fn exec_foreach_ref_rejects_wrong_integer_size() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = conn_with_rows()?;
    let err = check_err!(exec_stats(
        &mut conn,
        "SELECT id, small, flag FROM ref_rows"
    ));
    check!(
        err.to_string()
            .contains("column 1 has type MYSQL_TYPE_SHORT, expected a 4-byte integer")
    );
    Ok(())
}

#[test]
fn exec_foreach_ref_rejects_non_integer() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = conn_with_rows()?;
    let err = check_err!(exec_stats(&mut conn, "SELECT id, name, flag FROM ref_rows"));
    check!(
        err.to_string()
            .contains("column 1 has type MYSQL_TYPE_VAR_STRING")
    );
    Ok(())
}

#[test]
fn exec_foreach_ref_rejects_wrong_column_count() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = conn_with_rows()?;
    let err = check_err!(exec_stats(&mut conn, "SELECT id, count FROM ref_rows"));
    check!(err.to_string().contains("expects 3 columns, got 2"));
    Ok(())
}
