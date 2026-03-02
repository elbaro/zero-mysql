use std::mem::{align_of, size_of};

use zerocopy::FromBytes;

use crate::constant::{ColumnFlags, ColumnType};
use crate::protocol::command::{ColumnDefinition, ColumnDefinitionBytes, ColumnDefinitionTail};
use crate::test_macros::{check, check_eq, check_err};

#[test]
fn column_definition_tail_size() {
    // Verify the struct is exactly 12 bytes as per MySQL protocol
    assert_eq!(size_of::<ColumnDefinitionTail>(), 12);
}

#[test]
fn column_definition_tail_has_alignment_of_1() {
    assert_eq!(align_of::<ColumnDefinitionTail>(), 1);
}

#[test]
fn column_definition_tail_parsing() -> crate::error::Result<()> {
    // Example data: charset=33 (utf8), length=255, type=253 (VARCHAR), flags=0, decimals=0, reserved=0
    let data: [u8; 12] = [
        0x21, 0x00, // charset = 33 (0x0021) LE
        0xFF, 0x00, 0x00, 0x00, // column_length = 255 (0x000000FF) LE
        0xFD, // column_type = 253 (VARCHAR)
        0x00, 0x00, // flags = 0 (0x0000) LE
        0x00, // decimals = 0
        0x00, 0x00, // reserved = 0 (0x0000) LE
    ];

    let tail = ColumnDefinitionTail::ref_from_bytes(&data)?;

    check_eq!(tail.charset(), 33);
    check_eq!(tail.column_length(), 255);

    let flags = tail.flags()?;
    check!(flags.is_empty());

    let col_type = tail.column_type()?;
    check_eq!(col_type, ColumnType::MYSQL_TYPE_VAR_STRING);
    Ok(())
}

#[test]
fn column_definition_tail_with_flags() -> crate::error::Result<()> {
    // Example with NOT_NULL and UNSIGNED flags set
    let data: [u8; 12] = [
        0x21, 0x00, // charset = 33
        0xFF, 0x00, 0x00, 0x00, // column_length = 255
        0x01, // column_type = 1 (TINYINT)
        0x21, 0x00, // flags = 0x0021 (NOT_NULL_FLAG | UNSIGNED_FLAG) LE
        0x00, // decimals = 0
        0x00, 0x00, // reserved = 0
    ];

    let tail = ColumnDefinitionTail::ref_from_bytes(&data)?;

    let flags = tail.flags()?;
    check!(flags.contains(ColumnFlags::NOT_NULL_FLAG));
    check!(flags.contains(ColumnFlags::UNSIGNED_FLAG));
    check!(!flags.contains(ColumnFlags::AUTO_INCREMENT_FLAG));

    let col_type = tail.column_type()?;
    check_eq!(col_type, ColumnType::MYSQL_TYPE_TINY);
    Ok(())
}

#[test]
fn column_definition_tail_with_part_key_flag() -> crate::error::Result<()> {
    // Test with PART_KEY_FLAG (0x4000) - from actual MySQL response
    // This reproduces the bug: flags = 0x4203 (NOT_NULL | PRI_KEY | AUTO_INCREMENT | PART_KEY)
    let data: [u8; 12] = [
        0x3f, 0x00, // charset = 63 (binary)
        0x0B, 0x00, 0x00, 0x00, // column_length = 11
        0x03, // column_type = 3 (LONG/INT)
        0x03, 0x42, // flags = 0x4203 (NOT_NULL | PRI_KEY | AUTO_INCREMENT | PART_KEY) LE
        0x00, // decimals = 0
        0x00, 0x00, // reserved = 0
    ];

    let tail = ColumnDefinitionTail::ref_from_bytes(&data)?;

    check_eq!(tail.charset(), 63);
    check_eq!(tail.column_length(), 11);

    let flags = tail.flags()?;
    check!(flags.contains(ColumnFlags::NOT_NULL_FLAG));
    check!(flags.contains(ColumnFlags::PRI_KEY_FLAG));
    check!(flags.contains(ColumnFlags::AUTO_INCREMENT_FLAG));
    check!(flags.contains(ColumnFlags::PART_KEY_FLAG));

    let col_type = tail.column_type()?;
    check_eq!(col_type, ColumnType::MYSQL_TYPE_LONG);
    Ok(())
}

#[test]
fn column_definition_tail_invalid_column_type() -> crate::error::Result<()> {
    // Example with invalid column type
    let data: [u8; 12] = [
        0x21, 0x00, // charset = 33
        0xFF, 0x00, 0x00, 0x00, // column_length = 255
        0x50, // column_type = 0x50 (invalid, in the gap)
        0x00, 0x00, // flags = 0
        0x00, // decimals = 0
        0x00, 0x00, // reserved = 0
    ];

    let tail = ColumnDefinitionTail::ref_from_bytes(&data)?;

    // Should error on unknown column type
    let result = tail.column_type();
    let _err = check_err!(result);
    Ok(())
}

#[test]
fn column_definition_bytes() -> crate::error::Result<()> {
    // Simulate a minimal column definition packet with just the tail
    // In reality, there would be variable-length strings before the tail
    let data: &[u8; 12] = &[
        0x21, 0x00, // charset = 33 (utf8)
        0xFF, 0x00, 0x00, 0x00, // column_length = 255
        0x01, // column_type = 1 (TINYINT)
        0x21, 0x00, // flags = 0x0021 (NOT_NULL_FLAG | UNSIGNED_FLAG)
        0x00, // decimals = 0
        0x00, 0x00, // reserved = 0
    ];

    let col_bytes = ColumnDefinitionBytes(data);
    let tail = col_bytes.tail()?;

    check_eq!(tail.charset(), 33);
    check_eq!(tail.column_length(), 255);

    let flags = tail.flags()?;
    check!(flags.contains(ColumnFlags::NOT_NULL_FLAG));
    check!(flags.contains(ColumnFlags::UNSIGNED_FLAG));

    let col_type = tail.column_type()?;
    check_eq!(col_type, ColumnType::MYSQL_TYPE_TINY);
    Ok(())
}

#[test]
fn column_definition_bytes_too_short() -> crate::error::Result<()> {
    // Test with data that's too short
    let data: &[u8; 8] = &[0; 8];
    let col_bytes = ColumnDefinitionBytes(data);
    let result = col_bytes.tail();
    let _err = check_err!(result);
    Ok(())
}

#[test]
fn column_definition_try_from() -> crate::error::Result<()> {
    // Build a complete column definition packet
    let mut packet = Vec::new();

    // catalog (length-encoded string) - "def"
    packet.push(0x03);
    packet.extend_from_slice(b"def");

    // schema (length-encoded string) - "test"
    packet.push(0x04);
    packet.extend_from_slice(b"test");

    // table (length-encoded string) - "users"
    packet.push(0x05);
    packet.extend_from_slice(b"users");

    // org_table (length-encoded string) - "users"
    packet.push(0x05);
    packet.extend_from_slice(b"users");

    // name (length-encoded string) - "id"
    packet.push(0x02);
    packet.extend_from_slice(b"id");

    // org_name (length-encoded string) - "id"
    packet.push(0x02);
    packet.extend_from_slice(b"id");

    // length of fixed fields (0x0c = 12)
    packet.push(0x0c);

    // Fixed tail (12 bytes)
    packet.extend_from_slice(&[
        0x21, 0x00, // charset = 33 (utf8)
        0x0B, 0x00, 0x00, 0x00, // column_length = 11
        0x03, // column_type = 3 (LONG/INT)
        0x03, 0x00, // flags = 0x0003 (NOT_NULL_FLAG | PRI_KEY_FLAG)
        0x00, // decimals = 0
        0x00, 0x00, // reserved = 0
    ]);

    // Parse using TryFrom
    let col_bytes = ColumnDefinitionBytes(&packet);
    let col_def = ColumnDefinition::try_from(col_bytes)?;

    // Verify string fields
    check_eq!(col_def.schema, b"test");
    check_eq!(col_def.table_alias, b"users");
    check_eq!(col_def.table_original, b"users");
    check_eq!(col_def.name_alias, b"id");
    check_eq!(col_def.name_original, b"id");

    // Verify tail fields
    check_eq!(col_def.tail.charset(), 33);
    check_eq!(col_def.tail.column_length(), 11);

    let flags = col_def.tail.flags()?;
    check!(flags.contains(ColumnFlags::NOT_NULL_FLAG));
    check!(flags.contains(ColumnFlags::PRI_KEY_FLAG));

    let col_type = col_def.tail.column_type()?;
    check_eq!(col_type, ColumnType::MYSQL_TYPE_LONG);
    Ok(())
}
