use crate::constant::{ColumnFlags, ColumnType};
use crate::error::{Error, Result};
use crate::protocol::primitive::*;
use zerocopy::byteorder::little_endian::{U16 as U16LE, U32 as U32LE};
use zerocopy::{FromBytes, Immutable, KnownLayout};

/// Column definition bytes from MySQL protocol
///
/// This is a zero-copy wrapper around the raw bytes of a column definition packet.
#[derive(Debug, Clone, Copy)]
pub struct ColumnDefinitionBytes<'a>(pub &'a [u8]);

impl<'a> ColumnDefinitionBytes<'a> {
    /// Create a new ColumnDefinitionBytes from raw packet bytes
    pub fn new(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    /// Get a reference to the fixed-size tail of the column definition
    ///
    /// The tail is always the last 12 bytes of the column definition packet
    pub fn tail(&self) -> Result<&'a ColumnDefinitionTail> {
        if self.0.len() < 12 {
            return Err(Error::UnexpectedEof);
        }
        let tail_bytes = &self.0[self.0.len() - 12..];
        ColumnDefinitionTail::ref_from_bytes(tail_bytes).map_err(|_| Error::InvalidPacket)
    }
}

/// Column definition from MySQL protocol
#[derive(Debug, Clone)]
pub struct ColumnDefinition<'a> {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub org_table: String,
    pub name: String,
    pub org_name: String,
    pub tail: &'a ColumnDefinitionTail,
}

impl<'a> TryFrom<ColumnDefinitionBytes<'a>> for ColumnDefinition<'a> {
    type Error = Error;

    fn try_from(bytes: ColumnDefinitionBytes<'a>) -> Result<Self> {
        let mut data = bytes.0;

        // catalog (length-encoded string)
        let (catalog_bytes, rest) = read_string_lenenc(data)?;
        let catalog = String::from_utf8_lossy(catalog_bytes).to_string();
        data = rest;

        // schema (length-encoded string)
        let (schema_bytes, rest) = read_string_lenenc(data)?;
        let schema = String::from_utf8_lossy(schema_bytes).to_string();
        data = rest;

        // table (length-encoded string)
        let (table_bytes, rest) = read_string_lenenc(data)?;
        let table = String::from_utf8_lossy(table_bytes).to_string();
        data = rest;

        // org_table (length-encoded string)
        let (org_table_bytes, rest) = read_string_lenenc(data)?;
        let org_table = String::from_utf8_lossy(org_table_bytes).to_string();
        data = rest;

        // name (length-encoded string)
        let (name_bytes, rest) = read_string_lenenc(data)?;
        let name = String::from_utf8_lossy(name_bytes).to_string();
        data = rest;

        // org_name (length-encoded string)
        let (org_name_bytes, rest) = read_string_lenenc(data)?;
        let org_name = String::from_utf8_lossy(org_name_bytes).to_string();
        data = rest;

        // length of fixed-length fields (0x0c = 12) - we skip this
        let (_fixed_len, _rest) = read_int_lenenc(data)?;

        // Parse the tail using ColumnDefinitionBytes (zero-copy reference)
        let tail = bytes.tail()?;

        Ok(Self {
            catalog,
            schema,
            table,
            org_table,
            name,
            org_name,
            tail,
        })
    }
}

/// Combined column type and flags
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnTypeAndFlags {
    pub column_type: ColumnType,
    pub flags: ColumnFlags,
}

/// Fixed-size tail of Column Definition packet (12 bytes)
///
/// This structure represents the constant-size portion of the Column Definition packet
/// that follows the variable-length string fields (catalog, schema, table, org_table, name, org_name).
///
/// Structure (after length-encoded 0x0C indicator):
/// - Character set: 2 bytes (little-endian)
/// - Column length: 4 bytes (little-endian)
/// - Column type: 1 byte
/// - Flags: 2 bytes (little-endian)
/// - Decimals: 1 byte
/// - Reserved: 2 bytes (unused, little-endian)
///
/// Reference: https://mariadb.com/docs/server/reference/clientserver-protocol/4-server-response-packets/result-set-packets#column-definition-packet
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
pub struct ColumnDefinitionTail {
    /// Character set number (2 bytes LE)
    charset: U16LE,
    /// Maximum column size (4 bytes LE)
    column_length: U32LE,
    /// Column/field type (1 byte)
    column_type: u8,
    /// Field detail flags (2 bytes LE)
    flags: U16LE,
    /// Number of decimals (1 byte)
    decimals: u8,
    /// Reserved/unused (2 bytes LE)
    reserved: U16LE,
}

impl ColumnDefinitionTail {
    /// Get the character set as a native u16
    pub fn charset(&self) -> u16 {
        self.charset.get()
    }

    /// Get the column length as a native u32
    pub fn column_length(&self) -> u32 {
        self.column_length.get()
    }

    /// Get the flags as a ColumnFlags bitflags type
    ///
    /// Returns an error if the flags contain unknown bits
    pub fn flags(&self) -> Result<ColumnFlags> {
        ColumnFlags::from_bits(self.flags.get()).ok_or(Error::InvalidPacket)
    }

    /// Get the column type as a ColumnType enum
    ///
    /// Returns an error if the column type is unknown
    pub fn column_type(&self) -> Result<ColumnType> {
        ColumnType::from_u8(self.column_type).ok_or(Error::InvalidPacket)
    }

    /// Get both column type and flags together
    ///
    /// Returns an error if the column type or flags contain unknown values
    pub fn type_and_flags(&self) -> Result<ColumnTypeAndFlags> {
        Ok(ColumnTypeAndFlags {
            column_type: self.column_type()?,
            flags: self.flags()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    #[test]
    fn test_column_definition_tail_size() {
        // Verify the struct is exactly 12 bytes as per MySQL protocol
        assert_eq!(size_of::<ColumnDefinitionTail>(), 12);
    }

    #[test]
    fn test_column_definition_tail_parsing() {
        // Example data: charset=33 (utf8), length=255, type=253 (VARCHAR), flags=0, decimals=0, reserved=0
        let data: [u8; 12] = [
            0x21, 0x00, // charset = 33 (0x0021) LE
            0xFF, 0x00, 0x00, 0x00, // column_length = 255 (0x000000FF) LE
            0xFD, // column_type = 253 (VARCHAR)
            0x00, 0x00, // flags = 0 (0x0000) LE
            0x00, // decimals = 0
            0x00, 0x00, // reserved = 0 (0x0000) LE
        ];

        let tail = ColumnDefinitionTail::ref_from_bytes(&data).expect("Failed to parse");

        assert_eq!(tail.charset(), 33);
        assert_eq!(tail.column_length(), 255);
        assert_eq!(tail.decimals, 0);

        // Test conversion methods
        let flags = tail.flags().expect("Failed to parse flags");
        assert!(flags.is_empty());

        let col_type = tail.column_type().expect("Failed to parse column type");
        assert_eq!(col_type, ColumnType::MYSQL_TYPE_VAR_STRING);
    }

    #[test]
    fn test_column_definition_tail_with_flags() {
        // Example with NOT_NULL and UNSIGNED flags set
        let data: [u8; 12] = [
            0x21, 0x00, // charset = 33
            0xFF, 0x00, 0x00, 0x00, // column_length = 255
            0x01, // column_type = 1 (TINYINT)
            0x21, 0x00, // flags = 0x0021 (NOT_NULL_FLAG | UNSIGNED_FLAG) LE
            0x00, // decimals = 0
            0x00, 0x00, // reserved = 0
        ];

        let tail = ColumnDefinitionTail::ref_from_bytes(&data).expect("Failed to parse");

        let flags = tail.flags().expect("Failed to parse flags");
        assert!(flags.contains(ColumnFlags::NOT_NULL_FLAG));
        assert!(flags.contains(ColumnFlags::UNSIGNED_FLAG));
        assert!(!flags.contains(ColumnFlags::AUTO_INCREMENT_FLAG));

        let col_type = tail.column_type().expect("Failed to parse column type");
        assert_eq!(col_type, ColumnType::MYSQL_TYPE_TINY);
    }

    #[test]
    fn test_column_definition_tail_with_part_key_flag() {
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

        let tail = ColumnDefinitionTail::ref_from_bytes(&data).expect("Failed to parse");

        // Verify the fields
        assert_eq!(tail.charset(), 63);
        assert_eq!(tail.column_length(), 11);
        assert_eq!(tail.decimals, 0);

        // Verify flags can be parsed
        let flags = tail
            .flags()
            .expect("Failed to parse flags with PART_KEY_FLAG");
        assert!(flags.contains(ColumnFlags::NOT_NULL_FLAG));
        assert!(flags.contains(ColumnFlags::PRI_KEY_FLAG));
        assert!(flags.contains(ColumnFlags::AUTO_INCREMENT_FLAG));
        assert!(flags.contains(ColumnFlags::PART_KEY_FLAG));

        // Verify column type
        let col_type = tail.column_type().expect("Failed to parse column type");
        assert_eq!(col_type, ColumnType::MYSQL_TYPE_LONG);
    }

    #[test]
    fn test_column_definition_tail_invalid_column_type() {
        // Example with invalid column type
        let data: [u8; 12] = [
            0x21, 0x00, // charset = 33
            0xFF, 0x00, 0x00, 0x00, // column_length = 255
            0x50, // column_type = 0x50 (invalid, in the gap)
            0x00, 0x00, // flags = 0
            0x00, // decimals = 0
            0x00, 0x00, // reserved = 0
        ];

        let tail = ColumnDefinitionTail::ref_from_bytes(&data).expect("Failed to parse");

        // Should error on unknown column type
        let result = tail.column_type();
        assert!(result.is_err());
    }

    #[test]
    fn test_column_definition_bytes() {
        // Simulate a minimal column definition packet with just the tail
        // In reality, there would be variable-length strings before the tail
        let data: [u8; 12] = [
            0x21, 0x00, // charset = 33 (utf8)
            0xFF, 0x00, 0x00, 0x00, // column_length = 255
            0x01, // column_type = 1 (TINYINT)
            0x21, 0x00, // flags = 0x0021 (NOT_NULL_FLAG | UNSIGNED_FLAG)
            0x00, // decimals = 0
            0x00, 0x00, // reserved = 0
        ];

        let col_bytes = ColumnDefinitionBytes::new(&data);
        let tail = col_bytes.tail().expect("Failed to parse tail");

        assert_eq!(tail.charset(), 33);
        assert_eq!(tail.column_length(), 255);
        assert_eq!(tail.decimals, 0);

        let flags = tail.flags().expect("Failed to parse flags");
        assert!(flags.contains(ColumnFlags::NOT_NULL_FLAG));
        assert!(flags.contains(ColumnFlags::UNSIGNED_FLAG));

        let col_type = tail.column_type().expect("Failed to parse column type");
        assert_eq!(col_type, ColumnType::MYSQL_TYPE_TINY);
    }

    #[test]
    fn test_column_definition_bytes_too_short() {
        // Test with data that's too short
        let data: [u8; 8] = [0; 8];
        let col_bytes = ColumnDefinitionBytes::new(&data);
        let result = col_bytes.tail();
        assert!(result.is_err());
    }

    #[test]
    fn test_column_definition_try_from() {
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
        let col_bytes = ColumnDefinitionBytes::new(&packet);
        let col_def = ColumnDefinition::try_from(col_bytes).expect("Failed to parse");

        // Verify string fields
        assert_eq!(col_def.catalog, "def");
        assert_eq!(col_def.schema, "test");
        assert_eq!(col_def.table, "users");
        assert_eq!(col_def.org_table, "users");
        assert_eq!(col_def.name, "id");
        assert_eq!(col_def.org_name, "id");

        // Verify tail fields
        assert_eq!(col_def.tail.charset(), 33);
        assert_eq!(col_def.tail.column_length(), 11);
        assert_eq!(col_def.tail.decimals, 0);

        let flags = col_def.tail.flags().expect("Failed to parse flags");
        assert!(flags.contains(ColumnFlags::NOT_NULL_FLAG));
        assert!(flags.contains(ColumnFlags::PRI_KEY_FLAG));

        let col_type = col_def
            .tail
            .column_type()
            .expect("Failed to parse column type");
        assert_eq!(col_type, ColumnType::MYSQL_TYPE_LONG);
    }
}
