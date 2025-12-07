use crate::constant::{ColumnFlags, ColumnType};
use crate::error::{Error, Result, eyre};
use crate::protocol::primitive::*;
use zerocopy::byteorder::little_endian::{U16 as U16LE, U32 as U32LE};
use zerocopy::{FromBytes, Immutable, KnownLayout};

/// Represents a payload part of a column definition packet
#[derive(Debug, Clone, Copy)]
pub struct ColumnDefinitionBytes<'a>(pub &'a [u8]);

impl<'a> ColumnDefinitionBytes<'a> {
    /// Get a reference to the fixed-size tail of the column definition
    ///
    /// The tail is always the last 12 bytes of the column definition packet
    pub fn tail(&self) -> Result<&'a ColumnDefinitionTail> {
        if self.0.len() < 12 {
            return Err(Error::LibraryBug(eyre!(
                "column definition too short: {} < 12",
                self.0.len()
            )));
        }
        let tail_bytes = &self.0[self.0.len() - 12..];
        Ok(ColumnDefinitionTail::ref_from_bytes(tail_bytes)?)
    }
}

/// The column definition parsed from `ColumnDefinitionBytes`
#[derive(Debug, Clone)]
pub struct ColumnDefinition<'a> {
    pub schema: &'a [u8],
    pub table_alias: &'a [u8],
    pub table_original: &'a [u8],
    pub name_alias: &'a [u8],
    pub name_original: &'a [u8],
    pub tail: &'a ColumnDefinitionTail,
}

impl<'a> TryFrom<ColumnDefinitionBytes<'a>> for ColumnDefinition<'a> {
    type Error = Error;

    fn try_from(bytes: ColumnDefinitionBytes<'a>) -> Result<Self> {
        let data = bytes.0;

        // ─── Variable Length String Fields ───────────────────────────
        let (_catalog, data) = read_string_lenenc(data)?;
        let (schema, data) = read_string_lenenc(data)?;
        let (table_alias, data) = read_string_lenenc(data)?;
        let (table_original, data) = read_string_lenenc(data)?;
        let (name_alias, data) = read_string_lenenc(data)?;
        let (name_original, data) = read_string_lenenc(data)?;

        // ─── Columndefinitiontail ────────────────────────────────────
        // length is always 0x0c
        let (_length, data) = read_int_lenenc(data)?;
        let tail = ColumnDefinitionTail::ref_from_bytes(data)?;
        Ok(Self {
            // catalog,
            schema,
            table_alias,
            table_original,
            name_alias,
            name_original,
            tail,
        })
    }
}

/// Fixed-size tail of Column Definition packet (12 bytes)
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
pub struct ColumnDefinitionTail {
    charset: U16LE,
    column_length: U32LE,
    column_type: u8,
    flags: U16LE,
    decimals: u8,
    reserved: U16LE,
}

impl ColumnDefinitionTail {
    pub fn charset(&self) -> u16 {
        self.charset.get()
    }

    pub fn column_length(&self) -> u32 {
        self.column_length.get()
    }

    pub fn column_type(&self) -> Result<ColumnType> {
        ColumnType::from_u8(self.column_type).ok_or_else(|| {
            Error::LibraryBug(eyre!("unknown column type: 0x{:02X}", self.column_type))
        })
    }

    pub fn flags(&self) -> Result<ColumnFlags> {
        ColumnFlags::from_bits(self.flags.get()).ok_or_else(|| {
            Error::LibraryBug(eyre!("invalid column flags: 0x{:04X}", self.flags.get()))
        })
    }
}

pub struct ColumnDefinitions {
    _packets: Vec<u8>, // concatenation of packets (length(usize, native endian) + payload)
    definitions: Vec<ColumnDefinition<'static>>,
}

impl ColumnDefinitions {
    pub fn new(num_columns: usize, packets: Vec<u8>) -> Result<Self> {
        let definitions = {
            let mut buf = packets.as_slice();
            let mut definitions = Vec::with_capacity(num_columns);
            for _ in 0..num_columns {
                let len = u32::from_ne_bytes(buf[..4].try_into().unwrap()) as usize;
                definitions.push(ColumnDefinition::try_from(ColumnDefinitionBytes(
                    &buf[4..4 + len],
                ))?);
                buf = &buf[4 + len..]; // Advance past the length prefix and payload
            }

            // Safety: borrowed data is valid for 'static because Self holds packets
            unsafe {
                std::mem::transmute::<Vec<ColumnDefinition<'_>>, Vec<ColumnDefinition<'static>>>(
                    definitions,
                )
            }
        };

        Ok(Self {
            _packets: packets,
            definitions,
        })
    }

    pub fn definitions<'a>(&'a self) -> &'a [ColumnDefinition<'a>] {
        self.definitions.as_slice()
    }
}
