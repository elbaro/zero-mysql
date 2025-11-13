use crate::constant::{CapabilityFlags, StatusFlags};
use crate::error::{Error, Result};
use crate::protocol::packet::{ErrPayloadBytes, OkPayloadBytes};
use crate::protocol::primitive::*;
use zerocopy::byteorder::little_endian::U16 as U16LE;
use zerocopy::{FromBytes, Immutable, KnownLayout};

/// Packet type detection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    Ok,
    Err,
    Eof,
    ResultSet,
}

/// Detect packet type from the first byte
pub fn detect_packet_type(payload: &[u8], _capabilities: CapabilityFlags) -> Result<PacketType> {
    if payload.is_empty() {
        return Err(Error::InvalidPacket);
    }

    match payload[0] {
        0xFF => Ok(PacketType::Err),
        0xFE if payload.len() < 9 => Ok(PacketType::Eof),
        0x00 => Ok(PacketType::Ok),
        _ => Ok(PacketType::ResultSet),
    }
}

/// OK packet response
#[derive(Debug, Clone)]
pub struct OkPayload {
    pub affected_rows: u64,
    pub last_insert_id: u64,
    pub status_flags: StatusFlags,
    pub warnings: u16,
    // pub info: String,
}

impl TryFrom<OkPayloadBytes<'_>> for OkPayload {
    type Error = Error;

    fn try_from(bytes: OkPayloadBytes<'_>) -> Result<Self> {
        let payload = bytes.bytes();
        let (header, data) = read_int_1(payload)?;
        if header != 0x00 && header != 0xFE {
            return Err(Error::InvalidPacket);
        }

        let (affected_rows, rest) = read_int_lenenc(data)?;
        let (last_insert_id, rest) = read_int_lenenc(rest)?;
        let (status_flags, rest) = read_int_2(rest)?;
        let (warnings, _rest) = read_int_2(rest)?;

        // data = rest;

        // // Info string is the rest of the packet (can be empty)
        // let info = if !data.is_empty() {
        //     String::from_utf8_lossy(data).to_string()
        // } else {
        //     String::new()
        // };

        Ok(OkPayload {
            affected_rows,
            last_insert_id,
            status_flags: StatusFlags::new(status_flags),
            warnings,
            // info,
        })
    }
}

/// ERR packet response
#[derive(Debug, Clone, thiserror::Error)]
#[error("ERROR {} ({}): {}", self.error_code, self.sql_state, self.message)]
pub struct ErrPayload {
    pub error_code: u16,
    pub sql_state: String,
    pub message: String,
}

impl TryFrom<ErrPayloadBytes<'_>> for ErrPayload {
    type Error = Error;

    fn try_from(bytes: ErrPayloadBytes<'_>) -> Result<Self> {
        let payload = bytes.0;
        let (header, mut data) = read_int_1(payload)?;
        if header != 0xFF {
            return Err(Error::InvalidPacket);
        }

        let (error_code, rest) = read_int_2(data)?;
        data = rest;

        // Check for SQL state marker '#'
        let (sql_state, rest) = if !data.is_empty() && data[0] == b'#' {
            let (state_bytes, rest) = read_string_fix(&data[1..], 5)?;
            (String::from_utf8_lossy(state_bytes).to_string(), rest)
        } else {
            (String::new(), data)
        };

        // Rest is error message
        let message = String::from_utf8_lossy(rest).to_string();

        Ok(ErrPayload {
            error_code,
            sql_state,
            message,
        })
    }
}

/// EOF packet response (zero-copy)
///
/// Layout matches MySQL wire protocol after header byte 0xFE:
/// - warnings: 2 bytes (little-endian)
/// - status_flags: 2 bytes (little-endian)
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
pub struct EofPacket {
    pub warnings: U16LE,
    pub status_flags: U16LE,
}

impl EofPacket {
    /// Get status flags as StatusFlags wrapper
    pub fn status_flags(&self) -> StatusFlags {
        StatusFlags::new(self.status_flags.get())
    }
}

/// Read EOF packet (header byte 0xFE, length < 9) - zero-copy
pub fn read_eof_packet(payload: &[u8]) -> Result<&EofPacket> {
    let (header, data) = read_int_1(payload)?;
    if header != 0xFE {
        return Err(Error::InvalidPacket);
    }

    // EofPacket is 4 bytes (2 + 2)
    if data.len() < 4 {
        return Err(Error::UnexpectedEof);
    }

    // Zero-copy cast using zerocopy
    EofPacket::ref_from_bytes(&data[..4]).map_err(|_| Error::InvalidPacket)
}
