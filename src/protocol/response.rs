use crate::constant::StatusFlags;
use crate::error::{Error, Result};
use crate::protocol::packet::{ErrPayloadBytes, OkPayloadBytes};
use crate::protocol::primitive::*;
use zerocopy::byteorder::little_endian::U16 as U16LE;
use zerocopy::{FromBytes, Immutable, KnownLayout};

/// The OK packet parsed from OkPayloadBytes
#[derive(Debug, Clone)]
pub struct OkPayload {
    pub affected_rows: u64,
    pub last_insert_id: u64,
    pub status_flags: StatusFlags,
    pub warnings: u16,
    // pub info: String, // SERVER_SESSION_STATE_CHANGED
    // pub session_state_info: String, // SERVER_SESSION_STATE_CHANGED
}

impl TryFrom<OkPayloadBytes<'_>> for OkPayload {
    type Error = Error;

    fn try_from(bytes: OkPayloadBytes<'_>) -> Result<Self> {
        let (header, data) = read_int_1(bytes.bytes())?;
        if header != 0x00 && header != 0xFE {
            return Err(Error::InvalidPacket);
        }

        let (affected_rows, data) = read_int_lenenc(data)?;
        let (last_insert_id, data) = read_int_lenenc(data)?;
        let (status_flags, data) = read_int_2(data)?;
        let (warnings, _data) = read_int_2(data)?;

        // TODO: Supports SERVER_SESSION_STATE_CHANGED

        Ok(OkPayload {
            affected_rows,
            last_insert_id,
            status_flags: StatusFlags::new(status_flags),
            warnings,
        })
    }
}

/// The ERR packet parsed from ErrPayloadBytes
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
        let (header, data) = read_int_1(bytes.0)?;
        debug_assert_eq!(header, 0xFF);

        let (error_code, data) = read_int_2(data)?;

        // marker is '#'
        let (_sql_state_marker, data) = read_string_fix(data, 1)?;
        let (sql_state, data) = read_string_fix(data, 5)?;

        Ok(ErrPayload {
            error_code,
            sql_state: String::from_utf8_lossy(sql_state).to_string(),
            message: String::from_utf8_lossy(data).to_string(), // string<EOF>
        })
    }
}

/// EOF packet
///
/// # Examples
///
/// ```
/// EofPacket::ref_from_bytes(&buffer[..4])`
/// ```
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
pub struct EofPacket {
    pub(crate) warnings: U16LE,
    pub(crate) status_flags: U16LE,
}

impl EofPacket {
    pub fn warnings(&self) -> u16 {
        self.warnings.get()
    }
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
        return Err(Error::InvalidPacket);
    }

    // Zero-copy cast using zerocopy
    EofPacket::ref_from_bytes(&data[..4]).map_err(|_| Error::InvalidPacket)
}
