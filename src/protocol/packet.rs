use crate::error::{Error, Result};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, IntoBytes)]
pub struct PacketHeader {
    pub length: [u8; 3],
    pub sequence_id: u8,
}

impl PacketHeader {
    pub fn encode(length: usize, sequence_id: u8) -> Self {
        let len = u32::to_le_bytes(length as u32);
        Self {
            length: [len[0], len[1], len[2]],
            sequence_id,
        }
    }

    pub fn length(&self) -> usize {
        u32::from_le_bytes([self.length[0], self.length[1], self.length[2], 0]) as usize
    }

    pub fn from_bytes(data: &[u8]) -> Result<&Self> {
        if data.len() < 4 {
            return Err(Error::InvalidPacket);
        }
        Self::ref_from_bytes(&data[..4]).map_err(|_| Error::InvalidPacket)
    }
}
