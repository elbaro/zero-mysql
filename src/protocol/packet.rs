use bytes::{Buf, BytesMut};
use tokio_util::codec::Decoder;
use zerocopy::{FromBytes, Immutable, KnownLayout};

use crate::error::{Error, Result};

/// MySQL packet header (zero-copy)
///
/// Layout matches MySQL wire protocol:
/// - length: 3 bytes (little-endian, payload length)
/// - sequence_id: 1 byte
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
pub struct PacketHeader {
    pub length: [u8; 3],
    pub sequence_id: u8,
}

impl PacketHeader {
    /// Get payload length as usize
    pub fn length(&self) -> usize {
        u32::from_le_bytes([self.length[0], self.length[1], self.length[2], 0]) as usize
    }

    /// Read packet header from byte slice (zero-copy)
    pub fn from_bytes(data: &[u8]) -> Result<&Self> {
        if data.len() < 4 {
            return Err(Error::UnexpectedEof);
        }
        Self::ref_from_bytes(&data[..4]).map_err(|_| Error::InvalidPacket)
    }
}

/// MySQL packet decoder implementing tokio_util::Decoder
/// Handles framing but expects external code to handle 16MB packet concatenation
pub struct PacketDecoder {
    state: DecoderState,
}

enum DecoderState {
    ReadingHeader,
    ReadingPayload { length: usize, sequence_id: u8 },
}

impl PacketDecoder {
    pub fn new() -> Self {
        Self {
            state: DecoderState::ReadingHeader,
        }
    }
}

impl Default for PacketDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for PacketDecoder {
    type Item = (u8, BytesMut);
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        loop {
            match self.state {
                DecoderState::ReadingHeader => {
                    if src.len() < 4 {
                        return Ok(None);
                    }

                    // Read packet header (3 bytes length + 1 byte sequence_id)
                    let length = src.get_uint_le(3) as usize;
                    let sequence_id = src.get_u8();

                    self.state = DecoderState::ReadingPayload {
                        length,
                        sequence_id,
                    };
                }
                DecoderState::ReadingPayload {
                    length,
                    sequence_id,
                } => {
                    if src.len() < length {
                        return Ok(None);
                    }

                    // Extract payload
                    let payload = src.split_to(length);

                    // Reset state for next packet
                    self.state = DecoderState::ReadingHeader;

                    return Ok(Some((sequence_id, payload)));
                }
            }
        }
    }
}

/// Helper function to write packet header
pub fn write_packet_header(out: &mut Vec<u8>, sequence_id: u8, payload_length: usize) {
    // Write 3-byte length
    let bytes = (payload_length as u32).to_le_bytes();
    out.extend_from_slice(&bytes[..3]);
    // Write 1-byte sequence ID
    out.push(sequence_id);
}
