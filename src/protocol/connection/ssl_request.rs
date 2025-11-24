use zerocopy::byteorder::little_endian::U32 as U32LE;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, IntoBytes)]
pub(crate) struct SslRequest {
    client_flag: U32LE,
    max_packet_size: U32LE,
    character_set: u8,
    filler: [u8; 23],
}
