use crate::error::{Error, Result};
use zerocopy::FromBytes;
use zerocopy::byteorder::little_endian::{U16 as U16LE, U32 as U32LE, U64 as U64LE};

/// Read 1-byte integer
pub fn read_int_1(data: &[u8]) -> Result<(u8, &[u8])> {
    if data.is_empty() {
        return Err(Error::UnexpectedEof);
    }
    Ok((data[0], &data[1..]))
}

/// Read 2-byte little-endian integer
pub fn read_int_2(data: &[u8]) -> Result<(u16, &[u8])> {
    if data.len() < 2 {
        return Err(Error::UnexpectedEof);
    }
    let value = U16LE::ref_from_bytes(&data[..2])
        .map_err(|_| Error::InvalidPacket)?
        .get();
    Ok((value, &data[2..]))
}

/// Read 3-byte little-endian integer
pub fn read_int_3(data: &[u8]) -> Result<(u32, &[u8])> {
    if data.len() < 3 {
        return Err(Error::UnexpectedEof);
    }
    let value = u32::from_le_bytes([data[0], data[1], data[2], 0]);
    Ok((value, &data[3..]))
}

/// Read 4-byte little-endian integer
pub fn read_int_4(data: &[u8]) -> Result<(u32, &[u8])> {
    if data.len() < 4 {
        return Err(Error::UnexpectedEof);
    }
    let value = U32LE::ref_from_bytes(&data[..4])
        .map_err(|_| Error::InvalidPacket)?
        .get();
    Ok((value, &data[4..]))
}

/// Read 6-byte little-endian integer
pub fn read_int_6(data: &[u8]) -> Result<(u64, &[u8])> {
    if data.len() < 6 {
        return Err(Error::UnexpectedEof);
    }
    let value = u64::from_le_bytes([data[0], data[1], data[2], data[3], data[4], data[5], 0, 0]);
    Ok((value, &data[6..]))
}

/// Read 8-byte little-endian integer
pub fn read_int_8(data: &[u8]) -> Result<(u64, &[u8])> {
    if data.len() < 8 {
        return Err(Error::UnexpectedEof);
    }
    let value = U64LE::ref_from_bytes(&data[..8])
        .map_err(|_| Error::InvalidPacket)?
        .get();
    Ok((value, &data[8..]))
}

/// Read length-encoded integer
pub fn read_int_lenenc(data: &[u8]) -> Result<(u64, &[u8])> {
    if data.is_empty() {
        return Err(Error::UnexpectedEof);
    }

    match data[0] {
        0xFC => {
            // 2-byte integer
            let (val, rest) = read_int_2(&data[1..])?;
            Ok((val as u64, rest))
        }
        0xFD => {
            // 3-byte integer
            let (val, rest) = read_int_3(&data[1..])?;
            Ok((val as u64, rest))
        }
        0xFE => {
            // 8-byte integer
            let (val, rest) = read_int_8(&data[1..])?;
            Ok((val, rest))
        }
        val => {
            // 1-byte integer
            Ok((val as u64, &data[1..]))
        }
    }
}

/// Read fixed-length string
pub fn read_string_fix(data: &[u8], len: usize) -> Result<(&[u8], &[u8])> {
    if data.len() < len {
        return Err(Error::UnexpectedEof);
    }
    Ok((&data[..len], &data[len..]))
}

/// Read null-terminated string
/// TODO: use memchr
pub fn read_string_null(data: &[u8]) -> Result<(&[u8], &[u8])> {
    for (i, &byte) in data.iter().enumerate() {
        if byte == 0 {
            return Ok((&data[..i], &data[i + 1..]));
        }
    }
    Err(Error::UnexpectedEof)
}

/// Read length-encoded string
pub fn read_string_lenenc(data: &[u8]) -> Result<(&[u8], &[u8])> {
    let (len, rest) = read_int_lenenc(data)?;
    read_string_fix(rest, len as usize)
}

/// Read remaining data as string
pub fn read_string_eof(data: &[u8]) -> &[u8] {
    data
}

/// Write 1-byte integer
pub fn write_int_1(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

/// Write 2-byte little-endian integer
pub fn write_int_2(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

/// Write 3-byte little-endian integer
pub fn write_int_3(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes()[..3]);
}

/// Write 4-byte little-endian integer
pub fn write_int_4(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

/// Write 8-byte little-endian integer
pub fn write_int_8(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

/// Write length-encoded integer
pub fn write_int_lenenc(out: &mut Vec<u8>, value: u64) {
    if value < 251 {
        out.push(value as u8);
    } else if value < (1 << 16) {
        out.push(0xfc);
        write_int_2(out, value as u16);
    } else if value < (1 << 24) {
        out.push(0xfd);
        write_int_3(out, value as u32);
    } else {
        out.push(0xfe);
        write_int_8(out, value);
    }
}

/// Write fixed-length bytes
pub fn write_bytes_fix(out: &mut Vec<u8>, data: &[u8]) {
    out.extend_from_slice(data);
}

/// Write null-terminated string
pub fn write_string_null(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(s.as_bytes());
    out.push(0);
}

/// Write length-encoded string
pub fn write_string_lenenc(out: &mut Vec<u8>, s: &str) {
    write_int_lenenc(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

/// Write length-encoded bytes
pub fn write_bytes_lenenc(out: &mut Vec<u8>, data: &[u8]) {
    write_int_lenenc(out, data.len() as u64);
    out.extend_from_slice(data);
}
