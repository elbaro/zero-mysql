use std::io::Read;
use std::mem::MaybeUninit;

/// Hints to the compiler that this code path is cold (unlikely to be executed).
///
/// This can help the compiler make better optimization decisions by moving
/// cold code out of the hot path.
#[cold]
#[inline(always)]
pub const fn cold_path() {}

/// Hints to the compiler that the condition is likely to be true.
#[allow(dead_code)]
#[inline(always)]
pub const fn likely(b: bool) -> bool {
    if !b {
        cold_path();
    }
    b
}

/// Hints to the compiler that the condition is unlikely to be true.
#[inline(always)]
pub const fn unlikely(b: bool) -> bool {
    if b {
        cold_path();
    }
    b
}

/// Reads exactly `buf.len()` bytes into uninitialized memory.
///
/// This avoids the cost of zeroing memory before reading.
///
/// # Assumption
/// This function assumes the `Read` implementation only writes to the buffer
/// and never reads from it. This is not guaranteed by the `Read` trait contract,
/// but is true for:
/// - std types (TcpStream, BufReader, File)
/// - native-tls/OpenSSL: SSL_read writes decrypted data into buf, never reads from it
pub fn read_uninit_exact<R: Read>(
    reader: &mut R,
    buf: &mut [MaybeUninit<u8>],
) -> std::io::Result<()> {
    // SAFETY: MaybeUninit<u8> has the same layout as u8.
    // We rely on the assumption that `Read::read_exact` only writes to the buffer.
    let buf_ptr = buf.as_mut_ptr() as *mut u8;
    let buf_slice = unsafe { std::slice::from_raw_parts_mut(buf_ptr, buf.len()) };

    reader.read_exact(buf_slice)
}
