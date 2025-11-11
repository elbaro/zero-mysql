use crate::constant::CapabilityFlags;
use crate::error::{Error, Result};
use crate::protocol::packet::ErrPayloadBytes;
use crate::protocol::primitive::*;
use crate::protocol::response::ErrPayload;

// ============================================================================
// Initial Handshake Packet (Server -> Client)
// ============================================================================

/// Initial handshake packet from server (Protocol::HandshakeV10)
///
/// This is the first packet sent by MySQL server after TCP connection.
/// Server sends its capabilities, auth plugin name, and challenge data.
///
/// Packet format:
/// ```text
/// 1   [0a] protocol version (always 10)
/// n   server version (null-terminated string)
/// 4   connection id
/// 8   auth-plugin-data-part-1 (first 8 bytes of challenge)
/// 1   [00] filler
/// 2   capability flags (lower 2 bytes)
/// 1   character set
/// 2   status flags
/// 2   capability flags (upper 2 bytes)
/// 1   auth plugin data length
/// 10  reserved (all 0x00)
/// n   auth-plugin-data-part-2 (remaining challenge bytes)
/// n   auth plugin name (null-terminated)
/// ```
#[derive(Debug, Clone)]
pub struct InitialHandshake<'a> {
    pub protocol_version: u8,
    pub server_version: String,
    pub connection_id: u32,
    pub auth_plugin_data: Vec<u8>,
    pub capability_flags: CapabilityFlags,
    pub charset: u8,
    pub status_flags: u16,
    pub auth_plugin_name: &'a [u8],
}

/// Read initial handshake packet from server
pub fn read_initial_handshake(payload: &[u8]) -> Result<InitialHandshake<'_>> {
    let (protocol_version, mut data) = read_int_1(payload)?;

    // If first byte from server is 0xFF, Packet is an ERR_Packet, socket has to be closed.
    if protocol_version == 0xFF {
        let err_bytes = ErrPayloadBytes::from_payload(payload)
            .ok_or(Error::InvalidPacket)?;
        let err = ErrPayload::try_from(err_bytes)?;
        return Err(Error::ServerError {
            error_code: err.error_code,
            sql_state: err.sql_state,
            message: err.message,
        });
    }

    let (server_version_bytes, rest) = read_string_null(data)?;
    let server_version = String::from_utf8_lossy(server_version_bytes).to_string();
    data = rest;

    let (connection_id, rest) = read_int_4(data)?;
    data = rest;

    // auth-plugin-data-part-1 (8 bytes)
    let (auth_data_1, rest) = read_string_fix(data, 8)?;
    data = rest;

    // filler (1 byte)
    let (_filler, rest) = read_int_1(data)?;
    data = rest;

    // capability flags (lower 2 bytes)
    let (cap_lower, rest) = read_int_2(data)?;
    data = rest;

    // charset (1 byte)
    let (charset, rest) = read_int_1(data)?;
    data = rest;

    // status flags (2 bytes)
    let (status_flags, rest) = read_int_2(data)?;
    data = rest;

    // capability flags (upper 2 bytes)
    let (cap_upper, rest) = read_int_2(data)?;
    data = rest;

    let cap_bits = ((cap_upper as u32) << 16) | (cap_lower as u32);
    let capability_flags = CapabilityFlags::from_bits(cap_bits).ok_or(Error::InvalidPacket)?;

    // auth plugin data length (1 byte)
    let (auth_data_len, rest) = read_int_1(data)?;
    data = rest;

    // reserved (10 bytes)
    let (_reserved, rest) = read_string_fix(data, 10)?;
    data = rest;

    // auth-plugin-data-part-2
    let auth_data_2_len = (auth_data_len as usize).saturating_sub(9).max(12);
    let (auth_data_2, rest) = read_string_fix(data, auth_data_2_len)?;
    data = rest;
    let (_reserved, rest) = read_int_1(data)?;
    data = rest;

    // Combine auth plugin data
    let mut auth_plugin_data = Vec::new();
    auth_plugin_data.extend_from_slice(auth_data_1);
    auth_plugin_data.extend_from_slice(auth_data_2);

    // auth plugin name (null-terminated)
    let (auth_plugin_name, rest) = read_string_null(data)?;

    if !rest.is_empty() {
        return Err(Error::InvalidPacket);
    }

    Ok(InitialHandshake {
        protocol_version,
        server_version,
        connection_id,
        auth_plugin_data,
        capability_flags,
        charset,
        status_flags,
        auth_plugin_name,
    })
}

// ============================================================================
// Handshake Response Packet (Client -> Server)
// ============================================================================

/// Handshake response packet sent by client (HandshakeResponse41)
///
/// This is sent in response to the initial handshake from server.
/// Contains client capabilities, username, and authentication response.
///
/// Packet format (without SSL):
/// ```text
/// 4   capability flags
/// 4   max packet size
/// 1   character set
/// 23  reserved (all 0x00)
/// n   username (null-terminated string)
/// n   auth response length + data (length-encoded if CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
/// n   database name (null-terminated, if CLIENT_CONNECT_WITH_DB)
/// n   auth plugin name (null-terminated, if CLIENT_PLUGIN_AUTH)
/// ```
#[derive(Debug, Clone)]
pub struct HandshakeResponse41<'a> {
    pub capability_flags: CapabilityFlags,
    pub max_packet_size: u32,
    pub charset: u8,
    pub username: &'a str,
    pub auth_response: &'a [u8],
    pub database: Option<&'a str>,
    pub auth_plugin_name: Option<&'a str>,
}

/// Write handshake response packet (HandshakeResponse41)
///
/// This writes the client's response to the initial handshake.
/// The auth_response should be pre-computed using the appropriate auth plugin.
pub fn write_handshake_response(out: &mut Vec<u8>, response: &HandshakeResponse41) {
    // capability flags (4 bytes)
    write_int_4(out, response.capability_flags.bits());

    // max packet size (4 bytes)
    write_int_4(out, response.max_packet_size);

    // charset (1 byte)
    write_int_1(out, response.charset);

    // reserved (23 bytes of 0x00)
    out.extend_from_slice(&[0u8; 23]);

    // username (null-terminated)
    write_string_null(out, response.username);

    // auth response - if no password, '\0'
    if response
        .capability_flags
        .contains(CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
    {
        // length-encoded auth response (modern protocol)
        // TODO: NULL avlue in auth_response should be encoded as ? (mariadb docs)
        write_bytes_lenenc(out, response.auth_response);
    } else {
        // 1-byte length + data (older protocol)
        write_int_1(out, response.auth_response.len() as u8);
        out.extend_from_slice(response.auth_response);
    }

    // database name (null-terminated, if CLIENT_CONNECT_WITH_DB)
    if let Some(db) = response.database {
        write_string_null(out, db);
    }

    // auth plugin name (null-terminated, if CLIENT_PLUGIN_AUTH)
    if let Some(plugin) = response.auth_plugin_name {
        if response
            .capability_flags
            .contains(CapabilityFlags::CLIENT_PLUGIN_AUTH)
        {
            write_string_null(out, plugin);
        }
    }

    // TODO: CLIENT_CONNECT_ATTRS

    // TODO: CLIENT_ZSTD_COMPRESSION_ALGORITHM
    // if response.capability_flags.contains(CapabilityFlags::CLIENT_ZSTD_COMPRESSION_ALGORITHM) {
    //     write_int_1(out, compression_level);
    // }
}

// ============================================================================
// Auth Switch Request Packet (Server -> Client)
// ============================================================================

/// Auth switch request from server
///
/// Server sends this when it wants to use a different authentication method
/// than was specified in the initial handshake.
///
/// Packet format:
/// ```text
/// 1   [fe] status (0xFE for auth switch)
/// n   plugin name (null-terminated)
/// n   plugin data (challenge data for the new plugin)
/// ```
#[derive(Debug, Clone)]
pub struct AuthSwitchRequest<'a> {
    pub plugin_name: &'a [u8],
    pub plugin_data: &'a [u8],
}

/// Read auth switch request (0xFE with length >= 9)
pub fn read_auth_switch_request(payload: &[u8]) -> Result<AuthSwitchRequest<'_>> {
    let (header, mut data) = read_int_1(payload)?;
    if header != 0xFE {
        return Err(Error::InvalidPacket);
    }

    let (plugin_name, rest) = read_string_null(data)?;
    data = rest;

    if let Some(0) = data.last() {
        Ok(AuthSwitchRequest {
            plugin_name,
            plugin_data: &data[..data.len() - 1],
        })
    } else {
        Err(Error::InvalidPacket)
    }
}

/// Write auth switch response
///
/// Client sends the authentication data computed using the requested plugin.
pub fn write_auth_switch_response(out: &mut Vec<u8>, auth_data: &[u8]) {
    out.extend_from_slice(auth_data);
}

// ============================================================================
// Authentication Plugins
// ============================================================================

/// mysql_native_password authentication
///
/// This is the traditional MySQL authentication method using SHA1.
/// Formula: SHA1(password) XOR SHA1(challenge + SHA1(SHA1(password)))
///
/// # Arguments
/// * `password` - Plain text password
/// * `challenge` - 20-byte challenge from server (auth_plugin_data)
///
/// # Returns
/// 20-byte authentication response
pub fn auth_mysql_native_password(password: &str, challenge: &[u8]) -> [u8; 20] {
    use sha1::{Digest, Sha1};

    if password.is_empty() {
        return [0u8; 20];
    }

    // stage1_hash = SHA1(password)
    let mut hasher = Sha1::new();
    hasher.update(password.as_bytes());
    let stage1_hash = hasher.finalize();

    // stage2_hash = SHA1(stage1_hash)
    let mut hasher = Sha1::new();
    hasher.update(&stage1_hash);
    let stage2_hash = hasher.finalize();

    // token_hash = SHA1(challenge + stage2_hash)
    let mut hasher = Sha1::new();
    hasher.update(challenge);
    hasher.update(&stage2_hash);
    let token_hash = hasher.finalize();

    // result = stage1_hash XOR token_hash
    let mut result = [0u8; 20];
    for i in 0..20 {
        result[i] = stage1_hash[i] ^ token_hash[i];
    }

    result
}

/// caching_sha2_password authentication - initial response
///
/// This is the default authentication method in MySQL 8.0+.
/// Uses SHA256 hashing instead of SHA1.
/// Formula: XOR(SHA256(password), SHA256(SHA256(SHA256(password)), challenge))
///
/// # Arguments
/// * `password` - Plain text password
/// * `challenge` - 20-byte challenge from server (auth_plugin_data)
///
/// # Returns
/// 32-byte authentication response
pub fn auth_caching_sha2_password(password: &str, challenge: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};

    if password.is_empty() {
        return [0u8; 32];
    }

    // stage1 = SHA256(password)
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    let stage1 = hasher.finalize();

    // stage2 = SHA256(stage1)
    let mut hasher = Sha256::new();
    hasher.update(&stage1);
    let stage2 = hasher.finalize();

    // scramble = SHA256(stage2 + challenge)
    let mut hasher = Sha256::new();
    hasher.update(&stage2);
    hasher.update(challenge);
    let scramble = hasher.finalize();

    // result = stage1 XOR scramble
    let mut result = [0u8; 32];
    for i in 0..32 {
        result[i] = stage1[i] ^ scramble[i];
    }

    result
}

/// caching_sha2_password fast auth result
///
/// After sending the initial auth response, server may respond with:
/// - 0x03 (fast auth success) - cached authentication succeeded
/// - 0x04 (full auth required) - need to send password via RSA or cleartext
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachingSha2PasswordFastAuthResult {
    Success,
    FullAuthRequired,
}

/// Read caching_sha2_password fast auth result
pub fn read_caching_sha2_password_fast_auth_result(
    payload: &[u8],
) -> Result<CachingSha2PasswordFastAuthResult> {
    if payload.is_empty() {
        return Err(Error::UnexpectedEof);
    }

    match payload[0] {
        0x03 => Ok(CachingSha2PasswordFastAuthResult::Success),
        0x04 => Ok(CachingSha2PasswordFastAuthResult::FullAuthRequired),
        _ => Err(Error::InvalidPacket),
    }
}

// ============================================================================
// Helper Functions
// ============================================================================
