use crate::constant::CapabilityFlags;
use crate::error::{Error, Result};
use crate::protocol::primitive::*;

/// Initial handshake packet from server (Protocol::HandshakeV10)
#[derive(Debug, Clone)]
pub struct InitialHandshake {
    pub protocol_version: u8,
    pub server_version: String,
    pub connection_id: u32,
    pub auth_plugin_data: Vec<u8>,
    pub capability_flags: CapabilityFlags,
    pub charset: u8,
    pub status_flags: u16,
    pub auth_plugin_name: String,
}

/// Read initial handshake from server
pub fn read_initial_handshake(payload: &[u8]) -> Result<InitialHandshake> {
    let (protocol_version, mut data) = read_int_1(payload)?;

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

    let capability_flags = CapabilityFlags::new(((cap_upper as u32) << 16) | (cap_lower as u32));

    // auth plugin data length (1 byte)
    let (auth_data_len, rest) = read_int_1(data)?;
    data = rest;

    // reserved (10 bytes)
    let (_reserved, rest) = read_string_fix(data, 10)?;
    data = rest;

    // auth-plugin-data-part-2
    let auth_data_2_len = if auth_data_len > 0 {
        (auth_data_len as usize).saturating_sub(8).max(13)
    } else {
        13
    };

    let (auth_data_2, rest) = read_string_fix(data, auth_data_2_len)?;
    data = rest;

    // Combine auth plugin data
    let mut auth_plugin_data = Vec::new();
    auth_plugin_data.extend_from_slice(auth_data_1);
    auth_plugin_data.extend_from_slice(auth_data_2);
    // Remove trailing null byte if present
    if let Some(&0) = auth_plugin_data.last() {
        auth_plugin_data.pop();
    }

    // auth plugin name (null-terminated)
    let auth_plugin_name = if !data.is_empty() {
        let (name_bytes, _rest) = read_string_null(data)?;
        String::from_utf8_lossy(name_bytes).to_string()
    } else {
        String::new()
    };

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

/// Write handshake response (HandshakeResponse41)
/// Note: This is a minimal implementation without auth plugin support
pub fn write_handshake_response(
    out: &mut Vec<u8>,
    capability_flags: CapabilityFlags,
    max_packet_size: u32,
    charset: u8,
    username: &str,
    auth_response: &[u8],
    database: Option<&str>,
) {
    // capability flags (4 bytes)
    write_int_4(out, capability_flags.0);

    // max packet size (4 bytes)
    write_int_4(out, max_packet_size);

    // charset (1 byte)
    write_int_1(out, charset);

    // reserved (23 bytes of 0x00)
    out.extend_from_slice(&[0u8; 23]);

    // username (null-terminated)
    write_string_null(out, username);

    // auth response (length-encoded)
    write_bytes_lenenc(out, auth_response);

    // database name (null-terminated, if provided)
    if let Some(db) = database {
        write_string_null(out, db);
    }

    // Note: auth plugin name would go here if CLIENT_PLUGIN_AUTH is set
}

/// Auth switch request from server
#[derive(Debug, Clone)]
pub struct AuthSwitchRequest {
    pub plugin_name: String,
    pub plugin_data: Vec<u8>,
}

/// Read auth switch request (0xFE with length >= 9)
pub fn read_auth_switch_request(payload: &[u8]) -> Result<AuthSwitchRequest> {
    let (header, mut data) = read_int_1(payload)?;
    if header != 0xFE {
        return Err(Error::InvalidPacket);
    }

    let (plugin_name_bytes, rest) = read_string_null(data)?;
    let plugin_name = String::from_utf8_lossy(plugin_name_bytes).to_string();
    data = rest;

    // Remaining data is plugin-specific auth data
    let plugin_data = data.to_vec();

    Ok(AuthSwitchRequest {
        plugin_name,
        plugin_data,
    })
}

/// Write auth switch response
pub fn write_auth_switch_response(out: &mut Vec<u8>, auth_data: &[u8]) {
    out.extend_from_slice(auth_data);
}
