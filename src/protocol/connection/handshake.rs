use std::hint::cold_path;
use zerocopy::{FromBytes, Immutable, KnownLayout};
use zerocopy::byteorder::little_endian::{U16 as U16LE, U32 as U32LE};

use crate::constant::{CAPABILITIES_ALWAYS_ENABLED, CAPABILITIES_CONFIGURABLE, CapabilityFlags};
use crate::error::{Error, Result};
use crate::protocol::primitive::*;
use crate::protocol::response::ErrPayloadBytes;

#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
struct HandshakeFixedFields {
    connection_id: U32LE,
    auth_data_part1: [u8; 8],
    filler: u8,
    capability_flags_lower: U16LE,
    charset: u8,
    status_flags: U16LE,
    capability_flags_upper: U16LE,
    auth_data_len: u8,
}

#[derive(Debug, Clone)]
pub struct InitialHandshake {
    pub protocol_version: u8,
    pub server_version: std::ops::Range<usize>,
    pub connection_id: u32,
    pub auth_plugin_data: Vec<u8>,
    pub capability_flags: CapabilityFlags,
    pub charset: u8,
    pub status_flags: crate::constant::ServerStatusFlags,
    pub auth_plugin_name: std::ops::Range<usize>,
}

/// Read initial handshake packet from server
pub fn read_initial_handshake(payload: &[u8]) -> Result<InitialHandshake> {
    let (protocol_version, mut data) = read_int_1(payload)?;

    if protocol_version == 0xFF {
        cold_path();
        Err(ErrPayloadBytes(payload))?
    }

    let server_version_start = payload.len() - data.len();
    let (server_version_bytes, rest) = read_string_null(data)?;
    let server_version = server_version_start..server_version_start + server_version_bytes.len();
    data = rest;

    let (fixed, rest) = HandshakeFixedFields::ref_from_prefix(data)
        .map_err(|_| Error::InvalidPacket)?;

    let connection_id = fixed.connection_id.get();
    let charset = fixed.charset;
    let status_flags = fixed.status_flags.get();
    let cap_bits = ((fixed.capability_flags_upper.get() as u32) << 16)
                  | (fixed.capability_flags_lower.get() as u32);
    let capability_flags = CapabilityFlags::from_bits(cap_bits).ok_or(Error::InvalidPacket)?;
    let auth_data_len = fixed.auth_data_len;

    let (_reserved, rest) = read_string_fix(rest, 10)?;
    data = rest;

    let auth_data_2_len = (auth_data_len as usize).saturating_sub(9).max(12);
    let (auth_data_2, rest) = read_string_fix(data, auth_data_2_len)?;
    data = rest;
    let (_reserved, rest) = read_int_1(data)?;
    data = rest;

    let mut auth_plugin_data = Vec::new();
    auth_plugin_data.extend_from_slice(&fixed.auth_data_part1);
    auth_plugin_data.extend_from_slice(auth_data_2);

    let auth_plugin_name_start = payload.len() - data.len();
    let (auth_plugin_name_bytes, rest) = read_string_null(data)?;
    let auth_plugin_name = auth_plugin_name_start..auth_plugin_name_start + auth_plugin_name_bytes.len();

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
        status_flags: crate::constant::ServerStatusFlags::from_bits_truncate(status_flags),
        auth_plugin_name,
    })
}

/// Handshake response packet sent by client (HandshakeResponse41)
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
    if let Some(plugin) = response.auth_plugin_name
        && response
            .capability_flags
            .contains(CapabilityFlags::CLIENT_PLUGIN_AUTH)
    {
        write_string_null(out, plugin);
    }

    // TODO: CLIENT_CONNECT_ATTRS

    // TODO: CLIENT_ZSTD_COMPRESSION_ALGORITHM
    // if response.capability_flags.contains(CapabilityFlags::CLIENT_ZSTD_COMPRESSION_ALGORITHM) {
    //     write_int_1(out, compression_level);
    // }
}

/// Auth switch request from server
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
    hasher.update(stage1_hash);
    let stage2_hash = hasher.finalize();

    // token_hash = SHA1(challenge + stage2_hash)
    let mut hasher = Sha1::new();
    hasher.update(challenge);
    hasher.update(stage2_hash);
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
    hasher.update(stage1);
    let stage2 = hasher.finalize();

    // scramble = SHA256(stage2 + challenge)
    let mut hasher = Sha256::new();
    hasher.update(stage2);
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
        return Err(Error::InvalidPacket);
    }

    match payload[0] {
        0x03 => Ok(CachingSha2PasswordFastAuthResult::Success),
        0x04 => Ok(CachingSha2PasswordFastAuthResult::FullAuthRequired),
        _ => Err(Error::InvalidPacket),
    }
}

// ============================================================================
// State Machine API for Handshake
// ============================================================================

/// Configuration for handshake
pub struct HandshakeConfig {
    pub username: String,
    pub password: String,
    pub database: Option<String>,
    pub capabilities: CapabilityFlags,
    pub tls: bool,
}

/// Write SSL request packet (sent before HandshakeResponse when TLS is enabled)
pub fn write_ssl_request(out: &mut Vec<u8>, capability_flags: CapabilityFlags, charset: u8) {
    use crate::protocol::primitive::*;

    // capability flags (4 bytes)
    write_int_4(out, capability_flags.bits());

    // max packet size (4 bytes)
    write_int_4(out, 16777216);

    // charset (1 byte)
    write_int_1(out, charset);

    // reserved (23 bytes of 0x00)
    out.extend_from_slice(&[0u8; 23]);
}

/// Result of driving the handshake state machine
pub enum HandshakeResult {
    /// Initial handshake received - write response to server
    InitialHandshake {
        handshake_response: Vec<u8>,
        initial_handshake: InitialHandshake,
    },
    /// SSL request sent - upgrade connection to TLS, then call drive_after_tls()
    SslRequest {
        ssl_request: Vec<u8>,
        initial_handshake: InitialHandshake,
    },
    /// Write this packet to the server, then read next response
    Write(Vec<u8>),
    /// Handshake complete, connection established
    Connected {
        capability_flags: CapabilityFlags,
    },
}

/// State machine for MySQL handshake
///
/// Pure parsing and packet generation state machine without I/O dependencies.
pub enum Handshake {
    /// Waiting for initial handshake from server
    Start { config: HandshakeConfig },
    /// Sent SSL request, waiting for TLS upgrade to complete before sending handshake response
    WaitingTlsUpgrade {
        config: HandshakeConfig,
        auth_plugin_name: Vec<u8>,
        auth_plugin_data: Vec<u8>,
        capability_flags: CapabilityFlags,
    },
    /// Sent handshake response, waiting for auth result
    WaitingAuthResult {
        config: HandshakeConfig,
        initial_plugin: Vec<u8>,
        capability_flags: CapabilityFlags,
    },
    /// Sent auth switch response, waiting for final auth result
    WaitingFinalAuthResult {
        capability_flags: CapabilityFlags,
    },
    /// Connected (terminal state)
    Connected,
}

impl Handshake {
    /// Create a new handshake state machine
    pub fn new(username: String, password: String, database: Option<String>, capabilities: CapabilityFlags, tls: bool) -> Self {
        Self::Start {
            config: HandshakeConfig {
                username,
                password,
                database,
                capabilities,
                tls,
            },
        }
    }

    /// Drive the state machine with the next payload
    ///
    /// # Arguments
    /// * `payload` - The next packet payload to process
    ///
    /// # Returns
    /// * `Ok(HandshakeResult::Write)` - Write this packet, then read response
    /// * `Ok(HandshakeResult::Connected)` - Handshake complete
    /// * `Err(Error)` - An error occurred
    pub fn drive(&mut self, payload: &[u8]) -> Result<HandshakeResult> {
        match self {
            Self::Start { config } => {
                let handshake = read_initial_handshake(payload)?;
                let server_caps = handshake.capability_flags;

                let mut client_caps = CAPABILITIES_ALWAYS_ENABLED | (config.capabilities & CAPABILITIES_CONFIGURABLE);
                if config.database.is_some() {
                    client_caps |= CapabilityFlags::CLIENT_CONNECT_WITH_DB;
                }
                if config.tls {
                    client_caps |= CapabilityFlags::CLIENT_SSL;
                }

                // Negotiate capabilities
                let negotiated_caps = client_caps & server_caps;

                let auth_plugin_name = &payload[handshake.auth_plugin_name.clone()];

                // If TLS is requested and server supports it, send SSL request first
                if config.tls && negotiated_caps.contains(CapabilityFlags::CLIENT_SSL) {
                    let mut ssl_request_data = Vec::new();
                    write_ssl_request(&mut ssl_request_data, negotiated_caps, handshake.charset);

                    let config_owned = std::mem::replace(
                        config,
                        HandshakeConfig {
                            username: String::new(),
                            password: String::new(),
                            database: None,
                            capabilities: CapabilityFlags::empty(),
                            tls: false,
                        },
                    );

                    *self = Self::WaitingTlsUpgrade {
                        config: config_owned,
                        auth_plugin_name: auth_plugin_name.to_vec(),
                        auth_plugin_data: handshake.auth_plugin_data.clone(),
                        capability_flags: negotiated_caps,
                    };

                    return Ok(HandshakeResult::SslRequest {
                        ssl_request: ssl_request_data,
                        initial_handshake: handshake,
                    });
                }

                let auth_response = match auth_plugin_name {
                    b"mysql_native_password" => {
                        auth_mysql_native_password(&config.password, &handshake.auth_plugin_data)
                            .to_vec()
                    }
                    b"caching_sha2_password" => {
                        auth_caching_sha2_password(&config.password, &handshake.auth_plugin_data)
                            .to_vec()
                    }
                    plugin => {
                        return Err(Error::UnsupportedAuthPlugin(
                            String::from_utf8_lossy(plugin).to_string(),
                        ));
                    }
                };

                let response = HandshakeResponse41 {
                    capability_flags: negotiated_caps,
                    max_packet_size: 16777216,
                    charset: 45,
                    username: &config.username,
                    auth_response: &auth_response,
                    database: config.database.as_deref(),
                    auth_plugin_name: Some(
                        std::str::from_utf8(auth_plugin_name).unwrap(),
                    ),
                };

                let mut packet_data = Vec::new();
                write_handshake_response(&mut packet_data, &response);

                let initial_plugin = auth_plugin_name.to_vec();
                let config_owned = std::mem::replace(
                    config,
                    HandshakeConfig {
                        username: String::new(),
                        password: String::new(),
                        database: None,
                        capabilities: CapabilityFlags::empty(),
                        tls: false,
                    },
                );

                *self = Self::WaitingAuthResult {
                    config: config_owned,
                    initial_plugin,
                    capability_flags: negotiated_caps,
                };

                Ok(HandshakeResult::InitialHandshake {
                    handshake_response: packet_data,
                    initial_handshake: handshake,
                })
            }

            Self::WaitingAuthResult {
                config,
                initial_plugin,
                capability_flags,
            } => {
                if payload.is_empty() {
                    return Err(Error::InvalidPacket);
                }

                match payload[0] {
                    0x00 => {
                        // OK packet - authentication succeeded
                        let result = HandshakeResult::Connected {
                            capability_flags: *capability_flags,
                        };
                        *self = Self::Connected;
                        Ok(result)
                    }
                    0xFF => {
                        // ERR packet - authentication failed
                        Err(ErrPayloadBytes(payload).into())
                    }
                    0xFE => {
                        // Could be auth switch or fast auth result
                        if initial_plugin == b"caching_sha2_password" && payload.len() == 2 {
                            // Fast auth result
                            let result = read_caching_sha2_password_fast_auth_result(payload)?;
                            match result {
                                CachingSha2PasswordFastAuthResult::Success => {
                                    // Need to read final OK packet
                                    // Stay in same state but expect OK next
                                    Ok(HandshakeResult::Write(Vec::new()))
                                }
                                CachingSha2PasswordFastAuthResult::FullAuthRequired => {
                                    Err(Error::UnsupportedAuthPlugin(
                                        "caching_sha2_password full auth (requires SSL/RSA)"
                                            .to_string(),
                                    ))
                                }
                            }
                        } else {
                            // Auth switch request
                            let auth_switch = read_auth_switch_request(payload)?;

                            // Compute auth response for new plugin
                            let auth_response = match auth_switch.plugin_name {
                                b"mysql_native_password" => auth_mysql_native_password(
                                    &config.password,
                                    auth_switch.plugin_data,
                                )
                                .to_vec(),
                                b"caching_sha2_password" => auth_caching_sha2_password(
                                    &config.password,
                                    auth_switch.plugin_data,
                                )
                                .to_vec(),
                                plugin => {
                                    return Err(Error::UnsupportedAuthPlugin(
                                        String::from_utf8_lossy(plugin).to_string(),
                                    ));
                                }
                            };

                            // Build auth switch response
                            let mut packet_data = Vec::new();
                            write_auth_switch_response(&mut packet_data, &auth_response);

                            // Transition to waiting for final result
                            *self = Self::WaitingFinalAuthResult {
                                capability_flags: *capability_flags,
                            };

                            Ok(HandshakeResult::Write(packet_data))
                        }
                    }
                    _ => Err(Error::InvalidPacket),
                }
            }

            Self::WaitingFinalAuthResult {
                capability_flags,
            } => {
                if payload.is_empty() {
                    return Err(Error::InvalidPacket);
                }

                match payload[0] {
                    0x00 => {
                        // OK packet - authentication succeeded
                        let result = HandshakeResult::Connected {
                            capability_flags: *capability_flags,
                        };
                        *self = Self::Connected;
                        Ok(result)
                    }
                    0xFF => {
                        // ERR packet - authentication failed
                        Err(ErrPayloadBytes(payload).into())
                    }
                    _ => Err(Error::InvalidPacket),
                }
            }

            Self::WaitingTlsUpgrade { .. } => {
                // Should not call drive() in this state - use drive_after_tls() instead
                Err(Error::InvalidPacket)
            }

            Self::Connected => {
                // Should not receive more data after connected
                Err(Error::InvalidPacket)
            }
        }
    }

    /// Continue handshake after TLS upgrade is complete.
    /// Call this after receiving SslRequest result and upgrading the connection to TLS.
    pub fn drive_after_tls(&mut self) -> Result<HandshakeResult> {
        match std::mem::replace(self, Self::Connected) {
            Self::WaitingTlsUpgrade {
                config,
                auth_plugin_name,
                auth_plugin_data,
                capability_flags,
            } => {
                let auth_response = match auth_plugin_name.as_slice() {
                    b"mysql_native_password" => {
                        auth_mysql_native_password(&config.password, &auth_plugin_data).to_vec()
                    }
                    b"caching_sha2_password" => {
                        auth_caching_sha2_password(&config.password, &auth_plugin_data).to_vec()
                    }
                    plugin => {
                        return Err(Error::UnsupportedAuthPlugin(
                            String::from_utf8_lossy(plugin).to_string(),
                        ));
                    }
                };

                let response = HandshakeResponse41 {
                    capability_flags,
                    max_packet_size: 16777216,
                    charset: 45,
                    username: &config.username,
                    auth_response: &auth_response,
                    database: config.database.as_deref(),
                    auth_plugin_name: Some(std::str::from_utf8(&auth_plugin_name).unwrap()),
                };

                let mut packet_data = Vec::new();
                write_handshake_response(&mut packet_data, &response);

                *self = Self::WaitingAuthResult {
                    config,
                    initial_plugin: auth_plugin_name,
                    capability_flags,
                };

                Ok(HandshakeResult::Write(packet_data))
            }
            other => {
                *self = other;
                Err(Error::InvalidPacket)
            }
        }
    }
}
