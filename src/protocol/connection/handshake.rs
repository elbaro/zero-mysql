use std::hint::cold_path;
use zerocopy::byteorder::little_endian::{U16 as U16LE, U32 as U32LE};
use zerocopy::{FromBytes, Immutable, KnownLayout};

use crate::buffer::BufferSet;
use crate::constant::{
    CAPABILITIES_ALWAYS_ENABLED, CAPABILITIES_CONFIGURABLE, CapabilityFlags,
    MARIADB_CAPABILITIES_ENABLED, MAX_ALLOWED_PACKET, MariadbCapabilityFlags, UTF8MB4_GENERAL_CI,
};
use crate::error::{Error, Result, eyre};
use crate::opts::Opts;
use crate::protocol::primitive::*;
use crate::protocol::response::ErrPayloadBytes;

#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
struct HandshakeFixedFields {
    connection_id: U32LE,
    auth_data_part1: [u8; 8],
    _filler1: u8,
    capability_flags_lower: U16LE,
    charset: u8,
    status_flags: U16LE,
    capability_flags_upper: U16LE,
    auth_data_len: u8,
    _fillter2: [u8; 6],
    mariadb_capabilities: U32LE,
}

#[derive(Debug, Clone)]
pub struct InitialHandshake {
    pub protocol_version: u8,
    pub server_version: std::ops::Range<usize>,
    pub connection_id: u32,
    pub auth_plugin_data: Vec<u8>,
    pub capability_flags: CapabilityFlags,
    pub mariadb_capabilities: MariadbCapabilityFlags,
    pub charset: u8,
    pub status_flags: crate::constant::ServerStatusFlags,
    pub auth_plugin_name: std::ops::Range<usize>,
}

/// Read initial handshake packet from server
pub fn read_initial_handshake(payload: &[u8]) -> Result<InitialHandshake> {
    let (protocol_version, data) = read_int_1(payload)?;

    if protocol_version == 0xFF {
        cold_path();
        Err(ErrPayloadBytes(payload))?
    }

    let server_version_start = payload.len() - data.len();
    let (server_version_bytes, data) = read_string_null(data)?;
    let server_version = server_version_start..server_version_start + server_version_bytes.len();

    let (fixed, data) = HandshakeFixedFields::ref_from_prefix(data)?;

    let connection_id = fixed.connection_id.get();
    let charset = fixed.charset;
    let status_flags = fixed.status_flags.get();
    let capability_flags = CapabilityFlags::from_bits(
        ((fixed.capability_flags_upper.get() as u32) << 16)
            | (fixed.capability_flags_lower.get() as u32),
    )
    .ok_or_else(|| Error::LibraryBug(eyre!("invalid capability flags from server")))?;
    let mariadb_capabilities = MariadbCapabilityFlags::from_bits(fixed.mariadb_capabilities.get())
        .ok_or_else(|| Error::LibraryBug(eyre!("invalid mariadb capability flags from server")))?;
    let auth_data_len = fixed.auth_data_len;

    let auth_data_2_len = (auth_data_len as usize).saturating_sub(9).max(12);
    let (auth_data_2, data) = read_string_fix(data, auth_data_2_len)?;
    let (_reserved, data) = read_int_1(data)?;

    let mut auth_plugin_data = Vec::new();
    auth_plugin_data.extend_from_slice(&fixed.auth_data_part1);
    auth_plugin_data.extend_from_slice(auth_data_2);

    let auth_plugin_name_start = payload.len() - data.len();
    let (auth_plugin_name_bytes, rest) = read_string_null(data)?;
    let auth_plugin_name =
        auth_plugin_name_start..auth_plugin_name_start + auth_plugin_name_bytes.len();

    if !rest.is_empty() {
        return Err(Error::LibraryBug(eyre!(
            "unexpected trailing data in handshake packet: {} bytes",
            rest.len()
        )));
    }

    Ok(InitialHandshake {
        protocol_version,
        server_version,
        connection_id,
        auth_plugin_data,
        capability_flags,
        mariadb_capabilities,
        charset,
        status_flags: crate::constant::ServerStatusFlags::from_bits_truncate(status_flags),
        auth_plugin_name,
    })
}

/// Auth switch request from server
#[derive(Debug, Clone)]
pub struct AuthSwitchRequest<'buf> {
    pub plugin_name: &'buf [u8],
    pub plugin_data: &'buf [u8],
}

/// Read auth switch request (0xFE with length >= 9)
pub fn read_auth_switch_request(payload: &[u8]) -> Result<AuthSwitchRequest<'_>> {
    let (header, mut data) = read_int_1(payload)?;
    if header != 0xFE {
        return Err(Error::LibraryBug(eyre!(
            "expected auth switch header 0xFE, got 0x{:02X}",
            header
        )));
    }

    let (plugin_name, rest) = read_string_null(data)?;
    data = rest;

    if let Some(0) = data.last() {
        Ok(AuthSwitchRequest {
            plugin_name,
            plugin_data: &data[..data.len() - 1],
        })
    } else {
        Err(Error::LibraryBug(eyre!(
            "auth switch request plugin data not null-terminated"
        )))
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
        return [0_u8; 20];
    }

    // stage1_hash = SHA1(password)
    let stage1_hash = Sha1::digest(password.as_bytes());

    // stage2_hash = SHA1(stage1_hash)
    let stage2_hash = Sha1::digest(stage1_hash);

    // token_hash = SHA1(challenge + stage2_hash)
    let mut hasher = Sha1::new();
    hasher.update(challenge);
    hasher.update(stage2_hash);
    let token_hash = hasher.finalize();

    // result = stage1_hash XOR token_hash
    let mut result = [0_u8; 20];
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
        return [0_u8; 32];
    }

    // stage1 = SHA256(password)
    let stage1 = Sha256::digest(password.as_bytes());

    // stage2 = SHA256(stage1)
    let stage2 = Sha256::digest(stage1);

    // scramble = SHA256(stage2 + challenge)
    let mut hasher = Sha256::new();
    hasher.update(stage2);
    hasher.update(challenge);
    let scramble = hasher.finalize();

    // result = stage1 XOR scramble
    let mut result = [0_u8; 32];
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
        return Err(Error::LibraryBug(eyre!(
            "empty payload for caching_sha2_password fast auth result"
        )));
    }

    match payload[0] {
        0x03 => Ok(CachingSha2PasswordFastAuthResult::Success),
        0x04 => Ok(CachingSha2PasswordFastAuthResult::FullAuthRequired),
        _ => Err(Error::LibraryBug(eyre!(
            "unexpected caching_sha2_password fast auth result: 0x{:02X}",
            payload[0]
        ))),
    }
}

// ============================================================================
// State Machine API for Handshake
// ============================================================================

/// Write SSL request packet (sent before HandshakeResponse when TLS is enabled)
fn write_ssl_request(
    out: &mut Vec<u8>,
    capability_flags: CapabilityFlags,
    mariadb_capabilities: MariadbCapabilityFlags,
) {
    // capability flags (4 bytes)
    write_int_4(out, capability_flags.bits());

    // max packet size (4 bytes)
    write_int_4(out, MAX_ALLOWED_PACKET);

    // charset (1 byte)
    write_int_1(out, UTF8MB4_GENERAL_CI);

    // reserved (23 bytes of 0x00)
    out.extend_from_slice(&[0_u8; 19]);

    if capability_flags.is_mariadb() {
        write_int_4(out, mariadb_capabilities.bits());
    } else {
        write_int_4(out, 0);
    }
}

/// Action returned by the Handshake state machine indicating what I/O operation is needed next
pub enum HandshakeAction<'buf> {
    /// Read a packet into the provided buffer
    ReadPacket(&'buf mut Vec<u8>),

    /// Write the prepared packet with given sequence_id, then read next response
    WritePacket { sequence_id: u8 },

    /// Write SSL request, then upgrade stream to TLS
    UpgradeTls { sequence_id: u8 },

    /// Handshake complete - call finish() to get results
    Finished,
}

/// Internal state of the handshake state machine
enum HandshakeState {
    /// Initial state - need to read initial handshake from server
    Start,
    /// Waiting for initial handshake packet to be read
    WaitingInitialHandshake,
    /// SSL request written, waiting for TLS upgrade to complete
    WaitingTlsUpgrade,
    /// Handshake response written, waiting for auth result
    WaitingAuthResult,
    /// Auth switch response written, waiting for final result
    WaitingFinalAuthResult,
    /// Connected (terminal state)
    Connected,
}

/// State machine for MySQL handshake
///
/// Pure parsing and packet generation state machine without I/O dependencies.
pub struct Handshake<'a> {
    state: HandshakeState,
    opts: &'a Opts,
    initial_handshake: Option<InitialHandshake>,
    next_sequence_id: u8,
    capability_flags: Option<CapabilityFlags>,
    mariadb_capabilities: Option<MariadbCapabilityFlags>,
}

impl<'a> Handshake<'a> {
    /// Create a new handshake state machine
    pub fn new(opts: &'a Opts) -> Self {
        Self {
            state: HandshakeState::Start,
            opts,
            initial_handshake: None,
            next_sequence_id: 1,
            capability_flags: None,
            mariadb_capabilities: None,
        }
    }

    /// Drive the state machine forward
    ///
    /// Returns an action indicating what I/O operation the caller should perform.
    pub fn step<'buf>(&mut self, buffer_set: &'buf mut BufferSet) -> Result<HandshakeAction<'buf>> {
        match &mut self.state {
            HandshakeState::Start => {
                self.state = HandshakeState::WaitingInitialHandshake;
                Ok(HandshakeAction::ReadPacket(
                    &mut buffer_set.initial_handshake,
                ))
            }

            HandshakeState::WaitingInitialHandshake => {
                let handshake = read_initial_handshake(&buffer_set.initial_handshake)?;

                let mut client_caps = CAPABILITIES_ALWAYS_ENABLED
                    | (self.opts.capabilities & CAPABILITIES_CONFIGURABLE);
                if self.opts.db.is_some() {
                    client_caps |= CapabilityFlags::CLIENT_CONNECT_WITH_DB;
                }
                if self.opts.tls {
                    client_caps |= CapabilityFlags::CLIENT_SSL;
                }

                let negotiated_caps = client_caps & handshake.capability_flags;
                let mariadb_caps = if negotiated_caps.is_mariadb() {
                    if !handshake
                        .mariadb_capabilities
                        .contains(MARIADB_CAPABILITIES_ENABLED)
                    {
                        return Err(Error::Unsupported(format!(
                            "MariaDB server does not support the required capabilities. Server: {:?} Required: {:?}",
                            handshake.mariadb_capabilities, MARIADB_CAPABILITIES_ENABLED
                        )));
                    }
                    MARIADB_CAPABILITIES_ENABLED
                } else {
                    MariadbCapabilityFlags::empty()
                };

                // Store capabilities and initial handshake
                self.capability_flags = Some(negotiated_caps);
                self.mariadb_capabilities = Some(mariadb_caps);
                self.initial_handshake = Some(handshake);

                // TLS: SSLRequest + HandshakeResponse
                if self.opts.tls && negotiated_caps.contains(CapabilityFlags::CLIENT_SSL) {
                    write_ssl_request(buffer_set.new_write_buffer(), negotiated_caps, mariadb_caps);

                    let seq = self.next_sequence_id;
                    self.next_sequence_id = self.next_sequence_id.wrapping_add(1);
                    self.state = HandshakeState::WaitingTlsUpgrade;

                    Ok(HandshakeAction::UpgradeTls { sequence_id: seq })
                } else {
                    // No TLS: HandshakeResponse
                    self.write_handshake_response(buffer_set)?;
                    let seq = self.next_sequence_id;
                    self.next_sequence_id = self.next_sequence_id.wrapping_add(1);
                    self.state = HandshakeState::WaitingAuthResult;

                    Ok(HandshakeAction::WritePacket { sequence_id: seq })
                }
            }

            HandshakeState::WaitingTlsUpgrade => {
                // TLS upgrade completed, now send handshake response
                self.write_handshake_response(buffer_set)?;

                let seq = self.next_sequence_id;
                self.next_sequence_id = self.next_sequence_id.wrapping_add(1);
                self.state = HandshakeState::WaitingAuthResult;

                Ok(HandshakeAction::WritePacket { sequence_id: seq })
            }

            HandshakeState::WaitingAuthResult => {
                let payload = &buffer_set.read_buffer[..];
                if payload.is_empty() {
                    return Err(Error::LibraryBug(eyre!(
                        "empty payload while waiting for auth result"
                    )));
                }

                // Get initial plugin name from stored handshake
                let initial_handshake = self.initial_handshake.as_ref().ok_or_else(|| {
                    Error::LibraryBug(eyre!("initial_handshake not set in WaitingAuthResult"))
                })?;
                let initial_plugin =
                    &buffer_set.initial_handshake[initial_handshake.auth_plugin_name.clone()];

                match payload[0] {
                    0x00 => {
                        // OK packet - authentication succeeded
                        self.state = HandshakeState::Connected;
                        Ok(HandshakeAction::Finished)
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
                                    Ok(HandshakeAction::ReadPacket(&mut buffer_set.read_buffer))
                                }
                                CachingSha2PasswordFastAuthResult::FullAuthRequired => {
                                    Err(Error::Unsupported(
                                        "caching_sha2_password full auth (requires SSL/RSA)"
                                            .to_string(),
                                    ))
                                }
                            }
                        } else {
                            // Auth switch request
                            let auth_switch = read_auth_switch_request(payload)?;

                            // Compute auth response for new plugin
                            let password = self.opts.password.as_deref().unwrap_or("");
                            let auth_response = match auth_switch.plugin_name {
                                b"mysql_native_password" => {
                                    auth_mysql_native_password(password, auth_switch.plugin_data)
                                        .to_vec()
                                }
                                b"caching_sha2_password" => {
                                    auth_caching_sha2_password(password, auth_switch.plugin_data)
                                        .to_vec()
                                }
                                plugin => {
                                    return Err(Error::Unsupported(
                                        String::from_utf8_lossy(plugin).to_string(),
                                    ));
                                }
                            };

                            write_auth_switch_response(
                                buffer_set.new_write_buffer(),
                                &auth_response,
                            );

                            let seq = self.next_sequence_id;
                            self.next_sequence_id = self.next_sequence_id.wrapping_add(1);
                            self.state = HandshakeState::WaitingFinalAuthResult;

                            Ok(HandshakeAction::WritePacket { sequence_id: seq })
                        }
                    }
                    header => Err(Error::LibraryBug(eyre!(
                        "unexpected packet header 0x{:02X} while waiting for auth result",
                        header
                    ))),
                }
            }

            HandshakeState::WaitingFinalAuthResult => {
                let payload = &buffer_set.read_buffer[..];
                if payload.is_empty() {
                    return Err(Error::LibraryBug(eyre!(
                        "empty payload while waiting for final auth result"
                    )));
                }

                match payload[0] {
                    0x00 => {
                        // OK packet - authentication succeeded
                        self.state = HandshakeState::Connected;
                        Ok(HandshakeAction::Finished)
                    }
                    0xFF => {
                        // ERR packet - authentication failed
                        Err(ErrPayloadBytes(payload).into())
                    }
                    header => Err(Error::LibraryBug(eyre!(
                        "unexpected packet header 0x{:02X} while waiting for final auth result",
                        header
                    ))),
                }
            }

            HandshakeState::Connected => Err(Error::LibraryBug(eyre!(
                "step() called after handshake completed"
            ))),
        }
    }

    /// Consume the state machine and return the connection info
    ///
    /// Returns an error if called before handshake is complete (before Finished action)
    pub fn finish(self) -> Result<(InitialHandshake, CapabilityFlags, MariadbCapabilityFlags)> {
        if !matches!(self.state, HandshakeState::Connected) {
            return Err(Error::LibraryBug(eyre!(
                "finish() called before handshake completed"
            )));
        }

        let initial_handshake = self.initial_handshake.ok_or_else(|| {
            Error::LibraryBug(eyre!("initial_handshake not set in Connected state"))
        })?;
        let capability_flags = self.capability_flags.ok_or_else(|| {
            Error::LibraryBug(eyre!("capability_flags not set in Connected state"))
        })?;
        let mariadb_capabilities = self.mariadb_capabilities.ok_or_else(|| {
            Error::LibraryBug(eyre!("mariadb_capabilities not set in Connected state"))
        })?;

        Ok((initial_handshake, capability_flags, mariadb_capabilities))
    }

    /// Write handshake response packet (HandshakeResponse41)
    fn write_handshake_response(&self, buffer_set: &mut BufferSet) -> Result<()> {
        buffer_set.new_write_buffer();

        let handshake = self.initial_handshake.as_ref().ok_or_else(|| {
            Error::LibraryBug(eyre!(
                "initial_handshake not set in write_handshake_response"
            ))
        })?;
        let capability_flags = self.capability_flags.ok_or_else(|| {
            Error::LibraryBug(eyre!(
                "capability_flags not set in write_handshake_response"
            ))
        })?;
        let mariadb_capabilities = self.mariadb_capabilities.ok_or_else(|| {
            Error::LibraryBug(eyre!(
                "mariadb_capabilities not set in write_handshake_response"
            ))
        })?;

        // Copy auth plugin name before getting mutable borrow

        // Compute auth response based on plugin name
        let password = self.opts.password.as_deref().unwrap_or("");
        let auth_plugin_name = &buffer_set.initial_handshake[handshake.auth_plugin_name.clone()];
        let auth_response = {
            match auth_plugin_name {
                b"mysql_native_password" => {
                    auth_mysql_native_password(password, &handshake.auth_plugin_data).to_vec()
                }
                b"caching_sha2_password" => {
                    auth_caching_sha2_password(password, &handshake.auth_plugin_data).to_vec()
                }
                plugin => {
                    return Err(Error::Unsupported(
                        String::from_utf8_lossy(plugin).to_string(),
                    ));
                }
            }
        };

        let out = &mut buffer_set.write_buffer;
        // capability flags (4 bytes)
        write_int_4(out, capability_flags.bits());
        // max packet size (4 bytes)
        write_int_4(out, MAX_ALLOWED_PACKET);
        // charset (1 byte)
        write_int_1(out, UTF8MB4_GENERAL_CI);
        // reserved (19 bytes) + MariaDB capabilities (4 bytes) = 23 bytes
        out.extend_from_slice(&[0_u8; 19]);
        write_int_4(out, mariadb_capabilities.bits());
        // username (null-terminated)
        write_string_null(out, self.opts.user.as_bytes());
        // auth response (length-encoded)
        if capability_flags.contains(CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
            write_bytes_lenenc(out, &auth_response);
        } else {
            write_int_1(out, auth_response.len() as u8);
            out.extend_from_slice(&auth_response);
        }
        // database name (null-terminated, if CLIENT_CONNECT_WITH_DB)
        if let Some(db) = &self.opts.db {
            write_string_null(out, db.as_bytes());
        }

        // auth plugin name (null-terminated, if CLIENT_PLUGIN_AUTH)
        if capability_flags.contains(CapabilityFlags::CLIENT_PLUGIN_AUTH) {
            write_string_null(out, auth_plugin_name);
        }

        Ok(())
    }
}
