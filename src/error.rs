use thiserror::Error;

use crate::protocol::{packet::ErrPayloadBytes, response::ErrPayload};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Server Error: {0}")]
    ServerError(#[from] ErrPayload),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Unknown protocol error: {0}")]
    UnknownProtocolError(String),

    #[error("Bad input error: {0}")]
    BadInputError(String),

    #[error("Library bug: {0}")]
    LibraryBug(String),

    #[error("Unexpected EOF")]
    UnexpectedEof,

    #[error("Invalid packet")]
    InvalidPacket,

    #[error("Unsupported authentication plugin: {0}")]
    UnsupportedAuthPlugin(String),
}

impl<'a> From<ErrPayloadBytes<'a>> for Error {
    fn from(value: ErrPayloadBytes) -> Self {
        match ErrPayload::try_from(value) {
            Ok(err_payload) => Error::ServerError(err_payload),
            Err(err) => err,
        }
    }
}

impl From<std::convert::Infallible> for Error {
    fn from(err: std::convert::Infallible) -> Self {
        match err {}
    }
}

pub type Result<T> = std::result::Result<T, Error>;
