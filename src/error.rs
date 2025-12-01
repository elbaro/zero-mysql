use thiserror::Error;

pub use color_eyre::eyre::eyre;

use crate::protocol::{response::ErrPayload, response::ErrPayloadBytes};

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Server Error: {0}")]
    ServerError(#[from] ErrPayload),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Bad config error: {0}")]
    BadConfigError(String),

    #[error("A bug in zero-mysql: {0}")]
    LibraryBug(color_eyre::Report),

    #[error("Unsupported authentication plugin: {0}")]
    Unsupported(String),

    #[error(
        "Connection mismatch: transaction started on connection {expected}, but commit/rollback called on connection {actual}"
    )]
    ConnectionMismatch { expected: u64, actual: u64 },
}

impl<'buf> From<ErrPayloadBytes<'buf>> for Error {
    fn from(value: ErrPayloadBytes) -> Self {
        match ErrPayload::try_from(value) {
            Ok(err_payload) => Error::ServerError(err_payload),
            Err(err) => err,
        }
    }
}

impl From<core::convert::Infallible> for Error {
    fn from(err: core::convert::Infallible) -> Self {
        match err {}
    }
}

impl Error {
    pub fn from_debug(err: impl std::fmt::Debug) -> Self {
        Self::LibraryBug(color_eyre::eyre::eyre!(format!("{:#?}", err)))
    }
}
