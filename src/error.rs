use thiserror::Error;

pub use color_eyre::eyre::eyre;

use crate::protocol::{response::ErrPayload, response::ErrPayloadBytes};

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    // ─── Server Error ────────────────────────────────────────────────────
    #[error("Server Error: {0}")]
    ServerError(#[from] ErrPayload),
    // ─── Incorrect Usage ─────────────────────────────────────────────────
    #[error(
        "Connection mismatch: transaction started on connection {expected}, but commit/rollback called on connection {actual}"
    )]
    ConnectionMismatch { expected: u64, actual: u64 },
    #[error("Bad usage error: {0}")]
    BadUsageError(String),
    // ─── Temporary Error ─────────────────────────────────────────────────
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    // ─── Library Error ───────────────────────────────────────────────────
    #[error("A bug in zero-mysql: {0}")]
    LibraryBug(#[from] color_eyre::Report),
    #[error("Unsupported authentication plugin: {0}")]
    Unsupported(String),
    #[error("Cannot nest transactions - a transaction is already active")]
    NestedTransaction,
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

    /// Returns true if the error indicates the connection is broken and cannot be reused.
    ///
    /// This is conservative - returns true (broken) when in doubt.
    pub fn is_conn_broken(&self) -> bool {
        match self {
            Error::ServerError(err_payload) => {
                match err_payload.sql_state.as_str() {
                    // Integrity errors - connection still usable
                    "23000" => false,
                    // Data errors - connection still usable
                    "22001" | "22003" | "22007" | "22012" => false,
                    // Programming errors - connection still usable
                    "42000" | "42S02" | "42S22" => false,
                    // Not supported - connection still usable
                    "0A000" => false,
                    // Everything else - assume broken
                    _ => true,
                }
            }
            // All other errors - assume broken
            _ => true,
        }
    }
}

impl<Src, Dst: ?Sized> From<zerocopy::CastError<Src, Dst>> for Error {
    fn from(err: zerocopy::CastError<Src, Dst>) -> Self {
        Self::LibraryBug(color_eyre::eyre::eyre!("{:#?}", err))
    }
}
