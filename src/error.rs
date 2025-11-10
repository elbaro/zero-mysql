use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("MySQL server error: {error_code} - {message}")]
    ServerError {
        error_code: u16,
        sql_state: String,
        message: String,
    },

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
}

pub type Result<T> = std::result::Result<T, Error>;
