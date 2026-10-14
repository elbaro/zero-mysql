#![warn(
    clippy::expect_used,
    clippy::get_unwrap,
    clippy::unwrap_in_result,
    clippy::unwrap_used
)]

//! A high-performance MySQL client library.
//!
//! [User Guide](https://elbaro.github.io/zero-mysql/)

mod buffer;
mod buffer_pool;
pub mod constant;
pub mod error;
pub mod handler;
mod nightly;
mod opts;
mod prepared;
pub mod protocol;
pub mod raw;
pub mod ref_row;
pub mod sync;
#[cfg(any(feature = "sync-tls", feature = "tokio-tls", feature = "compio-tls"))]
mod tls;
pub mod value;

pub use buffer::BufferSet;
pub use buffer_pool::BufferPool;
pub use opts::{Opts, TlsMode};
pub use prepared::PreparedStatement;

/// The rustls version used for TLS, for building [`Opts::tls_config`].
#[cfg(any(feature = "sync-tls", feature = "tokio-tls", feature = "compio-tls"))]
pub use rustls;

#[cfg(feature = "tokio")]
pub mod tokio;

#[cfg(feature = "compio")]
pub mod compio;

#[cfg(all(feature = "diesel", feature = "sync"))]
pub mod diesel;

#[cfg(feature = "derive")]
pub use zero_mysql_derive as r#macro;

#[cfg(test)]
mod buffer_test;
#[cfg(test)]
mod constant_test;
#[cfg(test)]
mod opts_test;
#[cfg(test)]
mod test_macros;
#[cfg(test)]
mod value_test;
