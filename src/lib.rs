#![feature(cold_path)]
#![feature(likely_unlikely)]

mod buffer;
pub mod constant;
pub mod error;
mod opts;
mod prepared;
pub mod protocol;
pub mod sync;

pub use buffer::BufferSet;
pub use opts::Opts;
pub use prepared::PreparedStatement;

#[cfg(feature = "tokio")]
pub mod tokio;
