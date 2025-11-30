#![feature(cold_path)]
#![feature(likely_unlikely)]
#![feature(read_buf)]
#![feature(core_io_borrowed_buf)]

mod buffer;
mod buffer_pool;
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
