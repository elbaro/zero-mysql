#![feature(cold_path)]
#![feature(likely_unlikely)]
#![feature(read_buf)]
#![feature(core_io_borrowed_buf)]

mod buffer;
mod buffer_pool;
pub mod constant;
pub mod error;
pub mod handler;
mod opts;
mod prepared;
pub mod protocol;
pub mod raw;
pub mod sync;
pub mod value;

pub use buffer::BufferSet;
pub use buffer_pool::BufferPool;
pub use opts::Opts;
pub use prepared::PreparedStatement;

#[cfg(feature = "tokio")]
pub mod tokio;

#[cfg(test)]
mod buffer_test;
#[cfg(test)]
mod constant_test;
#[cfg(test)]
mod opts_test;
#[cfg(test)]
mod value_test;
