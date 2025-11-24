#![feature(cold_path)]
#![feature(likely_unlikely)]

mod buffer;
pub mod constant;
pub mod error;
mod opts;
pub mod protocol;
pub mod sync;

pub use buffer::BufferSet;
pub use opts::Opts;

#[cfg(feature = "tokio")]
pub mod tokio;
