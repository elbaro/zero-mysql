#![feature(write_all_vectored)]

pub mod col;
mod opts;
pub mod constant;
pub mod error;
pub mod protocol;
pub mod row;
pub mod sync;

pub use opts::Opts;

#[cfg(feature = "tokio")]
pub mod tokio;

#[cfg(feature = "compio")]
pub mod compio;
