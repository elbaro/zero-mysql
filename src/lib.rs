#![feature(write_all_vectored)]
#![feature(cold_path)]
#![feature(likely_unlikely)]

pub mod constant;
pub mod error;
mod opts;
pub mod protocol;
pub mod sync;

pub use opts::Opts;

#[cfg(feature = "tokio")]
pub mod tokio;

#[cfg(feature = "compio")]
pub mod compio;
