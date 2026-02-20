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
pub mod value;

pub use buffer::BufferSet;
pub use buffer_pool::BufferPool;
pub use opts::Opts;
pub use prepared::PreparedStatement;

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
mod value_test;
