mod column_definition;
pub mod prepared;
pub mod query;
pub mod resultset;
pub mod text;
pub mod utility;

pub use column_definition::ColumnDefinition;
pub use column_definition::ColumnDefinitionBytes;
pub use column_definition::ColumnDefinitionTail;
pub use column_definition::ColumnTypeAndFlags;

/// Action returned by state machines indicating what I/O operation is needed next
pub enum Action<'buf> {
    /// State machine needs more data - provides mutable reference to buffer to fill
    NeedPacket(&'buf mut Vec<u8>),
    /// State machine has finished processing
    Finished,
}
