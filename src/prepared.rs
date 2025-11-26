pub struct PreparedStatement<'Conn> {
    id: u32,

    /// The  MariaDB-only cache: only the first resultset provides the column metadata
    column_definitions: Vec<u8>,
    // The MariaDB-only cache: BULK_EXEC only sends the column metadata once
    // TODO: It's bulk anyway, does this actually help the perf?
    // column_metadata_sent: bool,
}
