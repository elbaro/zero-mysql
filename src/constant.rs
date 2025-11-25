#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandByte {
    Quit = 0x01,
    InitDb = 0x02,
    Query = 0x03,
    FieldList = 0x04,
    CreateDb = 0x05,
    DropDb = 0x06,
    Refresh = 0x07,
    Shutdown = 0x08,
    Statistics = 0x09,
    ProcessInfo = 0x0a,
    Connect = 0x0b,
    ProcessKill = 0x0c,
    Debug = 0x0d,
    Ping = 0x0e,
    Time = 0x0f,
    DelayedInsert = 0x10,
    ChangeUser = 0x11,
    BinlogDump = 0x12,
    TableDump = 0x13,
    ConnectOut = 0x14,
    RegisterSlave = 0x15,
    StmtPrepare = 0x16,
    StmtExecute = 0x17,
    StmtSendLongData = 0x18,
    StmtClose = 0x19,
    StmtReset = 0x1a,
    SetOption = 0x1b,
    StmtFetch = 0x1c,
    Daemon = 0x1d,
    BinlogDumpGtid = 0x1e,
    ResetConnection = 0x1f,
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct CapabilityFlags: u32 {
        /// Use the improved version of Old Password Authentication (deprecated, assumed set since 4.1.1)
        const CLIENT_LONG_PASSWORD = 0x00000001;
        /// Send found rows instead of affected rows
        const CLIENT_FOUND_ROWS = 0x00000002;
        /// Get all column flags
        const CLIENT_LONG_FLAG = 0x00000004;
        /// Database (schema) name can be specified on connect
        const CLIENT_CONNECT_WITH_DB = 0x00000008;
        /// Don't allow database.table.column (deprecated)
        const CLIENT_NO_SCHEMA = 0x00000010;
        /// Compression protocol supported
        const CLIENT_COMPRESS = 0x00000020;
        /// ODBC client (no special behavior since 3.22)
        const CLIENT_ODBC = 0x00000040;
        /// Can use LOAD DATA LOCAL
        const CLIENT_LOCAL_FILES = 0x00000080;
        /// Ignore spaces before '('
        const CLIENT_IGNORE_SPACE = 0x00000100;
        /// New 4.1 protocol
        const CLIENT_PROTOCOL_41 = 0x00000200;
        /// Interactive client (affects timeout)
        const CLIENT_INTERACTIVE = 0x00000400;
        /// Use SSL encryption for the session
        const CLIENT_SSL = 0x00000800;
        /// Client will not issue SIGPIPE (client-only, not sent to server)
        const CLIENT_IGNORE_SIGPIPE = 0x00001000;
        /// Client knows about transactions
        const CLIENT_TRANSACTIONS = 0x00002000;
        /// Old flag for 4.1 protocol (deprecated)
        const CLIENT_RESERVED = 0x00004000;
        /// Old flag for 4.1 authentication (deprecated)
        const CLIENT_SECURE_CONNECTION = 0x00008000;
        /// Enable multi-statement support
        const CLIENT_MULTI_STATEMENTS = 0x00010000;
        /// Enable multi-results
        const CLIENT_MULTI_RESULTS = 0x00020000;
        /// Multi-results in prepared statements
        const CLIENT_PS_MULTI_RESULTS = 0x00040000;
        /// Pluggable authentication
        const CLIENT_PLUGIN_AUTH = 0x00080000;
        /// Connection attributes
        const CLIENT_CONNECT_ATTRS = 0x00100000;
        /// Enable authentication response larger than 255 bytes
        const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;
        /// Can handle expired passwords
        const CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = 0x00400000;
        /// Track session state changes
        const CLIENT_SESSION_TRACK = 0x00800000;
        /// Use OK instead of EOF packet
        const CLIENT_DEPRECATE_EOF = 0x01000000;
        /// Optional metadata in resultsets
        const CLIENT_OPTIONAL_RESULTSET_METADATA = 0x02000000;
        /// Support zstd compression
        const CLIENT_ZSTD_COMPRESSION_ALGORITHM = 0x04000000;
        /// Query attributes support
        const CLIENT_QUERY_ATTRIBUTES = 0x08000000;
        /// Multi-factor authentication
        const CLIENT_MULTI_FACTOR_AUTHENTICATION = 0x10000000;
        /// Reserved for capability extension
        const CLIENT_CAPABILITY_EXTENSION = 0x20000000;
        /// Verify server certificate (deprecated, use --ssl-mode)
        const CLIENT_SSL_VERIFY_SERVER_CERT = 0x40000000;
        /// Remember options after failed connect (client-only, not sent to server)
        const CLIENT_REMEMBER_OPTIONS = 0x80000000;
    }
}

// Capabilities that are always enabled (required by zero-mysql)
pub const CAPABILITIES_ALWAYS_ENABLED: CapabilityFlags = CapabilityFlags::CLIENT_LONG_FLAG
    .union(CapabilityFlags::CLIENT_PROTOCOL_41)
    .union(CapabilityFlags::CLIENT_TRANSACTIONS)
    .union(CapabilityFlags::CLIENT_MULTI_STATEMENTS)
    .union(CapabilityFlags::CLIENT_MULTI_RESULTS)
    .union(CapabilityFlags::CLIENT_PS_MULTI_RESULTS) // prepared statement multi-resultset
    .union(CapabilityFlags::CLIENT_SECURE_CONNECTION) // On? Off?
    .union(CapabilityFlags::CLIENT_PLUGIN_AUTH)
    .union(CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
    .union(CapabilityFlags::CLIENT_DEPRECATE_EOF);

// Capabilities that are configurable by user
pub const CAPABILITIES_CONFIGURABLE: CapabilityFlags = CapabilityFlags::CLIENT_FOUND_ROWS
    .union(CapabilityFlags::CLIENT_COMPRESS)
    .union(CapabilityFlags::CLIENT_LOCAL_FILES)
    .union(CapabilityFlags::CLIENT_IGNORE_SPACE)
    .union(CapabilityFlags::CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS);

// Capabilities that are always disabled (deprecated, not implemented, or not applicable)
pub const CAPABILITIES_ALWAYS_DISABLED: CapabilityFlags = CapabilityFlags::CLIENT_LONG_PASSWORD
    .union(CapabilityFlags::CLIENT_CONNECT_WITH_DB) // This is automatically set if opts.db is provided
    .union(CapabilityFlags::CLIENT_OPTIONAL_RESULTSET_METADATA) // TODO
    .union(CapabilityFlags::CLIENT_NO_SCHEMA)
    .union(CapabilityFlags::CLIENT_ODBC)
    .union(CapabilityFlags::CLIENT_INTERACTIVE)
    .union(CapabilityFlags::CLIENT_IGNORE_SIGPIPE)
    .union(CapabilityFlags::CLIENT_RESERVED)
    .union(CapabilityFlags::CLIENT_QUERY_ATTRIBUTES)
    .union(CapabilityFlags::CLIENT_ZSTD_COMPRESSION_ALGORITHM)
    .union(CapabilityFlags::CLIENT_MULTI_FACTOR_AUTHENTICATION)
    .union(CapabilityFlags::CLIENT_CAPABILITY_EXTENSION)
    .union(CapabilityFlags::CLIENT_SSL) // set by opts.tls
    .union(CapabilityFlags::CLIENT_SSL_VERIFY_SERVER_CERT)
    .union(CapabilityFlags::CLIENT_REMEMBER_OPTIONS)
    .union(CapabilityFlags::CLIENT_CONNECT_ATTRS) // TODO
    .union(CapabilityFlags::CLIENT_SESSION_TRACK); // To support this flag, we need to update the parsing logic

bitflags::bitflags! {
    /// MySQL Server Status Flags
    /// Note: 0x0004 does not exist in the protocol
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ServerStatusFlags: u16 {
        /// A transaction is active
        const SERVER_STATUS_IN_TRANS = 0x0001;
        /// Autocommit mode is enabled
        const SERVER_STATUS_AUTOCOMMIT = 0x0002;
        /// More results exist (for multi-statement or multi-resultset)
        const SERVER_MORE_RESULTS_EXISTS = 0x0008;
        /// Query did not use a good index
        const SERVER_STATUS_NO_GOOD_INDEX_USED = 0x0010;
        /// Query did not use any index
        const SERVER_STATUS_NO_INDEX_USED = 0x0020;
        /// Cursor exists (for prepared statements)
        const SERVER_STATUS_CURSOR_EXISTS = 0x0040;
        /// Last row was sent
        const SERVER_STATUS_LAST_ROW_SENT = 0x0080;
        /// Database was dropped
        const SERVER_STATUS_DB_DROPPED = 0x0100;
        /// No backslash escapes mode is enabled
        const SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200;
        /// Metadata changed (for prepared statements)
        const SERVER_STATUS_METADATA_CHANGED = 0x0400;
        /// Query was slow
        const SERVER_QUERY_WAS_SLOW = 0x0800;
        /// Prepared statement has output parameters
        const SERVER_PS_OUT_PARAMS = 0x1000;
        /// In a read-only transaction
        const SERVER_STATUS_IN_TRANS_READONLY = 0x2000;
        /// Session state has changed
        const SERVER_SESSION_STATE_CHANGED = 0x4000;
    }
}

bitflags::bitflags! {
    /// MySQL Column Definition Flags
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ColumnFlags: u16 {
        /// Field can't be NULL
        const NOT_NULL_FLAG = 0x0001;
        /// Field is part of a primary key
        const PRI_KEY_FLAG = 0x0002;
        /// Field is part of a unique key
        const UNIQUE_KEY_FLAG = 0x0004;
        /// Field is part of a key
        const MULTIPLE_KEY_FLAG = 0x0008;
        /// Field is a blob
        const BLOB_FLAG = 0x0010;
        /// Field is unsigned
        const UNSIGNED_FLAG = 0x0020;
        /// Field is zerofill
        const ZEROFILL_FLAG = 0x0040;
        /// Field is binary
        const BINARY_FLAG = 0x0080;
        /// Field is an enum
        const ENUM_FLAG = 0x0100;
        /// Field is auto-increment
        const AUTO_INCREMENT_FLAG = 0x0200;
        /// Field is a timestamp
        const TIMESTAMP_FLAG = 0x0400;
        /// Field is a set
        const SET_FLAG = 0x0800;
        /// Field has no default value
        const NO_DEFAULT_VALUE_FLAG = 0x1000;
        /// Field is set to NOW on UPDATE
        const ON_UPDATE_NOW_FLAG = 0x2000;
        /// Field is part of some key (index)
        const PART_KEY_FLAG = 0x4000;
        /// Field is numeric
        const NUM_FLAG = 0x8000;
    }
}

#[allow(non_camel_case_types)]
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    MYSQL_TYPE_DECIMAL = 0x00,
    MYSQL_TYPE_TINY = 0x01,
    MYSQL_TYPE_SHORT = 0x02,
    MYSQL_TYPE_LONG = 0x03,
    MYSQL_TYPE_FLOAT = 0x04,
    MYSQL_TYPE_DOUBLE = 0x05,
    MYSQL_TYPE_NULL = 0x06,
    MYSQL_TYPE_TIMESTAMP = 0x07,
    MYSQL_TYPE_LONGLONG = 0x08,
    MYSQL_TYPE_INT24 = 0x09,
    MYSQL_TYPE_DATE = 0x0a,
    MYSQL_TYPE_TIME = 0x0b,
    MYSQL_TYPE_DATETIME = 0x0c,
    MYSQL_TYPE_YEAR = 0x0d,
    MYSQL_TYPE_NEWDATE = 0x0e,
    MYSQL_TYPE_VARCHAR = 0x0f,
    MYSQL_TYPE_BIT = 0x10,
    MYSQL_TYPE_TIMESTAMP2 = 0x11,
    MYSQL_TYPE_DATETIME2 = 0x12,
    MYSQL_TYPE_TIME2 = 0x13,
    MYSQL_TYPE_TYPED_ARRAY = 0x14,
    MYSQL_TYPE_JSON = 0xf5,
    MYSQL_TYPE_NEWDECIMAL = 0xf6,
    MYSQL_TYPE_ENUM = 0xf7,
    MYSQL_TYPE_SET = 0xf8,
    MYSQL_TYPE_TINY_BLOB = 0xf9,
    MYSQL_TYPE_MEDIUM_BLOB = 0xfa,
    MYSQL_TYPE_LONG_BLOB = 0xfb,
    MYSQL_TYPE_BLOB = 0xfc,
    MYSQL_TYPE_VAR_STRING = 0xfd,
    MYSQL_TYPE_STRING = 0xfe,
    MYSQL_TYPE_GEOMETRY = 0xff,
}

impl ColumnType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x00 => Some(Self::MYSQL_TYPE_DECIMAL),
            0x01 => Some(Self::MYSQL_TYPE_TINY),
            0x02 => Some(Self::MYSQL_TYPE_SHORT),
            0x03 => Some(Self::MYSQL_TYPE_LONG),
            0x04 => Some(Self::MYSQL_TYPE_FLOAT),
            0x05 => Some(Self::MYSQL_TYPE_DOUBLE),
            0x06 => Some(Self::MYSQL_TYPE_NULL),
            0x07 => Some(Self::MYSQL_TYPE_TIMESTAMP),
            0x08 => Some(Self::MYSQL_TYPE_LONGLONG),
            0x09 => Some(Self::MYSQL_TYPE_INT24),
            0x0a => Some(Self::MYSQL_TYPE_DATE),
            0x0b => Some(Self::MYSQL_TYPE_TIME),
            0x0c => Some(Self::MYSQL_TYPE_DATETIME),
            0x0d => Some(Self::MYSQL_TYPE_YEAR),
            0x0e => Some(Self::MYSQL_TYPE_NEWDATE),
            0x0f => Some(Self::MYSQL_TYPE_VARCHAR),
            0x10 => Some(Self::MYSQL_TYPE_BIT),
            0x11 => Some(Self::MYSQL_TYPE_TIMESTAMP2),
            0x12 => Some(Self::MYSQL_TYPE_DATETIME2),
            0x13 => Some(Self::MYSQL_TYPE_TIME2),
            0x14 => Some(Self::MYSQL_TYPE_TYPED_ARRAY),
            0xf5 => Some(Self::MYSQL_TYPE_JSON),
            0xf6 => Some(Self::MYSQL_TYPE_NEWDECIMAL),
            0xf7 => Some(Self::MYSQL_TYPE_ENUM),
            0xf8 => Some(Self::MYSQL_TYPE_SET),
            0xf9 => Some(Self::MYSQL_TYPE_TINY_BLOB),
            0xfa => Some(Self::MYSQL_TYPE_MEDIUM_BLOB),
            0xfb => Some(Self::MYSQL_TYPE_LONG_BLOB),
            0xfc => Some(Self::MYSQL_TYPE_BLOB),
            0xfd => Some(Self::MYSQL_TYPE_VAR_STRING),
            0xfe => Some(Self::MYSQL_TYPE_STRING),
            0xff => Some(Self::MYSQL_TYPE_GEOMETRY),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capability_flags_classification() {
        // All 32 bits set (complete set of possible flags)
        const ALL_FLAGS: u32 = 0xFFFFFFFF;

        let always_enabled = CAPABILITIES_ALWAYS_ENABLED;
        let configurable = CAPABILITIES_CONFIGURABLE;
        let always_disabled = CAPABILITIES_ALWAYS_DISABLED;

        // Test 1: No overlap between categories
        assert!(
            always_enabled.intersection(configurable).is_empty(),
            "ALWAYS_ENABLED and CONFIGURABLE must not overlap"
        );
        assert!(
            always_enabled.intersection(always_disabled).is_empty(),
            "ALWAYS_ENABLED and ALWAYS_DISABLED must not overlap"
        );
        assert!(
            configurable.intersection(always_disabled).is_empty(),
            "CONFIGURABLE and ALWAYS_DISABLED must not overlap"
        );

        // Test 2: Union covers all flags
        let union = always_enabled | configurable | always_disabled;
        assert_eq!(
            union.bits(),
            ALL_FLAGS,
            "Union of all three categories must equal all possible flags (0xFFFFFFFF). Missing flags: 0x{:08X}",
            ALL_FLAGS & !union.bits()
        );

        // Test 3: Verify specific critical flags are in correct categories
        assert!(
            always_enabled.contains(CapabilityFlags::CLIENT_PROTOCOL_41),
            "CLIENT_PROTOCOL_41 must be always enabled"
        );
        assert!(
            always_enabled.contains(CapabilityFlags::CLIENT_PLUGIN_AUTH),
            "CLIENT_PLUGIN_AUTH must be always enabled"
        );
        assert!(
            always_disabled.contains(CapabilityFlags::CLIENT_INTERACTIVE),
            "CLIENT_INTERACTIVE must be always disabled (we're not interactive)"
        );
        assert!(
            configurable.contains(CapabilityFlags::CLIENT_SSL),
            "CLIENT_SSL must be configurable"
        );
    }
}
