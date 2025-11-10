/// MySQL command bytes
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

/// Client capability flags
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapabilityFlags(pub u32);

impl CapabilityFlags {
    pub const CLIENT_LONG_PASSWORD: u32 = 0x00000001;
    pub const CLIENT_FOUND_ROWS: u32 = 0x00000002;
    pub const CLIENT_LONG_FLAG: u32 = 0x00000004;
    pub const CLIENT_CONNECT_WITH_DB: u32 = 0x00000008;
    pub const CLIENT_NO_SCHEMA: u32 = 0x00000010;
    pub const CLIENT_COMPRESS: u32 = 0x00000020;
    pub const CLIENT_ODBC: u32 = 0x00000040;
    pub const CLIENT_LOCAL_FILES: u32 = 0x00000080;
    pub const CLIENT_IGNORE_SPACE: u32 = 0x00000100;
    pub const CLIENT_PROTOCOL_41: u32 = 0x00000200;
    pub const CLIENT_INTERACTIVE: u32 = 0x00000400;
    pub const CLIENT_SSL: u32 = 0x00000800;
    pub const CLIENT_IGNORE_SIGPIPE: u32 = 0x00001000;
    pub const CLIENT_TRANSACTIONS: u32 = 0x00002000;
    pub const CLIENT_RESERVED: u32 = 0x00004000;
    pub const CLIENT_RESERVED2: u32 = 0x00008000;
    pub const CLIENT_MULTI_STATEMENTS: u32 = 0x00010000;
    pub const CLIENT_MULTI_RESULTS: u32 = 0x00020000;
    pub const CLIENT_PS_MULTI_RESULTS: u32 = 0x00040000;
    pub const CLIENT_PLUGIN_AUTH: u32 = 0x00080000;
    pub const CLIENT_CONNECT_ATTRS: u32 = 0x00100000;
    pub const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 0x00200000;
    pub const CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS: u32 = 0x00400000;
    pub const CLIENT_SESSION_TRACK: u32 = 0x00800000;
    pub const CLIENT_DEPRECATE_EOF: u32 = 0x01000000;
    pub const CLIENT_OPTIONAL_RESULTSET_METADATA: u32 = 0x02000000;
    pub const CLIENT_ZSTD_COMPRESSION_ALGORITHM: u32 = 0x04000000;
    pub const CLIENT_QUERY_ATTRIBUTES: u32 = 0x08000000;
    pub const CLIENT_MULTI_FACTOR_AUTHENTICATION: u32 = 0x10000000;
    pub const CLIENT_CAPABILITY_EXTENSION: u32 = 0x20000000;
    pub const CLIENT_SSL_VERIFY_SERVER_CERT: u32 = 0x40000000;
    pub const CLIENT_REMEMBER_OPTIONS: u32 = 0x80000000;

    pub fn new(value: u32) -> Self {
        Self(value)
    }

    pub fn has(&self, flag: u32) -> bool {
        (self.0 & flag) != 0
    }

    pub fn set(&mut self, flag: u32) {
        self.0 |= flag;
    }

    pub fn unset(&mut self, flag: u32) {
        self.0 &= !flag;
    }
}

/// Server status flags
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusFlags(pub u16);

impl StatusFlags {
    pub const SERVER_STATUS_IN_TRANS: u16 = 0x0001;
    pub const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;
    pub const SERVER_MORE_RESULTS_EXISTS: u16 = 0x0008;
    pub const SERVER_STATUS_NO_GOOD_INDEX_USED: u16 = 0x0010;
    pub const SERVER_STATUS_NO_INDEX_USED: u16 = 0x0020;
    pub const SERVER_STATUS_CURSOR_EXISTS: u16 = 0x0040;
    pub const SERVER_STATUS_LAST_ROW_SENT: u16 = 0x0080;
    pub const SERVER_STATUS_DB_DROPPED: u16 = 0x0100;
    pub const SERVER_STATUS_NO_BACKSLASH_ESCAPES: u16 = 0x0200;
    pub const SERVER_STATUS_METADATA_CHANGED: u16 = 0x0400;
    pub const SERVER_QUERY_WAS_SLOW: u16 = 0x0800;
    pub const SERVER_PS_OUT_PARAMS: u16 = 0x1000;
    pub const SERVER_STATUS_IN_TRANS_READONLY: u16 = 0x2000;
    pub const SERVER_SESSION_STATE_CHANGED: u16 = 0x4000;

    pub fn new(value: u16) -> Self {
        Self(value)
    }

    pub fn has(&self, flag: u16) -> bool {
        (self.0 & flag) != 0
    }
}

/// Column definition flags
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnFlags(pub u16);

impl ColumnFlags {
    pub const NOT_NULL_FLAG: u16 = 0x0001;
    pub const PRI_KEY_FLAG: u16 = 0x0002;
    pub const UNIQUE_KEY_FLAG: u16 = 0x0004;
    pub const MULTIPLE_KEY_FLAG: u16 = 0x0008;
    pub const BLOB_FLAG: u16 = 0x0010;
    pub const UNSIGNED_FLAG: u16 = 0x0020;
    pub const ZEROFILL_FLAG: u16 = 0x0040;
    pub const BINARY_FLAG: u16 = 0x0080;
    pub const ENUM_FLAG: u16 = 0x0100;
    pub const AUTO_INCREMENT_FLAG: u16 = 0x0200;
    pub const TIMESTAMP_FLAG: u16 = 0x0400;
    pub const SET_FLAG: u16 = 0x0800;
    pub const NO_DEFAULT_VALUE_FLAG: u16 = 0x1000;
    pub const ON_UPDATE_NOW_FLAG: u16 = 0x2000;
    pub const NUM_FLAG: u16 = 0x8000;

    pub fn new(value: u16) -> Self {
        Self(value)
    }

    pub fn has(&self, flag: u16) -> bool {
        (self.0 & flag) != 0
    }
}

/// MySQL column types
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
