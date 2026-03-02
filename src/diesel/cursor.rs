use std::os::raw as libc;
use std::sync::Arc;

use diesel::QueryResult;
use diesel::mysql::MysqlType;
use diesel::mysql::data_types::{MysqlTime, MysqlTimestampType};

use crate::constant::{ColumnFlags, ColumnType};
use crate::error::eyre;
use crate::protocol::BinaryRowPayload;
use crate::protocol::command::ColumnDefinition;
use crate::protocol::primitive::read_string_lenenc;
use crate::protocol::response::OkPayloadBytes;
use crate::protocol::r#trait::BinaryResultSetHandler;

use super::row::ZeroMysqlRow;

pub struct ColumnInfo {
    pub name: String,
    pub column_type: MysqlType,
}

pub struct Cursor {
    columns: Arc<[ColumnInfo]>,
    rows: Vec<Vec<Option<Vec<u8>>>>,
    current: usize,
}

impl Cursor {
    pub(in crate::diesel) fn new(
        columns: Arc<[ColumnInfo]>,
        rows: Vec<Vec<Option<Vec<u8>>>>,
    ) -> Self {
        Self {
            columns,
            rows,
            current: 0,
        }
    }
}

impl Iterator for Cursor {
    type Item = QueryResult<ZeroMysqlRow>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.rows.len() {
            return None;
        }
        let idx = self.current;
        self.current += 1;
        let values = std::mem::take(&mut self.rows[idx]);
        Some(Ok(ZeroMysqlRow {
            columns: Arc::clone(&self.columns),
            values,
        }))
    }
}

pub(in crate::diesel) struct CollectRawHandler {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
}

impl CollectRawHandler {
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }
}

/// Map zero-mysql ColumnType + ColumnFlags to diesel MysqlType.
fn to_mysql_type(col_type: ColumnType, flags: ColumnFlags) -> MysqlType {
    let unsigned = flags.contains(ColumnFlags::UNSIGNED_FLAG);
    match col_type {
        ColumnType::MYSQL_TYPE_TINY => {
            if unsigned {
                MysqlType::UnsignedTiny
            } else {
                MysqlType::Tiny
            }
        }
        ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
            if unsigned {
                MysqlType::UnsignedShort
            } else {
                MysqlType::Short
            }
        }
        ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => {
            if unsigned {
                MysqlType::UnsignedLong
            } else {
                MysqlType::Long
            }
        }
        ColumnType::MYSQL_TYPE_LONGLONG => {
            if unsigned {
                MysqlType::UnsignedLongLong
            } else {
                MysqlType::LongLong
            }
        }
        ColumnType::MYSQL_TYPE_FLOAT => MysqlType::Float,
        ColumnType::MYSQL_TYPE_DOUBLE => MysqlType::Double,
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => MysqlType::Numeric,
        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => MysqlType::Date,
        ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => MysqlType::Time,
        ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_DATETIME2 => MysqlType::DateTime,
        ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_TIMESTAMP2 => {
            MysqlType::Timestamp
        }
        ColumnType::MYSQL_TYPE_BIT => MysqlType::Bit,
        ColumnType::MYSQL_TYPE_ENUM => MysqlType::Enum,
        ColumnType::MYSQL_TYPE_SET => MysqlType::Set,
        ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB
        | ColumnType::MYSQL_TYPE_BLOB => MysqlType::Blob,
        ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_VAR_STRING
        | ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_JSON
        | ColumnType::MYSQL_TYPE_GEOMETRY
        | ColumnType::MYSQL_TYPE_TYPED_ARRAY
        | ColumnType::MYSQL_TYPE_NULL => MysqlType::String,
    }
}

/// Convert wire-format date/time bytes to a `MysqlTime` struct, then return its raw bytes.
///
/// Diesel's `FromSql` for date/time types expects the raw bytes of a C `MYSQL_TIME` struct,
/// which is different from the compact MySQL binary protocol wire format.
fn wire_datetime_to_bytes(data: &[u8], col_type: ColumnType) -> Vec<u8> {
    let time = match col_type {
        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => {
            let (year, month, day) = if let Some(d) = data.first_chunk::<4>() {
                (
                    u16::from_le_bytes([d[0], d[1]]) as libc::c_uint,
                    d[2] as libc::c_uint,
                    d[3] as libc::c_uint,
                )
            } else {
                (0, 0, 0)
            };
            MysqlTime::new(
                year,
                month,
                day,
                0,
                0,
                0,
                0,
                false,
                MysqlTimestampType::MYSQL_TIMESTAMP_DATE,
                0,
            )
        }
        ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => {
            let (neg, hours, minutes, seconds, usec) = match data.len() {
                0 => (false, 0u32, 0u32, 0u32, 0u64),
                8 => {
                    if let Some(d) = data.first_chunk::<8>() {
                        let neg = d[0] != 0;
                        let days = u32::from_le_bytes([d[1], d[2], d[3], d[4]]);
                        (neg, days * 24 + d[5] as u32, d[6] as u32, d[7] as u32, 0)
                    } else {
                        (false, 0, 0, 0, 0)
                    }
                }
                12 => {
                    if let Some(d) = data.first_chunk::<12>() {
                        let neg = d[0] != 0;
                        let days = u32::from_le_bytes([d[1], d[2], d[3], d[4]]);
                        let usec = u32::from_le_bytes([d[8], d[9], d[10], d[11]]) as u64;
                        (neg, days * 24 + d[5] as u32, d[6] as u32, d[7] as u32, usec)
                    } else {
                        (false, 0, 0, 0, 0)
                    }
                }
                _ => (false, 0, 0, 0, 0),
            };
            MysqlTime::new(
                0,
                0,
                0,
                hours,
                minutes,
                seconds,
                usec as libc::c_ulong,
                neg,
                MysqlTimestampType::MYSQL_TIMESTAMP_TIME,
                0,
            )
        }
        // DATETIME, TIMESTAMP
        _ => {
            let (year, month, day, hour, minute, second, usec) = match data.len() {
                0 => (0u32, 0u32, 0u32, 0u32, 0u32, 0u32, 0u64),
                4 => {
                    if let Some(d) = data.first_chunk::<4>() {
                        (
                            u16::from_le_bytes([d[0], d[1]]) as u32,
                            d[2] as u32,
                            d[3] as u32,
                            0,
                            0,
                            0,
                            0,
                        )
                    } else {
                        (0, 0, 0, 0, 0, 0, 0)
                    }
                }
                7 => {
                    if let Some(d) = data.first_chunk::<7>() {
                        (
                            u16::from_le_bytes([d[0], d[1]]) as u32,
                            d[2] as u32,
                            d[3] as u32,
                            d[4] as u32,
                            d[5] as u32,
                            d[6] as u32,
                            0,
                        )
                    } else {
                        (0, 0, 0, 0, 0, 0, 0)
                    }
                }
                11 => {
                    if let Some(d) = data.first_chunk::<11>() {
                        (
                            u16::from_le_bytes([d[0], d[1]]) as u32,
                            d[2] as u32,
                            d[3] as u32,
                            d[4] as u32,
                            d[5] as u32,
                            d[6] as u32,
                            u32::from_le_bytes([d[7], d[8], d[9], d[10]]) as u64,
                        )
                    } else {
                        (0, 0, 0, 0, 0, 0, 0)
                    }
                }
                _ => (0, 0, 0, 0, 0, 0, 0),
            };
            let timestamp_type = match col_type {
                ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_TIMESTAMP2 => {
                    MysqlTimestampType::MYSQL_TIMESTAMP_DATETIME
                }
                _ => MysqlTimestampType::MYSQL_TIMESTAMP_DATETIME,
            };
            MysqlTime::new(
                year,
                month,
                day,
                hour,
                minute,
                second,
                usec as libc::c_ulong,
                false,
                timestamp_type,
                0,
            )
        }
    };

    mysql_time_to_bytes(&time)
}

#[expect(unsafe_code)]
fn mysql_time_to_bytes(time: &MysqlTime) -> Vec<u8> {
    let size = std::mem::size_of::<MysqlTime>();
    let mut bytes = vec![0u8; size];
    // SAFETY: MysqlTime is a repr(C) struct with no padding requirements beyond alignment.
    // We copy the raw bytes into a Vec<u8> for diesel's MysqlValue consumption.
    unsafe {
        std::ptr::copy_nonoverlapping(
            time as *const MysqlTime as *const u8,
            bytes.as_mut_ptr(),
            size,
        );
    }
    bytes
}

impl BinaryResultSetHandler for CollectRawHandler {
    fn no_result_set(&mut self, _ok: OkPayloadBytes) -> crate::error::Result<()> {
        Ok(())
    }

    fn resultset_start(&mut self, cols: &[ColumnDefinition<'_>]) -> crate::error::Result<()> {
        self.columns = cols
            .iter()
            .map(|c| {
                let col_type = c.tail.column_type()?;
                let flags = c.tail.flags()?;
                Ok(ColumnInfo {
                    name: String::from_utf8_lossy(c.name_alias).into_owned(),
                    column_type: to_mysql_type(col_type, flags),
                })
            })
            .collect::<crate::error::Result<Vec<_>>>()?;
        Ok(())
    }

    fn row(
        &mut self,
        cols: &[ColumnDefinition<'_>],
        row: BinaryRowPayload<'_>,
    ) -> crate::error::Result<()> {
        let null_bitmap = row.null_bitmap();
        let mut data = row.values();
        let mut values = Vec::with_capacity(self.columns.len());

        for (i, col) in cols.iter().enumerate() {
            if null_bitmap.is_null(i) {
                values.push(None);
                continue;
            }

            let col_type = col.tail.column_type()?;

            match col_type {
                ColumnType::MYSQL_TYPE_NULL => {
                    values.push(None);
                }

                // 1-byte integer
                ColumnType::MYSQL_TYPE_TINY => {
                    let (chunk, rest) = data.split_first_chunk::<1>().ok_or_else(|| {
                        crate::error::Error::LibraryBug(eyre!("truncated TINY column"))
                    })?;
                    values.push(Some(chunk.to_vec()));
                    data = rest;
                }

                // 2-byte integer
                ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
                    let (chunk, rest) = data.split_first_chunk::<2>().ok_or_else(|| {
                        crate::error::Error::LibraryBug(eyre!("truncated SHORT column"))
                    })?;
                    values.push(Some(chunk.to_vec()));
                    data = rest;
                }

                // 4-byte integer/float
                ColumnType::MYSQL_TYPE_INT24
                | ColumnType::MYSQL_TYPE_LONG
                | ColumnType::MYSQL_TYPE_FLOAT => {
                    let (chunk, rest) = data.split_first_chunk::<4>().ok_or_else(|| {
                        crate::error::Error::LibraryBug(eyre!("truncated LONG column"))
                    })?;
                    values.push(Some(chunk.to_vec()));
                    data = rest;
                }

                // 8-byte integer/double
                ColumnType::MYSQL_TYPE_LONGLONG | ColumnType::MYSQL_TYPE_DOUBLE => {
                    let (chunk, rest) = data.split_first_chunk::<8>().ok_or_else(|| {
                        crate::error::Error::LibraryBug(eyre!("truncated LONGLONG column"))
                    })?;
                    values.push(Some(chunk.to_vec()));
                    data = rest;
                }

                // Date/time: variable-length wire format → MysqlTime struct bytes
                ColumnType::MYSQL_TYPE_DATE
                | ColumnType::MYSQL_TYPE_NEWDATE
                | ColumnType::MYSQL_TYPE_DATETIME
                | ColumnType::MYSQL_TYPE_DATETIME2
                | ColumnType::MYSQL_TYPE_TIMESTAMP
                | ColumnType::MYSQL_TYPE_TIMESTAMP2
                | ColumnType::MYSQL_TYPE_TIME
                | ColumnType::MYSQL_TYPE_TIME2 => {
                    let (&len_byte, payload) = data.split_first().ok_or_else(|| {
                        crate::error::Error::LibraryBug(eyre!("truncated datetime length"))
                    })?;
                    let len = len_byte as usize;
                    let (dt_data, rest) = payload.split_at_checked(len).ok_or_else(|| {
                        crate::error::Error::LibraryBug(eyre!("truncated datetime payload"))
                    })?;
                    values.push(Some(wire_datetime_to_bytes(dt_data, col_type)));
                    data = rest;
                }

                // Length-encoded string/blob/decimal
                _ => {
                    let (bytes, rest) = read_string_lenenc(data)?;
                    values.push(Some(bytes.to_vec()));
                    data = rest;
                }
            }
        }

        self.rows.push(values);
        Ok(())
    }

    fn resultset_end(&mut self, _eof: OkPayloadBytes) -> crate::error::Result<()> {
        Ok(())
    }
}
