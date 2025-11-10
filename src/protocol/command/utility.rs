use crate::constant::CommandByte;
use crate::protocol::primitive::*;

/// Write COM_QUIT command
pub fn write_quit(out: &mut Vec<u8>) {
    write_int_1(out, CommandByte::Quit as u8);
}

/// Write COM_PING command
pub fn write_ping(out: &mut Vec<u8>) {
    write_int_1(out, CommandByte::Ping as u8);
}

/// Write COM_INIT_DB command
pub fn write_init_db(out: &mut Vec<u8>, database: &str) {
    write_int_1(out, CommandByte::InitDb as u8);
    out.extend_from_slice(database.as_bytes());
}

/// Write COM_RESET_CONNECTION command
pub fn write_reset_connection(out: &mut Vec<u8>) {
    write_int_1(out, CommandByte::ResetConnection as u8);
}
