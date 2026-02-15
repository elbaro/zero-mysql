use super::Conn;
use crate::error::{Error, Result};

/// A MySQL transaction for the compio async connection.
pub struct Transaction {
    connection_id: u64,
}

impl Transaction {
    pub(crate) fn new(connection_id: u64) -> Self {
        Self { connection_id }
    }

    pub async fn commit(self, conn: &mut Conn) -> Result<()> {
        let actual = conn.connection_id();
        if self.connection_id != actual {
            return Err(Error::ConnectionMismatch {
                expected: self.connection_id,
                actual,
            });
        }
        conn.set_in_transaction(false);
        conn.query_drop("COMMIT").await
    }

    pub async fn rollback(self, conn: &mut Conn) -> Result<()> {
        let actual = conn.connection_id();
        if self.connection_id != actual {
            return Err(Error::ConnectionMismatch {
                expected: self.connection_id,
                actual,
            });
        }
        conn.set_in_transaction(false);
        conn.query_drop("ROLLBACK").await
    }
}
