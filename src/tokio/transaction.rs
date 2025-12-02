use super::Conn;
use crate::error::{Error, Result};

/// A MySQL transaction for the asynchronous connection
///
/// This struct provides transaction control. The connection is passed
/// to `commit` and `rollback` methods to execute the transaction commands.
pub struct Transaction {
    connection_id: u64,
}

impl Transaction {
    pub(crate) fn new(connection_id: u64) -> Self {
        Self { connection_id }
    }

    /// Commit the transaction
    ///
    /// Returns `Error::ConnectionMismatch` if the connection is not the same
    /// as the one that started the transaction.
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

    /// Rollback the transaction
    ///
    /// Returns `Error::ConnectionMismatch` if the connection is not the same
    /// as the one that started the transaction.
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
