use super::Conn;
use crate::error::{Error, Result};

/// A MySQL transaction for the synchronous connection
///
/// This struct provides transaction control. The connection is passed
/// to `commit` and `rollback` methods to execute the transaction commands.
pub struct Transaction {
    connection_id: u64,
}

impl Transaction {
    /// Create a new transaction (internal use only)
    pub(crate) fn new(connection_id: u64) -> Self {
        Self { connection_id }
    }

    /// Commit the transaction
    ///
    /// This consumes the transaction and sends a COMMIT statement to the server.
    /// The connection must be passed as an argument to execute the commit.
    ///
    /// # Errors
    ///
    /// Returns `Error::ConnectionMismatch` if the connection is not the same
    /// as the one that started the transaction.
    pub fn commit(self, conn: &mut Conn) -> Result<()> {
        let actual = conn.connection_id();
        if self.connection_id != actual {
            return Err(Error::ConnectionMismatch {
                expected: self.connection_id,
                actual,
            });
        }
        conn.set_in_transaction(false);
        conn.query_drop("COMMIT")
    }

    /// Rollback the transaction
    ///
    /// This consumes the transaction and sends a ROLLBACK statement to the server.
    /// The connection must be passed as an argument to execute the rollback.
    ///
    /// # Errors
    ///
    /// Returns `Error::ConnectionMismatch` if the connection is not the same
    /// as the one that started the transaction.
    pub fn rollback(self, conn: &mut Conn) -> Result<()> {
        let actual = conn.connection_id();
        if self.connection_id != actual {
            return Err(Error::ConnectionMismatch {
                expected: self.connection_id,
                actual,
            });
        }
        conn.set_in_transaction(false);
        conn.query_drop("ROLLBACK")
    }
}
