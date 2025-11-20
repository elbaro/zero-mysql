use crate::error::Result;
use super::Conn;

/// A MySQL transaction for the synchronous connection
///
/// This struct provides transaction control. The connection is passed
/// to `commit` and `rollback` methods to execute the transaction commands.
pub struct Transaction;

impl Transaction {
    /// Create a new transaction (internal use only)
    pub(crate) fn new() -> Self {
        Self
    }

    /// Commit the transaction
    ///
    /// This consumes the transaction and sends a COMMIT statement to the server.
    /// The connection must be passed as an argument to execute the commit.
    pub fn commit(self, conn: &mut Conn) -> Result<()> {
        conn.in_transaction = false;
        conn.query_drop("COMMIT")
    }

    /// Rollback the transaction
    ///
    /// This consumes the transaction and sends a ROLLBACK statement to the server.
    /// The connection must be passed as an argument to execute the rollback.
    pub fn rollback(self, conn: &mut Conn) -> Result<()> {
        conn.in_transaction = false;
        conn.query_drop("ROLLBACK")
    }
}
