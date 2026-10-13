use std::sync::Arc;

use diesel::connection::{
    AnsiTransactionManager, CacheSize, ConnectionSealed, DefaultLoadingMode, DynInstrumentation,
    Instrumentation, LoadConnection, SimpleConnection,
};
use diesel::expression::QueryMetadata;
use diesel::mysql::Mysql;
use diesel::query_builder::{Query, QueryBuilder, QueryFragment, QueryId};
use diesel::result::{ConnectionError, ConnectionResult, QueryResult};

use super::cursor::{CollectRawHandler, Cursor};

pub struct Connection {
    conn: crate::sync::Conn,
    transaction_manager: AnsiTransactionManager,
    instrumentation: DynInstrumentation,
}

#[expect(unsafe_code)]
// SAFETY: sync::Conn owns a TCP stream and buffer set, both of which are Send.
unsafe impl Send for Connection {}

impl SimpleConnection for Connection {
    fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        self.conn.query_drop(query).map_err(into_diesel_error)?;
        Ok(())
    }
}

impl ConnectionSealed for Connection {}

impl diesel::connection::Connection for Connection {
    type Backend = Mysql;
    type TransactionManager = AnsiTransactionManager;

    fn establish(database_url: &str) -> ConnectionResult<Self> {
        let opts = crate::Opts::try_from(database_url)
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;
        let conn = crate::sync::Conn::new(opts)
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;
        Ok(Self {
            conn,
            transaction_manager: AnsiTransactionManager::default(),
            instrumentation: DynInstrumentation::default_instrumentation(),
        })
    }

    fn execute_returning_count<T>(&mut self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Mysql> + QueryId,
    {
        let sql = self.build_query(source)?;
        let mut handler = crate::handler::DropHandler::default();
        self.conn
            .query(&sql, &mut handler)
            .map_err(into_diesel_error)?;
        Ok(handler.affected_rows() as usize)
    }

    fn transaction_state(&mut self) -> &mut AnsiTransactionManager {
        &mut self.transaction_manager
    }

    fn instrumentation(&mut self) -> &mut dyn Instrumentation {
        &mut *self.instrumentation
    }

    fn set_instrumentation(&mut self, instrumentation: impl Instrumentation) {
        self.instrumentation = instrumentation.into();
    }

    fn set_prepared_statement_cache_size(&mut self, _size: CacheSize) {
        // zero-mysql manages its own statement lifecycle
    }
}

impl LoadConnection<DefaultLoadingMode> for Connection {
    type Cursor<'conn, 'query> = Cursor;
    type Row<'conn, 'query> = super::row::ZeroMysqlRow;

    fn load<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> QueryResult<Self::Cursor<'conn, 'query>>
    where
        T: Query + QueryFragment<Mysql> + QueryId + 'query,
        Mysql: QueryMetadata<T::SqlType>,
    {
        let sql = self.build_query(&source)?;
        let mut stmt = self.conn.prepare(&sql).map_err(into_diesel_error)?;
        let mut handler = CollectRawHandler::new();
        self.conn
            .exec(&mut stmt, (), &mut handler)
            .map_err(into_diesel_error)?;
        let columns: Arc<[_]> = handler.columns.into();
        Ok(Cursor::new(columns, handler.rows))
    }
}

impl Connection {
    fn build_query<T: QueryFragment<Mysql>>(&mut self, source: &T) -> QueryResult<String> {
        let mut qb = diesel::mysql::MysqlQueryBuilder::default();
        source.to_sql(&mut qb, &Mysql)?;
        Ok(qb.finish())
    }
}

fn into_diesel_error(e: crate::error::Error) -> diesel::result::Error {
    match &e {
        crate::error::Error::ServerError(server_error) => {
            let code = server_error.error_code;
            let kind = match code {
                // ER_DUP_ENTRY, ER_DUP_ENTRY_WITH_KEY_NAME, ER_DUP_UNKNOWN_IN_INDEX
                1062 | 1586 | 1859 => diesel::result::DatabaseErrorKind::UniqueViolation,
                // ER_NO_REFERENCED_ROW, ER_ROW_IS_REFERENCED, ER_ROW_IS_REFERENCED_2,
                // ER_NO_REFERENCED_ROW_2, ER_FK_COLUMN_NOT_NULL, ER_FK_CANNOT_DELETE_PARENT
                1216 | 1217 | 1451 | 1452 | 1830 | 1834 => {
                    diesel::result::DatabaseErrorKind::ForeignKeyViolation
                }
                // ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION
                1792 => diesel::result::DatabaseErrorKind::ReadOnlyTransaction,
                // ER_BAD_NULL_ERROR, ER_NO_DEFAULT_FOR_FIELD
                1048 | 1364 => diesel::result::DatabaseErrorKind::NotNullViolation,
                // ER_CHECK_CONSTRAINT_VIOLATED
                3819 => diesel::result::DatabaseErrorKind::CheckViolation,
                // ER_LOCK_DEADLOCK
                1213 => diesel::result::DatabaseErrorKind::SerializationFailure,
                _ => diesel::result::DatabaseErrorKind::Unknown,
            };
            diesel::result::Error::DatabaseError(
                kind,
                Box::new(ServerErrorInfo {
                    message: server_error.message.clone(),
                }),
            )
        }
        _ => diesel::result::Error::DatabaseError(
            diesel::result::DatabaseErrorKind::Unknown,
            Box::new(e.to_string()),
        ),
    }
}

#[derive(Debug)]
struct ServerErrorInfo {
    message: String,
}

impl diesel::result::DatabaseErrorInformation for ServerErrorInfo {
    fn message(&self) -> &str {
        &self.message
    }

    fn details(&self) -> Option<&str> {
        None
    }

    fn hint(&self) -> Option<&str> {
        None
    }

    fn table_name(&self) -> Option<&str> {
        None
    }

    fn column_name(&self) -> Option<&str> {
        None
    }

    fn constraint_name(&self) -> Option<&str> {
        None
    }

    fn statement_position(&self) -> Option<i32> {
        None
    }
}
