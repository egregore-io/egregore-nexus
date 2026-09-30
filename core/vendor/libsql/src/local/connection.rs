#![allow(dead_code)]

use crate::local::rows::BatchedRows;
use crate::params::Params;
use crate::{connection::BatchRows, errors};

use super::{Database, Error, Result, Rows, RowsFuture, Statement, Transaction};

use crate::TransactionBehavior;

use libsql_sys::ffi;
use std::{
    ffi::c_int,
    fmt,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

/// A connection to a libSQL database.
#[derive(Clone)]
pub struct Connection {
    pub(crate) raw: *mut ffi::sqlite3,

    drop_ref: Arc<()>,
    quarantined: Arc<AtomicBool>,

    #[cfg(feature = "replication")]
    pub(crate) writer: Option<crate::replication::Writer>,
}

/// Hold SQLite's recursive connection mutex through the error code/message copy.
/// Per-call SQLite locking does not protect the gap after a failed operation.
/// The borrow keeps the connection alive, and the raw pointer makes this guard
/// !Send. It is only held in synchronous code, never across an await.
pub(super) struct ConnectionGuard<'a> {
    _conn: &'a Connection,
    mutex: *mut ffi::sqlite3_mutex,
}

impl Drop for ConnectionGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: lock() entered this mutex on this thread and the connection
        // remains alive for the guard's lifetime.
        unsafe { ffi::sqlite3_mutex_leave(self.mutex) };
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.disconnect()
    }
}

// SAFETY: SQLite is compiled with SQLITE_THREADSAFE=1, connect requests FULLMUTEX,
// and from_handle requires the same serialized connection mode.
unsafe impl Send for Connection {}
// SAFETY: Same serialized connection invariant as Send above.
unsafe impl Sync for Connection {}

impl Connection {
    pub(super) fn lock(&self) -> Result<ConnectionGuard<'_>> {
        // SAFETY: self owns a live SQLite connection. Connections opened here
        // explicitly request serialized mode; imported handles must do likewise.
        let mutex = unsafe { ffi::sqlite3_db_mutex(self.raw) };
        assert!(
            !mutex.is_null(),
            "local libSQL connections require SQLite serialized threading mode"
        );
        // SQLite uses a recursive mutex, so its own locking remains reentrant.
        unsafe { ffi::sqlite3_mutex_enter(mutex) };
        let guard = ConnectionGuard { _conn: self, mutex };
        // Check only after acquisition: a waiter may have started before the
        // current owner failed cleanup. Cleanup/finalization do not call lock.
        if self.quarantined.load(Ordering::Acquire) {
            return Err(Error::Misuse(
                "connection quarantined after unconfirmed transaction rollback".into(),
            ));
        }
        Ok(guard)
    }

    /// Connect to the database.
    pub(crate) fn connect(db: &Database) -> Result<Connection> {
        let mut raw = std::ptr::null_mut();
        let db_path = db.db_path.clone();
        let err = unsafe {
            ffi::sqlite3_open_v2(
                std::ffi::CString::new(db_path.as_str())
                    .unwrap()
                    .as_c_str()
                    .as_ptr() as *const _,
                &mut raw,
                db.flags.bits() as c_int | ffi::SQLITE_OPEN_FULLMUTEX,
                std::ptr::null(),
            )
        };
        match err {
            ffi::SQLITE_OK => {}
            _ => {
                return Err(Error::ConnectionFailed(format!(
                    "Unable to open connection to local database {db_path}: {err}",
                )));
            }
        }

        Ok(Connection {
            raw,
            drop_ref: Arc::new(()),
            quarantined: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "replication")]
            writer: db.writer()?,
        })
    }

    /// Get a raw handle to the underlying libSQL connection
    pub fn handle(&self) -> *mut ffi::sqlite3 {
        self.raw
    }

    /// Create a connection from a raw handle to the underlying libSQL connection
    ///
    /// The handle must use SQLite's serialized threading mode (a non-null
    /// sqlite3_db_mutex), as required by this connection's Send + Sync contract.
    pub fn from_handle(raw: *mut ffi::sqlite3) -> Self {
        Self {
            raw,
            drop_ref: Arc::new(()),
            quarantined: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "replication")]
            writer: None,
        }
    }

    /// Disconnect from the database.
    pub fn disconnect(&mut self) {
        // `LibsqlConnection::drop` explicitly disconnects before this value's own `Drop` runs.
        // Make that two-stage teardown idempotent: calling sqlite3_close_v2 twice on the same
        // allocation is a use-after-free that manifests as an access violation on Windows.
        if self.raw.is_null() {
            return;
        }
        if Arc::get_mut(&mut self.drop_ref).is_some() {
            unsafe { libsql_sys::ffi::sqlite3_close_v2(self.raw) };
            self.raw = std::ptr::null_mut();
        }
    }

    /// Prepare the SQL statement.
    pub fn prepare<S: Into<String>>(&self, sql: S) -> Result<Statement> {
        Statement::prepare(self.clone(), self.raw, sql.into().as_str())
    }

    /// Convenience method to run a prepared statement query.
    /// ## Example
    ///
    /// ```rust,no_run,ignore
    /// # use libsql::Result;
    /// # use libsql::v1::{Connection, Rows};
    /// # fn create_tables(conn: &Connection) -> Result<Option<Rows>> {
    /// conn.query("SELECT * FROM users WHERE name = ?1;", vec![libsql::Value::from(1)])
    /// # }
    /// ```
    pub fn query<S, P>(&self, sql: S, params: P) -> Result<Option<Rows>>
    where
        S: Into<String>,
        P: TryInto<Params>,
        P::Error: Into<crate::BoxError>,
    {
        let stmt = Statement::prepare(self.clone(), self.raw, sql.into().as_str())?;
        let params = params
            .try_into()
            .map_err(|e| Error::ToSqlConversionFailure(e.into()))?;
        let ret = stmt.query(&params)?;
        Ok(Some(ret))
    }

    /// Convenience method to run multiple SQL statements (that cannot take any
    /// parameters).
    ///
    /// ## Example
    ///
    /// ```rust,no_run,ignore
    /// # use libsql::Result;
    /// # use libsql::v1::Connection;
    /// # fn create_tables(conn: &Connection) -> Result<()> {
    /// conn.execute_batch(
    ///     "BEGIN;
    ///     CREATE TABLE foo(x INTEGER);
    ///     CREATE TABLE bar(y TEXT);
    ///     COMMIT;",
    /// )
    /// # }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying SQLite call fails.
    pub fn execute_batch<S>(&self, sql: S) -> Result<BatchRows>
    where
        S: Into<String>,
    {
        let sql = sql.into();
        let mut sql = sql.as_str();

        let mut batch_rows = Vec::new();

        while !sql.is_empty() {
            let stmt = self.prepare(sql)?;

            let tail = if !stmt.inner.raw_stmt.is_null() {
                let returned_rows = stmt.step()?;

                let tail = stmt.tail();

                // Check if there are rows to be extracted, we must do this upfront due to the lazy
                // nature of sqlite and our somewhat hacked batch command.
                if returned_rows {
                    // Extract columns
                    let cols = stmt
                        .columns()
                        .iter()
                        .enumerate()
                        .map(|(i, c)| {
                            use crate::value::ValueType;

                            let val = stmt.inner.column_type(i as i32);
                            let t = match val {
                                libsql_sys::ffi::SQLITE_INTEGER => ValueType::Integer,
                                libsql_sys::ffi::SQLITE_FLOAT => ValueType::Real,
                                libsql_sys::ffi::SQLITE_BLOB => ValueType::Blob,
                                libsql_sys::ffi::SQLITE_TEXT => ValueType::Text,
                                libsql_sys::ffi::SQLITE_NULL => ValueType::Null,
                                _ => unreachable!("unknown column type {} at index {}", val, i),
                            };

                            (c.name.to_string(), t)
                        })
                        .collect::<Vec<_>>();

                    let mut rows = Vec::new();

                    // If returned rows we must extract the rows available right away instead of
                    // using the `Rows` type we have already. This is due to the step api once its
                    // returned SQLITE_ROWS we must extract them before we call step again.
                    {
                        let row = crate::local::Row { stmt: stmt.clone() };

                        let mut values = Vec::with_capacity(cols.len());

                        for i in 0..cols.len() {
                            let value = row.get_value(i as i32)?;

                            values.push(value);
                        }

                        rows.push(values);
                    }

                    // Now we can use the normal rows type to extract any n+1 rows
                    let rows_sys = Rows::new(stmt);

                    while let Some(row) = rows_sys.next()? {
                        let mut values = Vec::with_capacity(cols.len());

                        for i in 0..cols.len() {
                            let value = row.get_value(i as i32)?;

                            values.push(value);
                        }

                        rows.push(values);
                    }

                    rows.len();

                    batch_rows.push(Some(crate::Rows::new(BatchedRows::new(cols, rows))));
                } else {
                    batch_rows.push(None);
                }

                tail
            } else {
                stmt.tail()
            };

            if tail == 0 || tail >= sql.len() {
                break;
            }

            sql = &sql[tail..];
        }

        Ok(BatchRows::new(batch_rows))
    }

    fn execute_transactional_batch_inner<S>(&self, sql: S) -> Result<()>
    where
        S: Into<String>,
    {
        let sql = sql.into();
        let mut sql = sql.as_str();
        while !sql.is_empty() {
            let stmt = self.prepare(sql)?;

            let tail = stmt.tail();
            let stmt_sql = if tail == 0 || tail >= sql.len() {
                sql
            } else {
                &sql[..tail]
            };
            let prefix_count = stmt_sql.chars().take_while(|c| c.is_whitespace()).count();
            let stmt_sql = &stmt_sql[prefix_count..];
            if stmt_sql.starts_with("BEGIN")
                || stmt_sql.starts_with("COMMIT")
                || stmt_sql.starts_with("ROLLBACK")
                || stmt_sql.starts_with("END")
            {
                return Err(Error::TransactionalBatchError(
                    "Transactions forbidden inside transactional batch".to_string(),
                ));
            }

            if !stmt.inner.raw_stmt.is_null() {
                stmt.step()?;
            }

            if tail == 0 || tail >= sql.len() {
                break;
            }

            sql = &sql[tail..];
        }

        Ok(())
    }

    pub fn execute_transactional_batch<S>(&self, sql: S) -> Result<()>
    where
        S: Into<String>,
    {
        let sql = sql.into();
        // Keep other clones (including already-prepared statements) out of this
        // synchronous transaction, not merely out of each individual step.
        let _guard = self.lock()?;
        self.execute("BEGIN TRANSACTION", Params::None)?;
        let result = self
            .execute_transactional_batch_inner(sql)
            .and_then(|()| self.execute("COMMIT", Params::None).map(|_| ()));
        match result {
            Ok(()) => Ok(()),
            Err(e) => {
                if !self.is_autocommit() {
                    let rollback = self.execute("ROLLBACK", Params::None);
                    let still_open = !self.is_autocommit();
                    if still_open {
                        self.quarantined.store(true, Ordering::Release);
                    }
                    if let Err(rollback) = rollback {
                        return Err(Error::TransactionalBatchError(format!(
                            "{e}; rollback failed: {rollback}"
                        )));
                    }
                    if still_open {
                        return Err(Error::TransactionalBatchError(format!(
                            "{e}; rollback returned success but transaction remains open"
                        )));
                    }
                }
                Err(e)
            }
        }
    }

    /// Execute the SQL statement synchronously.
    ///
    /// If you execute a SQL query statement (e.g. `SELECT` statement) that
    /// returns the number of rows changed.
    ///
    /// This method blocks the thread until the SQL statement is executed.
    pub fn execute<S, P>(&self, sql: S, params: P) -> Result<u64>
    where
        S: Into<String>,
        P: TryInto<Params>,
        P::Error: Into<crate::BoxError>,
    {
        let stmt = Statement::prepare(self.clone(), self.raw, sql.into().as_str())?;
        let params = params
            .try_into()
            .map_err(|e| Error::ToSqlConversionFailure(e.into()))?;
        stmt.execute(&params)
    }

    /// Execute the SQL statement synchronously.
    ///
    /// This method never blocks the thread until, but instead returns a
    /// `RowsFuture` object immediately that can be used to deferredly
    /// execute the statement.
    pub fn execute_async<S, P>(&self, sql: S, params: P) -> RowsFuture
    where
        S: Into<String>,
        P: Into<Params>,
    {
        RowsFuture {
            conn: self.clone(),
            sql: sql.into(),
            params: params.into(),
        }
    }

    /// Begin a new transaction in DEFERRED mode, which is the default.
    pub fn transaction(&self) -> Result<Transaction> {
        self.transaction_with_behavior(TransactionBehavior::Deferred)
    }

    /// Begin a new transaction in the given mode.
    pub fn transaction_with_behavior(
        &self,
        tx_behavior: TransactionBehavior,
    ) -> Result<Transaction> {
        Transaction::begin(self.clone(), tx_behavior)
    }

    pub fn is_autocommit(&self) -> bool {
        unsafe { ffi::sqlite3_get_autocommit(self.raw) != 0 }
    }

    pub fn changes(&self) -> u64 {
        unsafe { ffi::sqlite3_changes64(self.raw) as u64 }
    }

    pub fn total_changes(&self) -> u64 {
        unsafe { ffi::sqlite3_total_changes(self.raw) as u64 }
    }

    pub fn last_insert_rowid(&self) -> i64 {
        unsafe { ffi::sqlite3_last_insert_rowid(self.raw) }
    }

    #[cfg(feature = "replication")]
    pub(crate) fn writer(&self) -> Option<&crate::replication::Writer> {
        self.writer.as_ref()
    }

    #[cfg(feature = "replication")]
    pub(crate) fn new_connection_writer(&self) -> Option<crate::replication::Writer> {
        self.writer.as_ref().cloned().map(|mut w| {
            w.new_client_id();
            w
        })
    }

    pub fn enable_load_extension(&self, onoff: bool) -> Result<()> {
        // SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION configration verb accepts 2 additional parameters: an on/off flag and a pointer to an c_int where new state of the parameter will be written (or NULL if reporting back the setting is not needed)
        // See: https://sqlite.org/c3ref/c_dbconfig_defensive.html#sqlitedbconfigenableloadextension
        let err = unsafe {
            ffi::sqlite3_db_config(
                self.raw,
                ffi::SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION,
                onoff as i32,
                std::ptr::null::<c_int>(),
            )
        };
        match err {
            ffi::SQLITE_OK => Ok(()),
            _ => Err(errors::Error::SqliteFailure(
                err,
                errors::error_from_code(err),
            )),
        }
    }

    pub fn load_extension(&self, dylib_path: &Path, entry_point: Option<&str>) -> Result<()> {
        let mut raw_err_msg: *mut std::ffi::c_char = std::ptr::null_mut();
        let dylib_path = match dylib_path.to_str() {
            Some(dylib_path) => std::ffi::CString::new(dylib_path).unwrap(),
            None => {
                return Err(crate::Error::Misuse(format!(
                    "dylib path is not a valid utf8 string"
                )))
            }
        };
        let err = match entry_point {
            Some(entry_point) => {
                let entry_point = std::ffi::CString::new(entry_point).unwrap();
                unsafe {
                    ffi::sqlite3_load_extension(
                        self.raw,
                        dylib_path.as_ptr(),
                        entry_point.as_ptr(),
                        &mut raw_err_msg,
                    )
                }
            }
            None => unsafe {
                ffi::sqlite3_load_extension(
                    self.raw,
                    dylib_path.as_ptr(),
                    std::ptr::null(),
                    &mut raw_err_msg,
                )
            },
        };
        match err {
            ffi::SQLITE_OK => Ok(()),
            _ => {
                let err_msg = unsafe { std::ffi::CStr::from_ptr(raw_err_msg) };
                let err_msg = err_msg.to_string_lossy().to_string();
                unsafe { ffi::sqlite3_free(raw_err_msg as *mut std::ffi::c_void) };
                Err(errors::Error::SqliteFailure(err, err_msg))
            }
        }
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection").finish()
    }
}

#[cfg(test)]
#[path = "connection/tests.rs"]
mod batch_cleanup_tests;
