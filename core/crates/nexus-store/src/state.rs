//! The [`Store`] handle — the embedded libSQL connection the daemon owns as the sole durable
//! writer. Durable repositories borrow `store.conn`; the volatile stream schemas are attached to
//! that same daemon-owned connection.

use std::sync::Arc;
use std::time::Duration;

use libsql::{params::IntoParams, Builder, Connection, Rows};
use tokio::sync::{Mutex, OwnedMutexGuard};

use nexus_common::NexusError;

use crate::error::store_err;
use crate::events::StoreEventBus;

/// A cloneable handle for the daemon's embedded durable libSQL connection.
#[derive(Clone)]
pub struct StoreConnection {
    current: Connection,
}

impl StoreConnection {
    fn new(conn: Connection) -> Self {
        Self { current: conn }
    }

    /// Execute a statement on the current durable connection.
    pub async fn execute(&self, sql: &str, params: impl IntoParams) -> libsql::Result<u64> {
        const MAX_BUSY_ATTEMPTS: usize = 40;
        const BUSY_RETRY_DELAY: Duration = Duration::from_millis(25);

        let params = params.into_params()?;
        let mut busy_attempt = 0;
        loop {
            let conn = self.raw();
            let result = conn.execute(sql, params.clone()).await;
            match result {
                Ok(changed) => return Ok(changed),
                Err(err) if is_retryable_busy_error(&err) && busy_attempt < MAX_BUSY_ATTEMPTS => {
                    busy_attempt += 1;
                    tokio::time::sleep(BUSY_RETRY_DELAY).await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Execute a batch on the current durable connection.
    pub async fn execute_batch(&self, sql: &str) -> libsql::Result<()> {
        self.raw().execute_batch(sql).await.map(|_| ())
    }

    /// Execute a transactional batch on the current durable connection.
    ///
    /// This is intentionally never retried after an error; callers receive the exact embedded
    /// transaction result and decide how to settle their durable intent.
    pub async fn execute_transactional_batch(&self, sql: &str) -> libsql::Result<()> {
        self.raw()
            .execute_transactional_batch(sql)
            .await
            .map(|_| ())
    }

    /// Query rows from the current durable connection.
    pub async fn query(&self, sql: &str, params: impl IntoParams) -> libsql::Result<Rows> {
        self.raw().query(sql, params).await
    }

    /// Snapshot the current raw libSQL connection for code that must hold one connection across a
    /// multi-step transaction.
    pub fn raw(&self) -> Connection {
        self.current.clone()
    }
}

/// An explicit `BEGIN IMMEDIATE` write transaction over the store connection.
///
/// A `WriteTxn` holds the store's write lock, opens one real embedded transaction, and rolls back
/// from `Drop` if the owning future is cancelled, so an aborted task cannot leave the connection
/// holding the WAL write lock.
pub struct WriteTxn {
    conn: Connection,
    label: String,
    guard: Option<OwnedMutexGuard<()>>,
    active: bool,
}

impl WriteTxn {
    /// Fail loudly if the pinned embedded connection unexpectedly returns to autocommit.
    fn guard_in_txn(&self, when: &str) -> Result<(), NexusError> {
        if self.conn.is_autocommit() {
            return Err(NexusError::Store(format!(
                "write transaction '{}' lost transaction state {when}; the embedded connection is \
                 back in autocommit, so no further statements will be executed",
                self.label
            )));
        }
        Ok(())
    }

    /// Execute a statement inside this transaction.
    pub async fn execute(&self, sql: &str, params: impl IntoParams) -> Result<u64, NexusError> {
        self.guard_in_txn("before a statement")?;
        let changed = self.conn.execute(sql, params).await.map_err(store_err)?;
        self.guard_in_txn("during a statement")?;
        Ok(changed)
    }

    /// Execute a batch inside this transaction.
    pub async fn execute_batch(&self, sql: &str) -> Result<(), NexusError> {
        self.guard_in_txn("before a statement batch")?;
        self.conn.execute_batch(sql).await.map_err(store_err)?;
        self.guard_in_txn("during a statement batch")?;
        Ok(())
    }

    /// Query rows inside this transaction.
    pub async fn query(&self, sql: &str, params: impl IntoParams) -> Result<Rows, NexusError> {
        self.guard_in_txn("before a query")?;
        self.conn.query(sql, params).await.map_err(store_err)
    }

    /// Last insert rowid from this transaction's pinned connection.
    pub fn last_insert_rowid(&self) -> i64 {
        self.conn.last_insert_rowid()
    }

    /// Commit the transaction and release the write lock.
    pub async fn commit(mut self) -> Result<(), NexusError> {
        self.guard_in_txn("at commit")?;
        self.conn.execute("COMMIT", ()).await.map_err(store_err)?;
        self.active = false;
        self.guard.take();
        Ok(())
    }

    /// Roll back after `original` failed. The original error stays primary; a rollback failure is
    /// appended so neither is swallowed. A connection already back in autocommit has no active
    /// transaction left to roll back — that counts as rolled back, not as a second failure.
    pub async fn rollback(mut self, original: &NexusError) -> Result<(), NexusError> {
        if !self.conn.is_autocommit() {
            if let Err(err) = self.conn.execute("ROLLBACK", ()).await {
                return Err(NexusError::Store(format!(
                    "{original}; rollback failed: {err}"
                )));
            }
        }
        self.active = false;
        self.guard.take();
        Ok(())
    }

    /// Confirm only a successful explicit rollback while still owning the writer guard.
    /// Already-autocommit state cannot prove how the transaction ended. On rollback failure,
    /// leave ownership with `Drop`, exactly as the legacy rollback API does.
    pub(crate) async fn rollback_confirmed(
        mut self,
        original: &NexusError,
    ) -> Result<bool, NexusError> {
        let confirmed = if self.conn.is_autocommit() {
            false
        } else {
            if let Err(err) = self.conn.execute("ROLLBACK", ()).await {
                return Err(NexusError::Store(format!(
                    "{original}; rollback failed: {err}"
                )));
            }
            true
        };
        self.active = false;
        self.guard.take();
        Ok(confirmed)
    }
}

impl Drop for WriteTxn {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        // The owning future can be aborted mid-transaction (session teardown, daemon shutdown). A
        // dropped transaction must still release SQLite's write lock or the daemon wedges every
        // writer — the exact class this type exists to kill.
        let conn = self.conn.clone();
        let label = self.label.clone();
        let guard = self.guard.take();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _write_guard = guard;
                // Autocommit means the transaction is already gone. There is nothing to roll back,
                // and issuing ROLLBACK would only obscure the original cancellation/failure.
                if conn.is_autocommit() {
                    return;
                }
                if let Err(err) = conn.execute("ROLLBACK", ()).await {
                    tracing::error!(
                        target: "nexus_store::write_txn",
                        txn = %label,
                        error = %err,
                        "failed to roll back dropped write transaction"
                    );
                }
            });
        } else {
            tracing::error!(
                target: "nexus_store::write_txn",
                txn = %label,
                "dropped active write transaction outside a tokio runtime; cannot roll back"
            );
        }
    }
}

/// A handle to the recorded libSQL store. Wraps one durable [`libsql::Connection`]; the daemon is
/// the sole durable writer (the gateway's Drizzle view is read-only). Repos query durable state
/// through [`Store::conn`]. `migrate()` also prepares the volatile `mem` stream database in
/// anonymous memory. An explicit `NEXUS_STREAM_DB_PATH` opts nonstandard deployments into a named
/// cross-process stream file.
pub struct Store {
    /// The live libSQL connection. Public so repos (and the migration test) can issue queries.
    pub conn: StoreConnection,
    /// Optional file-backed identity/continuity authority. When absent (legacy/unit stores),
    /// identity and transport intentionally share `conn` for compatibility.
    identity_authority: Option<Arc<Store>>,
    /// Explicitly configured SQLite file used for cross-process live stream lanes. `None` uses an
    /// anonymous in-memory stream database, which is the production daemon default.
    stream_db_path: Option<String>,
    /// In-process store write topics for daemon-local wakeups.
    events: StoreEventBus,
    /// Serializes explicit write transactions on this handle's shared connection — two tasks
    /// issuing raw `BEGIN IMMEDIATE` on one connection would nest and error. Per-Store (not
    /// global): the hazard is per-connection, and the daemon is the only durable store owner.
    write_lock: Arc<Mutex<()>>,
    /// Serializes presence mutation plus its post-commit realtime projection across services that
    /// share this store handle. This is separate from `write_lock`: presence paths use repository
    /// autocommit writes and may emit asynchronously before releasing this guard.
    presence_transition_lock: Arc<Mutex<()>>,
    // Keep the database handle alive for the lifetime of the connection (in-memory dbs are
    // dropped with their `Database`).
    _db: Arc<libsql::Database>,
}

/// Parsed store location. Remote URLs remain classified only so legacy configurations fail with a
/// precise migration error instead of becoming accidental filenames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreLocation {
    Memory,
    LocalPath(String),
    RemoteUrl(String),
}

impl StoreLocation {
    /// Parse a user/config supplied store location.
    pub fn parse(raw: &str) -> StoreLocation {
        let location = raw.trim();
        if location == ":memory:" {
            return StoreLocation::Memory;
        }
        if let Some(rest) = location.strip_prefix("file:") {
            return StoreLocation::LocalPath(file_url_path(rest));
        }
        if is_remote_url(location) {
            return StoreLocation::RemoteUrl(location.to_string());
        }
        StoreLocation::LocalPath(location.to_string())
    }
}

impl Store {
    /// Open (or create) the store at `location`. Pass `":memory:"` for an ephemeral in-memory DB
    /// (used by tests). Bare paths and `file:` URLs open a local file. Remote URLs are rejected:
    /// only the daemon may own the embedded durable store.
    pub async fn open(location: &str) -> Result<Store, NexusError> {
        Self::open_with_auth(location, None).await
    }

    /// Compatibility entry point retained for callers compiled against the old remote-store API.
    /// `auth_token` is intentionally unused because remote stores are no longer supported.
    pub async fn open_with_auth(
        location: &str,
        _auth_token: Option<&str>,
    ) -> Result<Store, NexusError> {
        Self::open_with_stream_path(location, crate::migrate::resolve_stream_db_path()).await
    }

    /// Select the attached stream authority explicitly without changing process configuration.
    /// Split identity uses anonymous memory: BEGIN IMMEDIATE also locks attached databases, so
    /// sharing transport's named stream file would couple otherwise independent write gates.
    pub(crate) async fn open_with_stream_path(
        location: &str,
        stream_db_path: Option<String>,
    ) -> Result<Store, NexusError> {
        let parsed = StoreLocation::parse(location);
        if let StoreLocation::RemoteUrl(url) = &parsed {
            return Err(NexusError::Invalid(format!(
                "remote store URL '{url}' is no longer supported; Nexus uses a daemon-owned embedded store"
            )));
        }
        let db = match &parsed {
            StoreLocation::Memory => Builder::new_local(":memory:")
                .build()
                .await
                .map_err(store_err)?,
            StoreLocation::LocalPath(path) => {
                Builder::new_local(path).build().await.map_err(store_err)?
            }
            StoreLocation::RemoteUrl(_) => unreachable!("remote URLs rejected above"),
        };
        let db = Arc::new(db);
        let conn = db.connect().map_err(store_err)?;

        // WAL is REQUIRED for recorded state because the gateway concurrently reads the daemon-owned
        // file while command workers write canonical rows. Token-level stream deltas are volatile
        // (`mem.stream_events`) and never fsync into the file, but message/final-turn writes still
        // need reader-friendly journaling. `busy_timeout` makes rare contention wait rather than
        // fail; `synchronous=NORMAL` is the safe, fast WAL pairing.
        // Skip for in-memory (WAL is meaningless there and not all builds allow it).
        if let StoreLocation::LocalPath(_) = parsed {
            // Use `query` (not `execute`) — `PRAGMA journal_mode=WAL` RETURNS a row ("wal"), and
            // libsql's `execute` errors on a statement that yields rows. Drain each result.
            for pragma in [
                "PRAGMA journal_mode=WAL",
                "PRAGMA busy_timeout=5000",
                "PRAGMA synchronous=NORMAL",
            ] {
                let mut rows = conn.query(pragma, ()).await.map_err(store_err)?;
                while rows.next().await.map_err(store_err)?.is_some() {}
            }
        }

        Ok(Store {
            conn: StoreConnection::new(conn),
            identity_authority: None,
            stream_db_path,
            events: StoreEventBus::new(),
            write_lock: Arc::new(Mutex::new(())),
            presence_transition_lock: Arc::new(Mutex::new(())),
            _db: db,
        })
    }

    /// Compatibility probe retained for callers compiled against the old remote-store API.
    /// Production stores are always embedded, so this always returns false.
    pub fn is_server_mode(&self) -> bool {
        false
    }

    pub fn stream_db_path(&self) -> Option<&str> {
        self.stream_db_path.as_deref()
    }

    /// Build the compatibility handle used while repositories migrate from one mixed store to
    /// explicit identity and transport authorities.
    pub(crate) fn with_identity_authority(transport: &Store, identity: Arc<Store>) -> Store {
        Store {
            conn: transport.conn.clone(),
            identity_authority: Some(identity),
            stream_db_path: transport.stream_db_path.clone(),
            events: StoreEventBus::new(),
            write_lock: transport.write_lock.clone(),
            presence_transition_lock: transport.presence_transition_lock.clone(),
            _db: transport._db.clone(),
        }
    }

    /// Connection for stable identity, resurrection and unsettled-delivery continuity.
    pub fn identity_conn(&self) -> StoreConnection {
        self.identity_authority
            .as_ref()
            .map(|store| store.conn.clone())
            .unwrap_or_else(|| self.conn.clone())
    }

    /// Whether this handle separates persistent identity from boot-scoped transport.
    pub fn has_split_authority(&self) -> bool {
        self.identity_authority.is_some()
    }

    /// Open an explicit write transaction on the identity/continuity authority.
    pub async fn begin_identity_write_txn(&self, label: &str) -> Result<WriteTxn, NexusError> {
        match self.identity_authority.as_ref() {
            Some(identity) => identity.begin_write_txn(label).await,
            None => self.begin_write_txn(label).await,
        }
    }

    /// Guard a split authority's implicit write through its RETURNING cursor's
    /// lifetime. Unified command workers already own this store's write gate;
    /// reacquiring that same mutex here would deadlock them.
    pub(crate) async fn lock_split_identity_write(&self) -> Option<OwnedMutexGuard<()>> {
        match self.identity_authority.as_ref() {
            Some(identity) => Some(identity.write_lock.clone().lock_owned().await),
            None => None,
        }
    }

    /// Execute one synchronous, clone-isolated native batch on the identity
    /// authority. The async gate also excludes existing multi-step owners.
    pub(crate) async fn execute_identity_write_batch(
        &self,
        label: &str,
        sql: &str,
    ) -> Result<(), NexusError> {
        let identity = self.identity_authority.as_deref().unwrap_or(self);
        identity.execute_write_batch(label, sql).await
    }

    pub(crate) async fn execute_identity_parameterized_write(
        &self,
        label: &str,
        sql: &str,
        params: impl IntoParams,
    ) -> Result<u64, NexusError> {
        let identity = self.identity_authority.as_deref().unwrap_or(self);
        identity
            .execute_parameterized_write(label, sql, params)
            .await
    }

    /// Connection that owns the volatile `mem.stream_events` / `mem.stream_raw` lane.
    pub fn stream_conn(&self) -> Connection {
        self.conn.raw()
    }

    /// Open an explicit write transaction on the durable connection (see [`WriteTxn`]).
    ///
    /// Serializes on this store's write lock first, so concurrent daemon tasks queue instead
    /// of nesting `BEGIN`. `label` names the transaction in wedge/rollback logs.
    pub async fn begin_write_txn(&self, label: &str) -> Result<WriteTxn, NexusError> {
        let guard = self.write_lock.clone().lock_owned().await;
        let conn = self.begin_immediate_with_retry(label).await?;
        Ok(WriteTxn {
            conn,
            label: label.to_string(),
            guard: Some(guard),
            active: true,
        })
    }

    /// Serialize an authoritative presence mutation through its realtime fact emission.
    pub async fn lock_presence_transition(&self) -> OwnedMutexGuard<()> {
        self.presence_transition_lock.clone().lock_owned().await
    }

    /// Execute a multi-statement write as one libSQL transactional batch.
    ///
    /// The in-process write lock serializes this with [`WriteTxn`] users on the same store handle.
    pub async fn execute_write_batch(&self, label: &str, sql: &str) -> Result<(), NexusError> {
        let _guard = self.write_lock.clone().lock_owned().await;
        self.conn
            .execute_transactional_batch(sql)
            .await
            .map_err(|err| {
                tracing::error!(
                    target: "nexus_store::write_txn",
                    txn = %label,
                    error = %err,
                    "transactional write batch failed"
                );
                store_err(err)
            })
    }

    /// Execute one parameterized authoritative write under the store's write lock.
    ///
    /// SQLite makes a single statement atomic, so callers that express a multi-table operation
    /// through an `INSTEAD OF` trigger get one statement and one implicit transaction without
    /// interpolating values into SQL.
    pub async fn execute_parameterized_write(
        &self,
        label: &str,
        sql: &str,
        params: impl IntoParams,
    ) -> Result<u64, NexusError> {
        let _guard = self.write_lock.clone().lock_owned().await;
        let params = params.into_params().map_err(store_err)?;
        let conn = self.conn.raw();
        if !conn.is_autocommit() {
            return Err(NexusError::Store(format!(
                "{label}: refusing to join a foreign open transaction"
            )));
        }
        let result = conn.execute(sql, params).await;
        result.map_err(|error| {
            tracing::error!(
                target: "nexus_store::write_txn",
                txn = %label,
                error = %error,
                "parameterized atomic write failed"
            );
            store_err(error)
        })
    }

    /// Pin the embedded connection and retry only SQLite busy/locked failures before the
    /// transaction starts. Once `BEGIN IMMEDIATE` succeeds, nothing is replayed.
    async fn begin_immediate_with_retry(&self, label: &str) -> Result<Connection, NexusError> {
        const MAX_ATTEMPTS: usize = 40;
        const RETRY_DELAY: Duration = Duration::from_millis(50);

        let mut attempt = 0;
        let conn = self.conn.raw();
        loop {
            match conn.execute("BEGIN IMMEDIATE", ()).await {
                Ok(_) => return Ok(conn),
                Err(err) if is_retryable_busy_error(&err) && attempt < MAX_ATTEMPTS => {
                    attempt += 1;
                    tracing::warn!(
                        target: "nexus_store::write_txn",
                        txn = %label,
                        attempt,
                        error = %err,
                        "BEGIN IMMEDIATE busy; retrying"
                    );
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Err(err) => {
                    return Err(NexusError::Store(format!(
                        "write transaction '{label}' could not begin: {err}"
                    )))
                }
            }
        }
    }

    /// This store's write-transaction gate. For callers that manage their own transaction guard
    /// (the materializer): acquire this BEFORE issuing a raw `BEGIN` on [`Store::conn`].
    pub fn write_lock(&self) -> Arc<Mutex<()>> {
        self.write_lock.clone()
    }

    /// In-process write topics for daemon-local wakeups.
    pub fn events(&self) -> &StoreEventBus {
        &self.events
    }

    /// Compatibility diagnostic retained after remote reconnection was removed.
    pub fn connection_generation(&self) -> u64 {
        0
    }

    /// Current generation for in-process command-intent inserts.
    ///
    /// A caller snapshots this before checking the queue; if an insert lands after the check but
    /// before it awaits, [`Self::wait_for_command_intent_after`] returns immediately instead of
    /// losing the wake.
    pub fn command_intents_epoch(&self) -> u64 {
        self.events.command_intent_inserted().epoch()
    }

    /// Signal that a command intent was inserted through this store handle.
    pub fn notify_command_intent_inserted(&self) {
        self.events.command_intent_inserted().signal();
    }

    /// Wait until this store handle observes a command-intent insert after `epoch`.
    pub async fn wait_for_command_intent_after(&self, epoch: u64) {
        self.events
            .command_intent_inserted()
            .wait_after(epoch)
            .await;
    }

    /// Current generation for terminal command-intent transitions.
    pub fn command_completions_epoch(&self) -> u64 {
        self.events.command_intent_completed().epoch()
    }

    /// Wait until any command intent reaches a terminal state after `epoch`.
    ///
    /// Held daemon IPC responders re-read their one command row after this wake. The durable row
    /// remains authoritative; this in-process topic only removes result polling.
    pub async fn wait_for_command_completion_after(&self, epoch: u64) {
        self.events
            .command_intent_completed()
            .wait_after(epoch)
            .await;
    }
}

fn is_remote_url(location: &str) -> bool {
    location.starts_with("libsql://")
        || location.starts_with("http://")
        || location.starts_with("https://")
}

fn is_retryable_busy_error(err: &libsql::Error) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("database is locked")
        || message.contains("database is busy")
        || message.contains("sqlite_busy")
        || message.contains("busy")
        || message.contains("locked")
}

fn file_url_path(rest: &str) -> String {
    if let Some(path) = rest.strip_prefix("///") {
        format!("/{path}")
    } else {
        rest.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_busy_errors_as_retryable() {
        assert!(is_retryable_busy_error(&libsql::Error::SqliteFailure(
            5,
            "database is locked".into()
        )));
        assert!(is_retryable_busy_error(&libsql::Error::SqliteFailure(
            5,
            "SQLITE_BUSY: database is busy".into()
        )));
        assert!(!is_retryable_busy_error(&libsql::Error::SqliteFailure(
            19,
            "UNIQUE constraint failed".into()
        )));
    }

    #[tokio::test]
    async fn execute_write_batch_rolls_back_all_steps_on_error() {
        let store = Store::open(":memory:").await.expect("store");
        store
            .conn
            .execute(
                "CREATE TABLE batch_probe (id INTEGER PRIMARY KEY, body TEXT)",
                (),
            )
            .await
            .expect("schema");

        let error = store
            .execute_write_batch(
                "batch_probe",
                "INSERT INTO batch_probe (body) VALUES ('first');
                 INSERT INTO definitely_missing_table (body) VALUES ('boom');",
            )
            .await
            .expect_err("bad statement should fail the batch");
        assert!(error.to_string().contains("definitely_missing_table"));

        let mut rows = store
            .conn
            .query("SELECT COUNT(*) FROM batch_probe", ())
            .await
            .expect("count query");
        let row = rows.next().await.expect("row").expect("count row");
        let count = row.get::<i64>(0).expect("count");
        assert_eq!(count, 0, "transactional batch must not partially commit");
    }
}
