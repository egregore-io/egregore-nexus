//! Fresh v0.1.0 schema bootstrap.
//!
//! Nexus did not publish a database format before v0.1.0. The release therefore has one named
//! baseline and deliberately rejects every pre-release migration ladder. Operators archive the
//! pre-release home and start with fresh daemon and Gateway stores.

use std::path::{Path, PathBuf};

use nexus_common::{now, NexusError};

use crate::error::store_err;
use crate::state::Store;

pub const CURRENT_SCHEMA_VERSION: i64 = 1;
pub const CURRENT_SCHEMA_NAME: &str = "v0.1.0_baseline";
pub(crate) const IDENTITY_SCHEMA_NAME: &str = "v0.1.0_identity";
pub(crate) const TRANSPORT_SCHEMA_NAME: &str = "v0.1.0_transport";

const BASELINE_SCHEMA: &str = include_str!("../../../migrations/0001_init.sql");
const STREAM_DB_FILE_NAME: &str = "nexus-stream.db";

const EPHEMERAL_STREAM_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS mem.stream_events (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  kind       TEXT NOT NULL,
  data       TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS mem.idx_stream_events_session_id
  ON stream_events(session_id, id);

CREATE TABLE IF NOT EXISTS mem.stream_raw (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  chunk      BLOB NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS mem.idx_stream_raw_session_id
  ON stream_raw(session_id, id);
"#;

const CORE_REQUIRED_OBJECTS: &[(&str, &str)] = &[
    ("table", "agent_credentials"),
    ("table", "agent_runtimes"),
    ("table", "agent_groups"),
    ("table", "agent_group_members"),
    ("table", "messages"),
    ("table", "in_flight"),
    ("table", "threads"),
    ("table", "thread_members"),
    ("table", "topics"),
    ("table", "subscriptions"),
    ("table", "notifications"),
    ("table", "developer_event_topics"),
    ("table", "developer_events"),
    ("table", "command_intents"),
    ("table", "command_intent_events"),
    ("table", "command_queue_mutations"),
    ("table", "daemon_state"),
    ("table", "sources"),
    ("table", "agent_session_turns"),
    ("table", "agent_session_messages"),
    ("table", "agent_session_stream_cursors"),
    ("table", "messages_fts"),
    ("table", "agent_acl_grants"),
    ("table", "initial_prompt_deliveries"),
    ("table", "inbox_subscriptions"),
    ("table", "inbox_subscription_batches"),
    ("table", "producer_identities"),
    ("table", "transcript_archive"),
    ("table", "native_thread_bindings"),
    ("table", "sessions"),
    ("table", "agents"),
    ("table", "reply_contexts"),
    ("view", "nexus_broadcast_ingress"),
    ("index", "idx_agent_credentials_agent_active"),
    ("index", "idx_agent_runtimes_agent_active"),
    ("index", "idx_agent_runtimes_one_active"),
    ("index", "idx_agent_group_members_agent"),
    ("index", "idx_messages_to_created"),
    ("index", "idx_messages_thread"),
    ("index", "idx_messages_from"),
    ("index", "idx_messages_from_agent_created"),
    ("index", "idx_messages_to_agent_created"),
    ("index", "idx_messages_sender_idempotency"),
    ("index", "idx_in_flight_recipient_state"),
    ("index", "idx_in_flight_msg_recipient"),
    ("index", "idx_in_flight_agent_state"),
    ("index", "idx_in_flight_msg_agent"),
    ("index", "idx_in_flight_undrained"),
    ("index", "idx_in_flight_injecting"),
    ("index", "idx_in_flight_error"),
    ("index", "idx_thread_members_agent"),
    ("index", "idx_thread_members_thread_agent"),
    ("index", "idx_subscriptions_agent"),
    ("index", "idx_subscriptions_topic_agent"),
    ("index", "idx_developer_events_message"),
    ("index", "idx_command_intents_pending"),
    ("index", "idx_command_intents_lease"),
    ("index", "idx_command_intents_harness_claim_agent"),
    ("index", "idx_command_intents_harness_claim_name"),
    ("index", "idx_command_intents_idempotency_scope"),
    ("index", "idx_command_intent_events_session_seq"),
    ("index", "idx_command_queue_mutations_completed"),
    ("index", "idx_agent_session_turns_session_event"),
    ("index", "idx_agent_session_turns_streaming"),
    ("index", "idx_agent_session_messages_session_created"),
    ("index", "idx_agent_session_messages_turn"),
    ("index", "idx_agent_acl_grants_principal"),
    ("index", "idx_agent_acl_grants_principal_agent"),
    ("index", "idx_agent_acl_grants_grantor_agent"),
    ("index", "idx_initial_prompt_deliveries_session_status"),
    ("index", "idx_inbox_subscriptions_active"),
    ("index", "idx_inbox_subscription_batches_pending"),
    ("index", "idx_inbox_subscription_batches_pending_signature"),
    ("index", "idx_producer_identities_runtime_created"),
    ("index", "idx_transcript_archive_agent"),
    ("index", "idx_native_thread_bindings_agent"),
    ("index", "idx_native_thread_bindings_runtime"),
    ("index", "idx_sessions_name"),
    ("index", "idx_sessions_client_key"),
    ("index", "idx_sessions_harness"),
    ("index", "idx_sessions_project"),
    ("index", "idx_sessions_agent_id"),
    ("index", "idx_agents_name_unique"),
    ("index", "idx_agents_project_name"),
    ("index", "idx_agents_owner"),
    ("index", "idx_agents_owner_agent"),
    ("trigger", "trg_command_intent_queue_insert"),
    ("trigger", "trg_command_intent_queue_update"),
    ("trigger", "nexus_broadcast_ingress_insert"),
    ("trigger", "in_flight_reply_context_on_injecting"),
];

const IDENTITY_REQUIRED_TABLES: &[&str] = &[
    "agent_acl_grants",
    "agent_credentials",
    "agent_runtimes",
    "agents",
    "command_intent_events",
    "command_intents",
    "command_queue_mutations",
    "daemon_state",
    "delivery_obligations",
    "identity_sessions",
    "initial_prompt_deliveries",
    "native_thread_bindings",
    "producer_identities",
    "routing_thread_members",
    "routing_threads",
    "sources",
];

impl Store {
    /// Clear the boot-scoped stream lane, then validate/bootstrap the durable baseline.
    ///
    /// Only the daemon may call this owner variant. Other processes call [`Store::migrate`] so a
    /// live shared stream file is never unlinked under its writer.
    pub async fn migrate_as_stream_owner(&self) -> Result<(), NexusError> {
        if let Some(path) = self.stream_db_path() {
            let stream_conn = self.stream_conn();
            if !attached_database_exists_on(&stream_conn, "mem").await? {
                clear_stale_stream_db_files(path)?;
            }
        }
        self.migrate().await
    }

    /// Bootstrap or validate the single v0.1.0 schema baseline.
    ///
    /// Pre-release databases are intentionally unsupported. This method never adds columns,
    /// renames existing tables, backfills rows, or records an upgrade version.
    pub async fn migrate(&self) -> Result<(), NexusError> {
        match self.schema_marker().await? {
            Some(marker) if marker == CURRENT_SCHEMA_NAME => {
                self.validate_objects(CORE_REQUIRED_OBJECTS).await?;
            }
            Some(marker) if marker == IDENTITY_SCHEMA_NAME => {
                self.validate_identity_schema().await?;
            }
            Some(marker) if marker == TRANSPORT_SCHEMA_NAME => {}
            Some(marker) => return Err(unsupported_schema(&marker)),
            None if self.has_schema_migrations_table().await? => {
                return Err(unsupported_schema("pre-release migration ladder"));
            }
            None if self.user_schema_is_empty().await? => {
                self.install_baseline().await?;
                self.validate_objects(CORE_REQUIRED_OBJECTS).await?;
            }
            None => {
                return Err(NexusError::Store(
                    "unrecognized non-empty Nexus database; refusing to modify it. Back it up and start with a fresh v0.1.0 store".into(),
                ));
            }
        }

        self.ensure_ephemeral_stream_schema().await
    }

    pub(crate) async fn mark_schema_variant(&self, name: &str) -> Result<(), NexusError> {
        self.conn
            .execute(
                "UPDATE schema_migrations SET name = ?1 WHERE version = ?2",
                libsql::params![name, CURRENT_SCHEMA_VERSION],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    async fn install_baseline(&self) -> Result<(), NexusError> {
        let tx = self.begin_write_txn("v010_schema_baseline").await?;
        if let Err(error) = tx
            .execute_batch(
                "CREATE TABLE schema_migrations (
                   version INTEGER PRIMARY KEY,
                   name TEXT NOT NULL,
                   applied_at INTEGER NOT NULL
                 );",
            )
            .await
        {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx.execute_batch(BASELINE_SCHEMA).await {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx
            .execute(
                "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?1, ?2, ?3)",
                libsql::params![CURRENT_SCHEMA_VERSION, CURRENT_SCHEMA_NAME, now()],
            )
            .await
        {
            return rollback_error(tx, error).await;
        }
        tx.commit().await
    }

    async fn schema_marker(&self) -> Result<Option<String>, NexusError> {
        if !self.has_schema_migrations_table().await? {
            return Ok(None);
        }
        if !self.schema_migrations_has_name().await? {
            return Ok(None);
        }
        let mut rows = self
            .conn
            .query(
                "SELECT version, name FROM schema_migrations ORDER BY version",
                (),
            )
            .await
            .map_err(store_err)?;
        let Some(row) = rows.next().await.map_err(store_err)? else {
            return Ok(Some("empty migration ledger".into()));
        };
        let version: i64 = row.get(0).map_err(store_err)?;
        let name: String = row.get(1).map_err(store_err)?;
        if rows.next().await.map_err(store_err)?.is_some() || version != CURRENT_SCHEMA_VERSION {
            return Ok(Some(format!(
                "pre-release migration ladder ending at {version}"
            )));
        }
        Ok(Some(name))
    }

    async fn has_schema_migrations_table(&self) -> Result<bool, NexusError> {
        self.object_exists("table", "schema_migrations").await
    }

    async fn schema_migrations_has_name(&self) -> Result<bool, NexusError> {
        let mut rows = self
            .conn
            .query("PRAGMA table_info(schema_migrations)", ())
            .await
            .map_err(store_err)?;
        while let Some(row) = rows.next().await.map_err(store_err)? {
            let name: String = row.get(1).map_err(store_err)?;
            if name == "name" {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn user_schema_is_empty(&self) -> Result<bool, NexusError> {
        let mut rows = self
            .conn
            .query(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type IN ('table', 'view', 'trigger', 'index')
                   AND name NOT LIKE 'sqlite_%'",
                (),
            )
            .await
            .map_err(store_err)?;
        let count = rows
            .next()
            .await
            .map_err(store_err)?
            .map(|row| row.get::<i64>(0).map_err(store_err))
            .transpose()?
            .unwrap_or(0);
        Ok(count == 0)
    }

    async fn validate_objects(&self, objects: &[(&str, &str)]) -> Result<(), NexusError> {
        for (kind, name) in objects {
            if !self.object_exists(kind, name).await? {
                return Err(NexusError::Store(format!(
                    "incomplete v0.1.0 Nexus schema: missing {kind} {name}"
                )));
            }
        }
        Ok(())
    }

    async fn validate_identity_schema(&self) -> Result<(), NexusError> {
        for table in IDENTITY_REQUIRED_TABLES {
            if !self.object_exists("table", table).await? {
                return Err(NexusError::Store(format!(
                    "incomplete v0.1.0 identity schema: missing table {table}"
                )));
            }
        }
        Ok(())
    }

    async fn object_exists(&self, kind: &str, name: &str) -> Result<bool, NexusError> {
        let mut rows = self
            .conn
            .query(
                "SELECT 1 FROM sqlite_master WHERE type = ?1 AND name = ?2 LIMIT 1",
                libsql::params![kind, name],
            )
            .await
            .map_err(store_err)?;
        Ok(rows.next().await.map_err(store_err)?.is_some())
    }

    async fn ensure_ephemeral_stream_schema(&self) -> Result<(), NexusError> {
        let stream_conn = self.stream_conn();
        if !attached_database_exists_on(&stream_conn, "mem").await? {
            if let Some(path) = self.stream_db_path() {
                prepare_stream_db_file(path)?;
                let sql = format!("ATTACH DATABASE {} AS mem", sqlite_quote(path));
                stream_conn.execute(&sql, ()).await.map_err(store_err)?;
                drain_pragma(&stream_conn, "PRAGMA mem.journal_mode=WAL").await?;
                drain_pragma(&stream_conn, "PRAGMA mem.busy_timeout=5000").await?;
                drain_pragma(&stream_conn, "PRAGMA mem.synchronous=NORMAL").await?;
            } else {
                stream_conn
                    .execute("ATTACH DATABASE ':memory:' AS mem", ())
                    .await
                    .map_err(store_err)?;
            }
        }
        stream_conn
            .execute_batch(EPHEMERAL_STREAM_SCHEMA)
            .await
            .map_err(store_err)?;
        Ok(())
    }
}

async fn rollback_error(tx: crate::state::WriteTxn, error: NexusError) -> Result<(), NexusError> {
    match tx.rollback(&error).await {
        Ok(()) => Err(error),
        Err(rollback_error) => Err(rollback_error),
    }
}

fn unsupported_schema(marker: &str) -> NexusError {
    NexusError::Store(format!(
        "unsupported pre-release Nexus database schema ({marker}); v0.1.0 does not perform silent upgrades. Back it up and start with a fresh v0.1.0 store"
    ))
}

async fn attached_database_exists_on(
    conn: &libsql::Connection,
    schema: &str,
) -> Result<bool, NexusError> {
    let mut rows = conn
        .query("PRAGMA database_list", ())
        .await
        .map_err(store_err)?;
    while let Some(row) = rows.next().await.map_err(store_err)? {
        let name: String = row.get(1).map_err(store_err)?;
        if name == schema {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Resolve the cross-process stream DB path for local daemon stores.
///
/// `NEXUS_STREAM_DB_PATH` is an explicit override for tests and nonstandard deployments. Without
/// it, the stream file must live on a tmpfs-like filesystem so live output never writes to disk.
pub fn resolve_stream_db_path() -> Result<String, NexusError> {
    if let Ok(path) = std::env::var("NEXUS_STREAM_DB_PATH") {
        let path = path.trim();
        if !path.is_empty() {
            return Ok(path.to_string());
        }
    }

    let base = std::env::var("XDG_RUNTIME_DIR")
        .ok()
        .filter(|path| !path.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/dev/shm"));
    let path = base.join(STREAM_DB_FILE_NAME);
    guard_tmpfs(&path)?;
    Ok(path.to_string_lossy().into_owned())
}

fn prepare_stream_db_file(path: &str) -> Result<(), NexusError> {
    let path = Path::new(path);
    if std::env::var("NEXUS_STREAM_DB_PATH")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .is_none()
    {
        guard_tmpfs(path)?;
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            NexusError::Store(format!("create stream db dir {parent:?}: {error}"))
        })?;
    }
    if !path.exists() {
        match create_0600(path) {
            Ok(()) => {}
            Err(_) if path.exists() => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn clear_stale_stream_db_files(path: &str) -> Result<(), NexusError> {
    for suffix in ["", "-wal", "-shm"] {
        let stale = PathBuf::from(format!("{path}{suffix}"));
        match std::fs::remove_file(&stale) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(NexusError::Store(format!(
                    "remove stale stream db file {stale:?}: {error}"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
fn create_0600(path: &Path) -> Result<(), NexusError> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| NexusError::Store(format!("create stream db file {path:?}: {error}")))?;
    Ok(())
}

#[cfg(not(unix))]
fn create_0600(path: &Path) -> Result<(), NexusError> {
    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| NexusError::Store(format!("create stream db file {path:?}: {error}")))?;
    Ok(())
}

fn guard_tmpfs(path: &Path) -> Result<(), NexusError> {
    #[cfg(unix)]
    {
        let mounts = std::fs::read_to_string("/proc/mounts").map_err(|error| {
            NexusError::Store(format!("read /proc/mounts for stream tmpfs guard: {error}"))
        })?;
        let canonical_parent = path
            .parent()
            .unwrap_or_else(|| Path::new("/"))
            .canonicalize()
            .unwrap_or_else(|_| {
                path.parent()
                    .unwrap_or_else(|| Path::new("/"))
                    .to_path_buf()
            });
        let mut best: Option<(usize, String)> = None;
        for line in mounts.lines() {
            let mut parts = line.split_whitespace();
            let _source = parts.next();
            let Some(mount_point) = parts.next() else {
                continue;
            };
            let Some(fs_type) = parts.next() else {
                continue;
            };
            let mount_path = PathBuf::from(mount_point.replace("\\040", " "));
            if canonical_parent.starts_with(&mount_path) {
                let len = mount_path.as_os_str().len();
                if best
                    .as_ref()
                    .map(|(best_len, _)| len > *best_len)
                    .unwrap_or(true)
                {
                    best = Some((len, fs_type.to_string()));
                }
            }
        }
        let Some((_len, fs_type)) = best else {
            return Err(NexusError::Store(format!(
                "cannot prove stream db path {path:?} is on tmpfs"
            )));
        };
        if matches!(fs_type.as_str(), "tmpfs" | "ramfs" | "devtmpfs") {
            return Ok(());
        }
        return Err(NexusError::Store(format!(
            "stream db path {path:?} is on {fs_type}, not tmpfs; set NEXUS_STREAM_DB_PATH to override"
        )));
    }

    #[cfg(not(unix))]
    {
        let _ = path;
        Err(NexusError::Store(
            "named stream DB tmpfs guard is only implemented on Unix; set NEXUS_STREAM_DB_PATH to override".into(),
        ))
    }
}

fn sqlite_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

async fn drain_pragma(conn: &libsql::Connection, pragma: &str) -> Result<(), NexusError> {
    let mut rows = conn.query(pragma, ()).await.map_err(store_err)?;
    while rows.next().await.map_err(store_err)?.is_some() {}
    Ok(())
}
