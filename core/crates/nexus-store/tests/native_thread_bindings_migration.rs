use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use libsql::{Builder, Connection};
use nexus_store::repos::{NativeThreadBindings, NewNativeThreadBinding};
use nexus_store::{migrate_identity_with_fault, MigrationFault, Store};

const BASELINE_SCHEMA: &str = include_str!("../../../migrations/0001_init.sql");
const IDENTITY_SCHEMA: &str = include_str!("../../../migrations/identity/0001_identity.sql");
const LEGACY_MARKER: &str = "v0.1.0_identity";
const CURRENT_IDENTITY_MARKER: &str = "v0.1.6_identity_model_report";
const LEGACY_NATIVE_THREAD_BINDINGS_SCHEMA: &str = r#"
CREATE TABLE native_thread_bindings (
  harness          TEXT NOT NULL,
  native_thread_id TEXT NOT NULL,
  agent_id         TEXT NOT NULL,
  project          TEXT NOT NULL,
  first_runtime_id TEXT,
  last_runtime_id  TEXT,
  created_at       INTEGER NOT NULL,
  updated_at       INTEGER NOT NULL,
  released_at      INTEGER,
  PRIMARY KEY (harness, native_thread_id)
);

CREATE INDEX idx_native_thread_bindings_agent
  ON native_thread_bindings(agent_id, updated_at DESC);

CREATE INDEX idx_native_thread_bindings_runtime
  ON native_thread_bindings(last_runtime_id)
  WHERE last_runtime_id IS NOT NULL;
"#;

#[tokio::test]
async fn legacy_native_binding_upgrades_and_roundtrips_provider_kind() {
    let path = unique_store_path("upgrade");
    seed_legacy_identity(&path).await;

    let store = Store::open(path.to_string_lossy().as_ref())
        .await
        .expect("open legacy identity store");
    store
        .migrate()
        .await
        .expect("migrate legacy identity store");

    let repo = NativeThreadBindings::new(&store);
    let legacy = repo
        .find("claude", "claude-thread-1")
        .await
        .expect("find legacy binding")
        .expect("legacy binding exists");
    assert_eq!(legacy.provider, "claude");
    assert_eq!(legacy.kind, "harness");
    assert_eq!(legacy.agent_id, "a_legacy");

    let inserted = repo
        .claim(NewNativeThreadBinding {
            provider: "telegram".into(),
            kind: "im".into(),
            native_thread_id: "chat-1".into(),
            agent_id: "a_telegram".into(),
            project: "default".into(),
            runtime_id: None,
        })
        .await
        .expect("claim provider binding");
    assert_eq!(inserted.provider, "telegram");
    assert_eq!(inserted.kind, "im");
    assert_eq!(
        schema_marker(store.conn.raw()).await,
        CURRENT_IDENTITY_MARKER
    );

    drop(store);
    cleanup_store(&path);
}

#[tokio::test]
async fn injected_fault_rolls_back_both_alters_and_marker_then_clean_retry_succeeds() {
    let path = unique_store_path("fault");
    seed_legacy_identity(&path).await;

    let error = migrate_identity_with_fault(&path, MigrationFault::AfterFirstStatement)
        .await
        .expect_err("fault after first statement must abort migration");
    assert!(error
        .to_string()
        .contains("injected identity migration fault"));

    let (db, conn) = raw_connection(&path).await;
    let columns = table_columns(conn.clone(), "native_thread_bindings").await;
    assert!(columns.iter().any(|name| name == "harness"));
    assert!(!columns.iter().any(|name| name == "provider"));
    assert!(!columns.iter().any(|name| name == "kind"));
    assert_eq!(schema_marker(conn).await, LEGACY_MARKER);
    drop(db);

    let store = Store::open(path.to_string_lossy().as_ref())
        .await
        .expect("reopen rolled-back identity store");
    store.migrate().await.expect("clean retry succeeds");
    let columns = table_columns(store.conn.raw(), "native_thread_bindings").await;
    assert!(!columns.iter().any(|name| name == "harness"));
    assert!(columns.iter().any(|name| name == "provider"));
    assert!(columns.iter().any(|name| name == "kind"));
    assert_eq!(
        schema_marker(store.conn.raw()).await,
        CURRENT_IDENTITY_MARKER
    );

    drop(store);
    cleanup_store(&path);
}

#[tokio::test]
async fn upgraded_identity_migration_is_idempotent() {
    let path = unique_store_path("idempotent");
    seed_legacy_identity(&path).await;

    let store = Store::open(path.to_string_lossy().as_ref())
        .await
        .expect("open legacy identity store");
    store.migrate().await.expect("first migration");
    store.migrate().await.expect("second migration");

    let columns = table_columns(store.conn.raw(), "native_thread_bindings").await;
    assert_eq!(columns.iter().filter(|name| *name == "provider").count(), 1);
    assert_eq!(columns.iter().filter(|name| *name == "kind").count(), 1);
    assert_eq!(
        schema_marker(store.conn.raw()).await,
        CURRENT_IDENTITY_MARKER
    );

    drop(store);
    cleanup_store(&path);
}

async fn seed_legacy_identity(path: &Path) {
    let (db, conn) = raw_connection(path).await;
    conn.execute_batch(
        "CREATE TABLE schema_migrations (
           version INTEGER PRIMARY KEY,
           name TEXT NOT NULL,
           applied_at INTEGER NOT NULL
         );",
    )
    .await
    .expect("create raw migration marker");
    conn.execute_batch(BASELINE_SCHEMA)
        .await
        .expect("create raw baseline schema");
    conn.execute_batch(IDENTITY_SCHEMA)
        .await
        .expect("create raw split-identity schema");
    conn.execute(
        "ALTER TABLE command_intents DROP COLUMN caller_principal_id",
        (),
    )
    .await
    .expect("restore the verbatim v0.1.5 command intent schema");
    conn.execute_batch("DROP TABLE native_thread_bindings;")
        .await
        .expect("remove current native binding table");
    conn.execute_batch(LEGACY_NATIVE_THREAD_BINDINGS_SCHEMA)
        .await
        .expect("create the verbatim v0.1.5 native binding table");
    conn.execute(
        "INSERT INTO schema_migrations(version, name, applied_at) VALUES (1, ?1, 1)",
        libsql::params![LEGACY_MARKER],
    )
    .await
    .expect("stamp raw identity marker");
    conn.execute(
        "INSERT INTO native_thread_bindings (
           harness, native_thread_id, agent_id, project, first_runtime_id,
           last_runtime_id, created_at, updated_at, released_at
         ) VALUES ('claude', 'claude-thread-1', 'a_legacy', 'default',
                   's_legacy', 's_legacy', 1, 1, NULL)",
        (),
    )
    .await
    .expect("seed raw legacy native binding");
    drop(conn);
    drop(db);
}

async fn raw_connection(path: &Path) -> (libsql::Database, Connection) {
    let db = Builder::new_local(path)
        .build()
        .await
        .expect("open raw libsql database");
    let conn = db.connect().expect("connect raw libsql database");
    (db, conn)
}

async fn table_columns(conn: Connection, table: &str) -> Vec<String> {
    let mut rows = conn
        .query(&format!("PRAGMA table_info({table})"), ())
        .await
        .expect("query table columns");
    let mut columns = Vec::new();
    while let Some(row) = rows.next().await.expect("read table column") {
        columns.push(row.get::<String>(1).expect("column name"));
    }
    columns
}

async fn schema_marker(conn: Connection) -> String {
    let mut rows = conn
        .query("SELECT name FROM schema_migrations ORDER BY version", ())
        .await
        .expect("query schema marker");
    rows.next()
        .await
        .expect("read schema marker")
        .expect("schema marker row")
        .get::<String>(0)
        .expect("schema marker name")
}

fn unique_store_path(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "nexus-v016-native-binding-{label}-{}-{nonce}.db",
        std::process::id()
    ))
}

fn cleanup_store(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}
