use std::sync::{Mutex, OnceLock};

use nexus_store::Store;

struct EnvRestore {
    key: &'static str,
    original: Option<std::ffi::OsString>,
}

impl EnvRestore {
    fn capture(key: &'static str) -> Self {
        Self {
            key,
            original: std::env::var_os(key),
        }
    }
}

impl Drop for EnvRestore {
    fn drop(&mut self) {
        match &self.original {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}

#[tokio::test]
async fn local_store_attaches_named_stream_db_and_owner_boot_wipes_it() {
    let _guard = env_guard().lock().unwrap();
    let _stream_env = EnvRestore::capture("NEXUS_STREAM_DB_PATH");
    let dir = unique_temp_dir("stream-owner");
    let main = dir.join("nexus.db");
    let stream = dir.join("nexus-stream.db");
    let stream_path = stream.to_string_lossy().into_owned();
    std::env::set_var("NEXUS_STREAM_DB_PATH", &stream_path);

    let store = Store::open(main.to_string_lossy().as_ref()).await.unwrap();
    store.migrate().await.unwrap();
    assert_eq!(store.stream_db_path(), Some(stream_path.as_str()));
    assert!(stream.exists());
    assert!(attached_table_exists(&store, "mem", "stream_events").await);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&stream).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    drop(store);

    std::fs::write(&stream, b"stale").unwrap();
    std::fs::write(format!("{stream_path}-wal"), b"stale-wal").unwrap();
    std::fs::write(format!("{stream_path}-shm"), b"stale-shm").unwrap();

    let store = Store::open(main.to_string_lossy().as_ref()).await.unwrap();
    store.migrate_as_stream_owner().await.unwrap();
    let mut rows = store
        .conn
        .query("SELECT COUNT(*) FROM mem.stream_events", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        0
    );
    drop(rows);
    drop(store);

    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn non_owner_reopen_preserves_live_stream_file_and_rows() {
    let _guard = env_guard().lock().unwrap();
    let _stream_env = EnvRestore::capture("NEXUS_STREAM_DB_PATH");
    let dir = unique_temp_dir("stream-reader");
    let main = dir.join("nexus.db");
    let stream = dir.join("nexus-stream.db");
    std::env::set_var("NEXUS_STREAM_DB_PATH", stream.to_string_lossy().as_ref());

    let daemon = Store::open(main.to_string_lossy().as_ref()).await.unwrap();
    daemon.migrate_as_stream_owner().await.unwrap();
    daemon
        .conn
        .execute(
            "INSERT INTO mem.stream_events(session_id, kind, data, created_at)
             VALUES ('s_live', 'text', 'live-row', 1)",
            (),
        )
        .await
        .unwrap();
    #[cfg(unix)]
    let inode_before = {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(&stream).unwrap().ino()
    };

    let reader = Store::open(main.to_string_lossy().as_ref()).await.unwrap();
    reader.migrate().await.unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(std::fs::metadata(&stream).unwrap().ino(), inode_before);
    }
    let mut rows = reader
        .conn
        .query("SELECT COUNT(*) FROM mem.stream_events", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        1
    );
    drop(rows);
    drop(reader);
    drop(daemon);

    std::fs::remove_dir_all(dir).unwrap();
}

async fn attached_table_exists(store: &Store, schema: &str, table: &str) -> bool {
    let sql = format!("SELECT 1 FROM {schema}.sqlite_master WHERE type = 'table' AND name = ?1");
    let mut rows = store
        .conn
        .query(&sql, libsql::params![table])
        .await
        .unwrap();
    rows.next().await.unwrap().is_some()
}

fn unique_temp_dir(label: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nexus-{label}-{}-{}",
        std::process::id(),
        nexus_common::now()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn env_guard() -> &'static Mutex<()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD.get_or_init(|| Mutex::new(()))
}
