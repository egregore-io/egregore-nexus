use nexus_common::NexusError;
use nexus_store::{Store, StoreLocation};

#[test]
fn parses_legacy_local_paths() {
    assert_eq!(
        StoreLocation::parse("/tmp/nexus.db"),
        StoreLocation::LocalPath("/tmp/nexus.db".into())
    );
    assert_eq!(StoreLocation::parse(":memory:"), StoreLocation::Memory);
}

#[test]
fn parses_file_urls_as_local_paths() {
    assert_eq!(
        StoreLocation::parse("file:/tmp/nexus.db"),
        StoreLocation::LocalPath("/tmp/nexus.db".into())
    );
    assert_eq!(
        StoreLocation::parse("file:///tmp/nexus.db"),
        StoreLocation::LocalPath("/tmp/nexus.db".into())
    );
}

#[test]
fn classifies_legacy_server_urls_only_for_explicit_rejection() {
    assert_eq!(
        StoreLocation::parse("http://127.0.0.1:8080"),
        StoreLocation::RemoteUrl("http://127.0.0.1:8080".into())
    );
    assert_eq!(
        StoreLocation::parse("libsql://example.turso.io"),
        StoreLocation::RemoteUrl("libsql://example.turso.io".into())
    );
}

#[tokio::test]
async fn rejects_remote_store_urls_without_attempting_a_network_connection() {
    for url in [
        "http://127.0.0.1:1",
        "https://example.invalid/nexus",
        "libsql://example.invalid",
    ] {
        let error = match Store::open_with_auth(url, Some("must-not-be-used")).await {
            Ok(_) => panic!("remote store URL unexpectedly opened: {url}"),
            Err(error) => error,
        };
        assert!(
            matches!(error, NexusError::Invalid(_)),
            "remote URL must be rejected as invalid configuration, got: {error}"
        );
        assert!(
            error.to_string().contains("daemon-owned embedded store"),
            "rejection must explain the supported ownership model: {error}"
        );
    }
}

#[tokio::test]
async fn opens_file_url_with_local_store_path() {
    let _guard = env_guard().lock().unwrap();
    let _stream_env = EnvRestore::capture("NEXUS_STREAM_DB_PATH");
    let _runtime_env = EnvRestore::capture("XDG_RUNTIME_DIR");
    let run = format!(
        "nexus-store-file-url-{}-{}",
        std::process::id(),
        nexus_common::now()
    );
    let dir = std::env::temp_dir().join(run);
    std::fs::create_dir_all(&dir).unwrap();
    let runtime_default = dir.join("runtime-default");
    let test_stream = dir.join("test-stream.db");
    std::fs::create_dir_all(&runtime_default).unwrap();
    std::env::remove_var("NEXUS_STREAM_DB_PATH");
    std::env::set_var("XDG_RUNTIME_DIR", &runtime_default);
    std::env::set_var("NEXUS_STREAM_DB_PATH", &test_stream);

    let path = dir.join("nexus.db");
    let url = format!("file:{}", path.display());

    let store = Store::open(&url).await.expect("open local file URL");
    assert_eq!(
        store.stream_db_path(),
        Some(test_stream.to_string_lossy().as_ref()),
        "file-url store tests must use a unique NEXUS_STREAM_DB_PATH instead of the live default"
    );
    store.migrate().await.expect("migrate local file URL");

    let _ = std::fs::remove_dir_all(&dir);
}

fn env_guard() -> &'static std::sync::Mutex<()> {
    static GUARD: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    GUARD.get_or_init(|| std::sync::Mutex::new(()))
}

struct EnvRestore {
    key: &'static str,
    value: Option<std::ffi::OsString>,
}

impl EnvRestore {
    fn capture(key: &'static str) -> Self {
        Self {
            key,
            value: std::env::var_os(key),
        }
    }
}

impl Drop for EnvRestore {
    fn drop(&mut self) {
        match &self.value {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}
