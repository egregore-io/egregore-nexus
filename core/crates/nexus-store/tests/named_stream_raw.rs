use nexus_contracts::SessionId;
use nexus_store::repos::StreamRaw;
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
async fn named_stream_raw_is_visible_from_a_second_connection() {
    let _stream_env = EnvRestore::capture("NEXUS_STREAM_DB_PATH");
    let run = format!(
        "nexus-raw-test-{}-{}",
        std::process::id(),
        nexus_common::now()
    );
    let dir = std::env::temp_dir().join(run);
    std::fs::create_dir_all(&dir).unwrap();
    let main = dir.join("nexus.db");
    let stream = dir.join("nexus-stream.db");
    let stream_s = stream.to_string_lossy().into_owned();
    std::env::set_var("NEXUS_STREAM_DB_PATH", &stream_s);

    let store = Store::open(&main.to_string_lossy()).await.unwrap();
    store.migrate().await.unwrap();
    let session = SessionId("s_cross_raw".to_string());
    let id = StreamRaw::new(&store)
        .append(&session, b"cross-process")
        .await
        .unwrap();

    let reader = Store::open(":memory:").await.unwrap();
    reader
        .conn
        .execute(
            &format!("ATTACH DATABASE '{}' AS mem", stream_s.replace('\'', "''")),
            (),
        )
        .await
        .unwrap();
    let rows = StreamRaw::new(&reader).since(&session, 0).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, id);
    assert_eq!(rows[0].chunk, b"cross-process");

    drop(rows);
    drop(reader);
    drop(store);
    std::fs::remove_dir_all(&dir).unwrap();
}
