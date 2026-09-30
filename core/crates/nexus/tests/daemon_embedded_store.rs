use nexus::daemon::lifecycle;
use nexus_common::Config;

#[tokio::test]
async fn daemon_store_owner_ignores_legacy_server_url_and_opens_embedded_file() {
    let root = std::env::temp_dir().join(format!(
        "nexus-daemon-embedded-store-{}-{}",
        std::process::id(),
        nexus_common::now()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db_path = root.join("nexus.db");
    let config = Config {
        db_path: db_path.to_string_lossy().into_owned(),
        db_url: Some("http://127.0.0.1:1".into()),
        db_auth_token: "must-not-be-used".into(),
        ..Config::default()
    };

    let store = lifecycle::open_daemon_owned_store(&config).await.unwrap();
    assert!(!store.is_server_mode());
    assert!(store.has_split_authority());
    assert!(db_path.is_file());

    let mut identity = store
        .identity_conn()
        .query(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'agents'",
            (),
        )
        .await
        .unwrap();
    assert!(identity.next().await.unwrap().is_some());
    let mut transport = store
        .conn
        .query(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'sessions'",
            (),
        )
        .await
        .unwrap();
    assert!(transport.next().await.unwrap().is_some());

    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
