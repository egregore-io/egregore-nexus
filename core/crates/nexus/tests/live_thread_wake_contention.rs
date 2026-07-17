use std::sync::Arc;
use std::time::Duration;

use nexus::daemon::AppState;
use nexus_agent::{Adapter, AdapterRegistry, MockAdapter};
use nexus_common::{Config, NexusError};
use nexus_contracts::{Harness, SpawnRequest};
use nexus_store::repos::Sessions;
use nexus_store::Store;

#[tokio::test]
async fn already_live_headless_agent_wake_does_not_wait_for_an_unrelated_write_lock() {
    let stem = format!("nexus-live-wake-contention-{}", std::process::id());
    let db_path = std::env::temp_dir().join(format!("{stem}.db"));
    let db_url = db_path.to_string_lossy().into_owned();
    let store = Arc::new(Store::open(&db_url).await.unwrap());
    store.migrate().await.unwrap();

    let mut registry = AdapterRegistry::new();
    registry.register(
        Harness::Claude,
        Arc::new(|_| Arc::new(MockAdapter::new()) as Arc<dyn Adapter>),
    );
    let config = Config {
        db_url: Some(db_url.clone()),
        ..Config::default()
    };
    let state = AppState::wire_with_registry(store.clone(), &config, registry);
    state
        .launch_agent(
            SpawnRequest {
                kind: Harness::Claude,
                name: Some("live-wake-target".into()),
                identity_policy: None,
                cwd: Some("/tmp/nexus-live-wake-target".into()),
                project: Some("release-test".into()),
                role: None,
                initial_prompt: None,
                resume: None,
                harness_args: Vec::new(),
                headless: true,
                backend: None,
            },
            "release-test",
            None,
        )
        .await
        .unwrap();
    let target = Sessions::new(&store)
        .find_by_name("release-test", "live-wake-target")
        .await
        .unwrap()
        .expect("launched target session");
    let agent_id = target.agent_id.expect("stable target agent id");

    let contender = Store::open(&db_url).await.unwrap();
    let held = contender
        .begin_write_txn("hold unrelated writer")
        .await
        .unwrap();
    let wake = tokio::time::timeout(
        Duration::from_millis(250),
        state.ensure_alive_agent(&agent_id),
    )
    .await;
    let cleanup_error = NexusError::Store("test cleanup".into());
    held.rollback(&cleanup_error).await.unwrap();

    let session = wake
        .expect("an already-live target must not perform a contended presence write")
        .expect("already-live target remains reachable");
    assert_eq!(session, target.session_id);
}
