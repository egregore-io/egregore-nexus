use std::sync::Arc;

use async_trait::async_trait;
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_harness_codex::storage::CodexRuntimeStateRepo;
use nexus_harness_codex::{BridgeLaunchOptions, CodexBridge, SupervisorOpts};
use nexus_store::Store;

const FAKE_BIN: &str = env!("CARGO_BIN_EXE_fake_codex_app_server");

#[derive(Default)]
struct Sink;

#[async_trait]
impl EventSink for Sink {
    async fn emit(&self, _event: WsEvent) {}
}

async fn store() -> Arc<Store> {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    Arc::new(store)
}

fn tempdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nexus-codex-runtime-state-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
        tag,
    ));
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}

fn write_rollout(session_dir: &std::path::Path, thread_id: &str) -> std::path::PathBuf {
    let rollout_dir = session_dir
        .join("codex-home")
        .join("sessions")
        .join("2026")
        .join("06");
    std::fs::create_dir_all(&rollout_dir).expect("create rollout dir");
    let rollout = rollout_dir.join("rollout-test.jsonl");
    let first_line = serde_json::json!({
        "type": "session_meta",
        "payload": { "id": thread_id }
    });
    std::fs::write(&rollout, format!("{first_line}\n")).expect("write rollout");
    rollout
}

#[tokio::test]
async fn bridge_launch_persists_runtime_state_and_rollout_discovery_updates_thread() {
    let store = store().await;
    let repo = CodexRuntimeStateRepo::new(&store);
    let session_dir = tempdir("launch");
    let bridge = CodexBridge::new();
    let session = SessionId("s_codex_bridge_state".into());

    let sock = bridge
        .launch_with_options(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.to_string(),
                session_dir: session_dir.clone(),
                codex_home: None,
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![],
            },
            Arc::new(Sink) as Arc<dyn EventSink>,
            BridgeLaunchOptions {
                runtime_store: Some(store.clone()),
                ..BridgeLaunchOptions::default()
            },
        )
        .await
        .expect("bridge launch");

    let launched = repo
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .expect("runtime state");
    assert_eq!(launched.codex_home, Some(session_dir.join("codex-home")));
    assert_eq!(launched.app_server_sock, Some(sock));
    assert!(launched.app_server_pid.is_some());
    assert!(!launched.app_server_adopted);

    let rollout = write_rollout(&session_dir, "thread-from-rollout");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let state = repo
            .find_by_runtime_id(&session)
            .await
            .unwrap()
            .expect("runtime state");
        if state.codex_thread_id.as_deref() == Some("thread-from-rollout") {
            assert_eq!(state.rollout_path, Some(rollout));
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for runtime state thread discovery"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert!(bridge.kill(&session));
    let _ = std::fs::remove_dir_all(&session_dir);
}

#[tokio::test]
async fn bridge_adoption_updates_runtime_state_as_adopted_without_owner_pid() {
    let store = store().await;
    let repo = CodexRuntimeStateRepo::new(&store);
    let session_dir = tempdir("adopted");
    let owner_bridge = CodexBridge::new();
    let adopted_bridge = CodexBridge::new();
    let session = SessionId("s_codex_bridge_adopted".into());

    owner_bridge
        .launch_with_options(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.to_string(),
                session_dir: session_dir.clone(),
                codex_home: None,
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![],
            },
            Arc::new(Sink) as Arc<dyn EventSink>,
            BridgeLaunchOptions {
                runtime_store: Some(store.clone()),
                ..BridgeLaunchOptions::default()
            },
        )
        .await
        .expect("owner launch");

    adopted_bridge
        .launch_with_options(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.to_string(),
                session_dir: session_dir.clone(),
                codex_home: None,
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![],
            },
            Arc::new(Sink) as Arc<dyn EventSink>,
            BridgeLaunchOptions {
                runtime_store: Some(store.clone()),
                ..BridgeLaunchOptions::default()
            },
        )
        .await
        .expect("adopted launch");

    let state = repo
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .expect("runtime state");
    assert!(state.app_server_adopted);
    assert!(state.app_server_pid.is_none());

    assert!(adopted_bridge.kill(&session));
    assert!(owner_bridge.kill(&session));
    let _ = std::fs::remove_dir_all(&session_dir);
}

#[tokio::test]
async fn forced_fresh_bridge_launch_does_not_adopt_stale_app_server_socket() {
    let store = store().await;
    let repo = CodexRuntimeStateRepo::new(&store);
    let session_dir = tempdir("force-fresh");
    let owner_bridge = CodexBridge::new();
    let revived_bridge = CodexBridge::new();
    let session = SessionId("s_codex_bridge_force_fresh".into());

    owner_bridge
        .launch_with_options(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.to_string(),
                session_dir: session_dir.clone(),
                codex_home: None,
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![],
            },
            Arc::new(Sink) as Arc<dyn EventSink>,
            BridgeLaunchOptions {
                runtime_store: Some(store.clone()),
                ..BridgeLaunchOptions::default()
            },
        )
        .await
        .expect("owner launch");

    let owner_state = repo
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .expect("owner runtime state");
    assert!(!owner_state.app_server_adopted);
    assert!(owner_state.app_server_pid.is_some());

    revived_bridge
        .launch_with_options(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.to_string(),
                session_dir: session_dir.clone(),
                codex_home: None,
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![],
            },
            Arc::new(Sink) as Arc<dyn EventSink>,
            BridgeLaunchOptions {
                runtime_store: Some(store.clone()),
                force_fresh_app_server: true,
                ..BridgeLaunchOptions::default()
            },
        )
        .await
        .expect("forced-fresh revive launch");

    let revived_state = repo
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .expect("revived runtime state");
    assert!(
        !revived_state.app_server_adopted,
        "forced revive must spawn a Nexus-owned app-server instead of adopting stale codex.sock"
    );
    assert!(
        revived_state.app_server_pid.is_some(),
        "forced revive must persist the new owned app-server pid"
    );
    assert_ne!(
        owner_state.app_server_pid, revived_state.app_server_pid,
        "forced revive should replace the old app-server process"
    );

    assert!(revived_bridge.kill(&session));
    assert!(owner_bridge.kill(&session));
    let _ = std::fs::remove_dir_all(&session_dir);
}
