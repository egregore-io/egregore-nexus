use std::sync::Arc;

use async_trait::async_trait;
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_harness_codex::storage::CodexRuntimeStateRepo;
use nexus_harness_codex::{BridgeLaunchOptions, CodexBridge, SupervisorOpts};
use nexus_store::Store;

const FAKE_BIN: &str = env!("CARGO_BIN_EXE_fake_codex_app_server");
const THREAD_ID: &str = "seed-thread";

#[derive(Default)]
struct RecSink;

#[async_trait]
impl EventSink for RecSink {
    async fn emit(&self, _event: WsEvent) {}
}

fn tempdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nexus-codex-resume-seed-{}-{}-{}",
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

async fn store() -> Arc<Store> {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    Arc::new(store)
}

fn write_rollout(codex_home: &std::path::Path) -> std::path::PathBuf {
    let rollout_dir = codex_home
        .join("sessions")
        .join("2026")
        .join("06")
        .join("29");
    std::fs::create_dir_all(&rollout_dir).expect("create rollout dir");
    let rollout = rollout_dir.join("rollout-2026-06-29T12-00-00-seed-thread.jsonl");
    let first_line = serde_json::json!({
        "type": "session_meta",
        "payload": { "id": THREAD_ID }
    });
    std::fs::write(&rollout, format!("{first_line}\n")).expect("write rollout");
    rollout
}

#[tokio::test]
async fn known_thread_resume_uses_existing_codex_home_without_copying_rollout() {
    let store = store().await;
    let repo = CodexRuntimeStateRepo::new(&store);
    let state_root = tempdir("root");
    let external_codex_home = state_root.join("external-codex-home");
    let source_rollout = write_rollout(&external_codex_home);
    let session_dir = state_root.join("s_new");
    std::fs::create_dir_all(&session_dir).expect("create new session dir");

    let bridge = CodexBridge::new();
    let session = SessionId("s_new".into());
    bridge
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
            Arc::new(RecSink) as Arc<dyn EventSink>,
            BridgeLaunchOptions {
                known_thread_id: Some(THREAD_ID.to_string()),
                resume_codex_homes: vec![external_codex_home.clone()],
                on_thread_discovered: None,
                runtime_store: Some(store.clone()),
                ..BridgeLaunchOptions::default()
            },
        )
        .await
        .expect("bridge launch should start fake app-server");

    let launched = repo
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .expect("runtime state");
    assert_eq!(
        launched.codex_thread_id.as_deref(),
        Some(THREAD_ID),
        "known resume thread must be persisted before async binding/forwarding"
    );

    let seeded = session_dir.join("codex-home").join("sessions").join(
        source_rollout
            .strip_prefix(external_codex_home.join("sessions"))
            .unwrap(),
    );
    assert!(
        !seeded.exists(),
        "resume must not copy rollout history into a fresh CODEX_HOME"
    );

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let state = repo
            .find_by_runtime_id(&session)
            .await
            .unwrap()
            .expect("runtime state");
        assert_eq!(state.codex_home, Some(external_codex_home.clone()));
        if state.codex_thread_id.as_deref() == Some(THREAD_ID) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for known resume thread to persist"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert!(bridge.kill(&session));
    let _ = std::fs::remove_dir_all(state_root);
}
