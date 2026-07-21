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

#[tokio::test]
async fn resumed_in_progress_turn_blocks_new_prompt_until_native_boundary() {
    let state_root = tempdir("active-turn");
    let external_codex_home = state_root.join("external-codex-home");
    write_rollout(&external_codex_home);
    let session_dir = state_root.join("s_active");
    std::fs::create_dir_all(&session_dir).expect("create session dir");

    let bridge = CodexBridge::new();
    let session = SessionId("s_active".into());
    let resume_response = serde_json::json!({
        "thread": {
            "id": THREAD_ID,
            "status": {"type": "active", "activeFlags": []},
            "turns": [{
                "id": "turn-before-daemon-restart",
                "status": "inProgress",
                "items": []
            }]
        }
    });
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
                env: vec![(
                    "FAKE_CODEX_RESUME_RESPONSE".into(),
                    resume_response.to_string(),
                )],
            },
            Arc::new(RecSink) as Arc<dyn EventSink>,
            BridgeLaunchOptions {
                known_thread_id: Some(THREAD_ID.to_string()),
                resume_codex_homes: vec![external_codex_home],
                ..BridgeLaunchOptions::default()
            },
        )
        .await
        .expect("bridge launch should start fake app-server");

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    while bridge.transport().bound_thread_id(&session).as_deref() != Some(THREAD_ID) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for resumed transport binding"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        bridge.transport().active_turn_sessions(),
        vec![session.clone()],
        "thread/resume in-progress turn must seed transport authority before binding"
    );

    let transport = bridge.transport();
    let prompt_session = session.clone();
    let prompt_transport = transport.clone();
    let prompt = tokio::spawn(async move {
        prompt_transport
            .prompt(&prompt_session, "must wait for prior native turn".into())
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    assert!(
        !prompt.is_finished(),
        "a prompt must not start while thread/resume reports an in-progress native turn"
    );
    transport
        .turn_tracker()
        .complete(THREAD_ID, "turn-before-daemon-restart");
    tokio::time::timeout(std::time::Duration::from_secs(2), prompt)
        .await
        .expect("prompt should resume after the restored native boundary")
        .expect("prompt task should not panic")
        .expect("prompt should start after the restored native boundary");

    assert!(bridge.kill(&session));
    let _ = std::fs::remove_dir_all(state_root);
}

#[tokio::test]
async fn resume_refuses_mismatched_or_ambiguous_native_turn_authority() {
    let cases = [
        (
            "wrong-thread",
            serde_json::json!({
                "thread": {
                    "id": "different-thread",
                    "turns": [{"id": "active", "status": "inProgress"}]
                }
            }),
        ),
        (
            "ambiguous-turns",
            serde_json::json!({
                "thread": {
                    "id": THREAD_ID,
                    "turns": [
                        {"id": "active-a", "status": "inProgress"},
                        {"id": "active-b", "status": "inProgress"}
                    ]
                }
            }),
        ),
    ];

    for (tag, resume_response) in cases {
        let state_root = tempdir(tag);
        let external_codex_home = state_root.join("external-codex-home");
        write_rollout(&external_codex_home);
        let session_dir = state_root.join("session");
        std::fs::create_dir_all(&session_dir).expect("create session dir");

        let bridge = CodexBridge::new();
        let session = SessionId(format!("s_{tag}"));
        bridge
            .launch_with_options(
                session.clone(),
                SupervisorOpts {
                    codex_exe: FAKE_BIN.to_string(),
                    session_dir,
                    codex_home: None,
                    model: None,
                    bus_mcp: None,
                    cwd: None,
                    env: vec![(
                        "FAKE_CODEX_RESUME_RESPONSE".into(),
                        resume_response.to_string(),
                    )],
                },
                Arc::new(RecSink) as Arc<dyn EventSink>,
                BridgeLaunchOptions {
                    known_thread_id: Some(THREAD_ID.to_string()),
                    resume_codex_homes: vec![external_codex_home],
                    ..BridgeLaunchOptions::default()
                },
            )
            .await
            .expect("bridge launch should start fake app-server");

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        assert!(
            !bridge.transport().is_bound(&session),
            "{tag} resume authority must fail closed before transport publication"
        );

        assert!(bridge.kill(&session));
        let _ = std::fs::remove_dir_all(state_root);
    }
}
