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

#[tokio::test]
async fn superseded_resume_cannot_overwrite_newer_same_handle_persisted_thread() {
    let root = tempdir("superseded-resume-sidecar");
    let home = root.join("home");
    let old_rollout = write_rollout(&home);
    std::fs::write(
        old_rollout.with_file_name("rollout-new.jsonl"),
        "{\"type\":\"session_meta\",\"payload\":{\"id\":\"new-thread\"}}\n",
    )
    .unwrap();
    std::fs::create_dir_all(root.join("session")).unwrap();
    let entered = root.join("entered");
    let release = root.join("release");
    let store = store().await;
    let bridge = CodexBridge::new();
    let session = SessionId("superseded-resume-sidecar".into());
    let opts = SupervisorOpts {
        codex_exe: FAKE_BIN.into(),
        session_dir: root.join("session"),
        codex_home: Some(home),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![
            ("FAKE_CODEX_RESUME_GATE_THREAD".into(), THREAD_ID.into()),
            (
                "FAKE_CODEX_RESUME_ENTERED".into(),
                entered.to_string_lossy().into(),
            ),
            (
                "FAKE_CODEX_RESUME_RELEASE".into(),
                release.to_string_lossy().into(),
            ),
        ],
    };
    let old = tokio::spawn({
        let bridge = bridge.clone();
        let store = store.clone();
        let session = session.clone();
        let opts = opts.clone();
        async move {
            bridge
                .launch_with_options(
                    session,
                    opts,
                    Arc::new(RecSink),
                    BridgeLaunchOptions {
                        known_thread_id: Some(THREAD_ID.into()),
                        runtime_store: Some(store),
                        ..Default::default()
                    },
                )
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !entered.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    bridge
        .launch_with_options(
            session.clone(),
            opts,
            Arc::new(RecSink),
            BridgeLaunchOptions {
                known_thread_id: Some("new-thread".into()),
                runtime_store: Some(store.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let pid = bridge.process_ledger(&session).unwrap().os_pid;
    std::fs::write(release, b"release").unwrap();
    assert!(tokio::time::timeout(std::time::Duration::from_secs(3), old)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    assert_eq!(bridge.process_ledger(&session).unwrap().os_pid, pid);
    assert_eq!(
        bridge.transport().bound_thread_id(&session).as_deref(),
        Some("new-thread")
    );
    let row = CodexRuntimeStateRepo::new(&store)
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .unwrap();
    bridge.kill(&session);
    let _ = std::fs::remove_dir_all(root);
    assert_eq!(row.codex_thread_id.as_deref(), Some("new-thread"));
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
async fn resume_native_terminal_before_response_wins_over_stale_seed() {
    assert_native_terminal_before_resume_seed("FAKE_CODEX_RESUME_SCRIPT").await;
}

#[tokio::test]
async fn initialize_terminal_cannot_be_resurrected_by_resume_seed() {
    assert_native_terminal_before_resume_seed("FAKE_CODEX_INITIALIZE_SCRIPT").await;
}

async fn assert_native_terminal_before_resume_seed(script_env: &str) {
    let root = tempdir("native-before-resume-seed");
    let home = root.join("home");
    write_rollout(&home);
    std::fs::create_dir_all(root.join("session")).unwrap();
    let bridge = CodexBridge::new();
    let session = SessionId("native-before-seed".into());
    let notes = serde_json::json!([
        {"method": "turn/completed", "params": {"threadId": THREAD_ID, "turn": {"id": "old", "status": "completed"}}}
    ]);
    bridge.launch_with_options(session.clone(), SupervisorOpts {
        codex_exe: FAKE_BIN.into(), session_dir: root.join("session"),
        codex_home: Some(home), model: None, bus_mcp: None, cwd: None,
        env: vec![
            (script_env.into(), notes.to_string()),
            ("FAKE_CODEX_RESUME_RESPONSE".into(), serde_json::json!({"thread": {"id": THREAD_ID, "turns": [{"id": "old", "status": "inProgress"}]}}).to_string()),
        ],
    }, Arc::new(RecSink), BridgeLaunchOptions { known_thread_id: Some(THREAD_ID.into()), ..Default::default() }).await.unwrap();
    let active = bridge.transport().active_turn_sessions();
    bridge.kill(&session);
    let _ = std::fs::remove_dir_all(root);
    assert!(
        active.is_empty(),
        "resume response rewound a terminal ingested during that request"
    );
}

#[tokio::test]
async fn fresh_bridge_retains_initialize_ingress_only_for_thread_start_result() {
    let root = tempdir("fresh-initialize-ingress");
    std::fs::create_dir_all(root.join("session")).unwrap();
    let bridge = CodexBridge::new();
    let session = SessionId("fresh-initialize-ingress".into());
    let notes = serde_json::json!([
        {"method": "turn/started", "params": {"threadId": "foreign", "turn": {"id": "foreign-turn"}}},
        {"method": "turn/started", "params": {"threadId": "fake-thread", "turn": {"id": "selected-turn"}}}
    ]);
    bridge
        .launch_with_options(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.into(),
                session_dir: root.join("session"),
                codex_home: Some(root.join("home")),
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![("FAKE_CODEX_INITIALIZE_SCRIPT".into(), notes.to_string())],
            },
            Arc::new(RecSink),
            BridgeLaunchOptions {
                create_thread_if_missing: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let tracker = bridge.transport().turn_tracker();
    assert_eq!(
        bridge.transport().bound_thread_id(&session).as_deref(),
        Some("fake-thread")
    );
    assert_eq!(
        tracker.active_turn_id("fake-thread").as_deref(),
        Some("selected-turn")
    );
    assert_eq!(tracker.active_turn_id("foreign"), None);
    bridge.kill(&session);
    let _ = std::fs::remove_dir_all(root);
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
                codex_home: Some(external_codex_home.clone()),
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
                codex_home: Some(external_codex_home.clone()),
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
        let error = bridge
            .launch_with_options(
                session.clone(),
                SupervisorOpts {
                    codex_exe: FAKE_BIN.to_string(),
                    session_dir,
                    codex_home: Some(external_codex_home.clone()),
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
            .expect_err("invalid resume authority must fail the known-thread launch");

        assert!(
            error.to_string().contains("thread/resume"),
            "{tag} should report the rejected thread/resume authority: {error}"
        );
        assert!(
            !bridge.transport().is_bound(&session),
            "{tag} resume authority must fail closed before transport publication"
        );
        assert!(
            !bridge.has(&session),
            "{tag} failed launch must clean up the app-server handle"
        );

        let _ = std::fs::remove_dir_all(state_root);
    }
}
