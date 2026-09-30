//! `CodexBridge` integration tests for the fresh-headed codex launch path.
//!
//! These tests use the hermetic fake app-server binary and create the rollout file that a real
//! codex TUI writes after its first turn. The bridge must discover that rollout, resume the
//! thread on its own JSON-RPC connection, bind injection, and forward notifications.

use std::sync::Arc;

use async_trait::async_trait;
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_contracts::AgentUpdateKind;
use nexus_harness_codex::{BridgeLaunchOptions, CodexAppServerClient, CodexBridge, SupervisorOpts};

const FAKE_BIN: &str = env!("CARGO_BIN_EXE_fake_codex_app_server");

#[derive(Default)]
struct RecSink(tokio::sync::Mutex<Vec<WsEvent>>);

#[async_trait]
impl EventSink for RecSink {
    async fn emit(&self, event: WsEvent) {
        self.0.lock().await.push(event);
    }
}

fn tempdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nexus-codex-bridge-{}-{}-{}",
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

fn write_rollout(session_dir: &std::path::Path, thread_id: &str) {
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
}

#[tokio::test]
async fn launch_discovers_rollout_binds_transport_and_forwards_notifications() {
    let session_dir = tempdir("launch");
    let sink = Arc::new(RecSink::default());
    let bridge = CodexBridge::new();
    let session = SessionId("test-codex-bridge".into());

    let sock_path = bridge
        .launch(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.to_string(),
                session_dir: session_dir.clone(),
                codex_home: Some(session_dir.join("codex-home")),
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![],
            },
            sink.clone() as Arc<dyn EventSink>,
        )
        .await
        .expect("bridge launch should start fake app-server");

    assert!(
        bridge.has(&session),
        "bridge should track the launched session immediately"
    );

    write_rollout(&session_dir, "fake-thread");

    let transport = bridge.transport();
    let bind_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    while !transport.is_bound(&session) {
        assert!(
            tokio::time::Instant::now() < bind_deadline,
            "bridge did not bind transport after rollout discovery"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let driver = CodexAppServerClient::connect(&sock_path, "driver")
        .await
        .expect("driver client should connect");
    driver
        .turn_start("fake-thread", "go")
        .await
        .expect("turn_start should succeed");

    let event_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let got = sink.0.lock().await.clone();
        if got.iter().any(|e| {
            matches!(
                e,
                WsEvent::AgentUpdate {
                    kind: AgentUpdateKind::Text,
                    ..
                }
            )
        }) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < event_deadline,
            "timed out waiting for Text AgentUpdate; got events: {got:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert!(
        bridge.kill(&session),
        "kill should remove the launched session"
    );
    assert!(
        !bridge.has(&session),
        "killed session should no longer be tracked"
    );
    let _ = std::fs::remove_dir_all(&session_dir);
}

#[tokio::test]
async fn fresh_launch_can_create_and_bind_an_injectable_thread_immediately() {
    let session_dir = tempdir("eager-thread");
    let sink = Arc::new(RecSink::default());
    let bridge = CodexBridge::new();
    let session = SessionId("test-codex-eager-thread".into());

    bridge
        .launch_with_options(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.to_string(),
                session_dir: session_dir.clone(),
                codex_home: Some(session_dir.join("codex-home")),
                model: None,
                bus_mcp: None,
                cwd: Some(session_dir.clone()),
                env: vec![],
            },
            sink as Arc<dyn EventSink>,
            BridgeLaunchOptions {
                create_thread_if_missing: true,
                ..BridgeLaunchOptions::default()
            },
        )
        .await
        .expect("fresh bridge launch should create its initial thread");

    assert_eq!(
        bridge.transport().bound_thread_id(&session).as_deref(),
        Some("fake-thread"),
        "a fresh headed session must accept Nexus delivery before a human types in the TUI"
    );

    assert!(bridge.kill(&session));
    let _ = std::fs::remove_dir_all(session_dir);
}

#[tokio::test]
async fn launch_reports_discovered_thread_id_for_persistence() {
    let session_dir = tempdir("thread-callback");
    let sink = Arc::new(RecSink::default());
    let bridge = CodexBridge::new();
    let session = SessionId("test-codex-thread-callback".into());
    let seen = Arc::new(tokio::sync::Mutex::new(Vec::<(SessionId, String)>::new()));
    let seen_cb = seen.clone();

    let _sock_path = bridge
        .launch_with_options(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.to_string(),
                session_dir: session_dir.clone(),
                codex_home: Some(session_dir.join("codex-home")),
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![],
            },
            sink.clone() as Arc<dyn EventSink>,
            BridgeLaunchOptions {
                known_thread_id: None,
                on_thread_discovered: Some(Arc::new(move |session, thread_id| {
                    let seen = seen_cb.clone();
                    tokio::spawn(async move {
                        seen.lock().await.push((session, thread_id));
                    });
                })),
                ..BridgeLaunchOptions::default()
            },
        )
        .await
        .expect("bridge launch should start fake app-server");

    write_rollout(&session_dir, "persist-me-thread");

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let got = seen.lock().await.clone();
        if got == vec![(session.clone(), "persist-me-thread".to_string())] {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for thread persistence callback; got {got:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert!(bridge.kill(&session));
    let _ = std::fs::remove_dir_all(&session_dir);
}

#[tokio::test]
async fn launch_with_known_thread_rebinds_existing_bound_session_when_thread_differs() {
    let session_dir = tempdir("known-rebind");
    let sink = Arc::new(RecSink::default());
    let bridge = CodexBridge::new();
    let session = SessionId("test-codex-known-rebind".into());

    let sock_path = bridge
        .launch(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.to_string(),
                session_dir: session_dir.clone(),
                codex_home: Some(session_dir.join("codex-home")),
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![],
            },
            sink.clone() as Arc<dyn EventSink>,
        )
        .await
        .expect("bridge launch should start fake app-server");

    write_rollout(&session_dir, "thread-one");

    let transport = bridge.transport();
    let first_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    while transport.bound_thread_id(&session).as_deref() != Some("thread-one") {
        assert!(
            tokio::time::Instant::now() < first_deadline,
            "bridge did not bind initial thread"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    write_rollout(&session_dir, "thread-two");
    let resumed_sock_path = bridge
        .launch_with_options(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.to_string(),
                session_dir: session_dir.clone(),
                codex_home: Some(session_dir.join("codex-home")),
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![],
            },
            sink.clone() as Arc<dyn EventSink>,
            BridgeLaunchOptions {
                known_thread_id: Some("thread-two".to_string()),
                ..BridgeLaunchOptions::default()
            },
        )
        .await
        .expect("known-thread launch should reuse the live app-server");

    assert_eq!(
        resumed_sock_path, sock_path,
        "same app-server socket is reused"
    );

    assert_eq!(
        transport.bound_thread_id(&session).as_deref(),
        Some("thread-two"),
        "known-thread launch must not return before rebinding an existing live bridge"
    );

    assert!(bridge.kill(&session));
    let _ = std::fs::remove_dir_all(&session_dir);
}

#[tokio::test]
async fn launch_with_known_thread_id_binds_from_existing_codex_home() {
    let session_dir = tempdir("known-thread");
    let external_session_dir = tempdir("known-thread-source");
    let external_codex_home = external_session_dir.join("codex-home");
    write_rollout(&external_session_dir, "known-thread");
    let sink = Arc::new(RecSink::default());
    let bridge = CodexBridge::new();
    let session = SessionId("test-codex-known-thread".into());

    let _sock_path = bridge
        .launch_with_options(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.to_string(),
                session_dir: session_dir.clone(),
                codex_home: Some(session_dir.join("codex-home")),
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![],
            },
            sink.clone() as Arc<dyn EventSink>,
            BridgeLaunchOptions {
                known_thread_id: Some("known-thread".to_string()),
                resume_codex_homes: vec![external_codex_home],
                on_thread_discovered: None,
                ..BridgeLaunchOptions::default()
            },
        )
        .await
        .expect("bridge launch should start fake app-server");

    let transport = bridge.transport();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    while !transport.is_bound(&session) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "bridge did not bind known thread without rollout"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert!(bridge.kill(&session));
    let _ = std::fs::remove_dir_all(&session_dir);
    let _ = std::fs::remove_dir_all(&external_session_dir);
}

#[tokio::test]
async fn launch_with_known_thread_returns_only_after_transport_binding() {
    let session_dir = tempdir("known-thread-ready");
    let external_session_dir = tempdir("known-thread-ready-source");
    let external_codex_home = external_session_dir.join("codex-home");
    write_rollout(&external_session_dir, "known-thread-ready");
    let bridge = CodexBridge::new();
    let session = SessionId("test-codex-known-thread-ready".into());

    let _sock_path = bridge
        .launch_with_options(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.to_string(),
                session_dir: session_dir.clone(),
                codex_home: Some(session_dir.join("codex-home")),
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![("FAKE_CODEX_RESUME_DELAY_MS".into(), "250".into())],
            },
            Arc::new(RecSink::default()) as Arc<dyn EventSink>,
            BridgeLaunchOptions {
                known_thread_id: Some("known-thread-ready".to_string()),
                resume_codex_homes: vec![external_codex_home],
                ..BridgeLaunchOptions::default()
            },
        )
        .await
        .expect("known-thread launch should bind before returning");

    assert_eq!(
        bridge.transport().bound_thread_id(&session).as_deref(),
        Some("known-thread-ready"),
        "known-thread launch returned before publishing the exact transport binding"
    );

    assert!(bridge.kill(&session));
    let _ = std::fs::remove_dir_all(&session_dir);
    let _ = std::fs::remove_dir_all(&external_session_dir);
}
