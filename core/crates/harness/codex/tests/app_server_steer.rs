//! Focused native Codex steering tests.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nexus_contracts::{AgentUpdateKind, EventSink, SessionId, SteerDelivery, WsEvent};
use nexus_harness_codex::{
    spawn_codex_forwarder, AutoApprove, CodexAppServer, CodexAppServerClient,
    CodexAppServerTransport, SupervisorOpts,
};
use serde_json::json;

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
        "nexus-codex-steer-{}-{}-{tag}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
    ));
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}

async fn setup(
    tag: &str,
    mode: Option<&str>,
) -> (
    std::path::PathBuf,
    CodexAppServer,
    CodexAppServerTransport,
    SessionId,
) {
    let dir = tempdir(tag);
    let env = mode
        .map(|mode| vec![("FAKE_CODEX_STEER_MODE".to_string(), mode.to_string())])
        .unwrap_or_default();
    setup_with_env(tag, dir, env).await
}

async fn setup_with_env(
    tag: &str,
    dir: std::path::PathBuf,
    env: Vec<(String, String)>,
) -> (
    std::path::PathBuf,
    CodexAppServer,
    CodexAppServerTransport,
    SessionId,
) {
    let server = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: None,
        model: None,
        bus_mcp: None,
        cwd: None,
        env,
    })
    .await
    .expect("fake app-server starts");
    let client = Arc::new(
        CodexAppServerClient::connect(server.socket(), "nexus")
            .await
            .expect("client connects"),
    );
    let thread_id = client.thread_start().await.expect("thread starts");
    let session = SessionId(format!("s_{tag}"));
    let transport = CodexAppServerTransport::new();
    transport.bind(session.clone(), client, thread_id);
    (dir, server, transport, session)
}

fn accepted_event(session: &SessionId, text: &str) -> WsEvent {
    WsEvent::AgentUpdate {
        session_id: session.clone(),
        kind: AgentUpdateKind::UserInput,
        data: serde_json::json!({"text": text, "source": "steer"}),
    }
}

#[tokio::test]
async fn tracked_active_turn_uses_native_turn_steer() {
    let (dir, server, transport, session) = setup("steer_active", None).await;
    transport
        .turn_tracker()
        .observe_active_turn("fake-thread", "t1");
    let sink = Arc::new(RecSink::default());

    let steering = {
        let transport = transport.clone();
        let session = session.clone();
        let sink = sink.clone();
        tokio::spawn(async move {
            transport
                .steer_observed(
                    &session,
                    "focus tests".into(),
                    sink,
                    accepted_event(&session, "focus tests"),
                )
                .await
        })
    };

    tokio::time::timeout(Duration::from_secs(1), async {
        while sink.0.lock().await.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("native steer acceptance is emitted");
    assert!(
        !steering.is_finished(),
        "turn/steer acceptance alone must not settle Nexus delivery"
    );

    transport
        .turn_tracker()
        .observe_accepted_user_input_echo("fake-thread", "t1", "focus tests");
    let response = tokio::time::timeout(Duration::from_secs(1), steering)
        .await
        .expect("native input echo settles the steer")
        .expect("steer task joins")
        .expect("steer accepted");

    assert_eq!(response.delivery, SteerDelivery::Steered);
    assert_eq!(response.turn_id.as_deref(), Some("t1"));
    assert_eq!(sink.0.lock().await.len(), 1);
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn oversized_native_user_message_settles_exact_steer_receipt() {
    let text = "oversized native receipt ".repeat(512);
    assert!(text.len() > 8 * 1024);
    let script = json!([{
        "method": "item/completed",
        "params": {
            "threadId": "THREAD_ID",
            "turnId": "t1",
            "item": {
                "id": "user-oversized",
                "type": "userMessage",
                "content": [{"type": "text", "text": text}]
            }
        }
    }]);
    let dir = tempdir("steer_oversized_receipt");
    let (dir, server, transport, session) = setup_with_env(
        "steer_oversized_receipt",
        dir,
        vec![("FAKE_CODEX_SCRIPT".into(), script.to_string())],
    )
    .await;
    let thread_id = "fake-thread".to_string();
    let client = Arc::new(
        CodexAppServerClient::connect(server.socket(), "forwarder")
            .await
            .expect("forwarder client connects"),
    );
    client
        .thread_resume(&thread_id)
        .await
        .expect("forwarder subscribes to the thread");
    let forwarder = spawn_codex_forwarder(
        session.clone(),
        client.clone(),
        Arc::new(RecSink::default()),
        Arc::new(AutoApprove),
        transport.turn_tracker().clone(),
    );
    transport
        .turn_tracker()
        .observe_active_turn(&thread_id, "t1");

    let mut steering = {
        let transport = transport.clone();
        let session = session.clone();
        let text = text.clone();
        tokio::spawn(async move {
            transport
                .steer_observed(
                    &session,
                    text.clone(),
                    Arc::new(RecSink::default()),
                    accepted_event(&session, &text),
                )
                .await
        })
    };

    tokio::time::sleep(Duration::from_millis(50)).await;
    let driver = CodexAppServerClient::connect(server.socket(), "driver")
        .await
        .expect("driver connects");
    driver
        .thread_resume(&thread_id)
        .await
        .expect("driver resumes the thread");
    driver
        .turn_start(&thread_id, "emit scripted native receipt")
        .await
        .expect("scripted native receipt is emitted");
    let response = tokio::time::timeout(Duration::from_secs(1), &mut steering)
        .await
        .expect("raw native text must settle before projection sanitization")
        .expect("steer task joins")
        .expect("steer is delivered");
    assert_eq!(response.delivery, SteerDelivery::Steered);

    forwarder.abort();
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn stale_active_turn_rejects_without_starting_a_new_turn() {
    let (dir, server, transport, session) = setup("steer_missing", Some("missing")).await;
    transport
        .turn_tracker()
        .observe_active_turn("fake-thread", "stale");
    let sink = Arc::new(RecSink::default());

    let error = transport
        .steer_observed(
            &session,
            "continue".into(),
            sink.clone(),
            accepted_event(&session, "continue"),
        )
        .await
        .expect_err("a stale steer must not become turn/start");

    assert!(error.message.contains("no active turn to steer"));
    assert!(sink.0.lock().await.is_empty());
    assert!(
        transport
            .turn_tracker()
            .active_turn_id("fake-thread")
            .is_none(),
        "the stale active-turn marker must be cleared"
    );
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn inactive_turn_rejects_without_emitting_accepted_input() {
    let (dir, server, transport, session) = setup("steer_inactive", None).await;
    let sink = Arc::new(RecSink::default());

    let error = transport
        .steer_observed(
            &session,
            "do not start a new turn".into(),
            sink.clone(),
            accepted_event(&session, "do not start a new turn"),
        )
        .await
        .expect_err("inactive steer must be rejected");

    assert!(error.message.contains("no active turn to steer"));
    assert!(sink.0.lock().await.is_empty());
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn expected_turn_mismatch_refreshes_and_retries_once() {
    let (dir, server, transport, session) = setup("steer_mismatch", Some("mismatch_once")).await;
    transport
        .turn_tracker()
        .observe_active_turn("fake-thread", "t1");
    transport.turn_tracker().observe_accepted_user_input_echo(
        "fake-thread",
        "t2",
        "use the new turn",
    );

    let response = transport
        .steer_observed(
            &session,
            "use the new turn".into(),
            Arc::new(RecSink::default()),
            accepted_event(&session, "use the new turn"),
        )
        .await
        .expect("mismatch retry accepted");

    assert_eq!(response.delivery, SteerDelivery::Steered);
    assert_eq!(response.turn_id.as_deref(), Some("t2"));
    assert_eq!(
        transport
            .turn_tracker()
            .active_turn_id("fake-thread")
            .as_deref(),
        Some("t2")
    );
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn non_steerable_turn_rejects_without_emitting_accepted_input() {
    let (dir, server, transport, session) = setup("steer_review", Some("non_steerable")).await;
    transport
        .turn_tracker()
        .observe_active_turn("fake-thread", "t1");
    let sink = Arc::new(RecSink::default());

    let error = transport
        .steer_observed(
            &session,
            "do not queue this".into(),
            sink.clone(),
            accepted_event(&session, "do not queue this"),
        )
        .await
        .expect_err("review turn rejects steer");

    assert!(error.message.contains("cannot steer a review turn"));
    assert!(sink.0.lock().await.is_empty());
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}
