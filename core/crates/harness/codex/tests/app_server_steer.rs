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
        codex_home: Some(dir.join("codex-home")),
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
async fn busy_steer_publishes_only_native_recorded_input_and_later_send_progresses() {
    for (abort_first, staged_gate) in [
        (false, None),
        (true, None),
        (true, Some(b"".as_slice())),
        (true, Some(b"ab".as_slice())),
    ] {
        let dir = tempdir("steer-consumption");
        let gate = dir.join("consume");
        if let Some(bytes) = staged_gate {
            // Hold the create/truncate or partial-write state before the fake
            // reads it; file existence is not publication of a complete command.
            std::fs::write(&gate, bytes).unwrap();
        }
        let (dir, server, transport, session) = setup_with_env(
            "steer-consumption",
            dir,
            vec![(
                "FAKE_CODEX_STEER_CONSUME_GATE".into(),
                gate.to_string_lossy().into_owned(),
            )],
        )
        .await;
        let client = Arc::new(
            CodexAppServerClient::connect(server.socket(), "observer")
                .await
                .unwrap(),
        );
        client.thread_resume("fake-thread").await.unwrap();
        let sink = Arc::new(RecSink::default());
        let forwarder = spawn_codex_forwarder(
            session.clone(),
            client,
            sink.clone(),
            Arc::new(AutoApprove),
            transport.turn_tracker().clone(),
        );
        transport
            .turn_tracker()
            .observe_active_turn("fake-thread", "t1");
        let mut first = {
            let transport = transport.clone();
            let session = session.clone();
            let sink = sink.clone();
            tokio::spawn(async move {
                transport
                    .steer_observed(
                        &session,
                        "first input".into(),
                        sink,
                        accepted_event(&session, "first input"),
                    )
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if sink.0.lock().await.iter().any(|event| {
                    serde_json::to_string(event)
                        .unwrap()
                        .contains("still working before input")
                }) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("acknowledged steer emitted unrelated native output while consumption is gated");
        assert!(
            !first.is_finished(),
            "RPC queue admission is not native consumption"
        );
        if staged_gate.is_some() {
            assert!(
                tokio::time::timeout(Duration::from_millis(100), &mut first)
                    .await
                    .is_err(),
                "an empty/partial fixture gate must not manufacture a native receipt: {staged_gate:?}"
            );
        }
        assert!(
            !sink.0.lock().await.iter().any(|event| matches!(
                event,
                WsEvent::AgentUpdate {
                    kind: AgentUpdateKind::UserInput,
                    ..
                }
            )),
            "neither RPC acknowledgement nor unrelated native output may report input delivered"
        );
        std::fs::write(
            &gate,
            if abort_first {
                b"abort".as_slice()
            } else {
                b"consume".as_slice()
            },
        )
        .unwrap();
        let first_result = tokio::time::timeout(Duration::from_secs(3), first)
            .await
            .expect("a native terminal cannot pin the receipt for 600s")
            .unwrap();
        if abort_first {
            assert!(
                first_result.is_err(),
                "aborted input without a receipt cannot be reported delivered"
            );
            transport
                .turn_tracker()
                .observe_active_turn("fake-thread", "t2");
            std::fs::write(&gate, b"consume").unwrap();
        } else {
            assert!(first_result.unwrap().accepted);
        }
        let second_result = tokio::time::timeout(
            Duration::from_secs(3),
            transport.steer_observed(
                &session,
                "second input".into(),
                sink.clone(),
                accepted_event(&session, "second input"),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(second_result.accepted);
        let inputs: Vec<_> = sink
            .0
            .lock()
            .await
            .iter()
            .filter_map(|event| match event {
                WsEvent::AgentUpdate {
                    kind: AgentUpdateKind::UserInput,
                    data,
                    ..
                } => Some(data["text"].clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            inputs,
            if abort_first {
                vec![json!("second input")]
            } else {
                vec![json!("first input"), json!("second input")]
            }
        );
        forwarder.abort();
        server.shutdown().await;
        let _ = std::fs::remove_dir_all(dir);
    }
}

#[tokio::test]
async fn tracked_active_turn_uses_native_turn_steer() {
    let dir = tempdir("steer_active");
    let gate = dir.join("record-input");
    std::fs::write(&gate, b"consume").unwrap();
    let (dir, server, transport, session) = setup_with_env(
        "steer_active",
        dir,
        vec![(
            "FAKE_CODEX_STEER_CONSUME_GATE".into(),
            gate.to_string_lossy().into_owned(),
        )],
    )
    .await;
    transport
        .turn_tracker()
        .observe_active_turn("fake-thread", "t1");
    let sink = Arc::new(RecSink::default());
    let client = Arc::new(
        CodexAppServerClient::connect(server.socket(), "observer")
            .await
            .unwrap(),
    );
    client.thread_resume("fake-thread").await.unwrap();
    let forwarder = spawn_codex_forwarder(
        session.clone(),
        client,
        sink.clone(),
        Arc::new(AutoApprove),
        transport.turn_tracker().clone(),
    );

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

    let response = tokio::time::timeout(Duration::from_secs(1), steering)
        .await
        .expect("native input echo settles the steer")
        .expect("steer task joins")
        .expect("steer accepted");

    assert_eq!(response.delivery, SteerDelivery::Steered);
    assert_eq!(response.turn_id.as_deref(), Some("t1"));
    assert_eq!(
        sink.0
            .lock()
            .await
            .iter()
            .filter(|event| matches!(
                event,
                WsEvent::AgentUpdate {
                    kind: AgentUpdateKind::UserInput,
                    ..
                }
            ))
            .count(),
        1
    );
    forwarder.abort();
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
