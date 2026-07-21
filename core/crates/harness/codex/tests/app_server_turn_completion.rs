//! Integration coverage for headed Codex app-server turn completion.
//!
//! `CodexAppServerTransport::inject_turn` carries Nexus bus batches. A `turn/start` handoff to the
//! persistent app-server is not delivery to the actual Codex agent session; when Codex returns a
//! turn id, bus delivery must wait until the forwarder observes `turn/completed`/`error` so the
//! realtime loop only marks `in_flight.delivered_at` after real receipt.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nexus_contracts::batch::{BatchCounts, BatchMessage, NexusBatch};
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::{MessageId, ProjectId, SessionId};
use nexus_contracts::message::{Message, Provenance};
use nexus_contracts::ports::{AgentTurnExecutionPort, DispatchPort, EventSink, InjectError};
use nexus_contracts::ProviderLimitReason;
use nexus_contracts::{AgentUpdateKind, Kind, Scope};
use nexus_dispatch::{
    AgentRegistry, AgentState, Bell, DispatchService, EventLoop, LoopDeps, ServiceDeps,
};
use nexus_harness_codex::app_server::spawn_codex_forwarder;
use nexus_harness_codex::{
    AutoApprove, CodexAppServer, CodexAppServerClient, CodexAppServerTransport, CodexTurnTracker,
    SupervisorOpts,
};
use nexus_store::repos::Messages;
use nexus_store::Store;

const FAKE_BIN: &str = env!("CARGO_BIN_EXE_fake_codex_app_server");

struct NullSink;

#[async_trait]
impl EventSink for NullSink {
    async fn emit(&self, _event: WsEvent) {}
}

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
        "nexus-codex-turn-completion-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
        tag,
    ));
    std::fs::create_dir_all(&dir).expect("create test tempdir");
    dir
}

fn batch() -> NexusBatch {
    NexusBatch {
        counts: BatchCounts {
            dms: 0,
            thread: 1,
            total: 1,
        },
        dms: vec![],
        threads: vec![BatchMessage {
            id: MessageId("m_codex_wait".into()),
            from: "alex".into(),
            kind: Kind::Human,
            scope: Scope::Thread,
            thread: Some("smoke".into()),
            topic: None,
            body: "hold the realtime loop busy until turn completion".into(),
            truncated: false,
        }],
        dm_message_ids: vec![],
        thread_message_ids: vec![MessageId("m_codex_wait".into())],
        message_ids: vec![MessageId("m_codex_wait".into())],
    }
}

#[tokio::test]
async fn cancelled_native_receipt_waits_unregister_before_late_receipts() {
    let tracker = CodexTurnTracker::default();
    let mut keys = Vec::new();

    for index in 0..32 {
        let thread_id = format!("thread-cancel-{index}");
        let turn_id = format!("turn-cancel-{index}");
        let text = format!("unique cancelled prompt {index}");
        let mut wait = Box::pin(tracker.wait_for_accepted_user_input_echo(
            &thread_id,
            &turn_id,
            &text,
            Duration::from_secs(60),
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut wait)
                .await
                .is_err(),
            "receipt wait must be registered and pending before cancellation"
        );
        drop(wait);

        tracker.observe_accepted_user_input_echo(&thread_id, &turn_id, &text);
        keys.push((thread_id, turn_id, text));
    }

    for (thread_id, turn_id, text) in keys {
        tokio::time::timeout(
            Duration::from_millis(100),
            tracker.wait_for_accepted_user_input_echo(
                &thread_id,
                &turn_id,
                &text,
                Duration::from_millis(25),
            ),
        )
        .await
        .expect("late receipt lookup must remain bounded")
        .expect("late receipt must be buffered after the cancelled waiter unregisters");
    }
}

#[tokio::test]
async fn inject_turn_waits_for_forwarded_turn_completion() {
    let dir = tempdir("inject-waits");
    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: Some(dir.join("codex-home")),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![
            ("FAKE_CODEX_REPLY_BEFORE_NOTIFICATIONS".into(), "1".into()),
            ("FAKE_CODEX_NOTIFICATION_DELAY_MS".into(), "1000".into()),
        ],
    })
    .await
    .expect("fake app-server should start");

    let client = Arc::new(
        CodexAppServerClient::connect(srv.socket(), "nexus")
            .await
            .expect("nexus client should connect"),
    );
    let thread = client
        .thread_start()
        .await
        .expect("thread_start should succeed");

    let session = SessionId("s_codex_wait".into());
    let transport = CodexAppServerTransport::new();
    transport.bind(session.clone(), client.clone(), thread);
    let forwarder = spawn_codex_forwarder(
        session.clone(),
        client.clone(),
        Arc::new(NullSink),
        Arc::new(AutoApprove),
        transport.turn_tracker(),
    );

    let started = tokio::time::Instant::now();
    transport
        .inject_turn(&session, &batch())
        .await
        .expect("inject_turn should wait for forwarded turn completion and succeed");

    assert!(
        started.elapsed() >= Duration::from_millis(900),
        "bus inject returned before the delayed turn/completed notification"
    );

    forwarder.abort();
    drop(client);
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn inject_turn_settles_after_model_progress_while_the_turn_remains_active() {
    let dir = tempdir("inject-progress-receipt");
    let script = serde_json::to_string(&serde_json::json!([{
        "method": "item/agentMessage/delta",
        "params": {
            "threadId": "THREAD_ID",
            "turnId": "t1",
            "itemId": "am1",
            "delta": "the model saw the injected batch"
        }
    }]))
    .expect("script serializes");
    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: Some(dir.join("codex-home")),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![("FAKE_CODEX_SCRIPT".into(), script)],
    })
    .await
    .expect("fake app-server should start");

    let client = Arc::new(
        CodexAppServerClient::connect(srv.socket(), "nexus")
            .await
            .expect("nexus client should connect"),
    );
    let thread = client
        .thread_start()
        .await
        .expect("thread_start should succeed");

    let session = SessionId("s_codex_progress_receipt".into());
    let transport = CodexAppServerTransport::new();
    transport.bind(session.clone(), client.clone(), thread);
    let forwarder = spawn_codex_forwarder(
        session.clone(),
        client,
        Arc::new(NullSink),
        Arc::new(AutoApprove),
        transport.turn_tracker(),
    );

    tokio::time::timeout(
        Duration::from_secs(2),
        transport.inject_turn(&session, &batch()),
    )
    .await
    .expect("model progress must settle delivery without waiting for the turn to end")
    .expect("model progress is successful delivery evidence");
    assert_eq!(
        transport.active_turn_sessions(),
        vec![session.clone()],
        "delivery settlement must not clear the native turn needed for later steering"
    );

    forwarder.abort();
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn observed_inject_emits_user_input_before_codex_stream_that_precedes_response() {
    let dir = tempdir("observed-before-response");
    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: Some(dir.join("codex-home")),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![],
    })
    .await
    .expect("fake app-server should start");

    let client = Arc::new(
        CodexAppServerClient::connect(srv.socket(), "nexus")
            .await
            .expect("nexus client should connect"),
    );
    let thread = client
        .thread_start()
        .await
        .expect("thread_start should succeed");

    let session = SessionId("s_codex_observed_order".into());
    let transport = CodexAppServerTransport::new();
    transport.bind(session.clone(), client.clone(), thread);
    let sink = Arc::new(RecSink::default());
    let forwarder = spawn_codex_forwarder(
        session.clone(),
        client,
        sink.clone(),
        Arc::new(AutoApprove),
        transport.turn_tracker(),
    );

    let accepted_event = WsEvent::AgentUpdate {
        session_id: session.clone(),
        kind: AgentUpdateKind::UserInput,
        data: serde_json::json!({
            "text": "accepted bus input",
            "clientMessageId": "bus:m_codex_wait",
        }),
    };

    transport
        .inject_turn_observed(&session, &batch(), sink.clone(), accepted_event)
        .await
        .expect("observed inject should succeed");

    let kinds: Vec<AgentUpdateKind> = sink
        .0
        .lock()
        .await
        .iter()
        .filter_map(|event| match event {
            WsEvent::AgentUpdate { kind, .. } => Some(*kind),
            _ => None,
        })
        .collect();
    assert!(
        kinds.starts_with(&[
            AgentUpdateKind::UserInput,
            AgentUpdateKind::Text,
            AgentUpdateKind::TurnEnd,
        ]),
        "observed bus input must be visible before Codex assistant output and turn_end; got {kinds:?}"
    );
    assert!(
        transport.active_turn_sessions().is_empty(),
        "a turn that completed before its turn/start response must not be resurrected as active"
    );

    forwarder.abort();
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn observed_inject_suppresses_matching_codex_user_message_echo() {
    let dir = tempdir("observed-user-message-echo");
    let script = serde_json::to_string(&serde_json::json!([
        {
            "method": "item/completed",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1",
                "item": {
                    "type": "userMessage",
                    "id": "um1",
                    "content": [
                        {"type": "text", "text": "accepted bus input"}
                    ]
                }
            }
        },
        {
            "method": "item/agentMessage/delta",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1",
                "itemId": "am1",
                "delta": "reply"
            }
        },
        {
            "method": "turn/completed",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1"
            }
        }
    ]))
    .expect("script serializes");
    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: Some(dir.join("codex-home")),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![("FAKE_CODEX_SCRIPT".into(), script)],
    })
    .await
    .expect("fake app-server should start");

    let client = Arc::new(
        CodexAppServerClient::connect(srv.socket(), "nexus")
            .await
            .expect("nexus client should connect"),
    );
    let thread = client
        .thread_start()
        .await
        .expect("thread_start should succeed");

    let session = SessionId("s_codex_observed_echo".into());
    let transport = CodexAppServerTransport::new();
    transport.bind(session.clone(), client.clone(), thread);
    let sink = Arc::new(RecSink::default());
    let forwarder = spawn_codex_forwarder(
        session.clone(),
        client,
        sink.clone(),
        Arc::new(AutoApprove),
        transport.turn_tracker(),
    );

    let accepted_event = WsEvent::AgentUpdate {
        session_id: session.clone(),
        kind: AgentUpdateKind::UserInput,
        data: serde_json::json!({
            "text": "accepted bus input",
            "clientMessageId": "bus:m_codex_wait",
        }),
    };

    transport
        .inject_turn_observed(&session, &batch(), sink.clone(), accepted_event)
        .await
        .expect("observed inject should succeed");

    let events = sink.0.lock().await.clone();
    let user_inputs: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            WsEvent::AgentUpdate {
                kind: AgentUpdateKind::UserInput,
                data,
                ..
            } => data.get("text").and_then(|text| text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        user_inputs,
        vec!["accepted bus input"],
        "accepted event and matching Codex userMessage echo must collapse to one visible input"
    );

    let kinds: Vec<AgentUpdateKind> = events
        .iter()
        .filter_map(|event| match event {
            WsEvent::AgentUpdate { kind, .. } => Some(*kind),
            _ => None,
        })
        .collect();
    assert!(
        kinds.starts_with(&[
            AgentUpdateKind::UserInput,
            AgentUpdateKind::Text,
            AgentUpdateKind::TurnEnd,
        ]),
        "observed bus input must stay before assistant output and turn_end; got {kinds:?}"
    );

    forwarder.abort();
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn observed_inject_final_usage_limit_returns_provider_limit() {
    let dir = tempdir("observed-provider-limit");
    let script = serde_json::to_string(&serde_json::json!([
        {
            "method": "error",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1",
                "willRetry": false,
                "error": {
                    "message": "usage limit",
                    "codexErrorInfo": "usageLimitExceeded",
                    "retry_after_ms": 1000,
                    "provider": "openai",
                    "model": "gpt-5"
                }
            }
        }
    ]))
    .expect("script serializes");
    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: Some(dir.join("codex-home")),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![("FAKE_CODEX_SCRIPT".into(), script)],
    })
    .await
    .expect("fake app-server should start");

    let client = Arc::new(
        CodexAppServerClient::connect(srv.socket(), "nexus")
            .await
            .expect("nexus client should connect"),
    );
    let thread = client
        .thread_start()
        .await
        .expect("thread_start should succeed");

    let session = SessionId("s_codex_provider_limit".into());
    let transport = CodexAppServerTransport::new();
    transport.bind(session.clone(), client.clone(), thread);
    let sink = Arc::new(RecSink::default());
    let forwarder = spawn_codex_forwarder(
        session.clone(),
        client,
        sink.clone(),
        Arc::new(AutoApprove),
        transport.turn_tracker(),
    );

    let accepted_event = WsEvent::AgentUpdate {
        session_id: session.clone(),
        kind: AgentUpdateKind::UserInput,
        data: serde_json::json!({"text": "accepted bus input"}),
    };
    let err = transport
        .inject_turn_observed(&session, &batch(), sink.clone(), accepted_event)
        .await
        .expect_err("structured final usage limit must open provider-limit hold");

    match err {
        InjectError::ProviderLimit(limit) => {
            assert_eq!(limit.session, session);
            assert_eq!(limit.reason, ProviderLimitReason::UsageLimit);
            assert_eq!(limit.provider.as_deref(), Some("openai"));
            assert_eq!(limit.model.as_deref(), Some("gpt-5"));
            assert!(limit.reset_hint.is_some());
        }
        other => panic!("expected provider limit, got {other:?}"),
    }

    forwarder.abort();
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn accepted_then_final_usage_limit_settles_store_error_without_delivery() {
    let dir = tempdir("store-provider-limit");
    let script = serde_json::to_string(&serde_json::json!([
        {
            "method": "error",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1",
                "willRetry": false,
                "error": {
                    "message": "usage limit",
                    "codexErrorInfo": "usageLimitExceeded",
                    "retry_after_ms": 1000,
                    "provider": "openai",
                    "model": "gpt-5"
                }
            }
        }
    ]))
    .expect("script serializes");
    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: Some(dir.join("codex-home")),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![("FAKE_CODEX_SCRIPT".into(), script)],
    })
    .await
    .expect("fake app-server should start");

    let client = Arc::new(
        CodexAppServerClient::connect(srv.socket(), "nexus")
            .await
            .expect("nexus client should connect"),
    );
    let thread = client
        .thread_start()
        .await
        .expect("thread_start should succeed");

    let session = SessionId("s_codex_store_provider_limit".into());
    let transport = CodexAppServerTransport::new();
    transport.bind(session.clone(), client.clone(), thread);
    let events = Arc::new(RecSink::default());
    let forwarder = spawn_codex_forwarder(
        session.clone(),
        client,
        events.clone(),
        Arc::new(AutoApprove),
        transport.turn_tracker(),
    );

    let store = Arc::new(Store::open(":memory:").await.expect("open store"));
    store.migrate().await.expect("migrate store");
    let message_id = MessageId("m_codex_store_limit".into());
    Messages::new(&store)
        .insert(&Message {
            id: message_id.clone(),
            project: ProjectId("codex-store-limit".into()),
            from: "alex".into(),
            scope: Scope::Dm,
            thread: None,
            topic: None,
            body: "accepted first, then terminal usage limit".into(),
            summary: None,
            provenance: Provenance {
                from: "alex".into(),
                kind: Kind::Human,
                thread: None,
                topic: None,
                stamp: None,
            },
            created_at: 0,
        })
        .await
        .expect("insert message");

    let bell = Bell::new();
    let registry = AgentRegistry::new();
    let realtime = DispatchService::new(ServiceDeps {
        store: store.clone(),
        bell: bell.clone(),
        registry: registry.clone(),
        project: "codex-store-limit".into(),
        drain_limit: 50,
        preview_chars: 10_000,
    });
    let event_loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell,
            registry: registry.clone(),
            turn_exec: Arc::new(transport),
            events: events.clone(),
            project: "codex-store-limit".into(),
            drain_limit: 50,
            preview_chars: 10_000,
            completion_timeout: Duration::from_secs(5),
            provider_limit_default_cooldown: Duration::from_millis(10),
        },
    );
    realtime
        .enqueue(&session, &message_id)
        .await
        .expect("enqueue message");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let mut rows = store
            .conn
            .query(
                "SELECT state FROM in_flight WHERE message_id = ?1",
                [message_id.0.as_str()],
            )
            .await
            .expect("query delivery state");
        let state = rows
            .next()
            .await
            .expect("read delivery state")
            .map(|row| row.get::<String>(0).expect("state"));
        if state.as_deref() == Some("error") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "usage-limit delivery did not settle"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut rows = store
        .conn
        .query(
            "SELECT state, error_code, delivered_at, attempt_count FROM in_flight \
             WHERE message_id = ?1",
            [message_id.0.as_str()],
        )
        .await
        .expect("query settled delivery");
    let row = rows
        .next()
        .await
        .expect("read settled delivery")
        .expect("delivery row");
    assert_eq!(row.get::<String>(0).unwrap(), "error");
    assert_eq!(row.get::<String>(1).unwrap(), "provider_limit");
    assert_eq!(row.get::<Option<i64>>(2).unwrap(), None);
    assert_eq!(row.get::<i64>(3).unwrap(), 1);
    assert_eq!(registry.get(&session), AgentState::ProviderLimited);

    let recorded = events.0.lock().await;
    assert_eq!(
        recorded
            .iter()
            .filter(|event| matches!(
                event,
                WsEvent::AgentUpdate {
                    kind: AgentUpdateKind::UserInput,
                    ..
                }
            ))
            .count(),
        1,
        "app-server acceptance remains visible exactly once"
    );
    assert!(
        recorded
            .iter()
            .all(|event| !matches!(event, WsEvent::MessageDelivered { .. })),
        "terminal usage limit must never emit message.delivered"
    );
    drop(recorded);

    event_loop.abort();
    forwarder.abort();
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn observed_inject_retrying_usage_limit_waits_for_later_terminal_completion() {
    let dir = tempdir("observed-retrying-provider-limit");
    let script = serde_json::to_string(&serde_json::json!([
        {
            "method": "error",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1",
                "willRetry": true,
                "error": {
                    "message": "usage limit",
                    "codexErrorInfo": "usageLimitExceeded",
                    "retry_after_ms": 1000
                }
            }
        },
        {
            "method": "item/agentMessage/delta",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1",
                "itemId": "am1",
                "delta": "eventual reply"
            }
        },
        {
            "method": "turn/completed",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1"
            }
        }
    ]))
    .expect("script serializes");
    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: Some(dir.join("codex-home")),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![("FAKE_CODEX_SCRIPT".into(), script)],
    })
    .await
    .expect("fake app-server should start");

    let client = Arc::new(
        CodexAppServerClient::connect(srv.socket(), "nexus")
            .await
            .expect("nexus client should connect"),
    );
    let thread = client
        .thread_start()
        .await
        .expect("thread_start should succeed");

    let session = SessionId("s_codex_retrying_provider_limit".into());
    let transport = CodexAppServerTransport::new();
    transport.bind(session.clone(), client.clone(), thread);
    let sink = Arc::new(RecSink::default());
    let forwarder = spawn_codex_forwarder(
        session.clone(),
        client,
        sink.clone(),
        Arc::new(AutoApprove),
        transport.turn_tracker(),
    );

    let accepted_event = WsEvent::AgentUpdate {
        session_id: session.clone(),
        kind: AgentUpdateKind::UserInput,
        data: serde_json::json!({"text": "accepted bus input"}),
    };
    transport
        .inject_turn_observed(&session, &batch(), sink.clone(), accepted_event)
        .await
        .expect("willRetry:true must not open provider-limit hold");

    let turn_end_count = sink
        .0
        .lock()
        .await
        .iter()
        .filter(|event| {
            matches!(
                event,
                WsEvent::AgentUpdate {
                    kind: AgentUpdateKind::TurnEnd,
                    ..
                }
            )
        })
        .count();
    assert_eq!(
        turn_end_count, 1,
        "retrying error must not emit an extra terminal turn_end"
    );

    forwarder.abort();
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn observed_prompt_emits_initial_prompt_before_codex_stream_and_suppresses_echo() {
    let dir = tempdir("observed-prompt-initial");
    let script = serde_json::to_string(&serde_json::json!([
        {
            "method": "item/completed",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1",
                "item": {
                    "type": "userMessage",
                    "id": "um1",
                    "content": [
                        {"type": "text", "text": "You are ada."}
                    ]
                }
            }
        },
        {
            "method": "item/agentMessage/delta",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1",
                "itemId": "am1",
                "delta": "boot acknowledged"
            }
        },
        {
            "method": "turn/completed",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1"
            }
        }
    ]))
    .expect("script serializes");
    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: Some(dir.join("codex-home")),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![("FAKE_CODEX_SCRIPT".into(), script)],
    })
    .await
    .expect("fake app-server should start");

    let client = Arc::new(
        CodexAppServerClient::connect(srv.socket(), "nexus")
            .await
            .expect("nexus client should connect"),
    );
    let thread = client
        .thread_start()
        .await
        .expect("thread_start should succeed");

    let session = SessionId("s_codex_observed_prompt".into());
    let transport = CodexAppServerTransport::new();
    transport.bind(session.clone(), client.clone(), thread);
    let sink = Arc::new(RecSink::default());
    let forwarder = spawn_codex_forwarder(
        session.clone(),
        client,
        sink.clone(),
        Arc::new(AutoApprove),
        transport.turn_tracker(),
    );

    let accepted_event = WsEvent::AgentUpdate {
        session_id: session.clone(),
        kind: AgentUpdateKind::UserInput,
        data: serde_json::json!({
            "text": "You are ada.",
            "source": "initial_prompt",
            "clientMessageId": "initial-prompt:r_codex",
            "runtimeId": "r_codex",
            "harness": "codex",
        }),
    };

    transport
        .prompt_observed(
            &session,
            "You are ada.".to_string(),
            sink.clone(),
            accepted_event,
        )
        .await
        .expect("observed prompt should succeed");

    let events = sink.0.lock().await.clone();
    let user_inputs: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            WsEvent::AgentUpdate {
                kind: AgentUpdateKind::UserInput,
                data,
                ..
            } => Some(data.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        user_inputs.len(),
        1,
        "accepted initial prompt and matching Codex userMessage echo must collapse to one visible input"
    );
    assert_eq!(user_inputs[0]["source"], "initial_prompt");
    assert_eq!(user_inputs[0]["clientMessageId"], "initial-prompt:r_codex");

    let kinds: Vec<AgentUpdateKind> = events
        .iter()
        .filter_map(|event| match event {
            WsEvent::AgentUpdate { kind, .. } => Some(*kind),
            _ => None,
        })
        .collect();
    assert!(
        kinds.starts_with(&[
            AgentUpdateKind::UserInput,
            AgentUpdateKind::Text,
            AgentUpdateKind::TurnEnd,
        ]),
        "observed initial prompt must be visible before Codex assistant output and turn_end; got {kinds:?}"
    );

    forwarder.abort();
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn observed_prompt_requires_native_user_message_receipt_before_success() {
    let dir = tempdir("observed-prompt-native-receipt");
    let script = serde_json::to_string(&serde_json::json!([
        {
            "method": "turn/started",
            "params": {
                "threadId": "THREAD_ID",
                "turn": {"id": "t1"}
            }
        },
        {
            "method": "item/completed",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1",
                "item": {
                    "type": "userMessage",
                    "id": "um1",
                    "content": [{"type": "text", "text": "must reach native context"}]
                }
            }
        },
        {
            "method": "turn/completed",
            "params": {
                "threadId": "THREAD_ID",
                "turn": {"id": "t1"}
            }
        }
    ]))
    .expect("script serializes");
    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: Some(dir.join("codex-home")),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![
            ("FAKE_CODEX_SCRIPT".into(), script),
            ("FAKE_CODEX_REPLY_BEFORE_NOTIFICATIONS".into(), "1".into()),
            ("FAKE_CODEX_NOTIFICATION_DELAY_MS".into(), "400".into()),
        ],
    })
    .await
    .expect("fake app-server should start");

    let client = Arc::new(
        CodexAppServerClient::connect(srv.socket(), "nexus")
            .await
            .expect("nexus client should connect"),
    );
    let thread = client
        .thread_start()
        .await
        .expect("thread_start should succeed");

    let session = SessionId("s_codex_observed_prompt_native_receipt".into());
    let transport = CodexAppServerTransport::new();
    transport.bind(session.clone(), client.clone(), thread);
    let sink = Arc::new(RecSink::default());
    let forwarder = spawn_codex_forwarder(
        session.clone(),
        client,
        sink.clone(),
        Arc::new(AutoApprove),
        transport.turn_tracker(),
    );

    let accepted_event = WsEvent::AgentUpdate {
        session_id: session.clone(),
        kind: AgentUpdateKind::UserInput,
        data: serde_json::json!({
            "text": "must reach native context",
            "source": "session_prompt",
            "clientMessageId": "cm-native-receipt",
        }),
    };

    let result = tokio::time::timeout(
        Duration::from_millis(250),
        transport.prompt_observed(
            &session,
            "must reach native context".to_string(),
            sink.clone(),
            accepted_event,
        ),
    )
    .await;
    assert!(
        result.is_err(),
        "turn/start acknowledgement without item/completed userMessage must not complete an observed prompt"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        sink.0.lock().await.iter().all(|event| !matches!(
            event,
            WsEvent::AgentUpdate {
                kind: AgentUpdateKind::UserInput,
                ..
            }
        )),
        "a native receipt arriving after prompt cancellation must not publish stale user input"
    );

    forwarder.abort();
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn prompt_returns_after_acceptance_and_holds_boundary_until_turn_completion() {
    let dir = tempdir("prompt-accepted");
    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: Some(dir.join("codex-home")),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![
            ("FAKE_CODEX_REPLY_BEFORE_NOTIFICATIONS".into(), "1".into()),
            ("FAKE_CODEX_NOTIFICATION_DELAY_MS".into(), "1000".into()),
        ],
    })
    .await
    .expect("fake app-server should start");

    let client = Arc::new(
        CodexAppServerClient::connect(srv.socket(), "nexus")
            .await
            .expect("nexus client should connect"),
    );
    let thread = client
        .thread_start()
        .await
        .expect("thread_start should succeed");

    let session = SessionId("s_codex_prompt_wait".into());
    let transport = CodexAppServerTransport::new();
    transport.bind(session.clone(), client.clone(), thread);
    let forwarder = spawn_codex_forwarder(
        session.clone(),
        client,
        Arc::new(NullSink),
        Arc::new(AutoApprove),
        transport.turn_tracker(),
    );

    tokio::time::timeout(
        Duration::from_millis(250),
        transport.prompt(&session, "direct operator prompt".to_string()),
    )
    .await
    .expect("prompt should return after turn/start acceptance, not turn completion")
    .expect("prompt should succeed once Codex accepts the turn");

    assert_eq!(
        transport.active_turn_sessions(),
        vec![session.clone()],
        "accepted direct prompts must hold later prompt rows at the native turn boundary"
    );

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if transport.active_turn_sessions().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("turn completion should release the durable prompt boundary");

    forwarder.abort();
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn direct_prompt_suppresses_matching_codex_user_message_echo() {
    let dir = tempdir("direct-prompt-user-message-echo");
    let script = serde_json::to_string(&serde_json::json!([
        {
            "method": "item/completed",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1",
                "item": {
                    "type": "userMessage",
                    "id": "um1",
                    "content": [
                        {"type": "text", "text": "direct operator prompt"}
                    ]
                }
            }
        },
        {
            "method": "item/agentMessage/delta",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1",
                "itemId": "am1",
                "delta": "reply"
            }
        },
        {
            "method": "turn/completed",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1"
            }
        }
    ]))
    .expect("script serializes");
    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: Some(dir.join("codex-home")),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![("FAKE_CODEX_SCRIPT".into(), script)],
    })
    .await
    .expect("fake app-server should start");

    let client = Arc::new(
        CodexAppServerClient::connect(srv.socket(), "nexus")
            .await
            .expect("nexus client should connect"),
    );
    let thread = client
        .thread_start()
        .await
        .expect("thread_start should succeed");

    let session = SessionId("s_codex_direct_prompt_echo".into());
    let transport = CodexAppServerTransport::new();
    transport.bind(session.clone(), client.clone(), thread);
    let sink = Arc::new(RecSink::default());
    let forwarder = spawn_codex_forwarder(
        session.clone(),
        client,
        sink.clone(),
        Arc::new(AutoApprove),
        transport.turn_tracker(),
    );

    // This mirrors dispatch.rs: direct /prompt publishes a session-visible user row before
    // calling the transport prompt hook.
    sink.emit(WsEvent::AgentUpdate {
        session_id: session.clone(),
        kind: AgentUpdateKind::UserInput,
        data: serde_json::json!({
            "text": "direct operator prompt",
            "clientMessageId": "prompt:direct",
        }),
    })
    .await;

    transport
        .prompt(&session, "direct operator prompt".to_string())
        .await
        .expect("direct prompt should succeed");

    let events = sink.0.lock().await.clone();
    let user_inputs: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            WsEvent::AgentUpdate {
                kind: AgentUpdateKind::UserInput,
                data,
                ..
            } => data.get("text").and_then(|text| text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        user_inputs,
        vec!["direct operator prompt"],
        "dispatch user_input and matching Codex userMessage echo must collapse to one visible input"
    );

    forwarder.abort();
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
