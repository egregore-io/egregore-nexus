use super::test_support::spawn_fake_server;
use super::*;
use nexus_contracts::batch::{BatchCounts, NexusBatch};
use nexus_contracts::ids::SessionId;
use std::sync::Arc;

#[tokio::test]
async fn revoked_during_initialize_cannot_admit_thread_setup() {
    use futures::{SinkExt, StreamExt};
    use serde_json::json;
    use tokio_tungstenite::tungstenite::Message;
    for resume in [false, true] {
        let path =
            std::env::temp_dir().join(format!("codex-setup-revoke-{}.sock", uuid_for_test()));
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let server = tokio::spawn({
            let entered = entered.clone();
            let release = release.clone();
            let calls = calls.clone();
            async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                while let Some(Ok(Message::Text(text))) = ws.next().await {
                    let request: serde_json::Value = serde_json::from_str(&text).unwrap();
                    let Some(id) = request.get("id") else {
                        continue;
                    };
                    if request["method"] == "initialize" {
                        entered.notify_one();
                        release.notified().await;
                    } else {
                        calls.lock().unwrap().push(request["method"].clone());
                    }
                    ws.send(Message::Text(
                        json!({"id": id, "result": {"thread": {"id": "thread"}}})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();
                }
            }
        });
        let owner = CodexTurnTracker::default().new_owner(Some("thread".into()));
        let mut connecting = Box::pin(CodexAppServerClient::connect_with_tracker(
            &path,
            "old",
            owner.clone(),
        ));
        tokio::select! {
            _ = &mut connecting => panic!("initialize held"),
            _ = entered.notified() => {}
        }
        owner.revoke_owner();
        release.notify_one();
        let client = connecting.await.unwrap();
        let result = if resume {
            client.thread_resume("thread").await.map(|_| ())
        } else {
            client.thread_start().await.map(|_| ())
        };
        let observed = calls.lock().unwrap().clone();
        server.abort();
        let _ = std::fs::remove_file(path);
        assert!(result.is_err(), "revoked setup was accepted: {observed:?}");
        assert!(
            observed.is_empty(),
            "setup frame escaped after initialization wait"
        );
    }
}

#[tokio::test]
async fn delivery_receipt_timeout_preserves_native_open_until_matching_terminal() {
    use futures::{SinkExt, StreamExt};
    use serde_json::json;
    use tokio_tungstenite::tungstenite::Message;
    let path = std::env::temp_dir().join(format!("codex-receipt-timeout-{}.sock", uuid_for_test()));
    let listener = tokio::net::UnixListener::bind(&path).unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        while let Some(Ok(Message::Text(text))) = ws.next().await {
            let request: serde_json::Value = serde_json::from_str(&text).unwrap();
            let Some(id) = request.get("id") else {
                continue;
            };
            let result = if request["method"] == "turn/start" {
                json!({"turn": {"id": "native-open"}})
            } else {
                json!({})
            };
            ws.send(Message::Text(
                json!({"id": id, "result": result}).to_string().into(),
            ))
            .await
            .unwrap();
        }
    });
    let client = Arc::new(CodexAppServerClient::connect(&path, "test").await.unwrap());
    let transport = CodexAppServerTransport::new();
    let session = SessionId("receipt-timeout".into());
    transport.bind(session.clone(), client, "thread".into());
    let result = transport
        .inject_turn_inner(&session, &empty_batch(), None, Duration::ZERO)
        .await;
    assert!(matches!(result, Err(InjectError::CompletionTimeout { .. })));
    let tracker = transport.client_for(&session).unwrap().tracker;
    assert_eq!(
        tracker.active_turn_id("thread").as_deref(),
        Some("native-open")
    );
    tracker.ingest_native(&super::super::jsonrpc::Notification {
        id: None,
        method: "turn/completed".into(),
        params: json!({"threadId": "thread", "turnId": "native-open"}),
    });
    assert_eq!(tracker.active_turn_id("thread"), None);
    server.abort();
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn delayed_steer_missing_response_cannot_clear_newer_native_turn() {
    use futures::{SinkExt, StreamExt};
    use serde_json::json;
    use tokio_tungstenite::tungstenite::Message;
    let path = std::env::temp_dir().join(format!("codex-steer-order-{}.sock", uuid_for_test()));
    let listener = tokio::net::UnixListener::bind(&path).unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        while let Some(Ok(Message::Text(text))) = ws.next().await {
            let request: serde_json::Value = serde_json::from_str(&text).unwrap();
            let Some(id) = request.get("id") else {
                continue;
            };
            let response = if request["method"] == "turn/steer" {
                ws.send(Message::Text(json!({"method": "turn/started", "params": {"threadId": "thread", "turnId": "new"}}).to_string().into())).await.unwrap();
                json!({"id": id, "error": {"code": -32000, "message": "no active turn to steer"}})
            } else {
                json!({"id": id, "result": {}})
            };
            ws.send(Message::Text(response.to_string().into()))
                .await
                .unwrap();
        }
    });
    let client = Arc::new(CodexAppServerClient::connect(&path, "test").await.unwrap());
    let transport = CodexAppServerTransport::new();
    let session = SessionId("steer-order".into());
    transport.bind(session.clone(), client, "thread".into());
    let binding = transport.client_for(&session).unwrap();
    binding.tracker.observe_active_turn("thread", "old");
    assert!(transport
        .steer_active(&binding.client, &binding.tracker, "thread", "steer")
        .await
        .is_err());
    assert_eq!(
        binding.tracker.active_turn_id("thread").as_deref(),
        Some("new")
    );
    server.abort();
    let _ = std::fs::remove_file(path);
}

fn uuid_for_test() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

#[tokio::test]
async fn steer_mismatch_retry_keeps_original_owner_after_replacement() {
    use futures::{SinkExt, StreamExt};
    use serde_json::json;
    use tokio_tungstenite::tungstenite::Message;
    let path = std::env::temp_dir().join(format!("codex-steer-rebind-{}.sock", uuid_for_test()));
    let listener = tokio::net::UnixListener::bind(&path).unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let server = tokio::spawn({
        let entered = entered.clone();
        let release = release.clone();
        let calls = calls.clone();
        async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            while let Some(Ok(Message::Text(text))) = ws.next().await {
                let request: serde_json::Value = serde_json::from_str(&text).unwrap();
                let Some(id) = request.get("id") else {
                    continue;
                };
                let response = if request["method"] == "turn/steer" {
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    entered.notify_one();
                    release.notified().await;
                    json!({"id": id, "error": {"code": -32000, "message": "expected active turn id `old` but found `actual`"}})
                } else {
                    json!({"id": id, "result": {}})
                };
                ws.send(Message::Text(response.to_string().into()))
                    .await
                    .unwrap();
            }
        }
    });
    let client = Arc::new(CodexAppServerClient::connect(&path, "test").await.unwrap());
    let transport = CodexAppServerTransport::new();
    let session = SessionId("steer-rebind".into());
    transport.bind(session.clone(), client, "thread".into());
    let binding = transport.client_for(&session).unwrap();
    binding.tracker.observe_active_turn("thread", "old");
    let mut steer =
        Box::pin(transport.steer_active(&binding.client, &binding.tracker, "thread", "steer"));
    tokio::select! {
        _ = &mut steer => panic!("server holds the original response"),
        _ = entered.notified() => {}
    }
    let (replacement_socket, replacement_calls) =
        spawn_fake_server("origin", "steer-rebind-new").await;
    let replacement = Arc::new(
        CodexAppServerClient::connect(&replacement_socket, "new")
            .await
            .unwrap(),
    );
    transport.bind(session.clone(), replacement, "thread".into());
    release.notify_one();
    assert!(steer.await.is_err());
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(replacement_calls
        .lock()
        .unwrap()
        .iter()
        .all(|call| call["method"] != "turn/steer"));
    transport.prompt(&session, "control".into()).await.unwrap();
    server.abort();
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn parked_original_binding_cannot_admit_after_replacement() {
    let (sock, calls) = spawn_fake_server("origin", "parked-rebind").await;
    let old = Arc::new(CodexAppServerClient::connect(&sock, "old").await.unwrap());
    let new = Arc::new(CodexAppServerClient::connect(&sock, "new").await.unwrap());
    let transport = CodexAppServerTransport::new();
    let session = SessionId("origin-session".into());
    transport.bind(session.clone(), old, "same-thread".into());
    let lock = transport.client_for(&session).unwrap().start_lock;
    let guard = lock.lock().await;
    let mut original = Box::pin(transport.prompt(&session, "OLD".into()));
    assert!(futures::poll!(&mut original).is_pending());
    transport.bind(session.clone(), new, "same-thread".into());
    drop(guard);
    assert!(
        original.await.is_err(),
        "the captured old binding must reject"
    );
    assert!(calls
        .lock()
        .unwrap()
        .iter()
        .all(|call| call["method"] != "turn/start"));
    transport.prompt(&session, "NEW".into()).await.unwrap();
    let starts: Vec<_> = calls
        .lock()
        .unwrap()
        .iter()
        .filter(|call| call["method"] == "turn/start")
        .cloned()
        .collect();
    assert_eq!(starts.len(), 1);
    assert_eq!(starts[0]["params"]["input"][0]["text"], "NEW");
}

fn empty_batch() -> NexusBatch {
    NexusBatch {
        counts: BatchCounts {
            dms: 0,
            thread: 0,
            total: 0,
        },
        dms: vec![],
        threads: vec![],
        dm_message_ids: vec![],
        thread_message_ids: vec![],
        message_ids: vec![],
    }
}

fn batch_with_agent_dm(msg: &str) -> NexusBatch {
    use nexus_contracts::batch::BatchMessage;
    use nexus_contracts::enums::{Kind, Scope};
    use nexus_contracts::ids::MessageId;
    NexusBatch {
        counts: BatchCounts {
            dms: 1,
            thread: 0,
            total: 1,
        },
        dms: vec![BatchMessage {
            id: MessageId("m_test".into()),
            from: "test".to_string(),
            kind: Kind::Agent,
            scope: Scope::Dm,
            thread: None,
            topic: None,
            body: msg.to_string(),
            truncated: false,
        }],
        threads: vec![],
        dm_message_ids: vec![],
        thread_message_ids: vec![],
        message_ids: vec![],
    }
}

// -------------------------------------------------------------------------
// Test 1: inject_turn delivers turn/start with the rendered batch text
// -------------------------------------------------------------------------

#[tokio::test]
async fn inject_turn_delivers_turn_start_with_batch_text() {
    let (sock, calls) = spawn_fake_server("cat-transport", "inject-turn").await;

    let client = CodexAppServerClient::connect(&sock, "nexus-inject")
        .await
        .expect("client connect should succeed");

    // Resume (or start) a thread — fake server returns null, which is fine.
    // We supply a known thread_id directly.
    let thread_id = "test-thread-1".to_string();

    let transport = CodexAppServerTransport::new();
    let s1 = SessionId("s1".into());
    transport.bind(s1.clone(), Arc::new(client), thread_id.clone());

    // Use an agent DM so this path asserts envelope framing rather than the single-human-DM
    // plain-text exception.
    let batch = batch_with_agent_dm("hi");
    transport
        .inject_turn(&s1, &batch)
        .await
        .expect("inject_turn should succeed");

    // The fake server should have recorded a turn/start call.
    let recorded = calls.lock().unwrap().clone();
    let turn_start_call = recorded
        .iter()
        .find(|v| v.get("method").and_then(|m| m.as_str()) == Some("turn/start"))
        .expect("fake server must have received a turn/start request");

    // The rendered batch text should contain body text plus recipient framing.
    let input_text = turn_start_call["params"]["input"][0]["text"]
        .as_str()
        .unwrap_or("");
    assert!(
        input_text.contains("hi"),
        "turn/start input must contain 'hi'; got: {input_text}"
    );
    assert!(
        input_text.contains("receiver=\"s1\""),
        "turn/start input must name the recipient session; got: {input_text}"
    );
    assert!(
        input_text.contains("target=\"dm:s1\""),
        "turn/start input must name the per-recipient DM target; got: {input_text}"
    );
}

// -------------------------------------------------------------------------
// Test 2: prompt delivers turn/start with the raw text
// -------------------------------------------------------------------------

#[tokio::test]
async fn prompt_delivers_turn_start_with_raw_text() {
    let (sock, calls) = spawn_fake_server("cat-transport", "prompt").await;

    let client = CodexAppServerClient::connect(&sock, "nexus-inject")
        .await
        .expect("client connect should succeed");

    let thread_id = "test-thread-prompt".to_string();
    let transport = CodexAppServerTransport::new();
    let s1 = SessionId("s_prompt".into());
    transport.bind(s1.clone(), Arc::new(client), thread_id.clone());

    transport
        .prompt(&s1, "RAW-OPERATOR-TEXT-9999".to_string())
        .await
        .expect("prompt should succeed");

    let recorded = calls.lock().unwrap().clone();
    let turn_call = recorded
        .iter()
        .find(|v| v.get("method").and_then(|m| m.as_str()) == Some("turn/start"))
        .expect("fake server must have received a turn/start request");

    let input_text = turn_call["params"]["input"][0]["text"]
        .as_str()
        .unwrap_or("");
    assert!(
        input_text.contains("RAW-OPERATOR-TEXT-9999"),
        "turn/start input must contain the raw operator text; got: {input_text}"
    );
}

#[tokio::test]
async fn prompt_rechecks_native_turn_started_while_waiting_for_start_lock() {
    use nexus_contracts::TurnState;
    use serde_json::json;

    let (sock, calls) = spawn_fake_server("cat-transport", "prompt-idle-start-race").await;
    let client = CodexAppServerClient::connect(&sock, "nexus-inject")
        .await
        .expect("client connect should succeed");
    let transport = CodexAppServerTransport::new();
    let session = SessionId("s_prompt_idle_start_race".into());
    let thread = "thread-idle-start-race";
    transport.bind(session.clone(), Arc::new(client), thread.into());
    let binding = transport.client_for(&session).unwrap();
    let native = |turn: &str, method: &str| super::super::jsonrpc::Notification {
        id: None,
        method: method.into(),
        params: json!({"threadId": thread, "turnId": turn}),
    };
    binding
        .tracker
        .ingest_native(&native("tOld", "turn/completed"));
    assert_eq!(
        binding.tracker.observe_turn(thread).state,
        TurnState::VerifiedIdle
    );

    let start_guard = binding.start_lock.lock().await;
    let original = "ORIGINAL-IDLE-THEN-NATIVE-TURN";
    let mut prompt = Box::pin(transport.prompt(&session, original.into()));
    assert!(futures::poll!(&mut prompt).is_pending());
    assert_eq!(
        binding.tracker.live_completion_waiter_count(thread, "tNew"),
        0
    );

    binding
        .tracker
        .ingest_native(&native("tNew", "turn/started"));
    assert_eq!(
        binding.tracker.observe_turn(thread).state,
        TurnState::NativeOpen
    );
    drop(start_guard);
    assert!(futures::poll!(&mut prompt).is_pending());
    // The SAME future must now be parked on the newly observed native turn, not
    // merely waiting for an unscheduled fake RPC server to answer turn/start.
    assert_eq!(
        binding.tracker.live_completion_waiter_count(thread, "tNew"),
        1,
        "post-lock prompt must register the new native turn's completion waiter"
    );
    assert!(calls
        .lock()
        .unwrap()
        .iter()
        .all(|call| call["method"] != "turn/start"));

    binding
        .tracker
        .ingest_native(&native("tNew", "turn/completed"));
    binding.tracker.settle_completion(thread, "tNew");
    tokio::time::timeout(Duration::from_secs(1), &mut prompt)
        .await
        .expect("same prompt must resume after matching native completion")
        .expect("same prompt must succeed");
    assert_eq!(
        binding.tracker.live_completion_waiter_count(thread, "tNew"),
        0
    );
    let starts: Vec<_> = calls
        .lock()
        .unwrap()
        .iter()
        .filter(|call| call["method"] == "turn/start")
        .cloned()
        .collect();
    assert_eq!(starts.len(), 1);
    assert_eq!(starts[0]["params"]["threadId"], thread);
    assert_eq!(starts[0]["params"]["input"][0]["text"], original);
    // The helper's null response proves dispatch only, not a native acceptance
    // receipt. Direct headed-human input does not acquire this start mutex.
}

#[tokio::test]
async fn prompt_waits_for_existing_active_turn_boundary() {
    let (sock, calls) = spawn_fake_server("cat-transport", "prompt-active-boundary").await;

    let client = CodexAppServerClient::connect(&sock, "nexus-inject")
        .await
        .expect("client connect should succeed");

    let thread_id = "test-thread-prompt-active".to_string();
    let transport = CodexAppServerTransport::new();
    let session = SessionId("s_prompt_active".into());
    transport.bind(session.clone(), Arc::new(client), thread_id.clone());
    transport
        .turn_tracker()
        .observe_active_turn(&thread_id, "turn-existing");

    let pending = tokio::spawn({
        let transport = transport.clone();
        let session = session.clone();
        async move {
            transport
                .prompt(&session, "MUST-WAIT-FOR-BOUNDARY".to_string())
                .await
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        calls.lock().unwrap().iter().all(|call| {
            call.get("method").and_then(|method| method.as_str()) != Some("turn/start")
        }),
        "a queued prompt must not turn/start while the prior session turn is active"
    );

    transport
        .turn_tracker()
        .complete(&thread_id, "turn-existing");
    tokio::time::timeout(Duration::from_secs(1), pending)
        .await
        .expect("prompt did not resume after the prior turn completed")
        .expect("prompt task panicked")
        .expect("prompt failed after the prior turn completed");

    let recorded = calls.lock().unwrap().clone();
    assert_eq!(
        recorded
            .iter()
            .filter(|call| {
                call.get("method").and_then(|method| method.as_str()) == Some("turn/start")
            })
            .count(),
        1,
        "the queued prompt must start exactly once after the active boundary"
    );
}

#[tokio::test]
async fn compact_sends_thread_compact_start_for_the_bound_thread() {
    let (sock, calls) = spawn_fake_server("cat-transport", "compact").await;

    let client = CodexAppServerClient::connect(&sock, "nexus-inject")
        .await
        .expect("client connect should succeed");

    let thread_id = "test-thread-compact".to_string();
    let transport = CodexAppServerTransport::new();
    let s1 = SessionId("s_compact".into());
    transport.bind(s1.clone(), Arc::new(client), thread_id.clone());

    transport
        .compact(&s1)
        .await
        .expect("compact should succeed");

    let recorded = calls.lock().unwrap().clone();
    let call = recorded
        .iter()
        .find(|v| v.get("method").and_then(|m| m.as_str()) == Some("thread/compact/start"))
        .expect("fake server must have received a thread/compact/start request");
    assert_eq!(
        call["params"]["threadId"].as_str(),
        Some("test-thread-compact"),
        "compact must target the bound thread"
    );
    assert!(
        recorded
            .iter()
            .all(|v| v.get("method").and_then(|m| m.as_str()) != Some("turn/start")),
        "native slash compaction must not be delivered through turn/start: {recorded:?}"
    );
}

// -------------------------------------------------------------------------
// Test 3: is_bound / unbind / is_harness_alive
// -------------------------------------------------------------------------

#[tokio::test]
async fn is_bound_and_unbind_work() {
    let (sock, _calls) = spawn_fake_server("cat-transport", "is-bound").await;

    let client = Arc::new(
        CodexAppServerClient::connect(&sock, "nexus-inject")
            .await
            .expect("connect"),
    );
    let transport = CodexAppServerTransport::new();
    let s = SessionId("s_bound".into());

    assert!(!transport.is_bound(&s), "unbound session must not be bound");
    assert_eq!(transport.is_harness_alive(&s), None, "unbound → None");

    transport.bind(s.clone(), client, "t1".to_string());
    assert!(transport.is_bound(&s), "bound session must report bound");
    assert_eq!(
        transport.is_harness_alive(&s),
        Some(true),
        "bound → Some(true)"
    );

    transport.unbind(&s);
    assert!(!transport.is_bound(&s), "after unbind must not be bound");
    assert_eq!(transport.is_harness_alive(&s), None, "after unbind → None");
}

#[tokio::test]
async fn app_server_keepalive_without_bound_session_is_not_agent_liveness() {
    let transport = CodexAppServerTransport::new();
    let s = SessionId("s_app_server_only".into());

    transport.mark_live(s.clone());

    assert!(
        !transport.is_app_server_live(&s),
        "a stamp-only mark (no verifiable pid) must NOT count as a live app-server — \
         stale stamps masked dead agents (percy class)"
    );
    transport.mark_live_with_pid(s.clone(), std::process::id());
    assert!(
        transport.is_app_server_live(&s),
        "a pid-backed mark with a RUNNING process is a live app-server (D9/N22)"
    );
    assert!(
        !transport.is_bound(&s),
        "actual Codex turn session is not bound"
    );
    assert_eq!(
        transport.is_harness_alive(&s),
        None,
        "a live app-server process alone must not make the agent read online"
    );
}

// -------------------------------------------------------------------------
// Test 4: inject_turn returns error for unbound session
// -------------------------------------------------------------------------

#[tokio::test]
async fn inject_turn_errors_on_unbound_session() {
    let transport = CodexAppServerTransport::new();
    let err = transport
        .inject_turn(&SessionId("s_missing".into()), &empty_batch())
        .await
        .unwrap_err();
    assert!(
        err.message.contains("no codex app-server client bound"),
        "error must mention unbound; got: {}",
        err.message
    );
}
