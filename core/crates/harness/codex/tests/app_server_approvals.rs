//! approval-relay integration test.
//!
//! Verifies that `spawn_codex_forwarder` — when the fake server emits an
//! `item/commandExecution/requestApproval` SERVER-REQUEST (a JSON-RPC frame
//! with both a `method` and an `id`) — responds to the server with
//! `{ id: <same id>, result: { "decision": "accept" } }` via `AutoApprove`.
//!
//! # Pinned schema shape (Fix 1 — regression guard)
//! `item/commandExecution/requestApproval` response must be
//! `CommandExecutionRequestApprovalResponse { decision: "accept" }`.
//! Schema: `CommandExecutionRequestApprovalResponse.json` — `required: ["decision"]`.
//! The old shape `{"approved": true}` would fail codex deserialization.
//!
//! Run: `cargo test -p nexus-harness-codex --test app_server_approvals`

use std::sync::Arc;

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_harness_codex::app_server::spawn_codex_forwarder;
use nexus_harness_codex::{AutoApprove, CodexTurnTracker};
use serde_json::{json, Value};
use tokio::net::UnixListener;
use tokio::sync::{oneshot, Mutex};
use tokio_tungstenite::accept_async_with_config;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;

/// Max WebSocket frame size (mirror of `jsonrpc.rs` constant).
const MAX_WS_MESSAGE_SIZE: usize = 128 << 20;

fn ws_cfg() -> WebSocketConfig {
    WebSocketConfig {
        max_frame_size: Some(MAX_WS_MESSAGE_SIZE),
        max_message_size: Some(MAX_WS_MESSAGE_SIZE),
        ..WebSocketConfig::default()
    }
}

/// A do-nothing event sink (the forwarder test doesn't assert on emitted events here).
#[derive(Default)]
struct NullSink;

#[async_trait]
impl EventSink for NullSink {
    async fn emit(&self, _e: WsEvent) {}
}

/// Helper: create a temp unix socket path unique to this test run.
fn tmp_sock(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "nexus-approvals-{}-{}-{}.sock",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
        tag,
    ))
}

/// Spawn an in-process fake server that:
/// 1. Accepts one connection.
/// 2. Handles `initialize` / `thread/resume` with `{result:{}}`.
/// 3. After `thread/resume`, sends one `item/commandExecution/requestApproval`
///    SERVER-REQUEST frame (method + id = 99).
/// 4. Records the first non-request incoming message (the client's response)
///    and sends it through `resp_tx`.
///
/// Returns the `JoinHandle` for the server task.
fn spawn_fake_approval_server(
    sock_path: std::path::PathBuf,
    resp_tx: oneshot::Sender<Value>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let _ = std::fs::remove_file(&sock_path);
        let listener = UnixListener::bind(&sock_path).expect("bind");

        // Accept exactly one connection.
        let (stream, _) = listener.accept().await.expect("accept");
        let ws = accept_async_with_config(stream, Some(ws_cfg()))
            .await
            .expect("ws handshake");
        let (mut sink, mut stream) = ws.split();

        let resp_tx = Arc::new(Mutex::new(Some(resp_tx)));
        let mut resume_seen = false;

        loop {
            let msg = match stream.next().await {
                Some(Ok(m)) => m,
                _ => break,
            };
            let text = match msg {
                Message::Text(t) => t,
                Message::Close(_) => break,
                _ => continue,
            };

            let frame: Value = match serde_json::from_str(&text) {
                Ok(v) => v,
                Err(_) => continue,
            };

            let id = frame.get("id").cloned();
            let method = frame
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned();

            if !method.is_empty() {
                // Client → server request: reply. `thread/start` returns the real
                // nested `{thread:{id}}` shape so the client's `thread_start()`
                // parses an id; everything else gets `{result:{}}`.
                if let Some(id_val) = id {
                    let result = if method == "thread/start" {
                        json!({ "thread": { "id": "fake-thread", "sessionId": "fake-thread" } })
                    } else {
                        json!({})
                    };
                    let reply = serde_json::to_string(&json!({
                        "jsonrpc": "2.0",
                        "id": id_val,
                        "result": result
                    }))
                    .unwrap();
                    let _ = sink.send(Message::Text(reply.into())).await;
                }

                // After thread/start, emit the approval server-request (the
                // production forwarder shares the thread/start connection and
                // never resumes).
                if method == "thread/start" && !resume_seen {
                    resume_seen = true;
                    // Server→client REQUEST (has both method and id).
                    let srv_req = serde_json::to_string(&json!({
                        "jsonrpc": "2.0",
                        "id": 99,
                        "method": "item/commandExecution/requestApproval",
                        "params": {
                            "threadId": "fake-thread",
                            "itemId": "item-1",
                            "command": ["ls", "-la"]
                        }
                    }))
                    .unwrap();
                    let _ = sink.send(Message::Text(srv_req.into())).await;
                }
            } else {
                // No method → this is a client response to our server-request.
                // Record it and signal the test.
                let mut guard = resp_tx.lock().await;
                if let Some(tx) = guard.take() {
                    let _ = tx.send(frame);
                }
                // Close the connection so the forwarder task exits cleanly.
                let _ = sink.send(Message::Close(None)).await;
                break;
            }
        }
    })
}

/// Wait for the socket file to appear (up to 500 ms).
async fn wait_for_socket(path: &std::path::Path) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(500);
    loop {
        if path.exists() {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("socket {:?} never appeared", path);
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// Core assertion test: AutoApprove responds to a cmd approval request with
/// `{ id: 99, result: { "decision": "accept" } }`.
///
/// Pins the schema-correct shape for `item/commandExecution/requestApproval`:
/// `CommandExecutionRequestApprovalResponse { decision: CommandExecutionApprovalDecision }`.
/// Source: `CommandExecutionRequestApprovalResponse.json` — `required: ["decision"]`.
/// If AutoApprove reverts to the old `{"approved": true}` shape this test will fail.
#[tokio::test]
async fn forwarder_auto_approves_cmd_request() {
    let sock = tmp_sock("approvals");
    let (resp_tx, resp_rx) = oneshot::channel::<Value>();

    let _srv = spawn_fake_approval_server(sock.clone(), resp_tx);
    wait_for_socket(&sock).await;

    // Connect the client (same handshake as real usage).
    let client = nexus_harness_codex::CodexAppServerClient::connect(&sock, "nexus-test")
        .await
        .expect("connect");

    // Start the thread on this connection (mirrors production: the forwarder
    // shares the thread/start connection). This triggers the fake's approval.
    client.thread_start().await.expect("thread_start");

    // Spawn forwarder with AutoApprove on the same connection.
    let _fwd = spawn_codex_forwarder(
        SessionId("test-session".into()),
        Arc::new(client),
        Arc::new(NullSink),
        Arc::new(AutoApprove),
        CodexTurnTracker::default(),
    );

    // Wait for the server to receive the client's response (up to 2 s).
    let response = tokio::time::timeout(std::time::Duration::from_secs(2), resp_rx)
        .await
        .expect("timed out waiting for approval response")
        .expect("channel dropped");

    // Assert: id matches what the server sent (99).
    assert_eq!(
        response["id"],
        json!(99),
        "response id should match the server-request id; got: {response}"
    );
    // Assert: result is the schema-correct CommandExecutionRequestApprovalResponse shape.
    // Must be {"decision": "accept"} — NOT {"approved": true} (the old incorrect shape).
    assert_eq!(
        response["result"]["decision"],
        json!("accept"),
        "AutoApprove must return {{\"decision\":\"accept\"}} (CommandExecutionRequestApprovalResponse); got: {response}"
    );
    // Negative regression guard: the old wrong key must NOT be present.
    assert!(
        response["result"].get("approved").is_none(),
        "AutoApprove must NOT return the old {{\"approved\":true}} shape; got: {response}"
    );

    let _ = std::fs::remove_file(&sock);
}
