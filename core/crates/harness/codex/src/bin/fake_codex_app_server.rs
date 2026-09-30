//! Hermetic fake Codex app-server for integration tests.
//!
//! Supports cross-connection thread broadcast so a
//! `turn/start` on connection B delivers notifications to ALL connections
//! that did `thread/resume` or `thread/start` on the same threadId.
//!
//! # Invocation (same shape as the real `codex app-server`)
//!
//!   fake_codex_app_server app-server --listen unix://<sock>
//!
//! # Wire framing
//! WebSocket Text frames carrying JSON-RPC 2.0, same as the real server.
//!
//! # Scripting (`FAKE_CODEX_SCRIPT`)
//! Set `FAKE_CODEX_SCRIPT` to a JSON array of `{method, params}` objects.
//! Those notifications are emitted after `turn/start`, pushed to ALL
//! connections subscribed to that thread.
//!
//! DEFAULT (env unset): one `item/agentMessage/delta {delta:"hello"}` then
//! `turn/completed {threadId:"<actual_thread_id>", turnId:"t1"}`.
//! `FAKE_CODEX_REPLY_BEFORE_NOTIFICATIONS=1` sends the `turn/start` response before broadcasting
//! turn notifications, and `FAKE_CODEX_NOTIFICATION_DELAY_MS=<ms>` delays that broadcast. Together
//! they model the real app-server race where a `turn/start` request can return before the turn has
//! completed.
//!
//! # Test probes
//! If `FAKE_CODEX_CWD_PROBE` is set, the fake writes its process current directory to that path at
//! startup. Supervisor tests use this to assert launch cwd propagation.

use std::collections::HashSet;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::UnixListener;
use tokio::sync::{mpsc, Mutex};
use tokio::time::Duration;
use tokio_tungstenite::accept_async_with_config;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Mirror of `MAX_WS_MESSAGE_SIZE` in jsonrpc.rs (128 MiB).
const MAX_WS_MESSAGE_SIZE: usize = 128 << 20;

fn ws_config() -> WebSocketConfig {
    WebSocketConfig {
        max_frame_size: Some(MAX_WS_MESSAGE_SIZE),
        max_message_size: Some(MAX_WS_MESSAGE_SIZE),
        ..WebSocketConfig::default()
    }
}

/// A subscriber registered for a thread: holds the thread_id and a sender
/// that feeds the connection's outbound ws writer task.
struct Subscriber {
    thread_id: String,
    tx: mpsc::UnboundedSender<String>,
}

/// Process-global registry of subscribers (thread_id → outbound tx).
type Registry = Arc<Mutex<Vec<Subscriber>>>;

/// Load the notification script from `FAKE_CODEX_SCRIPT` or return the
/// default two-notification sequence. Notifications use a placeholder
/// threadId "THREAD_ID" that gets replaced at broadcast time with the
/// actual thread_id from the turn/start request.
fn load_script() -> Vec<Value> {
    match std::env::var("FAKE_CODEX_SCRIPT") {
        Ok(raw) => serde_json::from_str(&raw).expect("FAKE_CODEX_SCRIPT: invalid JSON array"),
        Err(_) => vec![
            json!({ "method": "item/agentMessage/delta", "params": { "delta": "hello", "threadId": "THREAD_ID", "turnId": "t1", "itemId": "i1" } }),
            json!({ "method": "turn/completed", "params": { "threadId": "THREAD_ID", "turnId": "t1" } }),
        ],
    }
}

/// Build a JSON-RPC notification frame string.
fn notification_frame(method: &str, params: &Value) -> String {
    serde_json::to_string(&json!({ "jsonrpc": "2.0", "method": method, "params": params })).unwrap()
}

/// Build a JSON-RPC response frame string.
fn response_frame(id: &Value, result: Value) -> String {
    serde_json::to_string(&json!({ "jsonrpc": "2.0", "id": id, "result": result })).unwrap()
}

fn error_frame(id: &Value, message: &str, data: Option<Value>) -> String {
    serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": -32600, "message": message, "data": data }
    }))
    .unwrap()
}

/// Serve a single WebSocket connection.
///
/// Runs two concurrent loops:
/// - A reader loop that handles incoming JSON-RPC requests.
/// - A writer task that drains an mpsc channel → ws Text frames.
///
/// When `thread/start` or `thread/resume` is received, registers a
/// Subscriber for that threadId in the registry.
///
/// When `turn/start { threadId }` is received, pushes the scripted
/// notifications to ALL subscribers for that threadId (cross-connection
/// broadcast), then sends the `{result:{}}` reply on THIS connection only.
async fn handle_conn(
    stream: tokio::net::UnixStream,
    script: Arc<Vec<Value>>,
    registry: Registry,
) -> Result<(), BoxError> {
    let ws = accept_async_with_config(stream, Some(ws_config())).await?;
    let (mut ws_sink, mut ws_stream) = ws.split();

    // Per-connection outbound channel: the reader pushes serialized frames
    // here; the writer task drains them to the ws sink.
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();

    // Spawn the writer task.
    let writer_task = tokio::spawn(async move {
        while let Some(frame) = out_rx.recv().await {
            if ws_sink.send(Message::Text(frame.into())).await.is_err() {
                break;
            }
        }
    });

    // Per-connection set of thread_ids already registered in the registry.
    // Prevents double-registration when a connection does both thread/start
    // and thread/resume for the same thread (which the forwarder does).
    let mut subscribed_threads: HashSet<String> = HashSet::new();
    let mut steer_calls = 0_u32;

    // Reader loop: handles incoming requests.
    loop {
        let msg = match ws_stream.next().await {
            Some(Ok(m)) => m,
            _ => break,
        };

        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => break,
            _ => continue,
        };

        let req: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let id = match req.get("id") {
            Some(v) => v.clone(),
            None => continue, // incoming notification — ignore
        };

        let method = req.get("method").and_then(Value::as_str).unwrap_or("");

        match method {
            "initialize" => {
                let _ = out_tx.send(response_frame(&id, json!({})));
            }
            "thread/start" => {
                let thread_id = "fake-thread".to_string();
                // Register this connection as a subscriber for this thread,
                // but only if this connection has not already subscribed to it.
                if subscribed_threads.insert(thread_id.clone()) {
                    registry.lock().await.push(Subscriber {
                        thread_id: thread_id.clone(),
                        tx: out_tx.clone(),
                    });
                }
                // Match real codex: ThreadStartResponse nests the id at
                // `thread.id`, NOT a flat `threadId`. (A flat-shape fake is what
                // let the bridge ship a bug that only real codex exposed.)
                let _ = out_tx.send(response_frame(
                    &id,
                    json!({"thread": {"id": thread_id, "sessionId": thread_id}}),
                ));
            }
            "thread/resume" => {
                // Register this connection as a subscriber for the given thread,
                // but only if this connection has not already subscribed to it.
                let thread_id = req
                    .get("params")
                    .and_then(|p| p.get("threadId"))
                    .and_then(Value::as_str)
                    .unwrap_or("fake-thread")
                    .to_string();
                if subscribed_threads.insert(thread_id.clone()) {
                    registry.lock().await.push(Subscriber {
                        thread_id: thread_id.clone(),
                        tx: out_tx.clone(),
                    });
                }
                let _ = out_tx.send(response_frame(&id, json!({})));
            }
            "turn/start" => {
                // Get the threadId from the request params.
                let thread_id = req
                    .get("params")
                    .and_then(|p| p.get("threadId"))
                    .and_then(Value::as_str)
                    .unwrap_or("fake-thread")
                    .to_string();
                let turn_id = "t1";
                let reply_before_notifications =
                    std::env::var_os("FAKE_CODEX_REPLY_BEFORE_NOTIFICATIONS").is_some();
                let notification_delay_ms = std::env::var("FAKE_CODEX_NOTIFICATION_DELAY_MS")
                    .ok()
                    .and_then(|raw| raw.parse::<u64>().ok())
                    .unwrap_or(0);

                if reply_before_notifications {
                    let _ = out_tx.send(response_frame(&id, json!({"turn": {"id": turn_id}})));
                    if notification_delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(notification_delay_ms)).await;
                    }
                }

                // Broadcast scripted notifications to ALL subscribers for this thread.
                {
                    let reg = registry.lock().await;
                    let subs: Vec<_> = reg.iter().filter(|s| s.thread_id == thread_id).collect();

                    for entry in script.iter() {
                        let m = entry
                            .get("method")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown");
                        // Replace THREAD_ID placeholder with actual thread_id.
                        let mut p = entry.get("params").cloned().unwrap_or(json!({}));
                        if let Some(obj) = p.as_object_mut() {
                            for v in obj.values_mut() {
                                if v.as_str() == Some("THREAD_ID") {
                                    *v = Value::String(thread_id.clone());
                                }
                            }
                        }
                        let frame = notification_frame(m, &p);
                        for sub in &subs {
                            let _ = sub.tx.send(frame.clone());
                        }
                    }
                }

                // Reply only on the originating connection.
                if !reply_before_notifications {
                    let _ = out_tx.send(response_frame(&id, json!({"turn": {"id": turn_id}})));
                }
            }
            "turn/steer" => {
                steer_calls += 1;
                let expected_turn_id = req
                    .get("params")
                    .and_then(|p| p.get("expectedTurnId"))
                    .and_then(Value::as_str)
                    .unwrap_or("t1");
                match std::env::var("FAKE_CODEX_STEER_MODE").as_deref() {
                    Ok("missing") => {
                        let _ = out_tx.send(error_frame(&id, "no active turn to steer", None));
                    }
                    Ok("mismatch_once") if steer_calls == 1 => {
                        let _ = out_tx.send(error_frame(
                            &id,
                            &format!("expected active turn id `{expected_turn_id}` but found `t2`"),
                            None,
                        ));
                    }
                    Ok("non_steerable") => {
                        let _ = out_tx.send(error_frame(
                            &id,
                            "cannot steer a review turn",
                            Some(json!({
                                "message": "cannot steer a review turn",
                                "codexErrorInfo": {
                                    "activeTurnNotSteerable": { "turnKind": "review" }
                                }
                            })),
                        ));
                    }
                    _ => {
                        let turn_id = if std::env::var("FAKE_CODEX_STEER_MODE").as_deref()
                            == Ok("mismatch_once")
                        {
                            "t2"
                        } else {
                            expected_turn_id
                        };
                        let _ = out_tx.send(response_frame(&id, json!({"turnId": turn_id})));
                    }
                }
            }
            _ => {
                let _ = out_tx.send(response_frame(&id, json!({})));
            }
        }
    }

    // Close the outbound channel so the writer task exits.
    drop(out_tx);
    let _ = writer_task.await;
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Ok(path) = std::env::var("FAKE_CODEX_CWD_PROBE") {
        if let Ok(cwd) = std::env::current_dir() {
            let _ = std::fs::write(path, cwd.to_string_lossy().as_bytes());
        }
    }

    // Parse socket path: find the last `unix://…` token in argv.
    let sock_path: String = std::env::args()
        .rev()
        .find(|a| a.starts_with("unix://"))
        .expect("Usage: fake_codex_app_server app-server --listen unix://<sock>")
        .strip_prefix("unix://")
        .unwrap()
        .to_owned();

    // Remove any stale socket file from a prior run.
    let _ = std::fs::remove_file(&sock_path);

    let listener = UnixListener::bind(&sock_path).expect("failed to bind UnixListener");

    // Load the notification script once — shared across all connections.
    let script = Arc::new(load_script());

    // Process-global subscriber registry.
    let registry: Registry = Arc::new(Mutex::new(Vec::new()));

    loop {
        let (stream, _addr) = listener.accept().await.expect("accept failed");

        let script = Arc::clone(&script);
        let registry = Arc::clone(&registry);
        tokio::spawn(async move {
            let _ = handle_conn(stream, script, registry).await;
        });
    }
}
