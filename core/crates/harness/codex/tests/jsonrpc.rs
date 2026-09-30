// Framing confirmed from:
//   codex-rs/app-server-client/src/remote.rs:738-783 (connect_unix_socket_endpoint)
//     — connects a raw UnixStream, then calls `client_async_with_config` with the
//       synthetic handshake URL "ws://localhost/rpc" to perform a WebSocket upgrade.
//   codex-rs/app-server-client/src/lib.rs:1474 (remote_unix_socket_typed_request_roundtrip_works)
//     — the server side calls `accept_async(stream)` on the accepted UnixStream.
//   codex-rs/app-server-client/src/remote.rs:966-983 (write_jsonrpc_message)
//     — JSON payload is serialised into a WebSocket *Text* frame (not NDJSON).
//
// VERDICT: WebSocket-over-Unix-socket (NOT NDJSON).
//   Each JSON-RPC message is one WebSocket text frame.
//   The client sends the WS handshake HTTP upgrade over the raw unix byte stream,
//   using the dummy URI "ws://localhost/rpc" (bytes never leave the machine).

use nexus_harness_codex::JsonRpc;
#[cfg(unix)]
use nexus_harness_codex::{CodexRpcError, Notification};
use std::path::PathBuf;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

use futures::SinkExt;
use futures::StreamExt;

// ---------------------------------------------------------------------------
// Helpers — an in-test WebSocket-over-unix-socket server
// ---------------------------------------------------------------------------

/// Bind a UnixListener, accept one connection, upgrade it to WebSocket,
/// then call `handler` with the resulting stream.
#[cfg(unix)]
async fn spawn_ws_server<F, Fut>(path: PathBuf, handler: F)
where
    F: FnOnce(tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = UnixListener::bind(&path).unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let websocket = accept_async(stream)
            .await
            .expect("WS upgrade should succeed");
        handler(websocket).await;
    });
}

#[cfg(unix)]
fn sock_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "nexus-codex-jsonrpc-test-{}-{}.sock",
        std::process::id(),
        tag
    ))
}

// ---------------------------------------------------------------------------
// Test 1 — request/response correlation
// ---------------------------------------------------------------------------

/// Echo server: for any request `{id, method, params}`, reply `{id, result: {method}}`.
#[cfg(unix)]
async fn echo_server(mut ws: tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>) {
    while let Some(Ok(msg)) = ws.next().await {
        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => break,
            _ => continue,
        };
        let v: serde_json::Value = serde_json::from_str(&text).unwrap();
        if let Some(id) = v.get("id") {
            let reply = serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": { "method": v["method"] }
            });
            ws.send(Message::Text(serde_json::to_string(&reply).unwrap().into()))
                .await
                .unwrap();
        }
    }
}

async fn tcp_echo_server(mut ws: tokio_tungstenite::WebSocketStream<TcpStream>) {
    while let Some(Ok(msg)) = ws.next().await {
        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => break,
            _ => continue,
        };
        let v: serde_json::Value = serde_json::from_str(&text).unwrap();
        if let Some(id) = v.get("id") {
            let reply = serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": { "method": v["method"] }
            });
            ws.send(Message::Text(serde_json::to_string(&reply).unwrap().into()))
                .await
                .unwrap();
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn request_resolves_with_correlated_result() {
    let path = sock_path("req");
    let _ = std::fs::remove_file(&path);
    spawn_ws_server(path.clone(), echo_server).await;

    let rpc = JsonRpc::connect_unix(&path).await.unwrap();
    let out = rpc
        .request("turn/start", serde_json::json!({"threadId": "t1"}))
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({"method": "turn/start"}));
}

#[tokio::test]
async fn loopback_websocket_endpoint_resolves_with_correlated_result() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let websocket = accept_async(stream)
            .await
            .expect("WS upgrade should succeed");
        tcp_echo_server(websocket).await;
    });

    let endpoint = PathBuf::from(format!("ws://{address}"));
    let rpc = JsonRpc::connect(&endpoint).await.unwrap();
    let out = rpc
        .request("turn/start", serde_json::json!({"threadId": "t1"}))
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({"method": "turn/start"}));
}

// ---------------------------------------------------------------------------
// Test 2 — notification delivery
// ---------------------------------------------------------------------------

/// Server that pushes one unsolicited notification, then drains without crashing.
#[cfg(unix)]
async fn notification_then_echo_server(
    mut ws: tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>,
) {
    // Push one unsolicited notification immediately.
    let notif = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "item/agentMessage/delta",
        "params": { "delta": "hi" }
    });
    ws.send(Message::Text(serde_json::to_string(&notif).unwrap().into()))
        .await
        .unwrap();

    // Keep the connection alive (drain without crashing).
    while let Some(Ok(msg)) = ws.next().await {
        match msg {
            Message::Close(_) => break,
            _ => {}
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn unsolicited_notification_arrives_on_channel() {
    let path = sock_path("notif");
    let _ = std::fs::remove_file(&path);
    spawn_ws_server(path.clone(), notification_then_echo_server).await;

    let rpc = JsonRpc::connect_unix(&path).await.unwrap();
    let mut notifs = rpc.notifications();
    let n: Notification = tokio::time::timeout(std::time::Duration::from_secs(5), notifs.recv())
        .await
        .expect("timed out waiting for notification")
        .expect("notifications channel closed unexpectedly");

    assert_eq!(n.method, "item/agentMessage/delta");
    assert_eq!(n.params, serde_json::json!({"delta": "hi"}));
    assert!(n.id.is_none(), "unsolicited notification must have id=None");

    // Verify CodexRpcError is accessible via crate root (type-check only).
    let _: CodexRpcError = CodexRpcError::Closed;
}
