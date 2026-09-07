//! `CodexAppServerClient` — thin wrapper over [`JsonRpc`] using the
//! [`super::protocol`] builders.
//!
//! This module is deliberately thin: all codex-specific message shapes live in
//! `protocol.rs`; this file only composes `rpc.request / rpc.notify` calls
//! with those builders and parses the scalar fields the caller needs.

use std::path::Path;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use tokio::sync::mpsc::UnboundedReceiver;

use super::jsonrpc::{CodexRpcError, JsonRpc, Notification};
use super::protocol::{
    initialize_params, method, thread_compact_params, thread_resume_params, thread_start_params,
    turn_interrupt_params, turn_start_params, turn_steer_params,
};
use super::turn_completion::CodexTurnTracker;

/// A connected, initialized Codex app-server client.
///
/// Construct via [`CodexAppServerClient::connect`]; each instance owns exactly
/// one WebSocket connection to the server.
pub struct CodexAppServerClient {
    rpc: JsonRpc,
    origin: Mutex<Option<CodexTurnTracker>>,
}

/// Extract a thread id from a `thread/start` or `thread/resume` result.
///
/// Real codex nests it at `result.thread.id` (the `Thread` struct); older /
/// hypothetical flat shapes put it at `result.threadId`. Accept both.
fn thread_id_from_result(result: &Value) -> Option<String> {
    result["thread"]["id"]
        .as_str()
        .or_else(|| result["threadId"].as_str())
        .map(str::to_owned)
}

impl CodexAppServerClient {
    /// Connect to a running Codex app-server at `sock`, perform the
    /// `initialize` / `initialized` handshake, and return the ready client.
    ///
    /// Steps:
    /// 1. `JsonRpc::connect(sock)` — local WebSocket transport upgrade.
    /// 2. `request(INITIALIZE, initialize_params(client_name))` — wait for the
    ///    server's `{result: …}` response.
    /// 3. `notify(INITIALIZED, {})` — fire-and-forget confirmation.
    pub async fn connect(sock: &Path, client_name: &str) -> Result<Self, CodexRpcError> {
        Self::connect_inner(sock, client_name, None).await
    }

    pub(super) async fn connect_with_tracker(
        sock: &Path,
        client_name: &str,
        tracker: CodexTurnTracker,
    ) -> Result<Self, CodexRpcError> {
        Self::connect_inner(sock, client_name, Some(tracker)).await
    }

    async fn connect_inner(
        sock: &Path,
        client_name: &str,
        tracker: Option<CodexTurnTracker>,
    ) -> Result<Self, CodexRpcError> {
        let ingress = tracker.clone().map(|tracker| {
            Arc::new(move |note: &Notification| tracker.ingest_native(note))
                as super::jsonrpc::NativeIngress
        });
        let rpc = JsonRpc::connect_with_ingress(sock, ingress).await?;
        if let Some(tracker) = tracker.clone() {
            rpc.install_close_observer(Arc::new(move || tracker.observe_disconnect()));
        }
        rpc.request(method::INITIALIZE, initialize_params(client_name))
            .await?;
        rpc.notify(method::INITIALIZED, serde_json::json!({}))
            .await?;
        Ok(Self {
            rpc,
            origin: Mutex::new(tracker),
        })
    }

    pub(super) fn origin_tracker(&self) -> Option<CodexTurnTracker> {
        self.origin.lock().unwrap().clone()
    }

    pub(super) fn install_tracker(&self, tracker: CodexTurnTracker) -> bool {
        let mut origin = self.origin.lock().unwrap();
        if let Some(existing) = origin.as_ref() {
            return existing.same_owner(&tracker);
        }
        let ingress = tracker.clone();
        self.rpc
            .install_ingress(Arc::new(move |note| ingress.ingest_native(note)));
        let closed = tracker.clone();
        self.rpc
            .install_close_observer(Arc::new(move || closed.observe_disconnect()));
        *origin = Some(tracker);
        true
    }

    async fn request_mutation(&self, method: &str, params: Value) -> Result<Value, CodexRpcError> {
        let origin = self.origin_tracker();
        match origin {
            Some(origin) => {
                self.rpc
                    .request_with_admission(method, params, |submit| origin.admit(submit))
                    .await
            }
            None => self.rpc.request(method, params).await,
        }
    }

    async fn request_setup(&self, method: &str, params: Value) -> Result<Value, CodexRpcError> {
        match self.origin_tracker() {
            Some(origin) => {
                self.rpc
                    .request_with_admission(method, params, |submit| origin.admit_setup(submit))
                    .await
            }
            None => self.rpc.request(method, params).await,
        }
    }

    /// Start a new thread and return its thread id.
    ///
    /// Sends `thread/start` with autonomous defaults. Real codex returns the id nested at
    /// `result.thread.id` (`ThreadStartResponse { thread: Thread, .. }`); we
    /// also accept a flat `result.threadId` for forward-compat.  Returns
    /// [`CodexRpcError::Decode`] when neither is present.
    pub async fn thread_start(&self) -> Result<String, CodexRpcError> {
        self.thread_start_in(None).await
    }

    /// Start a new thread rooted at `cwd` when supplied.
    ///
    /// This is used by launch paths that already know the operator's project directory. Keeping cwd
    /// in the request prevents the app-server from defaulting to the daemon's own working directory.
    pub async fn thread_start_in(&self, cwd: Option<&str>) -> Result<String, CodexRpcError> {
        let result = self
            .request_setup(method::THREAD_START, thread_start_params(cwd))
            .await?;
        thread_id_from_result(&result).ok_or_else(|| {
            CodexRpcError::Decode(format!("thread/start response missing thread.id: {result}"))
        })
    }

    /// Resume an existing thread.
    ///
    /// Sends `thread/resume {threadId}` and returns the raw result value.
    pub async fn thread_resume(&self, thread_id: &str) -> Result<Value, CodexRpcError> {
        self.thread_resume_in(thread_id, None).await
    }

    /// Resume an existing thread, optionally overriding its working directory.
    ///
    /// Explicit Codex resume launches need this override on the first app-server resume request.
    /// Passing only `-C` to the later TUI attach is too late once the bridge has already loaded the
    /// thread with its rollout-stored cwd.
    pub async fn thread_resume_in(
        &self,
        thread_id: &str,
        cwd: Option<&str>,
    ) -> Result<Value, CodexRpcError> {
        self.request_setup(method::THREAD_RESUME, thread_resume_params(thread_id, cwd))
            .await
    }

    /// Submit a user turn to an existing thread.
    ///
    /// Sends `turn/start {threadId, input:[{type:"text", text}]}`. The server may emit turn
    /// notifications before or after the `turn/start` response, so callers that need the stream
    /// must take the [`notifications`][Self::notifications] receiver **before** calling this
    /// method.
    pub async fn turn_start(&self, thread_id: &str, text: &str) -> Result<(), CodexRpcError> {
        self.turn_start_id(thread_id, text).await?;
        Ok(())
    }

    /// Submit a user turn and return its turn id (`result.turn.id`).
    ///
    /// Like [`turn_start`][Self::turn_start] but returns the turn id so the caller can
    /// [`turn_interrupt`][Self::turn_interrupt] it — used for the warmup turn that only exists to
    /// materialize the thread's rollout (so the human TUI can resume) and should not actually run.
    pub async fn turn_start_id(
        &self,
        thread_id: &str,
        text: &str,
    ) -> Result<Option<String>, CodexRpcError> {
        let result = self
            .request_mutation(method::TURN_START, turn_start_params(thread_id, text))
            .await?;
        Ok(result["turn"]["id"].as_str().map(str::to_owned))
    }

    /// Append input to the active regular turn and return the native turn id that accepted it.
    ///
    /// Codex rejects a missing active turn, an expected-id mismatch, and non-steerable review or
    /// compaction turns as JSON-RPC errors; callers decide whether a race warrants one retry or a
    /// fresh `turn/start` fallback.
    pub async fn turn_steer_id(
        &self,
        thread_id: &str,
        expected_turn_id: &str,
        text: &str,
    ) -> Result<String, CodexRpcError> {
        let result = self
            .request_mutation(
                method::TURN_STEER,
                turn_steer_params(thread_id, text, expected_turn_id),
            )
            .await?;
        result["turnId"].as_str().map(str::to_owned).ok_or_else(|| {
            CodexRpcError::Decode(format!("turn/steer response missing turnId: {result}"))
        })
    }

    /// Start native context compaction for a thread.
    ///
    /// Sends `thread/compact/start {threadId}` — the same operation the codex
    /// TUI's `/compact` performs. The server acks the request and emits a
    /// `thread/compacted` notification when done.
    pub async fn thread_compact_start(&self, thread_id: &str) -> Result<(), CodexRpcError> {
        self.request_mutation(
            method::THREAD_COMPACT_START,
            thread_compact_params(thread_id),
        )
        .await?;
        Ok(())
    }

    /// Interrupt a running turn.
    ///
    /// Sends `turn/interrupt {threadId, turnId}`.
    pub async fn turn_interrupt(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<(), CodexRpcError> {
        self.request_mutation(
            method::TURN_INTERRUPT,
            turn_interrupt_params(thread_id, turn_id),
        )
        .await?;
        Ok(())
    }

    /// Consume the unbounded receiver for server→client notifications and
    /// server→client requests.
    ///
    /// Delegates directly to [`JsonRpc::notifications`].  May only be called
    /// once; panics on a second call (same contract as `JsonRpc::notifications`).
    pub fn notifications(&self) -> UnboundedReceiver<Notification> {
        self.rpc.notifications()
    }

    /// Send a response to a server→client request identified by `id`.
    ///
    /// Delegates directly to [`JsonRpc::respond`].  Used by the forwarder to
    /// reply to approval requests without exposing `rpc` publicly.
    pub async fn respond(
        &self,
        id: serde_json::Value,
        result: serde_json::Value,
    ) -> Result<(), CodexRpcError> {
        self.rpc.respond(id, result).await
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Regression guard for Fix 1 (Task 5): real codex returns the thread id
    /// nested at `result.thread.id` (struct `ThreadStartResponse { thread:
    /// Thread, .. }`), NOT a flat `result.threadId`.  All three cases must be
    /// handled: nested shape, flat fallback, and missing field → None.
    #[test]
    fn thread_id_nested_shape() {
        let result = json!({"thread": {"id": "t-1"}});
        assert_eq!(
            thread_id_from_result(&result),
            Some("t-1".to_string()),
            "nested thread.id must be extracted"
        );
    }

    /// Flat `threadId` is a forward-compat fallback; must still resolve.
    #[test]
    fn thread_id_flat_fallback() {
        let result = json!({"threadId": "t-2"});
        assert_eq!(
            thread_id_from_result(&result),
            Some("t-2".to_string()),
            "flat threadId must be extracted as fallback"
        );
    }

    /// When neither field is present the function must return None (not panic).
    #[test]
    fn thread_id_missing_returns_none() {
        let result = json!({});
        assert_eq!(
            thread_id_from_result(&result),
            None,
            "empty result must yield None"
        );
    }
}
