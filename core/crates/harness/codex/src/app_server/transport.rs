//! `CodexAppServerTransport` — delivers injected turns to a headed codex app-server session
//! via `turn/start` on the app-server's JSON-RPC socket, instead of writing into a PTY.
//!
//! This is the third transport in the routing priority order used by `RoutingTurnExec`:
//! codex-app-server (this module) → PTY/tmux → ACP.
//!
//! Each session maps to a `(Arc<CodexAppServerClient>, thread_id)` pair. The client sends
//! `turn/start { threadId, input: [{type:"text", text}] }` over the shared JSON-RPC connection.
//! The batch is rendered with recipient addressee framing (`render_injected_turn_for`) before
//! transmission so the harness receives the full `<nexus-batch …>…</nexus-batch>` envelope
//! (same as the PTY path).
//! Bus delivery (`inject_turn`) is model-receipt-aware when Codex returns a turn id. A successful
//! app-server handoff is not enough to mark Nexus inbox rows delivered; model progress or a
//! terminal completion proves the input reached context. Direct operator prompts are
//! acceptance-based, but the native active turn id keeps later durable prompt rows at the visible
//! Queue boundary. Explicit steering uses `turn/steer` through a separate control lane.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use nexus_common::render_injected_turn_for;
use nexus_contracts::batch::NexusBatch;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::{
    AgentTurnExecutionPort, ContractError, EventSink, InjectError, InjectResult, PortResult,
};
use nexus_contracts::{
    RemoveRequest, RemoveResponse, SpawnRequest, SpawnResponse, SteerCapability, SteerDelivery,
    SteerResponse, WsEvent,
};
use tokio::sync::Mutex as AsyncMutex;

use super::provider_limit::{classify_rpc_error, classify_turn_error};
use super::turn_completion::{CodexTurnTracker, QueuedAcceptedEvent, QueuedAcceptedUserInputEcho};
use super::{CodexAppServerClient, CodexRpcError};

const TURN_COMPLETION_TIMEOUT: Duration = Duration::from_secs(600);
const PROMPT_INPUT_RECEIPT_TIMEOUT: Duration = Duration::from_secs(30);

struct PromptAcceptanceCleanup {
    tracker: CodexTurnTracker,
    accepted_event: QueuedAcceptedEvent,
    accepted_echo: QueuedAcceptedUserInputEcho,
    text: String,
    turn_id: Option<String>,
}

impl Drop for PromptAcceptanceCleanup {
    fn drop(&mut self) {
        self.tracker.cancel_accepted_event(&self.accepted_event);
        self.tracker.tombstone_cancelled_user_input_echo(
            &self.accepted_echo,
            self.turn_id.as_deref(),
            &self.text,
        );
    }
}

/// Delivers injected turns / operator prompts to a headed codex app-server session
/// by calling `turn/start` on a dedicated inject client. Each session is bound with
/// `bind(session, client, thread_id)` by [`super::bridge::CodexBridge`] after the
/// human TUI's rollout has been discovered and resumed.
///
/// `Clone` is cheap — all state is behind `Arc<Mutex<…>>`.
#[derive(Clone, Default)]
pub struct CodexAppServerTransport {
    sessions: Arc<Mutex<HashMap<SessionId, (Arc<CodexAppServerClient>, String)>>>,
    /// Serializes Nexus-owned `turn/start` boundaries per session. Durable prompts and realtime
    /// bus delivery run on independent lanes and must not race two starts onto one Codex thread.
    turn_start_locks: Arc<Mutex<HashMap<SessionId, Arc<AsyncMutex<()>>>>>,
    /// Sessions whose codex app-server PROCESS is alive — marked at launch, independent of
    /// `sessions` (the turn-routing binding), which is only populated after first-turn discovery.
    /// Value = the app-server root OS pid when the daemon spawned it; `None` for stamp-only
    /// marks (adopted sockets). Liveness is PROCESS-BACKED: a stamp without a verifiable pid
    /// never counts as alive, so a stale mark cannot mask a dead agent. A fresh pre-thread
    /// app-server with a live process remains alive.
    live: Arc<Mutex<HashMap<SessionId, Option<u32>>>>,
    turn_tracker: CodexTurnTracker,
}

/// Whether an OS pid is currently running (unix: `/proc/<pid>` exists; elsewhere: assume yes).
fn os_pid_alive(pid: u32) -> bool {
    #[cfg(any(target_os = "linux", windows))]
    {
        nexus_common::process_ids::runtime_process_ids_for_pid(pid).is_some()
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    {
        true
    }
}

impl CodexAppServerTransport {
    pub fn new() -> Self {
        Self::default()
    }

    /// Shared tracker used by the notification forwarder to complete turns started by this
    /// transport.
    pub fn turn_tracker(&self) -> CodexTurnTracker {
        self.turn_tracker.clone()
    }

    /// Bind `session` to a connected inject client + the thread it owns.
    /// Called once per session after `CodexAppServerClient::connect` + `thread_resume`.
    pub fn bind(&self, session: SessionId, client: Arc<CodexAppServerClient>, thread_id: String) {
        self.sessions
            .lock()
            .unwrap()
            .insert(session, (client, thread_id));
    }

    /// Whether `session` has a bound codex app-server client. O(1); no I/O.
    /// Used by `RoutingTurnExec` to decide the dispatch path before checking PTY.
    pub fn is_bound(&self, session: &SessionId) -> bool {
        self.sessions.lock().unwrap().contains_key(session)
    }

    /// Current Codex native thread bound to `session`, if the structured turn client is bound.
    pub fn bound_thread_id(&self, session: &SessionId) -> Option<String> {
        self.sessions
            .lock()
            .unwrap()
            .get(session)
            .map(|(_, thread_id)| thread_id.clone())
    }

    /// Remove the binding for `session` (called on kill/cleanup by `PtySupervisor`).
    pub fn unbind(&self, session: &SessionId) {
        self.sessions.lock().unwrap().remove(session);
    }

    /// Mark `session`'s codex app-server alive WITHOUT a verifiable pid (adopted socket).
    /// Stamp-only marks never count for [`Self::is_app_server_live`] — they exist for
    /// bookkeeping/cleanup symmetry only.
    pub fn mark_live(&self, session: SessionId) {
        self.live.lock().unwrap().insert(session, None);
    }

    /// Mark `session`'s codex app-server alive, backed by the spawned root process pid.
    pub fn mark_live_with_pid(&self, session: SessionId, pid: u32) {
        self.live.lock().unwrap().insert(session, Some(pid));
    }

    /// Clear the app-server liveness mark (called on kill/cleanup).
    pub fn unmark_live(&self, session: &SessionId) {
        self.live.lock().unwrap().remove(session);
    }

    /// Whether `session`'s app-server PROCESS is verifiably alive (spawned by this daemon and
    /// its pid still runs), even if not yet turn-bound. A pre-thread headed codex is alive-but-
    /// idle here. Stamp-only marks (no pid) return false so a stale mark can never
    /// mask a dead agent session.
    pub fn is_app_server_live(&self, session: &SessionId) -> bool {
        match self.live.lock().unwrap().get(session) {
            Some(Some(pid)) => os_pid_alive(*pid),
            Some(None) | None => false,
        }
    }

    /// Deliver a drained `NexusBatch` to the codex app-server thread via `turn/start`.
    /// The batch is rendered with receiver addressee framing so the harness receives the full
    /// `<nexus-batch>` envelope (spec §2.3.1). If Codex returns a turn id, this waits for the
    /// forwarder-observed model receipt before returning; otherwise the realtime inbox would mark
    /// rows delivered on app-server handoff even when the actual Codex agent session is dead.
    /// Returns `-32004` if session is not bound or the turn does not complete.
    pub async fn inject_turn(&self, recipient: &SessionId, batch: &NexusBatch) -> PortResult<()> {
        self.inject_turn_inner(recipient, batch, None)
            .await
            .map_err(ContractError::from)
    }

    async fn inject_turn_inner(
        &self,
        recipient: &SessionId,
        batch: &NexusBatch,
        accepted_event: Option<(Arc<dyn EventSink>, WsEvent)>,
    ) -> InjectResult<()> {
        let (client, thread_id) = self.client_for(recipient).map_err(InjectError::Contract)?;
        let turn_start_lock = self.turn_start_lock(recipient);
        let _turn_start_guard = turn_start_lock.lock().await;
        self.wait_for_prior_turn_boundary(&thread_id)
            .await
            .map_err(InjectError::Contract)?;
        let text = render_injected_turn_for(batch, &recipient.0);
        let accepted_event = accepted_event.map(|(events, event)| {
            self.turn_tracker
                .queue_accepted_event(&thread_id, events, event)
        });
        let turn_id = match client.turn_start_id(&thread_id, &text).await {
            Ok(turn_id) => turn_id,
            Err(e) => {
                if let Some(queued) = &accepted_event {
                    self.turn_tracker.cancel_accepted_event(queued);
                }
                if let Some(error) = classify_rpc_error(recipient, &e) {
                    return Err(error);
                }
                return Err(InjectError::Contract(ContractError {
                    code: -32004,
                    message: e.to_string(),
                }));
            }
        };
        if let Some(queued) = &accepted_event {
            self.turn_tracker
                .emit_accepted_event(queued, turn_id.as_deref())
                .await;
        }
        if let Some(turn_id) = turn_id.as_deref() {
            self.turn_tracker
                .record_turn_start_acceptance(&thread_id, turn_id);
        }
        if let Some(turn_id) = turn_id {
            match self
                .turn_tracker
                .wait_for_delivery_receipt(&thread_id, &turn_id, TURN_COMPLETION_TIMEOUT)
                .await
            {
                Ok(()) => {}
                Err(super::turn_completion::CodexTurnWaitError::Timeout { .. }) => {
                    self.turn_tracker.clear_active_turn(&thread_id);
                    return Err(InjectError::CompletionTimeout {
                        session: recipient.clone(),
                        source: "codex_app_server_turn_completion".into(),
                    });
                }
                Err(e) => {
                    if let super::turn_completion::CodexTurnWaitError::Failed(failure) = &e {
                        if let Some(error) = classify_turn_error(recipient, &failure.params) {
                            return Err(error);
                        }
                    }
                    return Err(InjectError::Contract(ContractError {
                        code: -32004,
                        message: e.to_string(),
                    }));
                }
            }
        }
        Ok(())
    }

    /// Deliver a raw operator prompt directly to the codex app-server thread via `turn/start`.
    /// No `<nexus-batch>` envelope — this is a direct operator send (same semantics as
    /// `PtyTransport::prompt` on the PTY path). Unlike bus delivery, this returns once Codex
    /// accepts the turn; model progress and completion are reported asynchronously by the
    /// notification forwarder / observe lane. Returns `-32004` if session is not bound or Codex
    /// rejects the turn.
    pub async fn prompt(&self, recipient: &SessionId, text: String) -> PortResult<()> {
        let (client, thread_id) = self.client_for(recipient)?;
        let turn_start_lock = self.turn_start_lock(recipient);
        let _turn_start_guard = turn_start_lock.lock().await;
        self.wait_for_prior_turn_boundary(&thread_id).await?;
        let accepted_echo = self
            .turn_tracker
            .queue_accepted_user_input_echo(&thread_id, text.clone());
        let turn_id = match client.turn_start_id(&thread_id, &text).await {
            Ok(turn_id) => turn_id,
            Err(e) => {
                self.turn_tracker
                    .cancel_accepted_user_input_echo(&accepted_echo);
                return Err(ContractError {
                    code: -32004,
                    message: e.to_string(),
                });
            }
        };
        if let Some(turn_id) = turn_id {
            self.turn_tracker
                .record_turn_start_acceptance(&thread_id, &turn_id);
            self.turn_tracker
                .record_accepted_user_input_echo_for_queued(&accepted_echo, &turn_id);
        }
        Ok(())
    }

    /// Deliver a raw prompt and emit a caller-supplied user-input row at the accepted boundary.
    ///
    /// The accepted event and Codex userMessage echo marker are queued before `turn/start` because
    /// Codex can emit turn notifications before the JSON-RPC response reaches us.
    pub async fn prompt_observed(
        &self,
        recipient: &SessionId,
        text: String,
        events: Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> PortResult<()> {
        let (client, thread_id) = self.client_for(recipient)?;
        let turn_start_lock = self.turn_start_lock(recipient);
        let _turn_start_guard = turn_start_lock.lock().await;
        self.wait_for_prior_turn_boundary(&thread_id).await?;
        let queued_event = self.turn_tracker.queue_accepted_event_on_native_user_input(
            &thread_id,
            events,
            accepted_event,
        );
        let accepted_echo = self
            .turn_tracker
            .queue_accepted_user_input_echo(&thread_id, text.clone());
        let mut acceptance_cleanup = PromptAcceptanceCleanup {
            tracker: self.turn_tracker.clone(),
            accepted_event: queued_event.clone(),
            accepted_echo: accepted_echo.clone(),
            text: text.clone(),
            turn_id: None,
        };
        let turn_id = match client.turn_start_id(&thread_id, &text).await {
            Ok(turn_id) => turn_id,
            Err(e) => {
                self.turn_tracker.cancel_accepted_event(&queued_event);
                self.turn_tracker
                    .cancel_accepted_user_input_echo(&accepted_echo);
                return Err(ContractError {
                    code: -32004,
                    message: e.to_string(),
                });
            }
        };
        let Some(turn_id) = turn_id else {
            return Err(ContractError {
                code: -32004,
                message: "codex turn/start response missing native turn id".into(),
            });
        };
        acceptance_cleanup.turn_id = Some(turn_id.clone());
        self.turn_tracker
            .record_turn_start_acceptance(&thread_id, &turn_id);
        self.turn_tracker
            .record_accepted_user_input_echo_for_queued(&accepted_echo, &turn_id);
        if let Err(error) = self
            .turn_tracker
            .wait_for_accepted_user_input_echo(
                &thread_id,
                &turn_id,
                &text,
                PROMPT_INPUT_RECEIPT_TIMEOUT,
            )
            .await
        {
            return Err(ContractError {
                code: -32004,
                message: format!(
                    "timed out waiting for codex native prompt context receipt: {error}"
                ),
            });
        }
        Ok(())
    }

    /// Explicitly steer the active regular Codex turn.
    ///
    /// The native turn id comes only from app-server responses/notifications. If no turn is active
    /// or the tracked turn ends before the request arrives, the steer is rejected; Nexus never
    /// converts the frontend's steer choice into `turn/start`. An expected-id mismatch refreshes
    /// from Codex and retries once because it remains the same active-turn operation.
    pub async fn steer_observed(
        &self,
        recipient: &SessionId,
        text: String,
        events: Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> PortResult<SteerResponse> {
        let (client, thread_id) = self.client_for(recipient)?;
        let queued_event =
            self.turn_tracker
                .queue_accepted_event(&thread_id, events, accepted_event);
        let accepted_echo = self
            .turn_tracker
            .queue_accepted_user_input_echo(&thread_id, text.clone());

        let decision = self
            .steer_active(&client, &thread_id, &text)
            .await
            .map_err(|error| {
                self.turn_tracker.cancel_accepted_event(&queued_event);
                self.turn_tracker
                    .cancel_accepted_user_input_echo(&accepted_echo);
                error
            })?;

        self.turn_tracker
            .emit_accepted_event(&queued_event, decision.turn_id.as_deref())
            .await;
        if let Some(turn_id) = decision.turn_id.as_deref() {
            self.turn_tracker
                .record_accepted_user_input_echo_for_queued(&accepted_echo, turn_id);
            self.turn_tracker
                .wait_for_accepted_user_input_echo(
                    &thread_id,
                    turn_id,
                    &text,
                    TURN_COMPLETION_TIMEOUT,
                )
                .await
                .map_err(|error| ContractError {
                    code: -32004,
                    message: format!(
                        "timed out waiting for codex native steer context receipt: {error}"
                    ),
                })?;
        }
        Ok(SteerResponse {
            accepted: true,
            delivery: decision.delivery,
            turn_id: decision.turn_id,
        })
    }

    async fn steer_active(
        &self,
        client: &CodexAppServerClient,
        thread_id: &str,
        text: &str,
    ) -> PortResult<SteerDecision> {
        let Some(mut expected_turn_id) = self.turn_tracker.active_turn_id(thread_id) else {
            return Err(active_turn_required());
        };
        let mut retried_mismatch = false;
        loop {
            match client
                .turn_steer_id(thread_id, &expected_turn_id, text)
                .await
            {
                Ok(turn_id) => {
                    self.turn_tracker.observe_active_turn(thread_id, &turn_id);
                    return Ok(SteerDecision {
                        delivery: SteerDelivery::Steered,
                        turn_id: Some(turn_id),
                    });
                }
                Err(error) => match active_turn_steer_race(&error) {
                    Some(ActiveTurnSteerRace::Missing) => {
                        self.turn_tracker.clear_active_turn(thread_id);
                        return Err(active_turn_required());
                    }
                    Some(ActiveTurnSteerRace::ExpectedTurnMismatch { actual_turn_id })
                        if !retried_mismatch && actual_turn_id != expected_turn_id =>
                    {
                        self.turn_tracker
                            .observe_active_turn(thread_id, &actual_turn_id);
                        expected_turn_id = actual_turn_id;
                        retried_mismatch = true;
                    }
                    _ => {
                        return Err(ContractError {
                            code: -32004,
                            message: error.to_string(),
                        });
                    }
                },
            }
        }
    }

    /// Native context compaction via the app-server: `thread/compact/start` on the bound
    /// thread — the REAL operation behind the TUI's `/compact` (text-injecting "/compact"
    /// through `turn/start` just hands the model a literal chat message). Completion is
    /// signalled by the `thread/compacted` notification on the forwarded stream.
    pub async fn compact(&self, recipient: &SessionId) -> PortResult<()> {
        let (client, thread_id) = self.client_for(recipient)?;
        client
            .thread_compact_start(&thread_id)
            .await
            .map_err(|e| ContractError {
                code: -32004,
                message: e.to_string(),
            })?;
        Ok(())
    }

    /// Interrupt the currently active native Codex turn for a bound session.
    pub async fn interrupt_active_turn(&self, recipient: &SessionId) -> PortResult<()> {
        let (client, thread_id) = self.client_for(recipient)?;
        let turn_id = self
            .turn_tracker
            .active_turn_id(&thread_id)
            .ok_or_else(|| ContractError {
                code: nexus_contracts::codes::ACTIVE_TURN_REQUIRED,
                message: "no active turn to interrupt".into(),
            })?;
        client
            .turn_interrupt(&thread_id, &turn_id)
            .await
            .map_err(|error| ContractError {
                code: -32004,
                message: error.to_string(),
            })
    }

    /// Probe liveness for the heartbeat keeper.
    /// `Some(true)` only if the Codex turn client is bound to the actual agent session. A live
    /// app-server process without a bound Codex session is not agent liveness.
    pub fn is_harness_alive(&self, recipient: &SessionId) -> Option<bool> {
        if self.is_bound(recipient) {
            Some(true)
        } else {
            None
        }
    }

    /// Bound sessions whose native Codex thread currently has an active turn.
    ///
    /// The daemon's durable `harness.prompt` scheduler uses this to preserve the visible Queue
    /// boundary. Explicit steer requests bypass that prompt queue through their own command lane
    /// and still use the same native turn id as authority.
    pub fn active_turn_sessions(&self) -> Vec<SessionId> {
        self.sessions
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(session, (_, thread_id))| {
                self.turn_tracker
                    .active_turn_id(thread_id)
                    .is_some()
                    .then(|| session.clone())
            })
            .collect()
    }

    // -- Internal helpers --

    fn turn_start_lock(&self, recipient: &SessionId) -> Arc<AsyncMutex<()>> {
        self.turn_start_locks
            .lock()
            .unwrap()
            .entry(recipient.clone())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }

    async fn wait_for_prior_turn_boundary(&self, thread_id: &str) -> PortResult<()> {
        while let Some(turn_id) = self.turn_tracker.active_turn_id(thread_id) {
            match self
                .turn_tracker
                .wait_for_completion(thread_id, &turn_id, TURN_COMPLETION_TIMEOUT)
                .await
            {
                Ok(()) | Err(super::turn_completion::CodexTurnWaitError::Failed(_)) => {}
                Err(error) => {
                    return Err(ContractError {
                        code: -32004,
                        message: format!(
                        "failed waiting for prior Codex turn boundary before turn/start: {error}"
                    ),
                    })
                }
            }
        }
        Ok(())
    }

    fn client_for(&self, recipient: &SessionId) -> PortResult<(Arc<CodexAppServerClient>, String)> {
        let map = self.sessions.lock().unwrap();
        map.get(recipient).cloned().ok_or_else(|| ContractError {
            code: -32004,
            message: format!(
                "no codex app-server client bound for session {}",
                recipient.0
            ),
        })
    }
}

#[async_trait]
impl AgentTurnExecutionPort for CodexAppServerTransport {
    async fn inject_turn(&self, recipient: &SessionId, batch: &NexusBatch) -> PortResult<()> {
        CodexAppServerTransport::inject_turn(self, recipient, batch).await
    }

    async fn inject_turn_observed(
        &self,
        recipient: &SessionId,
        batch: &NexusBatch,
        events: Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> InjectResult<()> {
        self.inject_turn_inner(recipient, batch, Some((events, accepted_event)))
            .await
    }

    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        Err(ContractError {
            code: -32601,
            message: "codex app-server transport only injects into already-launched sessions"
                .into(),
        })
    }

    async fn remove(&self, _req: RemoveRequest) -> PortResult<RemoveResponse> {
        Err(ContractError {
            code: -32601,
            message: "codex app-server transport cleanup is owned by CodexBridge".into(),
        })
    }

    async fn prompt(&self, recipient: &SessionId, text: String) -> PortResult<()> {
        CodexAppServerTransport::prompt(self, recipient, text).await
    }

    async fn prompt_observed(
        &self,
        recipient: &SessionId,
        text: String,
        events: Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> PortResult<()> {
        CodexAppServerTransport::prompt_observed(self, recipient, text, events, accepted_event)
            .await
    }

    async fn steer_observed(
        &self,
        recipient: &SessionId,
        text: String,
        events: Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> PortResult<SteerResponse> {
        CodexAppServerTransport::steer_observed(self, recipient, text, events, accepted_event).await
    }

    async fn interrupt_active_turn(&self, recipient: &SessionId) -> PortResult<()> {
        CodexAppServerTransport::interrupt_active_turn(self, recipient).await
    }

    fn steer_capability(&self, recipient: &SessionId) -> SteerCapability {
        if self.is_bound(recipient) {
            SteerCapability::NativeSteer
        } else {
            SteerCapability::None
        }
    }

    async fn compact(&self, recipient: &SessionId) -> PortResult<()> {
        CodexAppServerTransport::compact(self, recipient).await
    }

    fn is_harness_alive(&self, recipient: &SessionId) -> Option<bool> {
        CodexAppServerTransport::is_harness_alive(self, recipient)
    }

    fn active_turn_sessions(&self) -> Vec<SessionId> {
        CodexAppServerTransport::active_turn_sessions(self)
    }

    async fn wait_for_turn_completion(&self, recipient: &SessionId) -> PortResult<()> {
        let (_, thread_id) = self.client_for(recipient)?;
        let Some(turn_id) = self.turn_tracker.active_turn_id(&thread_id) else {
            return Ok(());
        };
        match self
            .turn_tracker
            .wait_for_completion(&thread_id, &turn_id, TURN_COMPLETION_TIMEOUT)
            .await
        {
            Ok(()) | Err(super::turn_completion::CodexTurnWaitError::Failed(_)) => Ok(()),
            Err(error) => Err(ContractError {
                code: -32004,
                message: format!("failed waiting for active Codex turn boundary: {error}"),
            }),
        }
    }
}

struct SteerDecision {
    delivery: SteerDelivery,
    turn_id: Option<String>,
}

fn active_turn_required() -> ContractError {
    ContractError {
        code: nexus_contracts::codes::ACTIVE_TURN_REQUIRED,
        message: "no active turn to steer".into(),
    }
}

enum ActiveTurnSteerRace {
    Missing,
    ExpectedTurnMismatch { actual_turn_id: String },
}

fn active_turn_steer_race(error: &CodexRpcError) -> Option<ActiveTurnSteerRace> {
    let CodexRpcError::Rpc { message, .. } = error else {
        return None;
    };
    if message == "no active turn to steer" {
        return Some(ActiveTurnSteerRace::Missing);
    }
    let actual_turn_id = message
        .strip_prefix("expected active turn id `")?
        .split_once("` but found `")?
        .1
        .strip_suffix('`')?
        .to_string();
    Some(ActiveTurnSteerRace::ExpectedTurnMismatch { actual_turn_id })
}

// =============================================================================
// Tests
// =============================================================================

/// Shared test helpers for in-process fake WebSocket-over-Unix-socket codex server.
/// Exported as `pub(crate)` so `routing_turn_exec` tests can reuse without duplication.
#[cfg(all(test, unix))]
pub(crate) mod test_support {
    use futures::SinkExt;
    use futures::StreamExt;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use tokio::net::UnixListener;
    use tokio_tungstenite::accept_async_with_config;
    use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
    use tokio_tungstenite::tungstenite::Message;

    /// Recorded JSON-RPC frames from the server's perspective.
    pub(crate) type RecordedCalls = Arc<Mutex<Vec<serde_json::Value>>>;

    /// Unique temp socket path scoped to this test run.
    pub(crate) fn sock_path(prefix: &str, tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "nexus-{}-test-{}-{}.sock",
            prefix,
            std::process::id(),
            tag
        ))
    }

    /// Spawn an in-process WS-over-Unix-socket fake codex server that:
    ///   1. Handles the `initialize` handshake (returns protocolVersion / serverInfo / capabilities).
    ///   2. Treats `initialized` as a no-op notification.
    ///   3. Records every subsequent request into `calls` and replies `{result: null}`.
    ///
    /// Returns the socket path and the shared `calls` recorder.
    pub(crate) async fn spawn_fake_server(prefix: &str, tag: &str) -> (PathBuf, RecordedCalls) {
        let path = sock_path(prefix, tag);
        let _ = std::fs::remove_file(&path);
        let calls: RecordedCalls = Arc::new(Mutex::new(Vec::new()));
        let calls_clone = calls.clone();
        let listener = UnixListener::bind(&path).expect("bind fake server socket");
        tokio::spawn(async move {
            // Accept multiple connections so tests can bind >1 client.
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let calls_inner = calls_clone.clone();
                let ws_config = WebSocketConfig {
                    max_frame_size: Some(128 << 20),
                    max_message_size: Some(128 << 20),
                    ..WebSocketConfig::default()
                };
                let ws = match accept_async_with_config(stream, Some(ws_config)).await {
                    Ok(ws) => ws,
                    Err(_) => continue,
                };
                tokio::spawn(handle_connection(ws, calls_inner));
            }
        });
        (path, calls)
    }

    pub(crate) async fn handle_connection(
        mut ws: tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>,
        calls: RecordedCalls,
    ) {
        use serde_json::json;
        while let Some(Ok(msg)) = ws.next().await {
            let text = match msg {
                Message::Text(t) => t,
                Message::Close(_) => break,
                _ => continue,
            };
            let v: serde_json::Value = match serde_json::from_str(&text) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let Some(id) = v.get("id").cloned() else {
                // Notification (no id) — just record it.
                calls.lock().unwrap().push(v);
                continue;
            };
            let method = v.get("method").and_then(|m| m.as_str()).unwrap_or("");

            let result = if method == "initialize" {
                json!({
                    "protocolVersion": "0.1.0",
                    "serverInfo": {"name": "fake", "version": "0.1.0"},
                    "capabilities": {}
                })
            } else {
                // Record all non-handshake requests.
                calls.lock().unwrap().push(v.clone());
                serde_json::Value::Null
            };

            let reply = json!({"jsonrpc": "2.0", "id": id, "result": result});
            if ws
                .send(Message::Text(serde_json::to_string(&reply).unwrap().into()))
                .await
                .is_err()
            {
                break;
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::test_support::spawn_fake_server;
    use super::*;
    use nexus_contracts::batch::{BatchCounts, NexusBatch};
    use nexus_contracts::ids::SessionId;
    use std::sync::Arc;

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
             stale stamps must not mask dead agents"
        );
        transport.mark_live_with_pid(s.clone(), std::process::id());
        assert!(
            transport.is_app_server_live(&s),
            "a pid-backed mark with a RUNNING process is a live app-server"
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
}
