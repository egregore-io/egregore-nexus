//! `PtyTransport` — the ONE swap vs. the ACP path (Phase 4 of the PTY-native-harness plan).
//!
//! Implements [`AgentTurnExecutionPort`] for a native-TUI harness running in a daemon-owned PTY.
//! Its [`inject_turn`](PtyTransport::inject_turn) renders the drained `<nexus-batch>` with
//! recipient addressee framing and WRITES it into the recipient's PTY input as a single
//! bracketed-paste block followed by a carriage return — instead of an ACP `session/prompt`. The
//! drain / `in_flight` machinery is unchanged; this is how DMs/threads/operator-sends reach a native
//! harness. Prompt-readiness misses from a headed tmux pane are retried with short backoff so a
//! transient Claude compaction redraw does not permanently strand a pending bus row. PTY launch is
//! `PtySupervisor`'s job (Task 7), so [`launch`](PtyTransport::launch) returns an error here.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use nexus_agent::adapter::opencode::classify_opencode_provider_error_payload;
use nexus_common::render_injected_turn_for;
use nexus_contracts::batch::NexusBatch;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::{
    AgentTurnExecutionPort, ContractError, EventSink, InjectError, InjectResult, PortResult,
};
use nexus_contracts::{
    RemoveRequest, RemoveResponse, SpawnRequest, SpawnResponse, SteerCapability, WsEvent,
};
use nexus_pty::{HarnessInput, TurnCompletionEvidence};
use serde_json::Value;

use super::opencode_plugin_bridge::OPENCODE_PROVIDER_ERROR_PREFIX;

const PROMPT_READY_RETRY_DELAYS: [std::time::Duration; 3] = [
    std::time::Duration::from_millis(250),
    std::time::Duration::from_secs(1),
    std::time::Duration::from_secs(3),
];

/// Delivers a drained batch to a native-harness agent by handing the rendered `<nexus-batch>` to its
/// bound [`HarnessInput`] backend (a raw [`PtySession`](nexus_pty::PtySession) for the cat-based
/// tests, or a [`TmuxHarness`](nexus_pty::TmuxHarness) in production), instead of an ACP
/// `session/prompt`. This is the ONE swap vs. the ACP path; the drain/`in_flight`/`render_batch`
/// machinery is unchanged. Prompt-readiness errors are retried because Claude compaction can redraw
/// the pane long enough for the first readiness wait to miss.
#[derive(Default, Clone)]
pub struct PtyTransport {
    sessions: Arc<Mutex<HashMap<SessionId, Arc<dyn HarnessInput>>>>,
}

impl PtyTransport {
    /// Bind a recipient session to the harness backend that should receive its injected turns. The
    /// backend is any [`HarnessInput`]: a raw `PtySession` (tests) or a `TmuxHarness` (production).
    pub fn bind(&self, session: SessionId, input: Arc<dyn HarnessInput>) {
        self.sessions.lock().unwrap().insert(session, input);
    }

    /// Whether `session` has a PTY/tmux harness bound here. This is the per-launch mode signal the
    /// `RoutingTurnExec` routes on: bound → headed (deliver via this transport), unbound → headless
    /// (deliver via the ACP agent). O(1); no I/O.
    pub fn is_bound(&self, session: &SessionId) -> bool {
        self.sessions.lock().unwrap().contains_key(session)
    }

    /// The harness backend bound for `recipient`, or a "no harness bound" port error. Shared by
    /// [`inject_turn`](AgentTurnExecutionPort::inject_turn) (drained bus batch) and
    /// [`prompt`](AgentTurnExecutionPort::prompt) (raw operator text) — both deliver to the SAME
    /// bound `HarnessInput`; only the payload differs.
    fn harness_for(&self, recipient: &SessionId) -> PortResult<Arc<dyn HarnessInput>> {
        let input = {
            let map = self.sessions.lock().unwrap();
            map.get(recipient).cloned()
        };
        input.ok_or_else(|| ContractError {
            code: -32004,
            message: format!("no harness bound for session {}", recipient.0),
        })
    }

    async fn inject_turn_inner(
        &self,
        recipient: &SessionId,
        batch: &NexusBatch,
        accepted_event: Option<(Arc<dyn EventSink>, WsEvent)>,
    ) -> PortResult<()> {
        let input = self.harness_for(recipient)?;
        send_turn_with_prompt_retries(input, &render_injected_turn_for(batch, &recipient.0))
            .await
            .map_err(|e| ContractError {
                code: -32004,
                message: e,
            })?;
        if let Some((events, event)) = accepted_event {
            events.emit(event).await;
        }
        Ok(())
    }
}

async fn send_turn_with_prompt_retries(
    input: Arc<dyn HarnessInput>,
    text: &str,
) -> Result<(), String> {
    let mut last_error = None;
    for attempt in 0..=PROMPT_READY_RETRY_DELAYS.len() {
        match input.send_turn(text).await {
            Ok(()) => return Ok(()),
            Err(error) if prompt_readiness_error(&error) => {
                last_error = Some(error);
                if let Some(delay) = PROMPT_READY_RETRY_DELAYS.get(attempt) {
                    tokio::time::sleep(*delay).await;
                }
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| "prompt readiness retry exhausted".to_string()))
}

async fn compact_with_prompt_retries(input: Arc<dyn HarnessInput>) -> Result<(), String> {
    let mut last_error = None;
    for attempt in 0..=PROMPT_READY_RETRY_DELAYS.len() {
        match input.compact().await {
            Ok(()) => return Ok(()),
            Err(error) if prompt_readiness_error(&error) => {
                last_error = Some(error);
                if let Some(delay) = PROMPT_READY_RETRY_DELAYS.get(attempt) {
                    tokio::time::sleep(*delay).await;
                }
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| "prompt readiness retry exhausted".to_string()))
}

fn prompt_readiness_error(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("input prompt never rendered") || error.contains("terminal did not become ready")
}

fn contract_harness_error(error: String) -> ContractError {
    ContractError {
        code: -32004,
        message: error,
    }
}

fn inject_harness_error(recipient: &SessionId, error: String) -> InjectError {
    classify_structured_harness_error(recipient, &error)
        .unwrap_or_else(|| InjectError::Contract(contract_harness_error(error)))
}

fn classify_structured_harness_error(recipient: &SessionId, error: &str) -> Option<InjectError> {
    let payload = error.strip_prefix(OPENCODE_PROVIDER_ERROR_PREFIX)?;
    let payload: Value = serde_json::from_str(payload).ok()?;
    classify_opencode_provider_error_payload(&payload, "opencode_plugin_bridge")
        .map(|error| error.into_inject_error(recipient))
}

// `bracketed_paste` now lives in `nexus_pty` (shared with the `PtySession` `HarnessInput` impl).

#[async_trait]
impl AgentTurnExecutionPort for PtyTransport {
    async fn inject_turn(&self, recipient: &SessionId, batch: &NexusBatch) -> PortResult<()> {
        self.inject_turn_inner(recipient, batch, None).await
    }

    async fn inject_turn_observed(
        &self,
        recipient: &SessionId,
        batch: &NexusBatch,
        events: Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> InjectResult<()> {
        let input = self.harness_for(recipient).map_err(InjectError::Contract)?;
        if input.turn_completion_evidence() == TurnCompletionEvidence::InputAcceptedOnly {
            return Err(InjectError::Contract(ContractError {
                code: nexus_contracts::codes::INTERNAL_ERROR,
                message: format!(
                    "native harness {} cannot observe context acceptance; this delivery path is \
                     unsupported for durable v0.1 settlement",
                    recipient.0
                ),
            }));
        }
        send_turn_with_prompt_retries(input, &render_injected_turn_for(batch, &recipient.0))
            .await
            .map_err(|error| inject_harness_error(recipient, error))?;
        events.emit(accepted_event).await;
        Ok(())
    }

    /// DIRECT operator→agent prompt (the web `/agent` view's send): inject the operator's RAW text
    /// straight into the recipient's native harness — exactly like a person typing into the TUI —
    /// with NO `<nexus-batch>` envelope. That envelope is for drained agent-to-agent mail; an
    /// operator prompt IS the harness's own input. Same bound `HarnessInput`/`send_turn` delivery as
    /// `inject_turn`, only the payload is verbatim text. The trait's default `prompt` is a silent
    /// no-op (it assumed an ACP `session/prompt`); without this override every web→TUI send was
    /// accepted (`delivered: true`) yet dropped, because a tmux harness has no ACP session.
    async fn prompt(&self, recipient: &SessionId, text: String) -> PortResult<()> {
        let input = self.harness_for(recipient)?;
        send_turn_with_prompt_retries(input, &text)
            .await
            .map_err(|e| ContractError {
                code: -32004,
                message: e,
            })?;
        Ok(())
    }

    async fn prompt_observed(
        &self,
        recipient: &SessionId,
        text: String,
        events: Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> PortResult<()> {
        let input = self.harness_for(recipient)?;
        send_turn_with_prompt_retries(input, &text)
            .await
            .map_err(|e| ContractError {
                code: -32004,
                message: e,
            })?;
        events.emit(accepted_event).await;
        Ok(())
    }

    fn steer_capability(&self, recipient: &SessionId) -> SteerCapability {
        if self.sessions.lock().unwrap().contains_key(recipient) {
            SteerCapability::InterruptAndSend
        } else {
            SteerCapability::None
        }
    }

    async fn interrupt_active_turn(&self, recipient: &SessionId) -> PortResult<()> {
        self.harness_for(recipient)?
            .interrupt_active_turn()
            .await
            .map_err(|message| ContractError {
                code: -32004,
                message,
            })
    }

    /// Native compaction for a HEADED harness: ask the bound harness input to run its compaction
    /// command. The default command is `/compact`; Hermes overrides this to `/compress`.
    async fn compact(&self, recipient: &SessionId) -> PortResult<()> {
        let input = self.harness_for(recipient)?;
        compact_with_prompt_retries(input)
            .await
            .map_err(|e| ContractError {
                code: -32004,
                message: e,
            })?;
        Ok(())
    }

    fn is_harness_alive(&self, recipient: &SessionId) -> Option<bool> {
        // The bound `HarnessInput` knows (tmux `has-session` / raw child not exited). No binding →
        // there's no harness for this session → not alive.
        Some(
            self.sessions
                .lock()
                .unwrap()
                .get(recipient)
                .map(|h| h.is_alive())
                .unwrap_or(false),
        )
    }

    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        Err(ContractError {
            code: -32601,
            message: "PtyTransport does not launch; use PtySupervisor".into(),
        })
    }

    async fn remove(&self, req: RemoveRequest) -> PortResult<RemoveResponse> {
        Err(ContractError {
            code: -32601,
            message: format!(
                "PtyTransport does not remove sessions; use PtySupervisor (req: {:?})",
                req
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_contracts::batch::{BatchCounts, BatchMessage, NexusBatch};
    use nexus_contracts::ids::{MessageId, SessionId};
    use nexus_contracts::{
        AgentUpdateKind, Harness, InjectError, Kind, ProviderLimitReason, Scope,
    };
    use nexus_pty::bracketed_paste;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct CapturingHarness {
        turns: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl HarnessInput for CapturingHarness {
        async fn send_turn(&self, text: &str) -> Result<(), String> {
            self.turns.lock().unwrap().push(text.to_string());
            Ok(())
        }
    }

    #[test]
    fn is_bound_reflects_binding() {
        let transport = PtyTransport::default();
        let session = SessionId("s_bound".into());
        assert!(
            !transport.is_bound(&session),
            "unbound session is not bound"
        );
        let harness = Arc::new(CapturingHarness::default());
        transport.bind(session.clone(), harness as Arc<dyn nexus_pty::HarnessInput>);
        assert!(transport.is_bound(&session), "bound session reports bound");
        assert!(
            !transport.is_bound(&SessionId("s_other".into())),
            "other session still unbound"
        );
    }

    #[test]
    fn bracketed_paste_wraps_multiline_in_paste_markers() {
        let out = bracketed_paste("a\nb");
        let s = String::from_utf8_lossy(&out);
        assert!(s.starts_with("\u{1b}[200~"), "starts with paste-begin");
        assert!(s.ends_with("\u{1b}[201~"), "ends with paste-end");
        assert!(s.contains("a\nb"), "carries the body verbatim");
    }

    #[tokio::test]
    async fn inject_turn_sends_the_rendered_batch_to_the_bound_harness() {
        let session = SessionId("s_pty".into());
        let harness = Arc::new(CapturingHarness::default());

        let transport = PtyTransport::default();
        transport.bind(
            session.clone(),
            harness.clone() as Arc<dyn nexus_pty::HarnessInput>,
        );

        let batch = NexusBatch {
            counts: BatchCounts {
                dms: 1,
                thread: 0,
                total: 1,
            },
            dms: vec![BatchMessage {
                id: MessageId("m_pty".into()),
                from: "roman".into(),
                kind: Kind::Agent,
                scope: Scope::Dm,
                thread: None,
                topic: None,
                body: "verify this".into(),
                truncated: false,
            }],
            threads: vec![],
            dm_message_ids: vec![MessageId("m_pty".into())],
            thread_message_ids: vec![],
            message_ids: vec![MessageId("m_pty".into())],
        };
        transport.inject_turn(&session, &batch).await.unwrap();

        let injected = harness.turns.lock().unwrap()[0].clone();
        assert!(
            injected.contains("<nexus-batch"),
            "the rendered <nexus-batch> must reach the PTY: {injected}"
        );
        assert!(
            injected.contains("receiver=\"s_pty\""),
            "the PTY prompt must name the receiver: {injected}"
        );
        assert!(
            injected.contains("target=\"dm:s_pty\""),
            "the PTY prompt must name the per-recipient DM target: {injected}"
        );
    }

    #[tokio::test]
    async fn prompt_sends_raw_operator_text_without_a_batch_envelope() {
        let session = SessionId("s_pty".into());
        let harness = Arc::new(CapturingHarness::default());

        let transport = PtyTransport::default();
        transport.bind(
            session.clone(),
            harness.clone() as Arc<dyn nexus_pty::HarnessInput>,
        );

        // The operator's verbatim text (the web `/agent` send). It must reach the harness AS-IS —
        // no `<nexus-batch>` envelope (that wrapper is for drained agent-to-agent mail).
        transport
            .prompt(&session, "WEB2TUI-RAW-9931".to_string())
            .await
            .unwrap();

        let sent = harness.turns.lock().unwrap();
        assert_eq!(sent.as_slice(), ["WEB2TUI-RAW-9931"]);
        assert!(
            !sent[0].contains("nexus-batch"),
            "an operator prompt must NOT be wrapped in a <nexus-batch> envelope"
        );
    }

    #[derive(Default)]
    struct CustomCompactHarness {
        sent: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait]
    impl HarnessInput for CustomCompactHarness {
        async fn send_turn(&self, text: &str) -> Result<(), String> {
            self.sent.lock().unwrap().push(format!("turn:{text}"));
            Ok(())
        }

        async fn compact(&self) -> Result<(), String> {
            self.sent
                .lock()
                .unwrap()
                .push("compact:/compress".to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn compact_uses_the_bound_harness_compaction_command() {
        let session = SessionId("s_custom_compact".into());
        let harness = Arc::new(CustomCompactHarness::default());
        let transport = PtyTransport::default();
        transport.bind(session.clone(), harness.clone() as Arc<dyn HarnessInput>);

        transport.compact(&session).await.unwrap();

        assert_eq!(
            harness.sent.lock().unwrap().as_slice(),
            ["compact:/compress"]
        );
    }

    #[tokio::test]
    async fn prompt_errors_when_no_harness_is_bound() {
        let transport = PtyTransport::default();
        let err = transport
            .prompt(&SessionId("s_missing".into()), "hi".to_string())
            .await
            .unwrap_err();
        assert!(
            err.message.contains("no harness bound"),
            "got: {}",
            err.message
        );
    }

    #[derive(Default)]
    struct FlakyPromptHarness {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl HarnessInput for FlakyPromptHarness {
        async fn send_turn(&self, _text: &str) -> Result<(), String> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                Err("harness terminal did not become ready within 30s (input prompt never rendered)"
                    .to_string())
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn inject_turn_retries_transient_prompt_readiness_failures() {
        let session = SessionId("s_retry_prompt".into());
        let harness = Arc::new(FlakyPromptHarness::default());
        let transport = PtyTransport::default();
        transport.bind(session.clone(), harness.clone() as Arc<dyn HarnessInput>);
        let batch = NexusBatch {
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
        };

        transport.inject_turn(&session, &batch).await.unwrap();

        assert_eq!(harness.calls.load(Ordering::SeqCst), 2);
    }

    struct StructuredProviderLimitHarness;

    #[async_trait]
    impl HarnessInput for StructuredProviderLimitHarness {
        async fn send_turn(&self, _text: &str) -> Result<(), String> {
            Err(
                "__nexus_opencode_provider_error__:{\"providerError\":{\"reason\":\"rate_limit\",\"retryAfterMs\":1200,\"provider\":\"openrouter\",\"model\":\"free-model\"}}"
                    .to_string(),
            )
        }

        fn turn_completion_evidence(&self) -> TurnCompletionEvidence {
            TurnCompletionEvidence::Terminal
        }
    }

    struct NoopSink;

    #[async_trait]
    impl EventSink for NoopSink {
        async fn emit(&self, _event: WsEvent) {}
    }

    #[tokio::test]
    async fn inject_turn_observed_maps_opencode_bridge_provider_error_to_typed_hold() {
        let session = SessionId("s_opencode_limit".into());
        let transport = PtyTransport::default();
        transport.bind(
            session.clone(),
            Arc::new(StructuredProviderLimitHarness) as Arc<dyn HarnessInput>,
        );
        let batch = NexusBatch {
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
        };

        let err = transport
            .inject_turn_observed(
                &session,
                &batch,
                Arc::new(NoopSink),
                WsEvent::AgentUpdate {
                    session_id: session.clone(),
                    kind: AgentUpdateKind::Text,
                    data: serde_json::json!({ "text": "accepted" }),
                },
            )
            .await
            .expect_err("structured bridge provider errors must not collapse to ContractError");

        let InjectError::ProviderLimit(limit) = err else {
            panic!("expected ProviderLimit, got {err:?}");
        };
        assert_eq!(limit.harness, Harness::OpenCode);
        assert_eq!(limit.session, session);
        assert_eq!(limit.reason, ProviderLimitReason::RateLimit);
        assert_eq!(limit.provider.as_deref(), Some("openrouter"));
        assert_eq!(limit.model.as_deref(), Some("free-model"));
        assert_eq!(limit.source, "opencode_plugin_bridge");
        assert!(limit.reset_hint.is_some());
    }
}
