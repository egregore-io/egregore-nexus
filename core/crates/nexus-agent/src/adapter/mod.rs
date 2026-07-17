//! The runtime-agnostic adapter seam (backend spec §5). Each agent `kind`/[`nexus_contracts::Harness`] maps to an
//! [`Adapter`] implementing a common inject/stream contract so the bus stays runtime-agnostic:
//! harnesses over the same uniform ACP transport, and [`MockAdapter`] for
//! tests/acceptance. ACP `session/prompt` is the injection transport; `session/update` is the
//! reply stream. There is no per-harness "app-server" route — nexus is the orchestrator.
//! Claude and Codex are wired by the composition root via their harness crates.

use async_trait::async_trait;

use nexus_common::{NexusError, RuntimeProcessIds};
use nexus_contracts::{
    ContractError, Harness, InjectError, OperatorAction, ProviderError, ProviderLimit,
    ProviderLimitReason, ResetHint, SessionId, SteerCapability,
};

pub mod acp;
pub mod bootstrap;
pub mod engine;
pub mod hermes;
pub mod mock;
pub mod opencode;
pub mod provider_limit;
pub mod skill;

pub use engine::{AcpEngine, HarnessCommand};
pub use hermes::HermesAdapter;
pub use mock::MockAdapter;
pub use opencode::OpenCodeAdapter;

/// Adapter-owned structured provider-limit metadata before the daemon has attached the Nexus
/// session id. The agent service wraps this into [`InjectError::ProviderLimit`] at the observed
/// injection seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterProviderLimit {
    pub harness: Harness,
    pub reason: ProviderLimitReason,
    pub reset_hint: Option<ResetHint>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub source: String,
}

/// Adapter-owned structured provider failure before the daemon attaches the Nexus session id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterProviderError {
    pub harness: Harness,
    pub reason: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub retryable: bool,
    pub source: String,
}

/// Adapter-owned structured operator-action metadata before the daemon has attached the Nexus
/// session id. These are account/auth/billing stops, not rapid retry candidates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterOperatorAction {
    pub harness: Harness,
    pub reason: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub source: String,
}

/// Injection failure returned by concrete adapters. Plain `Contract` preserves legacy behavior;
/// typed variants are consumed by the realtime breaker on the observed bus-delivery path.
#[derive(Debug, thiserror::Error)]
pub enum AdapterInjectError {
    #[error("{:?} provider limit ({:?})", .0.harness, .0.reason)]
    ProviderLimit(AdapterProviderLimit),
    #[error("{:?} provider error ({})", .0.harness, .0.reason)]
    ProviderError(AdapterProviderError),
    #[error("{:?} operator action required ({})", .0.harness, .0.reason)]
    OperatorAction(AdapterOperatorAction),
    #[error("harness turn completion timed out ({origin})")]
    CompletionTimeout { origin: String },
    #[error(transparent)]
    Contract(#[from] NexusError),
}

impl AdapterInjectError {
    /// Attach the Nexus session id and convert to the shared observed-injection error shape.
    pub fn into_inject_error(self, session: &SessionId) -> InjectError {
        match self {
            AdapterInjectError::ProviderLimit(limit) => InjectError::ProviderLimit(ProviderLimit {
                harness: limit.harness,
                session: session.clone(),
                reason: limit.reason,
                reset_hint: limit.reset_hint,
                provider: limit.provider,
                model: limit.model,
                source: limit.source,
            }),
            AdapterInjectError::ProviderError(error) => InjectError::ProviderError(ProviderError {
                harness: error.harness,
                session: session.clone(),
                reason: error.reason,
                provider: error.provider,
                model: error.model,
                retryable: error.retryable,
                source: error.source,
            }),
            AdapterInjectError::OperatorAction(action) => {
                InjectError::OperatorAction(OperatorAction {
                    harness: action.harness,
                    session: session.clone(),
                    reason: action.reason,
                    provider: action.provider,
                    model: action.model,
                    source: action.source,
                })
            }
            AdapterInjectError::CompletionTimeout { origin } => InjectError::CompletionTimeout {
                session: session.clone(),
                source: origin,
            },
            AdapterInjectError::Contract(error) => InjectError::Contract(error.to_contract_error()),
        }
    }

    /// Collapse to the legacy contract error for non-observed callers.
    pub fn into_contract_error(self, session: &SessionId) -> ContractError {
        ContractError::from(self.into_inject_error(session))
    }
}

/// One translated ACP `session/update` event from an agent turn — re-exported from
/// [`nexus_acp_stream`]. The daemon relays each as a tagged `WsEvent::AgentUpdate { kind, data }`.
/// (Replaces the former text-only `UpdateChunk`: the full stream is forwarded now, not just the
/// reply text.)
pub use nexus_acp_stream::StreamEvent;

/// The injection/stream contract every harness adapter implements (backend spec §2.3, §5).
///
/// A turn is two steps: [`Adapter::inject`] hands the rendered prompt to the harness over ACP
/// `session/prompt`, then [`Adapter::stream_updates`] yields the reply as ordered `session/update`
/// chunks. [`Adapter::open_session`] stands up (or, via [`Adapter::resume`], re-attaches) the ACP
/// session for a harness; a failure here surfaces as `agent.status=errored` and the session is
/// retained for retry (spec §11), never silently dropped.
#[async_trait]
pub trait Adapter: Send + Sync {
    /// Open a fresh ACP session for this harness. Fallible — a failure is surfaced as an errored
    /// status by the caller and the registered session is kept for retry (spec §11).
    async fn open_session(&self) -> Result<(), NexusError>;

    /// Re-attach an existing harness session by its resume key (`client_key` / ACP thread), reusing
    /// the same session so the name↔session binding is preserved (spec §5).
    async fn resume(&self, resume_key: &str) -> Result<(), NexusError>;

    /// Inject one already-rendered prompt as a single ACP `session/prompt` turn.
    async fn inject(&self, prompt: String) -> Result<(), AdapterInjectError>;

    /// Active-turn redirect capability exposed by this adapter.
    fn steer_capability(&self) -> SteerCapability {
        SteerCapability::None
    }

    /// Interrupt the adapter's active turn. Real ACP adapters map this to `session/cancel`.
    async fn interrupt_active_turn(&self) -> Result<(), NexusError> {
        Err(NexusError::Adapter(
            "this adapter does not support active-turn redirect".into(),
        ))
    }

    /// Inject one rendered prompt and, when the adapter can identify the prompt-accepted boundary,
    /// place `accepted_event` at the front of the turn's stream before assistant output.
    ///
    /// Default implementations ignore the event and preserve the legacy inject behavior. Real ACP
    /// adapters override through [`AcpEngine`] so bus/steer input appears before streamed replies.
    async fn inject_with_accepted_event(
        &self,
        prompt: String,
        _accepted_event: Option<StreamEvent>,
    ) -> Result<(), AdapterInjectError> {
        self.inject(prompt).await
    }

    /// Drain the most recent injected turn's reply as the ordered, translated ACP `session/update`
    /// stream — the full pass-through ([`StreamEvent`]s: text, thinking, tool calls, plans,
    /// available commands), in wire order.
    async fn stream_updates(&self) -> Result<Vec<StreamEvent>, NexusError>;

    /// Terminate the spawned harness OS process (SIGKILL). Called by the `kill=true` path in
    /// `admin.remove`. The default implementation is a no-op — [`MockAdapter`] inherits it (tests
    /// that need to verify the call override via [`MockAdapter`]'s explicit impl). The real
    /// adapters override to call `self.engine.kill()`.
    async fn kill(&self) {}

    /// Install the REALTIME relay channel for the turn about to be injected: each renderable
    /// `session/update` is forwarded as it arrives, so the caller emits `agent.update` per chunk
    /// (true streaming). Returns `None` when the adapter has no live engine ([`MockAdapter`] →
    /// the caller falls back to draining [`Adapter::stream_updates`] at turn-end). The real ACP
    /// adapters override to return `Some` from their engine. Pair with [`Adapter::clear_live`].
    fn install_live(&self) -> Option<tokio::sync::mpsc::UnboundedReceiver<StreamEvent>> {
        None
    }

    /// Close the realtime relay channel at turn-end. Default no-op (no live engine).
    fn clear_live(&self) {}

    /// The harness's ACP session id (the `session/load` resume key). The daemon persists this so a
    /// not-fresh agent can be re-spawned and resumed. Default `None` (no live engine); the real ACP
    /// adapters override from their engine.
    async fn acp_session_id(&self) -> Option<String> {
        None
    }

    /// Exact OS process tuple for a live ACP harness child, when this adapter owns one.
    fn runtime_process_ids(&self) -> Option<RuntimeProcessIds> {
        None
    }

    /// Open a FRESH `session/new` on the ALREADY-connected engine (no re-spawn). Used as the
    /// fallback when `resume` fails after the engine is already connected — re-calling
    /// `open_session` there would error with "acp engine already connected". Default delegates to
    /// `open_session` (for the mock, which has no separate connect step).
    async fn new_session_only(&self) -> Result<(), NexusError> {
        self.open_session().await
    }
}
