//! The cross-crate trait seam. Same-layer crates never depend on each
//! other's concrete structs — they call through these `Arc<dyn …Port>` traits, wired by the
//! `nexus` binary's `AppState`. All methods are async and return `Result<_, ContractError>`.
//! Store-backed command intents persist this same error shape for CLI/MCP/gateway callers.

use async_trait::async_trait;

use crate::ack::{AckRequest, AckResponse, AckThreadsRequest};
use crate::admin::{
    AdminAssignRequest, AdminAssignResponse, AdminRenameRequest, AdminRenameResponse,
    AssignProjectRequest, AssignProjectResponse, AssignRoleRequest, AssignRoleResponse,
    ChannelRequest, MonitorRequest, RemoveRequest, RemoveResponse, RouteForwardRequest,
    SpawnRequest, SpawnResponse,
};
use crate::batch::{ConsumeRequest, NexusBatch};
use crate::enums::Kind;
use crate::events::WsEvent;
use crate::harness::HarnessId;
pub use crate::hook_ports::MessageHookPort;
use crate::ids::{AgentId, MessageId, SessionId};
use crate::notify::{NotifyRequest, NotifyResponse, NotifySendRequest};
use crate::prompt::SteerResponse;
use crate::register::{
    HeartbeatResponse, MemberListRequest, MemberListResponse, RegisterRequest, RegisterResponse,
    RenameRequest, RenameResponse, StatusRequest, StatusResponse, Whoami,
};
use crate::search::{HistoryRequest, HistoryResponse, SearchRequest, SearchResponse};
use crate::send::{Ack, SendRequest};
use crate::threads::{
    ArchiveThreadRequest, CreateThreadRequest, DeleteThreadRequest, JoinThreadRequest,
    LeaveThreadRequest, RenameThreadRequest, ThreadListResponse, ThreadMemberRequest,
    ThreadMembersRequest, ThreadMembersResponse,
};
use crate::topics::{SubscribeRequest, SubscribeResponse, TopicListResponse, UnsubscribeRequest};

/// Wire-mappable error returned by every port. `code` is a [`crate::codes`] value; store-backed
/// command results persist it for callers that render human or `--json` errors. **Not**
/// `#[typeshare]` — it stays out of the TS mirror.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ContractError {
    pub code: i32,
    pub message: String,
}

impl std::fmt::Display for ContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}
impl std::error::Error for ContractError {}

/// Convenience result alias for port methods.
pub type PortResult<T> = Result<T, ContractError>;

/// A structured provider boundary reported by a harness adapter before it collapses into a generic
/// port error. The realtime loop persists this as a terminal delivery error; retry is explicit.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderLimit {
    pub harness: HarnessId,
    pub session: SessionId,
    pub reason: ProviderLimitReason,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reset_hint: Option<ResetHint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub source: String,
}

/// Structured provider failure that is neither a usage limit nor an operator-action stop.
/// `retryable` is advisory evidence for an explicit caller/operator retry; the daemon never
/// automatically replays a delivery after crossing the external harness boundary.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderError {
    pub harness: HarnessId,
    pub session: SessionId,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub retryable: bool,
    pub source: String,
}

/// Structured provider-limit classes. Adapter lanes may add variants as their native structured
/// payloads demand; Nexus must never infer these from visible assistant text.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderLimitReason {
    RateLimit,
    UsageLimit,
    QuotaExhausted,
    Overloaded,
}

/// Absolute retry timestamp in Unix milliseconds.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetHint {
    pub unix_ms: i64,
}

/// Structured operator-action stop reported by a harness adapter before generic error collapse.
/// The delivery becomes terminal until an operator explicitly requeues it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorAction {
    pub harness: HarnessId,
    pub session: SessionId,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub source: String,
}

/// Result shape for the observed injection path. Every variant is terminal for the current
/// delivery attempt; none authorizes an automatic retry.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum InjectError {
    ProviderLimit(ProviderLimit),
    ProviderError(ProviderError),
    OperatorAction(OperatorAction),
    CompletionTimeout { session: SessionId, source: String },
    Contract(ContractError),
}

impl From<ContractError> for InjectError {
    fn from(error: ContractError) -> Self {
        InjectError::Contract(error)
    }
}

impl From<InjectError> for ContractError {
    fn from(error: InjectError) -> Self {
        match error {
            InjectError::Contract(error) => error,
            InjectError::ProviderLimit(limit) => ContractError {
                code: crate::codes::INTERNAL_ERROR,
                message: format!(
                    "{:?} provider limit for {} ({:?})",
                    limit.harness, limit.session, limit.reason
                ),
            },
            InjectError::ProviderError(error) => ContractError {
                code: crate::codes::INTERNAL_ERROR,
                message: format!(
                    "{:?} provider error for {} ({})",
                    error.harness, error.session, error.reason
                ),
            },
            InjectError::OperatorAction(action) => ContractError {
                code: crate::codes::INTERNAL_ERROR,
                message: format!(
                    "{:?} operator action required for {} ({})",
                    action.harness, action.session, action.reason
                ),
            },
            InjectError::CompletionTimeout { session, source } => ContractError {
                code: crate::codes::INTERNAL_ERROR,
                message: format!("turn completion timed out for {session} ({source})"),
            },
        }
    }
}

impl std::fmt::Display for InjectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InjectError::Contract(error) => write!(f, "{error}"),
            InjectError::ProviderLimit(limit) => write!(
                f,
                "{:?} provider limit for {} ({:?})",
                limit.harness, limit.session, limit.reason
            ),
            InjectError::ProviderError(error) => write!(
                f,
                "{:?} provider error for {} ({})",
                error.harness, error.session, error.reason
            ),
            InjectError::OperatorAction(action) => write!(
                f,
                "{:?} operator action required for {} ({})",
                action.harness, action.session, action.reason
            ),
            InjectError::CompletionTimeout { session, source } => {
                write!(f, "turn completion timed out for {session} ({source})")
            }
        }
    }
}

impl std::error::Error for InjectError {}

pub type InjectResult<T> = Result<T, InjectError>;

/// The caller's resolved identity, threaded into every authenticated port call so the
/// implementation never trusts a client-supplied `from`/session (backend §3, §5.1).
///
/// `agent_id` is the durable identity. `session` is the active runtime. During migration,
/// `agent_id` remains optional so old session-only rows can still be resolved.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Caller {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    pub session: SessionId,
    pub name: String,
    pub project: String,
    pub tier: crate::enums::Tier,
    #[serde(default)]
    pub locality: crate::enums::Locality,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_id: Option<String>,
}

/// A message whose target, policy, and `before_send` hook have been accepted but whose durable
/// Message Post has not committed yet.
///
/// A capable bus returns an opaque `preparation_id` that binds the evaluated message to its
/// already-resolved recipient set. The trait defaults fail closed instead of pretending an
/// ordinary preflight/send pair has these semantics. Callers must either commit or discard every
/// preparation they receive.
#[derive(Debug)]
pub struct PreparedBusSend {
    preparation_id: String,
}

impl PreparedBusSend {
    /// Construct an opaque concrete-bus preparation. This is public only because the contracts
    /// and bus implementations live in separate crates; commit still authenticates and consumes
    /// the unguessable token from the concrete bus's private in-memory ledger.
    #[doc(hidden)]
    pub fn resolved(preparation_id: String) -> Self {
        Self { preparation_id }
    }

    /// Opaque implementation-owned token.
    #[doc(hidden)]
    pub fn preparation_id(&self) -> &str {
        &self.preparation_id
    }
}

/// Register-once + presence + tiers (backend §8). Implemented by `nexus-identity`.
#[async_trait]
pub trait IdentityPort: Send + Sync {
    /// Idempotent on `clientKey`: known key resumes the session, else creates + binds the name.
    async fn register(&self, req: RegisterRequest) -> PortResult<RegisterResponse>;
    /// Resolve the caller's bound identity (`whoami`).
    async fn whoami(&self, caller: &Caller) -> PortResult<Whoami>;
    /// Resolve a stable agent id or globally unique name -> its live `Caller` (used by daemon
    /// ingress paths that receive ambient identity instead of a pre-resolved caller). `project` is
    /// compatibility metadata and never narrows canonical target resolution.
    async fn resolve(&self, project: &str, name_or_id: &str) -> PortResult<Caller>;
    /// Directory / presence.
    async fn members(
        &self,
        caller: &Caller,
        req: MemberListRequest,
    ) -> PortResult<MemberListResponse>;
    /// Set self-status / pause / work (backend §2.4 self-pause).
    async fn status(&self, caller: &Caller, req: StatusRequest) -> PortResult<StatusResponse>;
    /// Keepalive; refreshes presence.
    async fn heartbeat(&self, caller: &Caller) -> PortResult<HeartbeatResponse>;
    /// Move a session to a different project. Reassigning to the current project is a no-op.
    /// Implementations may still reject legacy/store states where another row already holds the
    /// name in `to_project`.
    async fn assign_project(
        &self,
        name: &str,
        to_project: &str,
    ) -> PortResult<AssignProjectResponse>;

    /// Set an operator-facing role label for a durable identity and its compatibility session row.
    ///
    /// The role is display metadata only: it never authorizes, routes, prioritizes, or wakes work.
    /// Admin commands use this seam so the durable `agents` read model and legacy member/session
    /// views stay in sync.
    async fn assign_role(&self, name: &str, role: &str) -> PortResult<AssignRoleResponse> {
        Ok(AssignRoleResponse {
            name: Some(name.to_string()),
            role: role.to_string(),
        })
    }

    /// Self-service rename: the caller changes its OWN display name (an agent renames himself).
    /// The new name must be free in the caller's project. Default errors so test doubles needn't
    /// implement it; the real identity service overrides.
    async fn rename(&self, _caller: &Caller, _req: RenameRequest) -> PortResult<RenameResponse> {
        Err(ContractError {
            code: -32601,
            message: "rename not supported by this port".into(),
        })
    }

    /// Admin rename/first-name: an admin may bind any agent identity by name, stable `a_*` id, or
    /// exact `s_*` session id. This is distinct from self-service [`IdentityPort::rename`] because
    /// staged identities may have no previous public name.
    async fn admin_rename(
        &self,
        _caller: &Caller,
        _req: AdminRenameRequest,
    ) -> PortResult<AdminRenameResponse> {
        Err(ContractError {
            code: -32601,
            message: "admin rename not supported by this port".into(),
        })
    }

    /// Admin assign: a **staged (unnamed)** identity assumes a name whose previous owner died.
    /// Free names are plain first-naming; a dead/not-live holder is evicted to unnamed; a live
    /// holder is never takeable. See [`AdminAssignRequest`] for the full contract.
    async fn admin_assign(
        &self,
        _caller: &Caller,
        _req: AdminAssignRequest,
    ) -> PortResult<AdminAssignResponse> {
        Err(ContractError {
            code: -32601,
            message: "admin assign not supported by this port".into(),
        })
    }

    /// Mark a session offline in the store immediately (used by `admin.remove` so the read-view
    /// roster reflects an evicted/killed agent at once, instead of waiting out the heartbeat TTL).
    /// Default is a no-op so test doubles need not implement it; the real identity service overrides.
    async fn set_offline(&self, _session: &SessionId) -> PortResult<()> {
        Ok(())
    }

    /// Persist the harness's real ACP session id (the `session/load` resume key) onto the session
    /// row, so a not-fresh agent can be re-spawned and resumed after a daemon/process restart.
    /// Default no-op; the real identity service writes it to `harness_session_id`.
    async fn set_resume_key(&self, _session: &SessionId, _resume_key: &str) -> PortResult<()> {
        Ok(())
    }
}

/// Inbox + bell + per-agent loop (backend §2). Implemented by `nexus-dispatch`.
#[async_trait]
pub trait DispatchPort: Send + Sync {
    /// Write an in_flight row for one recipient and ring its bell (called by the bus on send).
    async fn enqueue(&self, recipient: &SessionId, message: &MessageId) -> PortResult<()>;
    /// Drain-once: return the recipient's pending queue as one `NexusBatch` (held-receive window).
    async fn consume(&self, caller: &Caller, req: ConsumeRequest) -> PortResult<NexusBatch>;
    /// Per-message ack (DMs).
    async fn ack(&self, caller: &Caller, req: AckRequest) -> PortResult<AckResponse>;
    /// Bulk ack (threads).
    async fn ack_threads(&self, caller: &Caller, req: AckThreadsRequest)
        -> PortResult<AckResponse>;
}

/// Compatibility name for the pre-v0.1 internal delivery port.
///
/// New code should use [`DispatchPort`]. The alias keeps source compatibility for embedders while
/// the first public release adopts dispatch terminology; it does not create a second transport.
pub use DispatchPort as RealtimePort;

/// The one `to` contract: DM | thread | topic, fan-out, no orchestrator (backend §3).
#[async_trait]
pub trait BusPort: Send + Sync {
    /// Resolve and validate one send target without committing a message, delivery row, or wake.
    /// Multi-effect producers use this to fail atomically before their first durable side effect.
    async fn preflight_send(&self, caller: &Caller, req: &SendRequest) -> PortResult<()>;
    /// Resolve, authorize, and run `before_send` without committing any message/delivery/wake.
    /// The concrete bus freezes the resolved recipients and evaluated hook result in an opaque
    /// preparation so a later commit cannot re-resolve a renamed alias or execute the hook twice.
    async fn prepare_send(
        &self,
        _caller: &Caller,
        _req: SendRequest,
        _sender_kind: Option<Kind>,
    ) -> PortResult<PreparedBusSend> {
        Err(ContractError {
            code: crate::codes::METHOD_NOT_FOUND,
            message: "atomic prepared sends are not supported by this bus".into(),
        })
    }
    /// Resolve, authorize, and run `before_send` for a one-shot notification before daemon
    /// lifecycle state is touched.
    async fn prepare_notify(
        &self,
        _caller: &Caller,
        _req: NotifySendRequest,
    ) -> PortResult<PreparedBusSend> {
        Err(ContractError {
            code: crate::codes::METHOD_NOT_FOUND,
            message: "atomic prepared notifications are not supported by this bus".into(),
        })
    }
    /// Return the lifecycle hold target authenticated by the concrete preparation ledger.
    /// Callers must never trust target data carried by or reconstructed from an opaque handle.
    async fn prepared_hold_agent(
        &self,
        _caller: &Caller,
        _prepared: &PreparedBusSend,
    ) -> PortResult<Option<AgentId>> {
        Err(ContractError {
            code: crate::codes::METHOD_NOT_FOUND,
            message: "atomic prepared sends are not supported by this bus".into(),
        })
    }
    /// Commit exactly one preparation. Implementations must consume the opaque token without
    /// resolving aliases or executing hooks again.
    async fn commit_prepared(
        &self,
        _caller: &Caller,
        _prepared: PreparedBusSend,
    ) -> PortResult<Ack> {
        Err(ContractError {
            code: crate::codes::METHOD_NOT_FOUND,
            message: "atomic prepared sends are not supported by this bus".into(),
        })
    }
    /// Abandon an accepted preparation without committing any durable effect.
    async fn discard_prepared(
        &self,
        _caller: &Caller,
        _prepared: PreparedBusSend,
    ) -> PortResult<()> {
        Err(ContractError {
            code: crate::codes::METHOD_NOT_FOUND,
            message: "atomic prepared sends are not supported by this bus".into(),
        })
    }
    /// Resolve `to` and deliver (write message + in_flight rows + ring bells). Returns the `Ack`.
    async fn send(&self, caller: &Caller, req: SendRequest) -> PortResult<Ack>;
    /// Trusted internal-origin variant used by notification transports whose synthetic caller has
    /// no durable session row. Ordinary client sends must use [`Self::send`]; the default keeps
    /// test doubles and non-overriding ports source-compatible.
    async fn send_with_kind(
        &self,
        caller: &Caller,
        req: SendRequest,
        _kind: Kind,
    ) -> PortResult<Ack> {
        self.send(caller, req).await
    }
    /// Send one explicitly targeted notification through the canonical message/delivery spine.
    async fn notify(&self, _caller: &Caller, _req: NotifySendRequest) -> PortResult<Ack> {
        Err(ContractError {
            code: crate::codes::METHOD_NOT_FOUND,
            message: "one-shot notify is not supported by this bus".into(),
        })
    }
    /// Resolve a one-shot notification before daemon lifecycle state is touched. A single-agent
    /// target returns its immutable id so the caller can hold that exact runtime; fan-out targets
    /// return `None` after successful validation.
    async fn preflight_notify_target(
        &self,
        caller: &Caller,
        target: &crate::notify::NotifyTarget,
    ) -> PortResult<Option<crate::ids::AgentId>>;
    async fn create_thread(&self, caller: &Caller, req: CreateThreadRequest) -> PortResult<()>;
    async fn join_thread(&self, caller: &Caller, req: JoinThreadRequest) -> PortResult<()>;
    async fn leave_thread(&self, caller: &Caller, req: LeaveThreadRequest) -> PortResult<()>;
    /// Archive a thread so it no longer appears as an active routing/read/search target.
    async fn archive_thread(&self, _caller: &Caller, _req: ArchiveThreadRequest) -> PortResult<()> {
        Err(ContractError {
            code: crate::codes::METHOD_NOT_FOUND,
            message: "thread.archive not implemented".into(),
        })
    }
    /// Delete a thread registry and memberships. Durable messages are retained but unreachable
    /// through scoped thread reads after the registry is gone.
    async fn delete_thread(&self, _caller: &Caller, _req: DeleteThreadRequest) -> PortResult<()> {
        Err(ContractError {
            code: crate::codes::METHOD_NOT_FOUND,
            message: "thread.delete not implemented".into(),
        })
    }
    /// Rename a thread registry row while preserving durable messages and memberships.
    async fn rename_thread(&self, _caller: &Caller, _req: RenameThreadRequest) -> PortResult<()> {
        Err(ContractError {
            code: crate::codes::METHOD_NOT_FOUND,
            message: "thread.rename not implemented".into(),
        })
    }
    /// Add a SPECIFIC member to an existing thread (operator manages the roster).
    async fn add_thread_member(&self, caller: &Caller, req: ThreadMemberRequest) -> PortResult<()>;
    /// Remove a SPECIFIC member from an existing thread.
    async fn remove_thread_member(
        &self,
        caller: &Caller,
        req: ThreadMemberRequest,
    ) -> PortResult<()>;
    async fn threads(&self, caller: &Caller) -> PortResult<ThreadListResponse>;
    async fn thread_members(
        &self,
        caller: &Caller,
        req: ThreadMembersRequest,
    ) -> PortResult<ThreadMembersResponse>;
    async fn subscribe(
        &self,
        caller: &Caller,
        req: SubscribeRequest,
    ) -> PortResult<SubscribeResponse>;
    async fn unsubscribe(&self, caller: &Caller, req: UnsubscribeRequest) -> PortResult<()>;
    async fn topics(&self, caller: &Caller) -> PortResult<TopicListResponse>;
}

/// Drives ACP `session/prompt` and relays `session/update` (backend §2.3). Implemented by `nexus-agent`.
#[async_trait]
pub trait AgentTurnExecutionPort: Send + Sync {
    /// Inject one drained batch as a single turn (renders `<nexus-batch>` from `NexusBatch`).
    async fn inject_turn(&self, recipient: &SessionId, batch: &NexusBatch) -> PortResult<()>;

    /// Inject one drained batch and emit the already-built session-visible input event at the
    /// transport's accepted boundary.
    ///
    /// Implementations that stream assistant output or turn-end internally should override this so
    /// `accepted_event` is emitted after the harness accepts the input but before assistant output
    /// and `turn_end`. The default preserves old test-double behavior: inject first, then emit only
    /// if injection succeeded.
    async fn inject_turn_observed(
        &self,
        recipient: &SessionId,
        batch: &NexusBatch,
        events: std::sync::Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> InjectResult<()> {
        self.inject_turn(recipient, batch).await?;
        events.emit(accepted_event).await;
        Ok(())
    }

    /// Spawn/attach a harness and stand up its ACP session.
    async fn launch(&self, req: SpawnRequest) -> PortResult<SpawnResponse>;
    /// Tear down a session's loop (store retained).
    async fn remove(&self, req: RemoveRequest) -> PortResult<RemoveResponse>;
    /// DIRECT operator→agent prompt: inject `text` straight into `recipient`'s ACP
    /// session (`session/prompt`), BYPASSING the bus — the web-console DM path.
    /// The reply streams back as `agent.update` over the WS (no bus membership
    /// required). Default no-op so test doubles need not implement it; the real
    /// agent service overrides. See [`crate::prompt`].
    async fn prompt(&self, _recipient: &SessionId, _text: String) -> PortResult<()> {
        Ok(())
    }

    /// DIRECT operator→agent prompt with an externally supplied session-visible accepted event.
    ///
    /// This is the accepted-boundary variant used by launch boot prompts and other callers that
    /// need a stable user-input row before assistant output. Production implementations must emit
    /// `accepted_event` only after the harness accepts the prompt and before assistant output or
    /// `turn_end`. The default preserves test-double behavior.
    async fn prompt_observed(
        &self,
        recipient: &SessionId,
        text: String,
        events: std::sync::Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> PortResult<()> {
        self.prompt(recipient, text).await?;
        events.emit(accepted_event).await;
        Ok(())
    }

    /// Redirect input at the active-turn boundary, emitting `accepted_event` only after the
    /// adapter accepts it. Native-steer adapters append to the active turn; interrupt-and-send
    /// adapters cancel that turn and start this same durable message next. The operation stays
    /// server-side so a client never composes a lossy cancel-plus-send sequence.
    async fn steer_observed(
        &self,
        _recipient: &SessionId,
        _text: String,
        _events: std::sync::Arc<dyn EventSink>,
        _accepted_event: WsEvent,
    ) -> PortResult<SteerResponse> {
        self.interrupt_active_turn(_recipient).await?;
        self.prompt_observed(_recipient, _text, _events, _accepted_event)
            .await?;
        Ok(SteerResponse {
            session_id: None,
            accepted: true,
            delivery: crate::SteerDelivery::InterruptedAndStarted,
            turn_id: None,
        })
    }

    /// Capability advertised by the adapter underneath this session.
    fn steer_capability(&self, _recipient: &SessionId) -> crate::SteerCapability {
        crate::SteerCapability::None
    }

    /// Read native evidence without I/O or changing execution authority. Legacy implementations
    /// are unknown, not verified idle, and do not invent a binding/revision stamp.
    fn observe_turn(&self, recipient: &SessionId) -> crate::TurnObservation {
        crate::TurnObservation {
            steer_capability: self.steer_capability(recipient),
            ..Default::default()
        }
    }

    /// Interrupt the adapter's active turn. Implementations must address the adapter's own turn
    /// authority (ACP `session/cancel`, terminal interrupt, etc.), never presence or UI state.
    async fn interrupt_active_turn(&self, _recipient: &SessionId) -> PortResult<()> {
        Err(ContractError {
            code: crate::codes::METHOD_NOT_FOUND,
            message: "this harness does not support redirecting an active turn".into(),
        })
    }

    /// Trigger native CONTEXT COMPACTION on `recipient`'s session. Transport-specific:
    /// codex app-server → `thread/compact/start`; headed PTY harnesses → the typed
    /// `/compact` command. Default: unsupported — transports that cannot compact
    /// must say so loudly instead of pretending (no silent plain-text injection).
    async fn compact(&self, _recipient: &SessionId) -> PortResult<()> {
        Err(ContractError {
            code: -32601,
            message: "compact is not supported on this transport".into(),
        })
    }

    /// Is `recipient`'s harness alive? `Some(true)`/`Some(false)` when the executor owns a harness it
    /// can probe (the PTY/tmux path: `tmux has-session`); `None` when liveness isn't knowable here
    /// (the ACP path / test doubles → presence falls back to the heartbeat). Used for truthful
    /// presence (a dead harness must read offline) and to decide when to respawn before delivering.
    fn is_harness_alive(&self, _recipient: &SessionId) -> Option<bool> {
        None
    }

    /// Sessions this transport wants the daemon prompt scheduler to treat as busy.
    ///
    /// Store-backed prompt command rows may complete as soon as the harness accepts the turn. A
    /// transport reports active sessions here when Nexus must preserve prompt boundary ordering
    /// after command-row completion. Explicit native steering uses a separate operation and never
    /// weakens this queue contract.
    fn active_turn_sessions(&self) -> Vec<SessionId> {
        Vec::new()
    }

    /// Await the adapter's authoritative final-turn boundary for an already active session.
    /// Implementations must use protocol completion, never rendered text or inferred tool counts.
    async fn wait_for_turn_completion(&self, _recipient: &SessionId) -> PortResult<()> {
        Err(ContractError {
            code: crate::codes::METHOD_NOT_FOUND,
            message: "this transport cannot observe an active turn completion boundary".into(),
        })
    }
}

/// Internal notification ingest + Pub feed + routing rules (backend §7). Implemented by `nexus-notify`.
///
/// **v4:** there is no HTTP `/notify` webhook in the daemon. External producers hit the TS gateway's
/// public API; gateway ingress verifies the signature before enqueueing the notification command.
/// `hmac_ok` reflects that verification result.
#[async_trait]
pub trait NotifyPort: Send + Sync {
    /// Legacy/internal ingest seam. A false verification result is audited and dropped; verified
    /// public command ingress uses [`NotifyPort::ingest_verified`] instead.
    async fn ingest(&self, req: NotifyRequest, hmac_ok: bool) -> PortResult<NotifyResponse>;
    /// Ingest a public notification whose durable command envelope was independently verified.
    /// `idempotency_root` is domain-separated by the implementation for every bus side effect so
    /// reclaim cannot create a second routed message or recipient delivery.
    async fn ingest_verified(
        &self,
        req: NotifyRequest,
        _idempotency_root: String,
    ) -> PortResult<NotifyResponse> {
        self.ingest(req, true).await
    }
    /// Ingest a verified notification and explicitly route it to named recipients through the same
    /// notification dispatch spine used by source/topic routing. Default implementations preserve
    /// existing test doubles; the real notify service records the recipients in the audit row.
    async fn ingest_for(
        &self,
        req: NotifyRequest,
        hmac_ok: bool,
        _project: String,
        _recipients: Vec<String>,
    ) -> PortResult<NotifyResponse> {
        self.ingest(req, hmac_ok).await
    }
    async fn forward(&self, caller: &Caller, req: RouteForwardRequest) -> PortResult<()>;
    async fn channel(&self, caller: &Caller, req: ChannelRequest) -> PortResult<()>;
}

/// Scoped FTS + vector search (backend §9). Implemented by `nexus-search`.
#[async_trait]
pub trait SearchPort: Send + Sync {
    async fn search(&self, caller: &Caller, req: SearchRequest) -> PortResult<SearchResponse>;
    async fn history(&self, caller: &Caller, req: HistoryRequest) -> PortResult<HistoryResponse>;
}

/// Admin-tier commands — gated by tier, never the message path (backend §8). Impl by `nexus-admin`.
#[async_trait]
pub trait AdminPort: Send + Sync {
    async fn spawn(&self, caller: &Caller, req: SpawnRequest) -> PortResult<SpawnResponse>;
    async fn remove(&self, caller: &Caller, req: RemoveRequest) -> PortResult<RemoveResponse>;
    async fn assign_role(
        &self,
        caller: &Caller,
        req: AssignRoleRequest,
    ) -> PortResult<AssignRoleResponse>;
    async fn assign_project(
        &self,
        caller: &Caller,
        req: AssignProjectRequest,
    ) -> PortResult<AssignProjectResponse>;
    async fn channel(&self, caller: &Caller, req: ChannelRequest) -> PortResult<()>;
    async fn route(&self, caller: &Caller, req: RouteForwardRequest) -> PortResult<()>;
    async fn monitor(&self, caller: &Caller, req: MonitorRequest) -> PortResult<()>;
}

/// A sink for `WsEvent`s. The daemon implements this to persist session activity, update
/// materialized projections, and notify local observers; crates emit through `Arc<dyn EventSink>`.
/// CLI and MCP commands do not consume `WsEvent`s directly.
#[async_trait]
pub trait EventSink: Send + Sync {
    async fn emit(&self, event: WsEvent);

    /// Publish one canonical product fact that has already committed in the daemon store.
    ///
    /// The default is intentionally a no-op so embedders that do not run a Gateway keep their
    /// existing event-sink contract. The production daemon overrides it with the bounded
    /// same-epoch Gateway projection publisher.
    async fn project(&self, _effect: crate::GatewayProjectionEffect) {}

    /// Refresh canonical Gateway identity/runtime snapshots after a successful exact binding.
    /// This is not a new spawn or a compatibility presence transition. The production sink
    /// re-reads and validates both identities; embedders without projections remain unchanged.
    async fn project_runtime_binding(&self, _session: &SessionId, _agent: &AgentId) {}
}
