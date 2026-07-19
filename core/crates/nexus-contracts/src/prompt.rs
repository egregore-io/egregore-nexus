//! Direct ACP prompt — the operator↔agent DM path for the web console.
//!
//! This is DELIBERATELY NOT the bus (`send`). The bus is for agent↔agent routing
//! (inbox, fan-out, membership). A web-console DM is a DIRECT conversation with
//! one agent's ACP session — `prompt` injects the operator's text straight into
//! that session (`session/prompt`), and the reply streams back over the WS as
//! `agent.update` (formatted to AG-UI by the gateway's `observe`). No router, no
//! "operator must be a bus member" — exactly the AionUi `sendMessage`/
//! `responseStream` model.

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::ids::AgentId;

/// Inject one operator message directly into an agent's ACP session by agent name.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PromptRequest {
    /// The stable target agent id. When present, the daemon revives this id before `name`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    /// The target agent's registered name fallback (resolved to its live ACP session).
    pub name: String,
    /// The operator's message text (injected as a `session/prompt` turn).
    pub text: String,
    /// Optional client-generated message id for reconciling the optimistic web echo
    /// with the streamed `user_input` event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_message_id: Option<String>,
}

/// Response: whether the turn was injected (the reply itself streams over the WS).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PromptResponse {
    pub delivered: bool,
}

/// Durable lifecycle of one session-composer command.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CommandQueueState {
    Queued,
    Claimed,
    Started,
    Completed,
    Failed,
    Cancelled,
}

/// One command-intent projection for the session queue UI.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommandQueueEntry {
    pub command_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub text: String,
    pub state: CommandQueueState,
    /// `queue` retains the normal turn boundary; `redirect` uses the adapter strategy.
    pub mode: String,
    #[typeshare(serialized_as = "number")]
    pub revision: i64,
    #[typeshare(serialized_as = "number")]
    pub seq: i64,
    #[typeshare(serialized_as = "number")]
    pub created_at: i64,
    #[typeshare(serialized_as = "Option<number>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claimed_at: Option<i64>,
    #[typeshare(serialized_as = "Option<number>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<i64>,
    #[typeshare(serialized_as = "Option<number>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Reconnect snapshot for one daemon-owned session lane.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommandQueueSnapshot {
    pub target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Derived exclusively from the durable normalized session-turn projection.
    pub turn_active: bool,
    pub steer_capability: SteerCapability,
    /// Monotonic global queue cursor used as the reconnect `afterSeq` boundary.
    #[typeshare(serialized_as = "number")]
    pub seq: i64,
    /// Queue revision. It advances with `seq`; named separately for compare-and-set UI state.
    #[typeshare(serialized_as = "number")]
    pub revision: i64,
    pub commands: Vec<CommandQueueEntry>,
}

/// Immediate durable receipt for a newly queued session command.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommandQueueReceipt {
    pub command_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub state: CommandQueueState,
    #[typeshare(serialized_as = "number")]
    pub revision: i64,
    #[typeshare(serialized_as = "number")]
    pub seq: i64,
}

/// Atomic pending-queue mutation selected by the UI.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CommandQueueAction {
    Cancel,
    Edit,
    Reorder,
    RedirectNow,
}

/// Compare-and-set revision for one command in a multi-row reorder.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommandExpectedRevision {
    pub command_id: String,
    #[typeshare(serialized_as = "number")]
    pub revision: i64,
}

/// Request for one server-owned queue mutation. Fields not used by the selected action are absent.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommandQueueMutationRequest {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    pub action: CommandQueueAction,
    pub client_mutation_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_id: Option<String>,
    #[typeshare(serialized_as = "Option<number>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default)]
    pub command_ids: Vec<String>,
    #[serde(default)]
    pub expected_revisions: Vec<CommandExpectedRevision>,
}

/// Result of an atomic queue mutation.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommandQueueMutationResponse {
    pub client_mutation_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub state: CommandQueueState,
    pub steer_capability: SteerCapability,
    #[typeshare(serialized_as = "Option<number>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<i64>,
    #[typeshare(serialized_as = "number")]
    pub seq: i64,
}

/// One monotonic pushed transition from the durable command-event projection.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommandQueueTransition {
    #[typeshare(serialized_as = "number")]
    pub seq: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub command_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_message_id: Option<String>,
    pub state: CommandQueueState,
    pub mode: String,
    #[typeshare(serialized_as = "number")]
    pub revision: i64,
}

/// Redirect one durable operator message into the active turn. This is deliberately separate from
/// [`PromptRequest`]: normal prompts retain the daemon's
/// per-session `harness.prompt` boundary queue. Redirects use the target adapter's declared
/// capability: native steering when available, otherwise an adapter-owned interrupt-and-send.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SteerRequest {
    /// The stable target agent id. When present, the daemon revives this id before `name`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    /// The target agent's registered name fallback.
    pub name: String,
    /// Additional operator input for the active turn.
    pub text: String,
    /// Client-generated id used for optimistic-row reconciliation and command idempotency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_message_id: Option<String>,
}

/// Adapter strategy used for an explicit redirect request.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SteerCapability {
    /// The adapter can append input to the active turn without interrupting it.
    NativeSteer,
    /// The adapter atomically interrupts the active turn and starts the durable message next.
    InterruptAndSend,
    /// The adapter exposes neither safe strategy.
    None,
}

/// Delivery decision for an explicit redirect request.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SteerDelivery {
    /// A native-steer adapter accepted the input into the active regular turn.
    Steered,
    /// Legacy decode-only value from releases that converted a stale steer into `turn/start`.
    ///
    /// Nexus no longer emits this value: the frontend owns the turn/steer choice, so a missing
    /// active turn is rejected with [`crate::codes::ACTIVE_TURN_REQUIRED`].
    #[deprecated(note = "stale steer is rejected; use an explicit prompt to start a turn")]
    FallbackStarted,
    /// A non-native adapter interrupted the prior turn and accepted this message as the next turn.
    InterruptedAndStarted,
}

/// Response returned only after the selected adapter accepts the redirect operation.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SteerResponse {
    pub accepted: bool,
    pub delivery: SteerDelivery,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
}

/// Pre-warm an agent's ACP session WITHOUT sending a prompt — the AionUi "spawn at
/// conversation-open" move. Called when the web console opens a DM pane (the `observe`
/// subscribe), so the harness subprocess is spawned + the session opened/resumed AHEAD of the
/// operator's first message. Without this the first DM pays the full cold-start
/// (`npx` + ACP `initialize` + `session/new`) synchronously — the "huge send latency".
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WarmRequest {
    /// The stable target agent id. When present, the daemon revives this id before `name`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    /// The target agent's registered name fallback (resolved + brought live).
    pub name: String,
}

/// Response: whether the session is now live (spawned/resumed and ready for `prompt`).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WarmResponse {
    pub warm: bool,
}

/// Trigger NATIVE context compaction on an agent's session by agent name.
/// codex app-server sessions run `thread/compact/start`; headed PTY sessions get
/// the typed `/compact`; transports without a compaction verb error loudly.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CompactRequest {
    /// The stable target agent id. When present, the daemon revives this id before `name`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    /// The target agent's registered name fallback (resolved to its live session).
    pub name: String,
}

/// Response: whether compaction was started (completion streams via observe).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CompactResponse {
    pub started: bool,
}
