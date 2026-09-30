//! Nexus event taxonomy (backend §10): bus observations and agent-session activity. Events are
//! internally tagged by `type` with dotted spec names and can be projected to AG-UI or delivered
//! to in-process observers. Canonical daemon-to-Gateway persistence uses the separate, versioned
//! [`crate::GatewayProjectionEvent`] contract; `agent.update` and terminal bytes never enter it.

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::enums::Presence;
use crate::ids::{MessageId, SessionId};

/// Developer-visible event envelope for the reserved `sys.*` event topics.
/// Durable rows are deliberately metadata-only: they carry enough identity and sequence data for
/// developer tooling, unread counters, and live dashboards, but never include message bodies or
/// turn payloads. Ephemeral tool-call envelopes use the same contract for socket-local,
/// ring-buffered observations and are still observational only; consumers must not feed these
/// events into the agent turn injector.
#[typeshare]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeveloperEventEnvelope {
    /// Event class. Durable values include message, agent lifecycle, and command action events;
    /// `tool_call` is an ephemeral socket-local developer event.
    pub kind: DeveloperEventKind,
    /// Reserved topic this envelope was written to, for example `sys.message.thread.nexus-project`.
    pub topic: String,
    /// Per-topic monotonic sequence number.
    #[typeshare(serialized_as = "number")]
    pub seq: i64,
    /// Epoch milliseconds when the source event happened.
    #[typeshare(serialized_as = "number")]
    pub ts: i64,
    /// Thread name for thread message events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread: Option<String>,
    /// Counterparty/display DM name for DM message events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dm: Option<String>,
    /// Display sender for message events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// Durable message id for message events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<MessageId>,
    /// Display agent name for lifecycle events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    /// Runtime/session id for lifecycle events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    /// Lifecycle phase, for example `started`, `stopped`, `turn_end`, `offline`, or `current_work`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<String>,
    /// Current-work value when the lifecycle event is about work-state changes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_work: Option<String>,
    /// Lifecycle-specific metadata. For example, rename events carry old/new display names.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    /// Tool display name for ephemeral tool-call events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    /// Tool-call phase for ephemeral tool-call events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<DeveloperToolCallPhase>,
    /// Whether the post phase finished successfully. `pre` events normally set this to `true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ok: Option<bool>,
}

/// Developer event classes for [`DeveloperEventEnvelope`].
#[typeshare]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeveloperEventKind {
    /// A bus message became visible to the target.
    Message,
    /// A lifecycle/work-state change became visible on the system lifecycle topic.
    AgentLifecycle,
    /// A state-changing command action was recorded as metadata-only telemetry.
    Action,
    /// An observed tool call started or completed on a session-scoped ephemeral topic.
    ToolCall,
}

/// Tool-call phases for ephemeral `sys.agent.<name>.tool_call` developer events.
#[typeshare]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeveloperToolCallPhase {
    /// The tool call was observed before execution or when it first became visible.
    Pre,
    /// The tool call produced a result or terminal status.
    Post,
}

/// What kind of ACP activity an `agent.update` carries — 1:1 with the renderable ACP
/// `session/update` variants the daemon forwards (the full-stream pass-through). The UI interprets
/// the loosely-typed `data` payload per `kind`.
///
/// snake_case on the wire: `text` / `thinking` / `tool_call` / `plan` / `commands`.
#[typeshare]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentUpdateKind {
    /// A streamed chunk of the agent's reply text (`AgentMessageChunk`). `data`: `{"text": …}`.
    Text,
    /// A streamed chunk of the agent's internal reasoning (`AgentThoughtChunk`). `data`: `{"text": …}`.
    Thinking,
    /// A tool call started or updated (`ToolCall` / `ToolCallUpdate`). `data`: a [`ToolCallData`]
    /// value (C-TOOL v1, docs/tool-call-contract.md); the UI merges successive updates by `id`.
    ToolCall,
    /// The agent's execution plan (`Plan`). `data`: `{"entries": […]}`.
    Plan,
    /// The harness's own advertised slash commands (`AvailableCommandsUpdate`). `data`:
    /// `{"commands": [{name, description}, …]}`.
    Commands,
    /// A user-role turn the harness received, echoed back by ACP as `UserMessageChunk`. `data`:
    /// `{"text": …}`. This is the operator's own typed input (or a daemon-injected `<nexus-batch>`
    /// turn) — surfacing it makes the web `/agent/<name>:<session_id>` view and the `nexus attach`
    /// TUI mirror each other: what you type on one shows on both, because it rides the SAME ACP
    /// session stream. Faithful to "what the harness saw."
    UserInput,
    /// Turn complete — the harness emitted its terminal boundary (for example ACP
    /// `session/prompt` returned `StopReason`, Codex emitted `turn/completed`, or OpenCode reported
    /// `session.idle`). Direct/legacy ACP display paths may infer this UI lifecycle marker from
    /// stream quiescence, but durable bus settlement never treats silence as completion evidence.
    /// Carries no content (`data`: `{}`); the gateway maps it to AG-UI `RUN_FINISHED` so the web
    /// console can close the streaming row.
    TurnEnd,
}

/// Canonical `agent.update` payload for `kind: "tool_call"` — C-TOOL v1
/// (docs/tool-call-contract.md). PRODUCER LAW: every harness adapter fills `tool` with the machine
/// tool name and `input` with the structured args object whenever the harness exposes them.
/// `title` is display-only and may be a command line; consumers must never treat it as identity.
#[typeshare]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolCallData {
    /// Merge key across the call's phases (START/ARGS/RESULT).
    pub id: String,
    /// Machine tool name: `shell`, `read`, `write`, `edit`, `search`, `fetch`, or the harness's
    /// registered tool name (`Bash`, `Write`, …). NEVER a human title or command line.
    pub tool: String,
    /// Human display label (an ACP title; may be a command line).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// ACP tool kind (`read`/`edit`/`execute`/`search`/…) when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// Structured arguments — a JSON object/array, NEVER a pre-stringified JSON string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locations: Option<serde_json::Value>,
}

impl ToolCallData {
    /// A minimal call with only the required identity fields set.
    pub fn start(id: impl Into<String>, tool: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            tool: tool.into(),
            title: None,
            kind: None,
            status: None,
            input: None,
            output: None,
            locations: None,
        }
    }

    /// Serialize to the wire `data` value for an `agent.update` event.
    pub fn into_value(self) -> serde_json::Value {
        serde_json::to_value(self).expect("ToolCallData serializes")
    }

    /// Parse a wire `data` value; `None` when the payload does not carry the contract's
    /// required identity fields (`id` + `tool`, both non-empty).
    pub fn from_value(v: &serde_json::Value) -> Option<Self> {
        let parsed: Self = serde_json::from_value(v.clone()).ok()?;
        if parsed.id.is_empty() || parsed.tool.is_empty() {
            return None;
        }
        Some(parsed)
    }
}

/// The canonical event stream. `#[serde(tag = "type")]` puts the dotted name in a `type` field.
///
/// This is an internally-tagged (`type`) serde union. typeshare 1.13 cannot emit internally-tagged
/// algebraic enums, so we map it via `serialized_as` to the hand-authored TS union `WsEventWire`
/// (appended to the generated mirror by the gen step). TS-only annotation — the serde wire shape is
/// unchanged and the inline round-trip tests assert the internally-tagged JSON.
#[typeshare(serialized_as = "WsEventWire")]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type")]
pub enum WsEvent {
    /// A new message hit the bus (dm/thread/topic).
    #[serde(rename = "message.created", rename_all = "camelCase")]
    MessageCreated { message_id: MessageId },

    /// Injected as a turn to a recipient.
    #[serde(rename = "message.delivered", rename_all = "camelCase")]
    MessageDelivered {
        message_id: MessageId,
        recipient: SessionId,
    },

    /// One ACP `session/update` event, forwarded verbatim as a tagged pass-through (the full
    /// stream — text, thinking, tool calls, plans, available commands — never just the reply). The
    /// `kind` selects how the UI renders the loosely-typed `data` payload (see [`AgentUpdateKind`]).
    #[serde(rename = "agent.update", rename_all = "camelCase")]
    AgentUpdate {
        session_id: SessionId,
        kind: AgentUpdateKind,
        data: serde_json::Value,
    },

    /// Presence / busy / paused change. (overview §6 synonym: `presence.changed`.)
    #[serde(rename = "agent.status", rename_all = "camelCase")]
    AgentStatus {
        session_id: SessionId,
        presence: Presence,
        paused: bool,
    },

    /// Launch attached a new session. `agent_id` is the durable identity behind the runtime
    /// (identity-by-id); `None` only for legacy emitters that predate stable-id plumbing.
    #[serde(rename = "agent.spawned", rename_all = "camelCase")]
    AgentSpawned {
        session_id: SessionId,
        name: Option<String>,
        agent_id: Option<String>,
    },

    /// A session was removed.
    #[serde(rename = "agent.removed", rename_all = "camelCase")]
    AgentRemoved {
        session_id: SessionId,
        name: Option<String>,
    },

    /// A thread was created.
    #[serde(rename = "thread.created", rename_all = "camelCase")]
    ThreadCreated {
        thread: String,
        members: Vec<String>,
    },

    /// Thread membership changed (spec name kept verbatim: `thread.member.changed`).
    #[serde(rename = "thread.member.changed", rename_all = "camelCase")]
    ThreadMemberChanged {
        thread: String,
        members: Vec<String>,
    },

    /// A pub-feed item was published.
    #[serde(rename = "topic.published", rename_all = "camelCase")]
    TopicPublished {
        topic: String,
        message_id: MessageId,
    },

    /// External push-in landed (with routed_to). (overview §6 synonym: `notification.arrived`.)
    #[serde(rename = "notification.received", rename_all = "camelCase")]
    NotificationReceived {
        notif_id: MessageId,
        routed_to: Vec<String>,
    },

    /// Metadata-only developer event on a reserved `sys.*` topic.
    #[serde(rename = "developer.event", rename_all = "camelCase")]
    DeveloperEvent { event: DeveloperEventEnvelope },
}
