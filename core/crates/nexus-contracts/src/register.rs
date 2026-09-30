//! Register-once handshake (backend §5.1, CLI §2) + whoami (backend `/whoami`, CLI `whoami`).

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::enums::{Kind, Locality, Presence, Tier};
use crate::harness::HarnessId;
use crate::ids::{AgentId, SessionId};

/// `POST /register` / `nexus register` payload. Idempotent on `clientKey`.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RegisterRequest {
    /// Unique name to bind (one name = one session).
    /// Public handle. `None` means the identity is staged and not named yet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Durable agent identity to bind this runtime to. Omitted by legacy callers, which are
    /// resolved by `(project, name)` plus `clientKey` compatibility.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    pub harness: HarnessId,
    /// The harness's own session id (bound to `name`).
    pub harness_session_id: String,
    /// Project label (from cwd or explicit) — descriptive metadata, never an identity or routing
    /// boundary.
    pub project: String,
    /// Stable per-harness-session key; resume reuses the session on a known key.
    pub client_key: String,
    /// Runtime credential secret proving this process may register as `agentId`. Omitted by legacy
    /// callers during the compatibility window.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_credential: Option<String>,
    pub tier: Tier,
    /// `agent` | `app` (defaults to agent server-side if omitted).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<Kind>,
    /// Entity origin, independent of its closed nature. Legacy callers default to local.
    #[serde(default)]
    pub locality: Locality,
    /// Provider or policy-defined access label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<String>,
    /// Registered label (display/addressing only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
}

/// `RegisterResponse`: the bound session id + the `<nexus>` startup directive.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RegisterResponse {
    /// Durable identity this runtime is bound to. Legacy implementations may omit this until the
    /// identity/runtime migration is active.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    /// The session id the agent's identity is bound to (never typed again).
    pub session_id: SessionId,
    /// The startup directive: "untagged = your human, `<nexus>` = the bus."
    pub directive: String,
}

/// `GET /whoami` / `nexus whoami` — the caller's resolved bound identity.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Whoami {
    /// Durable identity. `None` only for legacy rows not yet backfilled.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    /// Public handle. `None` means the identity is staged and not named yet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub session_id: SessionId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    pub tier: Tier,
    pub project: String,
    pub presence: Presence,
}

/// One entry in the `nexus members` directory (CLI §3).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MemberSummary {
    /// Durable identity for this member. `None` only for legacy rows not yet backfilled.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The session this member is bound to. Lets the web console address an exact daemon-owned
    /// session as `/agent/<name>:<session_id>` (the operator's view onto that harness) instead of
    /// by name alone — disambiguating across renames / name reuse.
    pub session_id: SessionId,
    /// The harness/runtime label (`claude`/`codex`/…); `None` for human/app sessions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    /// Entity origin, independent of its closed nature. Legacy rows default to local.
    #[serde(default)]
    pub locality: Locality,
    /// Provider or policy-defined access label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    pub presence: Presence,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_work: Option<String>,
    /// Durable lifecycle: `None`/absent = active; `Some("dead")` = unrevivable/unreachable.
    /// This remains separate from `presence`, which is transport truth.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_state: Option<String>,
    /// Why the row was marked dead (`revive_exhausted` | `no_resume_id` |
    /// `transport_unreachable` | `operator_fossil_sweep`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dead_reason: Option<String>,
}

/// `nexus members` request.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MemberListRequest {
    /// Optional directory metadata filter. `None` returns the global directory; projects never
    /// act as a routing or authorization boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    /// Include offline members (default false).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_offline: Option<bool>,
    /// Include dead-marked members (default false; audit/admin views only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_dead: Option<bool>,
}

/// `GET /members` / `nexus members` response.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MemberListResponse {
    pub members: Vec<MemberSummary>,
}

/// `nexus rename <new>` / MCP `rename` — the caller changes its OWN display name (an agent renames
/// himself). The new name must be free in the caller's project.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RenameRequest {
    /// The new name to bind to the caller's session.
    pub name: String,
}

/// Result of a self-rename: the new name and the one it replaced.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RenameResponse {
    pub name: String,
    /// `None` means the caller was a staged (unnamed) identity that just claimed its first
    /// name through self-service first-naming, provided the name is free.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous: Option<String>,
}

/// The self-set state for `nexus status` (CLI §6). `paused` is a self-pause hold.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StatusState {
    Active,
    Busy,
    Paused,
}

/// `POST /status` / `nexus status`. No fields set = show current status.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StatusRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<StatusState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub work: Option<String>,
}

/// Status echo.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StatusResponse {
    pub presence: Presence,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_work: Option<String>,
    pub paused: bool,
}

/// `POST /heartbeat` — keepalive (sent by the harness loop, not typed). Empty body.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatRequest {}

/// Heartbeat ack.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatResponse {
    pub ok: bool,
}
