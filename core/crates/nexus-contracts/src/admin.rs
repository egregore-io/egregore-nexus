//! Admin-tier command types (CLI §8, backend §10). Privilege-gated at the daemon (tier==admin);
//! the gate is NOT in the wire shape. Admin = extra commands only, never the message path.

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::enums::{Harness, Tier};
use crate::ids::{AgentId, MessageId, SessionId};

/// `nexus admin route <notif> --to <name|thread>` — ad-hoc, one-shot forward (NOT a standing rule).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RouteForwardRequest {
    /// The notification message id to forward.
    pub notif: MessageId,
    /// Recipient: an agent name or thread name.
    pub to: String,
}

/// `nexus admin spawn <kind>` — admin-initiated spawn (the agent still self-registers).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum SpawnIdentityPolicy {
    /// No explicit identity was supplied; resume should prefer an existing native owner.
    Implicit,
    /// The request name was supplied by the caller and should be treated as intentional.
    ExplicitName,
    /// The request name is an explicit stable `a_*` agent id.
    ExplicitAgentId,
}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SpawnRequest {
    pub kind: Harness,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Whether `name` was caller-supplied identity intent or generated/implicit launch metadata.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_policy: Option<SpawnIdentityPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// Fresh-launch model-visible boot prompt template. The daemon expands `<var.*>` after
    /// allocating identity/runtime and rejects this field for resume/reuse paths.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_prompt: Option<String>,
    /// Harness-native resume key. For headed Codex app-server launches this is the Codex thread id
    /// used to start `codex resume --remote ... <thread>`. Defaults to `None` for older callers and
    /// fresh launches.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume: Option<String>,
    /// Harness-native argv tail from `nexus launch <harness> [args...]`. The daemon passes these to
    /// headed harness launches after applying harness-specific normalization in the CLI. Defaults to
    /// an empty vector so older callers keep launching fresh sessions.
    #[serde(default)]
    pub harness_args: Vec<String>,
    /// When `true`, the agent is spawned headlessly via the daemon's ACP runner (no TUI/PTY).
    /// Defaults to `false` so existing callers that omit the field continue to work.
    #[serde(default)]
    pub headless: bool,
    /// Local terminal backend for headed launches: `"pty"` (raw daemon-owned PTY) or `"tmux"`
    /// (multiplexer-owned; survives daemon restarts). `None` resolves via
    /// [`SpawnRequest::resolved_backend`]: the operator's configured default
    /// (`launch_backend` in `nexus.toml` / `NEXUS_LAUNCH_BACKEND`), else `"pty"`.
    /// Hermes always resolves to `"tmux"` — its gateway bridge is
    /// welded to the tmux viewer profile.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend: Option<String>,
}

impl SpawnRequest {
    /// The effective headed viewer backend for this spawn. Resolution order: an explicit
    /// `backend` always wins; hermes is forced to `"tmux"` (see the field doc); otherwise the
    /// operator-configured default applies; otherwise `"pty"` — the raw daemon-owned PTY is the
    /// default and tmux is strictly opt-in.
    pub fn resolved_backend<'a>(&'a self, configured_default: Option<&'a str>) -> &'a str {
        match self.backend.as_deref() {
            Some(explicit) => explicit,
            None if self.kind == crate::enums::Harness::Hermes => "tmux",
            None => configured_default.unwrap_or("pty"),
        }
    }
}

/// Spawn result.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SpawnResponse {
    pub session_id: SessionId,
}

/// `nexus admin remove <name>` — tear down a session's loop (message store retained).
/// `kill=true` additionally terminates the spawned harness OS process; `kill=false` (default)
/// is the old evict-only behaviour and keeps backward compatibility for callers that omit the
/// field.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RemoveRequest {
    /// Stable target agent id carried for id-capable clients. When present, `admin.remove` and
    /// `admin.delete` select the target session by this id; `name` is the legacy selector when the id
    /// is omitted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    pub name: String,
    /// When `true`, the spawned harness process is SIGKILL-ed in addition to the session being
    /// detached. Defaults to `false` so existing callers (no field) continue to work.
    #[serde(default)]
    pub kill: bool,
}

/// Remove result.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RemoveResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub status: String,
}

/// `nexus admin rename <source> <target>` — admin first-naming/rename for any agent. `source`
/// accepts an agent name, stable `a_*` id, or exact `s_*` session id. `target` is the new globally
/// unique public handle to bind. This is the explicit naming surface for staged identities.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AdminRenameRequest {
    pub source: String,
    pub target: String,
}

/// Admin rename result, including the stable target that was changed. `previous = None` means the
/// operation first-named a staged identity.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AdminRenameResponse {
    pub agent_id: AgentId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous: Option<String>,
}

/// `nexus admin assign <id> <name>` — a **staged (unnamed)** identity assumes a name whose
/// previous owner died. `id` must be a stable `a_*` agent id or exact
/// `s_*` session id resolving to an identity with **no name yet**; `name` may be free (plain
/// first-naming) or held by an agent that is dead / has no live session or active runtime — that
/// holder is evicted back to unnamed (the staged surface). A name held by a live agent is never
/// takeable. Only the label and future name-addressed traffic transfer; the previous holder's
/// thread memberships, subscriptions, and queued mail stay keyed to its `agent_id`.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AdminAssignRequest {
    /// Staged assignee: stable `a_*` agent id or exact `s_*` session id (never a name).
    pub id: String,
    /// The name to assume.
    pub name: String,
}

/// Admin assign result. `evicted_agent_id` names the dead previous holder when the name was
/// taken over; `None` means the name was free (plain staged first-naming).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AdminAssignResponse {
    pub agent_id: AgentId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evicted_agent_id: Option<AgentId>,
}

/// `nexus admin assign-role <name> <role>` — set the registered label (display/addressing only).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AssignRoleRequest {
    /// Stable target agent id. When present, the daemon resolves this before `name`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    pub name: String,
    pub role: String,
}

/// Assign-role result.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AssignRoleResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub role: String,
}

/// `nexus admin assign-project <name> <project>` — set the active project for an agent session.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AssignProjectRequest {
    /// Stable target agent id. When present, the daemon resolves this before `name`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    pub name: String,
    pub project: String,
}

/// Assign-project result.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AssignProjectResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub project: String,
}

/// `nexus admin grant-tier <name> <agent|admin>` — set an agent's durable privilege tier.
/// This command is intentionally narrower than role assignment: roles are display labels, while
/// tiers are authorization ceilings. Only a human/local admin may grant tiers; agent-admins may use
/// admin capability but may not mint more admins or modify protected human admins.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GrantTierRequest {
    /// Stable target agent id. When present, the daemon resolves this before `name`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    pub name: String,
    pub tier: Tier,
}

/// Durable tier grant result.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GrantTierResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub tier: Tier,
}

/// `nexus admin group assign <group> <name>` — assign a durable agent to a policy group.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AdminGroupAssignRequest {
    /// Project scope for the group. Defaults to the caller's project.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    /// Project-scoped group name.
    pub group: String,
    /// Stable target agent id. When present, the daemon resolves this before `name`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    /// Public agent name to assign, or fallback when `agent_id` is omitted.
    pub name: String,
}

/// Group assignment result.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AdminGroupAssignResponse {
    pub project: String,
    pub group: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub agent_id: AgentId,
}

/// Channel op for `nexus admin channel <op> <topic>` (CLI §8).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ChannelOp {
    Create,
    Delete,
    SetRoute,
}

/// `nexus admin channel <op> <topic> [--source <s>]` — manage topics / Pub-feed routing.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ChannelRequest {
    pub op: ChannelOp,
    pub topic: String,
    /// Source for `setRoute` (route-by-source).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

/// `nexus admin monitor` — stream the web console event feed for oversight.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MonitorRequest {
    pub follow: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

/// `nexus admin dlq list` — inspect terminal delivery failures without mutating them.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DlqListRequest {
    /// Agent name, stable `a_*` id, or session `s_*` id. Serialized as `for` for CLI/API parity.
    #[serde(rename = "for")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub for_target: Option<String>,
    /// Timestamp or duration string accepted by the daemon-side parser.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// One operator-visible dead-letter row.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DlqEntry {
    pub in_flight_id: String,
    pub message_id: MessageId,
    pub sender: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recipient_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recipient_agent_id: Option<AgentId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recipient_session: Option<SessionId>,
    #[typeshare(serialized_as = "number")]
    pub created_at: i64,
    #[typeshare(serialized_as = "Option<number>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dead_lettered_at: Option<i64>,
    /// Number of completed external harness attempts. Retained across explicit requeues.
    #[serde(default)]
    pub attempt_count: u32,
    /// Stable machine-readable terminal class used for operator policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_reason: Option<String>,
    /// Structured terminal evidence; retryability is advisory and never triggers daemon replay.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_details: Option<serde_json::Value>,
    pub body_preview: String,
}

/// DLQ list response.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DlqListResponse {
    pub rows: Vec<DlqEntry>,
    #[typeshare(serialized_as = "number")]
    pub total: u64,
}

/// `nexus admin dlq requeue` — move dead-letter rows back to pending.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DlqRequeueRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_flight_id: Option<String>,
    #[serde(rename = "for")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub for_target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,
}

/// `nexus admin dlq purge` — explicitly discard dead-letter rows.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DlqPurgeRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_flight_id: Option<String>,
    #[serde(rename = "for")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub for_target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,
    #[serde(default)]
    pub yes: bool,
}

/// DLQ requeue/purge response.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DlqMutationResponse {
    #[typeshare(serialized_as = "number")]
    pub count: u64,
    pub in_flight_ids: Vec<String>,
}
