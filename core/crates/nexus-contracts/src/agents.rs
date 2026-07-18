//! Stable agent identity management DTOs.
//!
//! These types describe durable agents, runtime credentials, and disposable runtimes. They are
//! intentionally generic: harness-specific state such as Codex app-server sockets belongs in the
//! harness crate, not in this contract surface.

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::enums::{AgentAccessRole, Presence};
use crate::harness::HarnessId;
use crate::ids::{AgentId, CredentialId, SessionId};

/// `nexus agents create <name>` — create a durable agent identity.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentCreateRequest {
    /// Globally unique display/addressing name for the first implementation pass.
    pub name: String,
    /// Default harness used by launch/resume commands when one is not supplied.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_harness: Option<HarnessId>,
    /// Project scope the agent belongs to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    /// Operator-facing label; not used for authorization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
}

/// Agent creation result.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentCreateResponse {
    pub agent: AgentSummary,
    /// Optional first runtime credential. When present, `secret` is shown once and never stored in
    /// plaintext.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential: Option<AgentCredentialCreateResponse>,
}

/// One durable agent identity with its current runtime snapshot when available.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentSummary {
    pub agent_id: AgentId,
    /// Public handle. `None` means the identity is staged and not named yet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub project: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_harness: Option<HarnessId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tier: Option<String>,
    pub disabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_runtime: Option<AgentRuntimeSummary>,
}

/// List durable agents.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentListRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_disabled: Option<bool>,
}

/// Durable agent list response.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentListResponse {
    pub agents: Vec<AgentSummary>,
}

/// Show one durable agent by id or name.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentShowRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// Detailed durable agent response.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentShowResponse {
    pub agent: AgentSummary,
    pub runtimes: Vec<AgentRuntimeSummary>,
}

/// Grant a principal access to a managed agent session.
///
/// Owners and co-owners may delegate `viewer` or `coOwner` access. The daemon records the grant in
/// its durable ACL table and the gateway `/agent` observe route reads that projection.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentAccessGrantRequest {
    /// Stable target managed agent id. When present, the daemon resolves this before `name`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    /// Target managed agent name fallback for callers that do not know the id.
    pub name: String,
    /// Stable principal id receiving access. When present, the daemon resolves this before
    /// `principal` and snapshots the principal's current name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal_agent_id: Option<AgentId>,
    /// Principal name receiving access, or fallback when `principal_agent_id` is omitted.
    pub principal: String,
    /// Project scope for the principal. Defaults to the caller's project when omitted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    /// Access role to grant.
    pub role: AgentAccessRole,
}

/// Grant result for `agent.grantAccess`.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentAccessGrantResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub principal: String,
    pub project: String,
    pub role: AgentAccessRole,
}

/// Revoke a principal's delegated access to a managed agent session.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentAccessRevokeRequest {
    /// Stable target managed agent id. When present, the daemon resolves this before `name`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    /// Target managed agent name fallback for callers that do not know the id.
    pub name: String,
    /// Stable principal id losing access. When present, the daemon resolves this before
    /// `principal`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal_agent_id: Option<AgentId>,
    /// Principal name losing access, or fallback when `principal_agent_id` is omitted.
    pub principal: String,
    /// Project scope for the principal. Defaults to the caller's project when omitted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
}

/// Revoke result for `agent.revokeAccess`.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentAccessRevokeResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub principal: String,
    pub project: String,
    pub revoked: bool,
}

/// Transfer managed agent ownership to another principal.
///
/// The current owner, or an admin override allowed by the protected-target policy, may replace the
/// `owner_*` fact. Co-owner access grants do not authorize ownership transfer.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentOwnerTransferRequest {
    /// Stable target managed agent id. When present, the daemon resolves this before `name`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    /// Target managed agent name fallback for callers that do not know the id.
    pub name: String,
    /// Stable principal id receiving ownership. When present, the daemon resolves this before
    /// `owner` and snapshots the owner's current name/project.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_agent_id: Option<AgentId>,
    /// Principal name receiving ownership, or fallback when `owner_agent_id` is omitted.
    pub owner: String,
    /// Project scope for the new owner. Defaults to the caller's project when omitted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
}

/// Transfer result for `agent.transferOwner`.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentOwnerTransferResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_project: Option<String>,
    pub owner: String,
    pub project: String,
}

/// Create a scoped runtime credential for a durable agent.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentCredentialCreateRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Human-editable label such as "local laptop" or "headed codex".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Descriptive only. Authorization uses `scopes`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub purpose: Option<String>,
    /// Authorization scopes, e.g. `runtime:register`.
    pub scopes: Vec<String>,
}

/// Runtime credential creation result. `secret` is returned once.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentCredentialCreateResponse {
    pub credential_id: CredentialId,
    pub agent_id: AgentId,
    pub secret: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub purpose: Option<String>,
    pub scopes: Vec<String>,
}

/// Revoke a runtime credential.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentCredentialRevokeRequest {
    pub credential_id: CredentialId,
}

/// Credential revoke result.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentCredentialRevokeResponse {
    pub credential_id: CredentialId,
    pub revoked: bool,
}

/// Runtime summary for the current process/session representing a durable agent.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentRuntimeSummary {
    /// The disposable runtime id. In the first pass this is the Nexus `SessionId`.
    pub runtime_id: SessionId,
    pub agent_id: AgentId,
    pub harness: HarnessId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport: Option<String>,
    pub presence: Presence,
    pub active: bool,
    #[typeshare(serialized_as = "number")]
    pub started_at: i64,
    #[typeshare(serialized_as = "Option<number>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stopped_at: Option<i64>,
    #[typeshare(serialized_as = "Option<number>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_heartbeat: Option<i64>,
}

/// List runtimes for one durable agent.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentRuntimeListRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_stopped: Option<bool>,
}

/// Runtime list response.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentRuntimeListResponse {
    pub agent_id: AgentId,
    pub runtimes: Vec<AgentRuntimeSummary>,
}
