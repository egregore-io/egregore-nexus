//! Store-local row types that are not part of the wire contract (`nexus-contracts` owns those).
//! A [`SessionRow`] mirrors a `sessions` table row; messages map directly to
//! [`nexus_contracts::Message`].
//!
//! [`AgentRow`], [`AgentCredentialRow`], and [`AgentRuntimeRow`] mirror the generic identity and
//! runtime tables. They intentionally carry no harness-private resume/control state; Codex/OpenCode
//! etc. extension data belongs in the owning harness crate. Runtime rows do carry a generic OS
//! process ledger `(pid, pgid)` so daemon boot can reap orphaned process groups without parsing
//! harness-private sidecars.

use nexus_common::{NexusError, RuntimeProcessIds};
use nexus_contracts::ids::SessionId;

/// A row of the `sessions` table — the daemon-internal identity record. Wire-facing identity
/// DTOs (`RegisterResponse`, `Whoami`, member lists) are assembled from this by `nexus-identity`.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionRow {
    pub session_id: SessionId,
    /// Mutable public handle. `None` is a staged identity that has not been named yet.
    pub name: Option<String>,
    pub agent: Option<String>,
    pub kind: String,
    pub role: Option<String>,
    pub tier: String,
    pub harness_session_id: Option<String>,
    pub client_key: Option<String>,
    pub cwd: Option<String>,
    pub project: String,
    pub current_work: Option<String>,
    pub presence: Option<String>,
    pub paused: bool,
    pub paused_by: Option<String>,
    pub callback_url: Option<String>,
    pub last_heartbeat: Option<i64>,
    pub created_at: i64,
    /// Transport this session uses: `'pty'` | `'acp'` | `'codex-appserver'`.
    /// `None` means legacy/unknown and revive falls back according to daemon policy.
    pub transport: Option<String>,
    pub metadata_json: Option<String>,
    /// Durable identity backing this compatibility/live row (identity-by-id slice 1).
    /// `None` marks a fossil/ambiguous legacy row that audits must surface, never rebind.
    pub agent_id: Option<String>,
}

/// A durable agent identity. `agent_id` is the stable internal key; `name` is the globally unique
/// public address for this first implementation pass. Managed-agent owner columns are nullable so
/// legacy identities remain readable; daemon spawns stamp them when a caller creates a runtime.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentRow {
    pub agent_id: String,
    pub project: String,
    /// Mutable public handle. `None` is a staged identity that has not been named yet.
    pub name: Option<String>,
    pub default_harness: Option<String>,
    pub role: Option<String>,
    pub tier: String,
    pub disabled_at: Option<i64>,
    pub created_at: i64,
    pub metadata_json: Option<String>,
    pub owner_name: Option<String>,
    pub owner_project: Option<String>,
    pub owner_session_id: Option<String>,
    pub owner_agent_id: Option<String>,
}

impl SessionRow {
    pub fn display_name(&self) -> String {
        self.name
            .clone()
            .or_else(|| self.agent_id.clone())
            .unwrap_or_else(|| self.session_id.0.clone())
    }

    pub fn require_name(&self, context: &str) -> Result<&str, NexusError> {
        self.name
            .as_deref()
            .ok_or_else(|| NexusError::Invalid(format!("{context}: session has no assigned name")))
    }
}

impl AgentRow {
    pub fn display_name(&self) -> String {
        self.name.clone().unwrap_or_else(|| self.agent_id.clone())
    }

    pub fn require_name(&self, context: &str) -> Result<&str, NexusError> {
        self.name
            .as_deref()
            .ok_or_else(|| NexusError::Invalid(format!("{context}: agent has no assigned name")))
    }
}

/// A delegated `/agent` session access grant for one managed agent identity.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentAccessGrantRow {
    pub agent_id: String,
    pub principal_project: String,
    pub principal_name: String,
    pub principal_session_id: Option<String>,
    pub principal_agent_id: Option<String>,
    pub role: String,
    pub granted_by_name: String,
    pub granted_by_project: String,
    pub created_at: i64,
    pub updated_at: i64,
}

/// A hashed credential that authorizes a runtime to register for a durable agent identity.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentCredentialRow {
    pub credential_id: String,
    pub agent_id: String,
    pub secret_hash: String,
    pub purpose: Option<String>,
    pub label: Option<String>,
    pub scopes_json: String,
    pub metadata_json: Option<String>,
    pub revoked_at: Option<i64>,
    pub last_used_at: Option<i64>,
    pub created_at: i64,
}

/// A disposable runtime for a durable agent identity. In the first pass `runtime_id` can be the
/// existing Nexus session id string.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentRuntimeRow {
    pub runtime_id: String,
    pub agent_id: String,
    pub harness: String,
    pub cwd: Option<String>,
    pub transport: Option<String>,
    pub presence: Option<String>,
    pub active: bool,
    pub started_at: i64,
    pub stopped_at: Option<i64>,
    pub last_heartbeat: Option<i64>,
    pub os_pid: Option<i64>,
    pub os_pgid: Option<i64>,
}

impl AgentRuntimeRow {
    /// Return the durable process id tuple when both OS identity columns are present.
    pub fn runtime_process_ids(&self) -> Option<RuntimeProcessIds> {
        RuntimeProcessIds::from_parts(self.os_pid, self.os_pgid)
    }
}

/// Durable ownership binding between a harness-native conversation/session id and a Nexus agent.
///
/// Runtime rows can be stopped, deleted, or replaced. This row answers the separate identity
/// question: "which stable agent owns this native conversation?"
#[derive(Debug, Clone, PartialEq)]
pub struct NativeThreadBindingRow {
    pub harness: String,
    pub native_thread_id: String,
    pub agent_id: String,
    pub project: String,
    pub first_runtime_id: Option<String>,
    pub last_runtime_id: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub released_at: Option<i64>,
}
