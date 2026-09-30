//! Durable agent identity repo.
//!
//! `agents` is the stable identity table. Runtime/session state lives in `agent_runtimes`, and
//! harness-private state lives outside this generic store layer.

use libsql::params;

use nexus_common::{now, NexusError};

use crate::error::store_err;
use crate::repos::sessions::{get_opt_int, get_opt_text, get_text};
use crate::state::Store;
use crate::types::AgentRow;

/// Fields needed to create a durable agent identity.
#[derive(Debug, Clone)]
pub struct NewAgent {
    pub agent_id: String,
    pub project: String,
    pub name: Option<String>,
    pub default_harness: Option<String>,
    pub role: Option<String>,
    pub tier: Option<String>,
    pub owner: Option<AgentOwner>,
}

/// Default owner for a managed agent runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentOwner {
    pub name: String,
    pub project: String,
    pub session_id: Option<String>,
    pub agent_id: Option<String>,
}

/// A parsed `name-or-id` agent reference (identity-by-id slice 1).
///
/// Edges (CLI, gateway, admin requests) parse operator input once with [`AgentRef::parse`] and
/// resolve it to a durable [`AgentRow`] via [`Agents::resolve_ref`]. Everything past the edge
/// carries the resolved `agent_id`; the mutable name never re-enters routing decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentRef {
    /// An explicit stable id (`a_*`). Resolution never reads names.
    Id(String),
    /// A display-name address, resolved project-scoped at the edge.
    Name(String),
}

impl AgentRef {
    /// Parse operator input: `a_*` tokens are ids, everything else is a name label.
    pub fn parse(token: &str) -> AgentRef {
        if token.starts_with("a_") {
            AgentRef::Id(token.to_string())
        } else {
            AgentRef::Name(token.to_string())
        }
    }

    /// Compatibility constructor for request DTOs that carry an optional explicit id next to a
    /// legacy name field: the id wins whenever present, so callers migrate field-by-field.
    pub fn from_request(agent_id: Option<&str>, name: &str) -> AgentRef {
        match agent_id {
            Some(id) if !id.trim().is_empty() => AgentRef::Id(id.to_string()),
            _ => AgentRef::parse(name),
        }
    }
}

/// Persistence for the `agents` table.
pub struct Agents<'a> {
    store: &'a Store,
}

impl<'a> Agents<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        Agents { store }
    }

    /// Insert a durable identity. Public names are globally unique in this first pass.
    pub async fn create(&self, agent: NewAgent) -> Result<String, NexusError> {
        let agent_id = agent.agent_id.clone();
        self.store
            .identity_conn()
            .execute(
                "INSERT INTO agents (agent_id, project, name, default_harness, role, tier, \
                 disabled_at, created_at, owner_name, owner_project, owner_session_id, \
                 owner_agent_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, ?8, ?9, ?10, ?11)",
                params![
                    agent.agent_id,
                    agent.project,
                    agent.name,
                    agent.default_harness,
                    agent.role,
                    agent.tier.unwrap_or_else(|| "agent".to_string()),
                    now(),
                    agent.owner.as_ref().map(|o| o.name.as_str()),
                    agent.owner.as_ref().map(|o| o.project.as_str()),
                    agent.owner.as_ref().and_then(|o| o.session_id.as_deref()),
                    agent.owner.as_ref().and_then(|o| o.agent_id.as_deref()),
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(agent_id)
    }

    /// Find a durable identity by stable id.
    /// Mark an agent unrevivable/unreachable. This is durable lifecycle
    /// state separate from presence. `reason` ∈ revive_exhausted | no_resume_id |
    /// transport_unreachable | operator_fossil_sweep.
    pub async fn mark_dead(&self, agent_id: &str, reason: &str) -> Result<bool, NexusError> {
        let n = self
            .store
            .identity_conn()
            .execute(
                "UPDATE agents SET lifecycle_state = 'dead', dead_reason = ?2 WHERE agent_id = ?1",
                params![agent_id, reason],
            )
            .await
            .map_err(store_err)?;
        Ok(n > 0)
    }

    /// `(lifecycle_state, dead_reason)` for one agent; `(None, None)` = active or unknown id.
    pub async fn lifecycle_for_id(
        &self,
        agent_id: &str,
    ) -> Result<(Option<String>, Option<String>), NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT lifecycle_state, dead_reason FROM agents WHERE agent_id = ?1",
                params![agent_id],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok((row.get(0).ok().flatten(), row.get(1).ok().flatten())),
            None => Ok((None, None)),
        }
    }

    pub async fn find_by_id(&self, agent_id: &str) -> Result<Option<AgentRow>, NexusError> {
        self.query_one("WHERE agent_id = ?1", params![agent_id])
            .await
    }

    /// Find a durable identity by its globally unique public name.
    pub async fn find_by_name(&self, name: &str) -> Result<Option<AgentRow>, NexusError> {
        self.query_one("WHERE name = ?1", params![name]).await
    }

    /// Find a durable identity by project-scoped name (the edge-resolution read).
    pub async fn find_by_project_name(
        &self,
        project: &str,
        name: &str,
    ) -> Result<Option<AgentRow>, NexusError> {
        self.query_one("WHERE project = ?1 AND name = ?2", params![project, name])
            .await
    }

    /// List every identity carrying a name label, across projects (ambiguity detection).
    pub async fn find_all_by_name(&self, name: &str) -> Result<Vec<AgentRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!("{SELECT} WHERE name = ?1 ORDER BY created_at"),
                params![name],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_agent(&row)?);
        }
        Ok(out)
    }

    /// Resolve a parsed [`AgentRef`] to its durable row (identity-by-id edge resolution).
    ///
    /// Stable-identity edge resolution rules:
    /// 1. `a_*` ids resolve directly; a missing id is a loud [`NexusError::NotFound`].
    /// 2. Names resolve project-scoped in `project`.
    /// 3. `any_project` is an explicit legacy opt-in for direct-address paths: it may widen the
    ///    search, but MUST fail loudly on cross-project ambiguity instead of guessing.
    pub async fn resolve_ref(
        &self,
        project: &str,
        agent_ref: &AgentRef,
        any_project: bool,
    ) -> Result<AgentRow, NexusError> {
        match agent_ref {
            AgentRef::Id(id) => self
                .find_by_id(id)
                .await?
                .ok_or_else(|| NexusError::NotFound(format!("agent id {id} does not exist"))),
            AgentRef::Name(name) => {
                if let Some(row) = self.find_by_project_name(project, name).await? {
                    return Ok(row);
                }
                if !any_project {
                    return Err(NexusError::NotFound(format!(
                        "agent named {name:?} does not exist in project {project:?} \
                         (pass an a_* id or the owning project for cross-project addresses)"
                    )));
                }
                let mut matches = self.find_all_by_name(name).await?;
                match matches.len() {
                    0 => Err(NexusError::NotFound(format!(
                        "agent named {name:?} does not exist in any project"
                    ))),
                    1 => Ok(matches.remove(0)),
                    n => Err(NexusError::Ambiguous(format!(
                        "agent name {name:?} matches {n} identities across projects; \
                         address it by a_* id or project-scoped name"
                    ))),
                }
            }
        }
    }

    /// Rename a durable identity. The unique index enforces global name uniqueness.
    pub async fn rename(&self, agent_id: &str, name: &str) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE agents SET name = ?2 WHERE agent_id = ?1",
                params![agent_id, name],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Set an operator-facing role label for a durable identity.
    ///
    /// Roles are display metadata only; policy and routing must not read this field for
    /// authorization or fan-out decisions.
    pub async fn set_role(&self, agent_id: &str, role: &str) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE agents SET role = ?2 WHERE agent_id = ?1",
                params![agent_id, role],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Move a durable identity to another project by stable id.
    ///
    /// Id-aware admin paths call this after selecting the target row once, so project moves do not
    /// re-resolve through mutable display names.
    pub async fn set_project(&self, agent_id: &str, project: &str) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE agents SET project = ?2 WHERE agent_id = ?1",
                params![agent_id, project],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Set the durable authorization tier for an identity.
    ///
    /// This is the stable source of truth for `agents show` and future runtime bindings. Callers
    /// that need the currently running process to inherit the tier must also update the
    /// compatibility session row.
    pub async fn set_tier(&self, agent_id: &str, tier: &str) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE agents SET tier = ?2 WHERE agent_id = ?1",
                params![agent_id, tier],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Stamp an owner on a legacy managed-agent row only when it does not already have one.
    pub async fn set_owner_if_missing(
        &self,
        agent_id: &str,
        owner: &AgentOwner,
    ) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE agents \
                 SET owner_name = ?2, owner_project = ?3, owner_session_id = ?4, \
                     owner_agent_id = ?5 \
                 WHERE agent_id = ?1 AND owner_name IS NULL",
                params![
                    agent_id,
                    owner.name.as_str(),
                    owner.project.as_str(),
                    owner.session_id.as_deref(),
                    owner.agent_id.as_deref(),
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Replace the durable owner for a managed agent row.
    ///
    /// Ownership transfer is explicit and overwrites all `owner_*` fields atomically. Callers are
    /// responsible for applying policy before invoking this store primitive.
    pub async fn set_owner(&self, agent_id: &str, owner: &AgentOwner) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE agents \
                 SET owner_name = ?2, owner_project = ?3, owner_session_id = ?4, \
                     owner_agent_id = ?5 \
                 WHERE agent_id = ?1",
                params![
                    agent_id,
                    owner.name.as_str(),
                    owner.project.as_str(),
                    owner.session_id.as_deref(),
                    owner.agent_id.as_deref(),
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Disable a durable identity without deleting its history.
    pub async fn disable(&self, agent_id: &str) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE agents SET disabled_at = ?2 WHERE agent_id = ?1",
                params![agent_id, now()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// List durable identities, optionally scoped to one project.
    pub async fn list(
        &self,
        project: Option<&str>,
        include_disabled: bool,
    ) -> Result<Vec<AgentRow>, NexusError> {
        let mut rows = match (project, include_disabled) {
            (Some(project), true) => {
                self.store
                    .identity_conn()
                    .query(
                        &format!("{SELECT} WHERE project = ?1 ORDER BY COALESCE(name, agent_id)"),
                        params![project],
                    )
                    .await
            }
            (Some(project), false) => {
                self.store
                    .identity_conn()
                    .query(
                        &format!(
                            "{SELECT} WHERE project = ?1 AND disabled_at IS NULL ORDER BY COALESCE(name, agent_id)"
                        ),
                        params![project],
                    )
                    .await
            }
            (None, true) => {
                self.store
                    .identity_conn()
                    .query(&format!("{SELECT} ORDER BY COALESCE(name, agent_id)"), ())
                    .await
            }
            (None, false) => {
                self.store
                    .identity_conn()
                    .query(
                        &format!("{SELECT} WHERE disabled_at IS NULL ORDER BY COALESCE(name, agent_id)"),
                        (),
                    )
                    .await
            }
        }
        .map_err(store_err)?;

        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_agent(&row)?);
        }
        Ok(out)
    }

    async fn query_one(
        &self,
        where_clause: &str,
        p: impl libsql::params::IntoParams,
    ) -> Result<Option<AgentRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(&format!("{SELECT} {where_clause}"), p)
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_agent(&row)?)),
            None => Ok(None),
        }
    }
}

const SELECT: &str = "SELECT agent_id, project, name, default_harness, role, tier, disabled_at, \
     created_at, metadata_json, owner_name, owner_project, owner_session_id, owner_agent_id \
     FROM agents";

pub(crate) fn row_to_agent(row: &libsql::Row) -> Result<AgentRow, NexusError> {
    Ok(AgentRow {
        agent_id: get_text(row, 0)?,
        project: get_text(row, 1)?,
        name: get_opt_text(row, 2)?,
        default_harness: get_opt_text(row, 3)?,
        role: get_opt_text(row, 4)?,
        tier: get_text(row, 5)?,
        disabled_at: get_opt_int(row, 6)?,
        created_at: get_opt_int(row, 7)?.unwrap_or(0),
        metadata_json: get_opt_text(row, 8)?,
        owner_name: get_opt_text(row, 9)?,
        owner_project: get_opt_text(row, 10)?,
        owner_session_id: get_opt_text(row, 11)?,
        owner_agent_id: get_opt_text(row, 12)?,
    })
}
