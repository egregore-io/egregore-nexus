//! Durable managed-agent access grants.
//!
//! `agents.owner_*` remains the primary owner fact. Rows here are explicit delegations that let a
//! principal observe or co-own a managed `/agent` session without changing ownership.

use libsql::params;

use nexus_common::{now, NexusError};

use crate::error::store_err;
use crate::repos::sessions::{get_opt_int, get_opt_text, get_text};
use crate::state::Store;
use crate::types::AgentAccessGrantRow;

/// Role labels persisted in `agent_acl_grants.role`.
pub const ROLE_VIEWER: &str = "viewer";
pub const ROLE_CO_OWNER: &str = "co_owner";

/// New or updated grant payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewAgentAccessGrant {
    pub agent_id: String,
    pub principal_project: String,
    pub principal_name: String,
    pub principal_session_id: Option<String>,
    pub principal_agent_id: Option<String>,
    pub role: String,
    pub granted_by_name: String,
    pub granted_by_project: String,
}

/// Persistence for `agent_acl_grants`.
pub struct AgentAccessGrants<'a> {
    store: &'a Store,
}

impl<'a> AgentAccessGrants<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        AgentAccessGrants { store }
    }

    /// Insert or replace the role for one principal on one managed agent.
    pub async fn grant(
        &self,
        grant: NewAgentAccessGrant,
    ) -> Result<AgentAccessGrantRow, NexusError> {
        let ts = now();
        self.store
            .identity_conn()
            .execute(
                "INSERT INTO agent_acl_grants \
                 (agent_id, principal_project, principal_name, principal_session_id, \
                  principal_agent_id, role, granted_by_name, granted_by_project, created_at, \
                  updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9) \
                 ON CONFLICT(agent_id, principal_project, principal_name) DO UPDATE SET \
                   principal_session_id = excluded.principal_session_id, \
                   principal_agent_id = excluded.principal_agent_id, \
                   role = excluded.role, \
                   granted_by_name = excluded.granted_by_name, \
                   granted_by_project = excluded.granted_by_project, \
                   updated_at = excluded.updated_at",
                params![
                    grant.agent_id.as_str(),
                    grant.principal_project.as_str(),
                    grant.principal_name.as_str(),
                    grant.principal_session_id.as_deref(),
                    grant.principal_agent_id.as_deref(),
                    grant.role.as_str(),
                    grant.granted_by_name.as_str(),
                    grant.granted_by_project.as_str(),
                    ts,
                ],
            )
            .await
            .map_err(store_err)?;
        self.find(
            &grant.agent_id,
            &grant.principal_project,
            &grant.principal_name,
        )
        .await?
        .ok_or_else(|| NexusError::Invalid("agent access grant was not persisted".into()))
    }

    /// Delete one principal's grant. Returns whether a row was removed.
    pub async fn revoke(
        &self,
        agent_id: &str,
        principal_project: &str,
        principal_name: &str,
    ) -> Result<bool, NexusError> {
        let changed = self
            .store
            .identity_conn()
            .execute(
                "DELETE FROM agent_acl_grants \
                 WHERE agent_id = ?1 AND principal_project = ?2 AND principal_name = ?3",
                params![agent_id, principal_project, principal_name],
            )
            .await
            .map_err(store_err)?;
        Ok(changed > 0)
    }

    /// Delete one principal's grant by durable principal id.
    ///
    /// This is the rename-proof companion to [`Self::revoke`]. It intentionally ignores the
    /// mutable `principal_name`; legacy rows without `principal_agent_id` stay revocable through
    /// the name-keyed fallback.
    pub async fn revoke_by_principal_agent_id(
        &self,
        agent_id: &str,
        principal_agent_id: &str,
    ) -> Result<bool, NexusError> {
        let changed = self
            .store
            .identity_conn()
            .execute(
                "DELETE FROM agent_acl_grants \
                 WHERE agent_id = ?1 AND principal_agent_id = ?2",
                params![agent_id, principal_agent_id],
            )
            .await
            .map_err(store_err)?;
        Ok(changed > 0)
    }

    /// Delete grants where this identity is the principal or grantor.
    ///
    /// `admin.remove` uses this actor cascade while retaining the target's own managed-agent row
    /// and any grants on that target. A removed actor cannot keep delegated observe/co-owner rights
    /// or remain the authoritative `granted_by_*` edge for another principal.
    pub async fn purge_actor(
        &self,
        project: &str,
        name: Option<&str>,
        agent_id: Option<&str>,
    ) -> Result<u64, NexusError> {
        self.store
            .identity_conn()
            .execute(
                "DELETE FROM agent_acl_grants \
                 WHERE (?2 IS NOT NULL AND principal_project = ?1 AND principal_name = ?2) \
                    OR (?2 IS NOT NULL AND granted_by_project = ?1 AND granted_by_name = ?2) \
                    OR (?3 IS NOT NULL AND principal_agent_id = ?3)",
                params![project, name, agent_id],
            )
            .await
            .map_err(store_err)
    }

    /// Delete every grant edge tied to an agent being fully deleted.
    ///
    /// `admin.delete` removes the managed-agent target itself, so this purges target grants in
    /// addition to the actor cleanup covered by [`Self::purge_actor`].
    pub async fn purge_deleted_agent(
        &self,
        agent_id: Option<&str>,
        project: &str,
        name: Option<&str>,
    ) -> Result<u64, NexusError> {
        self.store
            .identity_conn()
            .execute(
                "DELETE FROM agent_acl_grants \
                 WHERE (?1 IS NOT NULL AND agent_id = ?1) \
                    OR (?3 IS NOT NULL AND principal_project = ?2 AND principal_name = ?3) \
                    OR (?3 IS NOT NULL AND granted_by_project = ?2 AND granted_by_name = ?3) \
                    OR (?1 IS NOT NULL AND principal_agent_id = ?1)",
                params![agent_id, project, name],
            )
            .await
            .map_err(store_err)
    }

    /// Find one principal's current grant role, if any.
    pub async fn find(
        &self,
        agent_id: &str,
        principal_project: &str,
        principal_name: &str,
    ) -> Result<Option<AgentAccessGrantRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!(
                    "{SELECT} WHERE agent_id = ?1 AND principal_project = ?2 \
                     AND principal_name = ?3"
                ),
                params![agent_id, principal_project, principal_name],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_grant(&row)?)),
            None => Ok(None),
        }
    }

    /// Return whether the principal has a viewer or co-owner grant.
    pub async fn can_view(
        &self,
        agent_id: &str,
        principal_project: &str,
        principal_name: &str,
    ) -> Result<bool, NexusError> {
        Ok(self
            .find(agent_id, principal_project, principal_name)
            .await?
            .is_some())
    }

    /// Find a principal's grant by durable ids only (identity-by-id slice 1).
    ///
    /// Rename-proof read: neither side consults `principal_project`/`principal_name`, so a
    /// relabeled principal keeps exactly the grants its id already had. Legacy rows with NULL
    /// `principal_agent_id` are invisible here by design — they stay on the name-keyed reads
    /// until backfill or re-grant stamps them.
    pub async fn find_by_principal_agent_id(
        &self,
        agent_id: &str,
        principal_agent_id: &str,
    ) -> Result<Option<AgentAccessGrantRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!("{SELECT} WHERE agent_id = ?1 AND principal_agent_id = ?2"),
                params![agent_id, principal_agent_id],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_grant(&row)?)),
            None => Ok(None),
        }
    }

    /// Id-keyed twin of [`Self::can_view`].
    pub async fn can_view_agent_id(
        &self,
        agent_id: &str,
        principal_agent_id: &str,
    ) -> Result<bool, NexusError> {
        Ok(self
            .find_by_principal_agent_id(agent_id, principal_agent_id)
            .await?
            .is_some())
    }

    /// Id-keyed twin of [`Self::is_co_owner`].
    pub async fn is_co_owner_agent_id(
        &self,
        agent_id: &str,
        principal_agent_id: &str,
    ) -> Result<bool, NexusError> {
        Ok(matches!(
            self.find_by_principal_agent_id(agent_id, principal_agent_id)
                .await?
                .as_ref()
                .map(|row| row.role.as_str()),
            Some(ROLE_CO_OWNER)
        ))
    }

    /// Return whether the principal has co-owner grant rights.
    pub async fn is_co_owner(
        &self,
        agent_id: &str,
        principal_project: &str,
        principal_name: &str,
    ) -> Result<bool, NexusError> {
        Ok(matches!(
            self.find(agent_id, principal_project, principal_name)
                .await?
                .as_ref()
                .map(|row| row.role.as_str()),
            Some(ROLE_CO_OWNER)
        ))
    }
}

const SELECT: &str = "SELECT agent_id, principal_project, principal_name, principal_session_id, \
     principal_agent_id, role, granted_by_name, granted_by_project, created_at, updated_at \
     FROM agent_acl_grants";

fn row_to_grant(row: &libsql::Row) -> Result<AgentAccessGrantRow, NexusError> {
    Ok(AgentAccessGrantRow {
        agent_id: get_text(row, 0)?,
        principal_project: get_text(row, 1)?,
        principal_name: get_text(row, 2)?,
        principal_session_id: get_opt_text(row, 3)?,
        principal_agent_id: get_opt_text(row, 4)?,
        role: get_text(row, 5)?,
        granted_by_name: get_text(row, 6)?,
        granted_by_project: get_text(row, 7)?,
        created_at: get_opt_int(row, 8)?.unwrap_or(0),
        updated_at: get_opt_int(row, 9)?.unwrap_or(0),
    })
}
