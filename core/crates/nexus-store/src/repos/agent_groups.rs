//! Project-scoped agent policy groups.
//!
//! Groups are the first policy substrate for Message Post. They are deliberately generic store
//! state: admin commands assign durable agent identities to groups, and the bus policy evaluator
//! reads those memberships before writing canonical `messages`/`in_flight` rows.

use libsql::params;

use nexus_common::{now, NexusError};
use nexus_contracts::ids::AgentId;

use crate::error::store_err;
use crate::repos::sessions::{get_opt_text, get_text};
use crate::repos::Agents;
use crate::state::Store;

/// Persistence for `agent_groups` and `agent_group_members`.
pub struct AgentGroups<'a> {
    store: &'a Store,
}

impl<'a> AgentGroups<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        AgentGroups { store }
    }

    /// Assign one durable agent identity to a project-scoped group. The group row is created
    /// idempotently first so assignment is one admin operation.
    pub async fn assign(
        &self,
        project: &str,
        group: &str,
        agent_id: &AgentId,
        agent_name: &str,
    ) -> Result<(), NexusError> {
        let ts = now();
        let tx = self.store.begin_write_txn("agent_group_assign").await?;
        tx.execute(
            "INSERT OR IGNORE INTO agent_groups (project, group_name, created_at) \
             VALUES (?1, ?2, ?3)",
            params![project, group, ts],
        )
        .await?;
        tx.execute(
            "INSERT OR REPLACE INTO agent_group_members \
             (project, group_name, agent_id, agent_name, assigned_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![project, group, agent_id.0.clone(), agent_name, ts],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Whether a durable agent is currently a member of `group`.
    pub async fn is_member(
        &self,
        project: &str,
        group: &str,
        agent_id: &AgentId,
    ) -> Result<bool, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT 1 FROM agent_group_members \
                 WHERE project = ?1 AND group_name = ?2 AND agent_id = ?3 LIMIT 1",
                params![project, group, agent_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        Ok(rows.next().await.map_err(store_err)?.is_some())
    }

    /// Return the group names assigned to one durable agent in a project.
    pub async fn groups_for_agent(
        &self,
        project: &str,
        agent_id: &AgentId,
    ) -> Result<Vec<String>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT group_name FROM agent_group_members \
                 WHERE project = ?1 AND agent_id = ?2 ORDER BY group_name",
                params![project, agent_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(get_text(&row, 0)?);
        }
        Ok(out)
    }

    /// Whether a group label exists anywhere. Project is descriptive metadata and does not
    /// partition notification routing.
    pub async fn exists_any_project(&self, group: &str) -> Result<bool, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT 1 FROM agent_groups WHERE group_name = ?1 LIMIT 1",
                params![group],
            )
            .await
            .map_err(store_err)?;
        Ok(rows.next().await.map_err(store_err)?.is_some())
    }

    /// Resolve a group label across project metadata into stable agent identities. One agent is
    /// returned once even if legacy rows assigned it under multiple project labels.
    pub async fn members_any_project(
        &self,
        group: &str,
    ) -> Result<Vec<(AgentId, Option<String>)>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT agent_id, MAX(agent_name) FROM agent_group_members \
                 WHERE group_name = ?1 GROUP BY agent_id ORDER BY agent_id",
                params![group],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            let agent_id = get_text(&row, 0)?;
            let fallback = get_opt_text(&row, 1)?;
            let name = Agents::new(self.store)
                .find_by_id(&agent_id)
                .await?
                .and_then(|agent| agent.name)
                .or(fallback);
            out.push((AgentId(agent_id), name));
        }
        Ok(out)
    }

    /// Whether two durable agent identities share at least one group in the project.
    pub async fn share_group(
        &self,
        project: &str,
        left: &AgentId,
        right: &AgentId,
    ) -> Result<bool, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT 1 FROM agent_group_members l \
                 JOIN agent_group_members r \
                   ON r.project = l.project AND r.group_name = l.group_name \
                 WHERE l.project = ?1 AND l.agent_id = ?2 AND r.agent_id = ?3 LIMIT 1",
                params![project, left.0.clone(), right.0.clone()],
            )
            .await
            .map_err(store_err)?;
        Ok(rows.next().await.map_err(store_err)?.is_some())
    }
}
