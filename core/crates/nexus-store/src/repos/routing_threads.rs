//! Persistent minimal thread-routing capsules.
//!
//! Gateway owns durable thread messages, history, search and presentation. The daemon retains only
//! stable thread ids/names plus member identity edges so a post can still fan out and auto-wake its
//! intended Nexus agents after a daemon restart.

use libsql::params;

use nexus_common::{now, NexusError};
use nexus_contracts::ids::ThreadId;

use crate::error::store_err;
use crate::repos::sessions::{get_opt_int, get_opt_text, get_text};
use crate::Store;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingThreadRow {
    pub thread_id: ThreadId,
    pub name: String,
    pub project: String,
    pub created_by: Option<String>,
    pub created_at: i64,
    pub archived_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingThreadMemberRow {
    pub session_name: String,
    pub agent_id: Option<String>,
    pub joined_at: i64,
}

pub struct RoutingThreads<'a> {
    store: &'a Store,
}

impl<'a> RoutingThreads<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    pub async fn upsert_thread(
        &self,
        thread_id: &ThreadId,
        name: &str,
        project: &str,
        created_by: Option<&str>,
        created_at: Option<i64>,
    ) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "INSERT INTO routing_threads
                 (thread_id, name, project, created_by, created_at, archived_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, NULL)
                 ON CONFLICT(thread_id) DO UPDATE SET
                   name = excluded.name,
                   project = excluded.project,
                   created_by = COALESCE(routing_threads.created_by, excluded.created_by)",
                params![
                    thread_id.0.clone(),
                    name,
                    project,
                    created_by,
                    created_at.unwrap_or_else(now)
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    pub async fn rename(&self, thread_id: &ThreadId, name: &str) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE routing_threads SET name = ?2 WHERE thread_id = ?1",
                params![thread_id.0.clone(), name],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    pub async fn archive(&self, thread_id: &ThreadId) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE routing_threads SET archived_at = COALESCE(archived_at, ?2)
                 WHERE thread_id = ?1",
                params![thread_id.0.clone(), now()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    pub async fn delete(&self, thread_id: &ThreadId) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "DELETE FROM routing_thread_members WHERE thread_id = ?1",
                params![thread_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        self.store
            .identity_conn()
            .execute(
                "DELETE FROM routing_threads WHERE thread_id = ?1",
                params![thread_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    pub async fn add_member(
        &self,
        thread_id: &ThreadId,
        session_name: &str,
        agent_id: Option<&str>,
    ) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "INSERT INTO routing_thread_members
                 (thread_id, session_name, agent_id, joined_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(thread_id, session_name) DO UPDATE SET
                   agent_id = COALESCE(excluded.agent_id, routing_thread_members.agent_id)",
                params![thread_id.0.clone(), session_name, agent_id, now()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    pub async fn remove_member(
        &self,
        thread_id: &ThreadId,
        session_name: &str,
        agent_id: Option<&str>,
    ) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "DELETE FROM routing_thread_members
                 WHERE thread_id = ?1 AND (session_name = ?2 OR
                   (?3 IS NOT NULL AND agent_id = ?3))",
                params![thread_id.0.clone(), session_name, agent_id],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    pub async fn remove_member_all(
        &self,
        session_name: &str,
        agent_id: Option<&str>,
    ) -> Result<(), NexusError> {
        self.remove_member_all_ref(Some(session_name), agent_id)
            .await
    }

    /// Remove every routing membership matching the stable identity and/or legacy display name.
    /// Callers with an explicit stable id pass no name so stale aliases cannot widen deletion.
    pub async fn remove_member_all_ref(
        &self,
        session_name: Option<&str>,
        agent_id: Option<&str>,
    ) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "DELETE FROM routing_thread_members
                 WHERE (?1 IS NOT NULL AND session_name = ?1)
                    OR (?2 IS NOT NULL AND agent_id = ?2)",
                params![session_name, agent_id],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    pub async fn list_active(&self) -> Result<Vec<RoutingThreadRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT thread_id, name, project, created_by, created_at, archived_at
                 FROM routing_threads WHERE archived_at IS NULL ORDER BY created_at, thread_id",
                (),
            )
            .await
            .map_err(store_err)?;
        let mut threads = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            threads.push(RoutingThreadRow {
                thread_id: ThreadId(get_text(&row, 0)?),
                name: get_text(&row, 1)?,
                project: get_text(&row, 2)?,
                created_by: get_opt_text(&row, 3)?,
                created_at: get_opt_int(&row, 4)?.unwrap_or(0),
                archived_at: get_opt_int(&row, 5)?,
            });
        }
        Ok(threads)
    }

    pub async fn members(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Vec<RoutingThreadMemberRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT session_name, agent_id, joined_at
                 FROM routing_thread_members WHERE thread_id = ?1
                 ORDER BY joined_at, session_name",
                params![thread_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        let mut members = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            members.push(RoutingThreadMemberRow {
                session_name: get_text(&row, 0)?,
                agent_id: get_opt_text(&row, 1)?,
                joined_at: get_opt_int(&row, 2)?.unwrap_or(0),
            });
        }
        Ok(members)
    }
}
