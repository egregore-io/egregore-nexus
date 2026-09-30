//! The `threads` + `thread_members` repos — named threads (ids hidden from agents). Membership
//! drives fan-out and the search scope (context hygiene §9). The persisted `project` value is
//! legacy metadata; thread names are globally unique.

use libsql::params;

use nexus_common::{now, NexusError};
use nexus_contracts::ids::ThreadId;

use crate::error::store_err;
use crate::repos::sessions::{get_opt_int, get_opt_text, get_text};
use crate::repos::{Agents, RoutingThreads};
use crate::state::Store;

/// A `threads` row.
#[derive(Debug, Clone, PartialEq)]
pub struct ThreadRow {
    pub thread_id: ThreadId,
    pub name: String,
    pub project: String,
    pub topic: Option<String>,
    pub description: Option<String>,
    pub created_by: Option<String>,
    pub created_at: i64,
    pub archived_at: Option<i64>,
}

/// A stored thread membership edge.
///
/// `agent_id` is the stable routing key when available; `session_name` is the compatibility display
/// and legacy fallback key for old rows that predate the identity-by-id migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadMemberRef {
    pub session_name: String,
    pub agent_id: Option<String>,
}

/// Persistence for `threads` + `thread_members`.
pub struct Threads<'a> {
    store: &'a Store,
}

impl<'a> Threads<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        Threads { store }
    }

    /// Create a named thread. The `name` is globally `UNIQUE`; `project` is retained as legacy
    /// metadata for older rows/readers.
    pub async fn create(
        &self,
        thread_id: &ThreadId,
        name: &str,
        project: &str,
        created_by: &str,
    ) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "INSERT INTO threads (thread_id, name, project, created_by, created_at, \
                 archived_at) VALUES (?1, ?2, ?3, ?4, ?5, NULL)",
                params![thread_id.0.clone(), name, project, created_by, now()],
            )
            .await
            .map_err(store_err)?;
        if self.store.has_split_authority() {
            RoutingThreads::new(self.store)
                .upsert_thread(thread_id, name, project, Some(created_by), None)
                .await?;
        }
        Ok(())
    }

    /// Legacy scoped lookup. Runtime thread identity should use `find_any_by_name` because thread
    /// names are globally unique and `project` is metadata.
    pub async fn find_by_name(
        &self,
        project: &str,
        name: &str,
    ) -> Result<Option<ThreadRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT thread_id, name, project, topic, description, created_by, created_at, archived_at \
                 FROM threads \
                 WHERE project = ?1 AND name = ?2",
                params![project, name],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_thread(&row)?)),
            None => Ok(None),
        }
    }

    /// Resolve a thread by its display name across all projects.
    ///
    /// Thread names are globally unique in the current schema, so rename collision checks need
    /// the unscoped lookup.
    pub async fn find_any_by_name(&self, name: &str) -> Result<Option<ThreadRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT thread_id, name, project, topic, description, created_by, created_at, archived_at \
                 FROM threads WHERE name = ?1",
                params![name],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_thread(&row)?)),
            None => Ok(None),
        }
    }

    /// Resolve a non-archived thread by global display name.
    pub async fn find_active_any_by_name(
        &self,
        name: &str,
    ) -> Result<Option<ThreadRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT thread_id, name, project, topic, description, created_by, created_at, archived_at \
                 FROM threads WHERE name = ?1 AND archived_at IS NULL",
                params![name],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_thread(&row)?)),
            None => Ok(None),
        }
    }

    /// Legacy scoped active lookup. Prefer `find_active_any_by_name` for runtime identity.
    pub async fn find_active_by_name(
        &self,
        project: &str,
        name: &str,
    ) -> Result<Option<ThreadRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT thread_id, name, project, topic, description, created_by, created_at, archived_at \
                 FROM threads WHERE project = ?1 AND name = ?2 AND archived_at IS NULL",
                params![project, name],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_thread(&row)?)),
            None => Ok(None),
        }
    }

    /// Archive a named thread. Archived threads keep their registry row and memberships for audit
    /// and future unarchive work, but active reads/routing/search treat them as absent.
    pub async fn archive(&self, name: &str) -> Result<(), NexusError> {
        let row = self
            .find_any_by_name(name)
            .await?
            .ok_or_else(|| NexusError::NotFound(format!("thread:{name}")))?;
        self.store
            .conn
            .execute(
                "UPDATE threads SET archived_at = COALESCE(archived_at, ?2) \
                 WHERE thread_id = ?1",
                params![row.thread_id.0.clone(), now()],
            )
            .await
            .map_err(store_err)?;
        if self.store.has_split_authority() {
            RoutingThreads::new(self.store)
                .archive(&row.thread_id)
                .await?;
        }
        Ok(())
    }

    /// Delete a named thread registry and memberships. Durable message rows are not mutated; once
    /// the registry/membership is gone, scoped history/search can no longer reach those rows.
    pub async fn delete(&self, name: &str) -> Result<(), NexusError> {
        let row = self
            .find_any_by_name(name)
            .await?
            .ok_or_else(|| NexusError::NotFound(format!("thread:{name}")))?;
        self.store
            .conn
            .execute(
                "DELETE FROM thread_members WHERE thread_id = ?1",
                params![row.thread_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        self.store
            .conn
            .execute(
                "DELETE FROM threads WHERE thread_id = ?1",
                params![row.thread_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        if self.store.has_split_authority() {
            RoutingThreads::new(self.store)
                .delete(&row.thread_id)
                .await?;
        }
        Ok(())
    }

    /// Rename a thread's display name. The `thread_id` is preserved, so all members and
    /// messages (which key on `thread_id`, not the name) survive the rename untouched.
    /// Errors if `name` does not exist. Caller enforces admin + collision.
    pub async fn rename(&self, name: &str, new_name: &str) -> Result<(), NexusError> {
        let row = self
            .find_any_by_name(name)
            .await?
            .ok_or_else(|| NexusError::NotFound(format!("thread:{name}")))?;
        self.store
            .conn
            .execute(
                "UPDATE threads SET name = ?2 WHERE thread_id = ?1",
                params![row.thread_id.0.clone(), new_name],
            )
            .await
            .map_err(store_err)?;
        if self.store.has_split_authority() {
            RoutingThreads::new(self.store)
                .rename(&row.thread_id, new_name)
                .await?;
        }
        Ok(())
    }

    /// Add a member (by session name) to a thread. Idempotent on `(thread_id, session_name)`.
    /// Returns `true` only when the membership row was newly inserted.
    pub async fn add_member(
        &self,
        thread_id: &ThreadId,
        session_name: &str,
    ) -> Result<bool, NexusError> {
        let agent_id = Agents::new(self.store)
            .find_by_name(session_name)
            .await?
            .map(|agent| agent.agent_id);
        let changed = self
            .store
            .conn
            .execute(
                "INSERT OR IGNORE INTO thread_members (thread_id, session_name, agent_id, \
                 joined_at) VALUES (?1, ?2, ?3, ?4)",
                params![thread_id.0.clone(), session_name, agent_id.clone(), now()],
            )
            .await
            .map_err(store_err)?;
        if self.store.has_split_authority() {
            RoutingThreads::new(self.store)
                .add_member(thread_id, session_name, agent_id.as_deref())
                .await?;
        }
        Ok(changed > 0)
    }

    /// Rehydrate one boot-scoped routing edge from the persistent continuity capsule.
    ///
    /// Unlike [`Self::add_member`], this keeps the already-resolved stable `agent_id` even when a
    /// display name changed while the daemon was down, and it deliberately does not write back to
    /// the capsule it is reading.
    pub async fn restore_member_ref(
        &self,
        thread_id: &ThreadId,
        session_name: &str,
        agent_id: Option<&str>,
    ) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "INSERT OR IGNORE INTO thread_members
                 (thread_id, session_name, agent_id, joined_at) VALUES (?1, ?2, ?3, ?4)",
                params![thread_id.0.clone(), session_name, agent_id, now()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Remove a member from a thread.
    pub async fn remove_member(
        &self,
        thread_id: &ThreadId,
        session_name: &str,
    ) -> Result<(), NexusError> {
        let agent_id = Agents::new(self.store)
            .find_by_name(session_name)
            .await?
            .map(|agent| agent.agent_id);
        self.store
            .conn
            .execute(
                "DELETE FROM thread_members WHERE thread_id = ?1 AND (session_name = ?2 OR \
                 (?3 IS NOT NULL AND agent_id = ?3))",
                params![thread_id.0.clone(), session_name, agent_id.clone()],
            )
            .await
            .map_err(store_err)?;
        if self.store.has_split_authority() {
            RoutingThreads::new(self.store)
                .remove_member(thread_id, session_name, agent_id.as_deref())
                .await?;
        }
        Ok(())
    }

    /// Remove a member from EVERY thread it belongs to (the `evict` op).
    pub async fn remove_member_all(&self, session_name: &str) -> Result<(), NexusError> {
        let agent_id = Agents::new(self.store)
            .find_by_name(session_name)
            .await?
            .map(|agent| agent.agent_id);
        self.remove_member_all_ref(Some(session_name), agent_id.as_deref())
            .await
    }

    /// Remove every membership matching the stable identity and/or legacy display name.
    /// Explicit-id admin paths pass only `agent_id`, making the mutable request label inert.
    pub async fn remove_member_all_ref(
        &self,
        session_name: Option<&str>,
        agent_id: Option<&str>,
    ) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "DELETE FROM thread_members WHERE (?1 IS NOT NULL AND session_name = ?1) OR \
                 (?2 IS NOT NULL AND agent_id = ?2)",
                params![session_name, agent_id],
            )
            .await
            .map_err(store_err)?;
        if self.store.has_split_authority() {
            RoutingThreads::new(self.store)
                .remove_member_all_ref(session_name, agent_id)
                .await?;
        }
        Ok(())
    }

    /// The member names of a thread for display/roster APIs.
    ///
    /// Routing code should use [`Self::member_refs`] so it can prefer the stable member
    /// `agent_id` and only fall back to `session_name` for legacy rows.
    pub async fn members(&self, thread_id: &ThreadId) -> Result<Vec<String>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT session_name, agent_id FROM thread_members \
                 WHERE thread_id = ?1 ORDER BY joined_at",
                params![thread_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            let fallback = get_text(&row, 0)?;
            let name = match get_opt_text(&row, 1)? {
                Some(agent_id) => Agents::new(self.store)
                    .find_by_id(&agent_id)
                    .await?
                    .and_then(|agent| agent.name)
                    .unwrap_or(fallback),
                None => fallback,
            };
            out.push(name);
        }
        Ok(out)
    }

    /// The stored member identity refs of a thread for routing fan-out.
    ///
    /// New rows carry `agent_id`; fossil rows may still have only `session_name`, so callers must
    /// retain name fallback until the compatibility window closes.
    pub async fn member_refs(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Vec<ThreadMemberRef>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT session_name, agent_id FROM thread_members \
                 WHERE thread_id = ?1 ORDER BY joined_at",
                params![thread_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(ThreadMemberRef {
                session_name: get_text(&row, 0)?,
                agent_id: get_opt_text(&row, 1)?,
            });
        }
        Ok(out)
    }

    /// Whether `session_name` is a member of the thread.
    pub async fn is_member(
        &self,
        thread_id: &ThreadId,
        session_name: &str,
    ) -> Result<bool, NexusError> {
        let agent_id = Agents::new(self.store)
            .find_by_name(session_name)
            .await?
            .map(|agent| agent.agent_id);
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT 1 FROM thread_members WHERE thread_id = ?1 AND (session_name = ?2 OR \
                 (?3 IS NOT NULL AND agent_id = ?3))",
                params![thread_id.0.clone(), session_name, agent_id],
            )
            .await
            .map_err(store_err)?;
        Ok(rows.next().await.map_err(store_err)?.is_some())
    }

    /// Whether two durable agent identities are current co-members of any active thread. Archived
    /// threads do not grant policy, and the query always reads current `thread_members` state so
    /// removing either member immediately revokes this relationship.
    pub async fn share_active_thread_by_agent(
        &self,
        left_agent_id: &str,
        right_agent_id: &str,
    ) -> Result<bool, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT 1 FROM thread_members l \
                 JOIN thread_members r ON r.thread_id = l.thread_id \
                 JOIN threads t ON t.thread_id = l.thread_id \
                 WHERE t.archived_at IS NULL \
                   AND l.agent_id = ?1 AND r.agent_id = ?2 LIMIT 1",
                params![left_agent_id, right_agent_id],
            )
            .await
            .map_err(store_err)?;
        Ok(rows.next().await.map_err(store_err)?.is_some())
    }

    /// List every active thread. The legacy `project` column is returned as metadata.
    pub async fn list(&self) -> Result<Vec<ThreadRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT thread_id, name, project, topic, description, created_by, created_at, archived_at \
                 FROM threads WHERE archived_at IS NULL ORDER BY created_at",
                (),
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_thread(&row)?);
        }
        Ok(out)
    }
}

fn row_to_thread(row: &libsql::Row) -> Result<ThreadRow, NexusError> {
    Ok(ThreadRow {
        thread_id: ThreadId(get_text(row, 0)?),
        name: get_text(row, 1)?,
        project: get_text(row, 2)?,
        topic: crate::repos::sessions::get_opt_text(row, 3)?,
        description: crate::repos::sessions::get_opt_text(row, 4)?,
        created_by: crate::repos::sessions::get_opt_text(row, 5)?,
        created_at: get_opt_int(row, 6)?.unwrap_or(0),
        archived_at: get_opt_int(row, 7)?,
    })
}
