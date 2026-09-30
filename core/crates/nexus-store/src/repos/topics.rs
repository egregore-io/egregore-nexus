//! The `topics` + `subscriptions` repos — pub/sub. Subscribers drain via cursors; a `sub_group`
//! makes a competing-consumer group. Project-scoped.

use libsql::params;

use nexus_common::{now, NexusError};

use crate::error::store_err;
use crate::repos::sessions::{get_opt_int, get_opt_text, get_text};
use crate::repos::AgentRuntimes;
use crate::state::Store;

/// A stored topic subscription edge.
///
/// `subscriber_agent_id` is the stable delivery key when available; `subscriber_session` remains
/// the compatibility runtime snapshot for legacy rows and immediate wake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicSubscriberRef {
    pub subscriber_session: String,
    pub subscriber_agent_id: Option<String>,
}

/// Persistence for `topics` + `subscriptions`.
pub struct Topics<'a> {
    store: &'a Store,
}

impl<'a> Topics<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        Topics { store }
    }

    /// Ensure a topic row exists in a project (idempotent).
    pub async fn ensure(&self, topic: &str, project: &str) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "INSERT OR IGNORE INTO topics (topic, project, created_at) VALUES (?1, ?2, ?3)",
                params![topic, project, now()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Whether a globally named topic exists. `project` is descriptive metadata and never gates
    /// publish routing.
    pub async fn exists(&self, topic: &str) -> Result<bool, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT 1 FROM topics WHERE topic = ?1 LIMIT 1",
                params![topic],
            )
            .await
            .map_err(store_err)?;
        Ok(rows.next().await.map_err(store_err)?.is_some())
    }

    /// Subscribe a session to a topic (idempotent on `(topic, subscriber_session)`); returns the
    /// subscriber's current cursor.
    pub async fn subscribe(
        &self,
        topic: &str,
        session: &str,
        group: Option<&str>,
    ) -> Result<i64, NexusError> {
        let agent_id = AgentRuntimes::new(self.store)
            .find_by_runtime_id(session)
            .await?
            .map(|runtime| runtime.agent_id);
        self.store
            .conn
            .execute(
                "INSERT OR IGNORE INTO subscriptions \
                 (topic, subscriber_session, subscriber_agent_id, sub_group, cursor, \
                 subscribed_at) VALUES (?1, ?2, ?3, ?4, 0, ?5)",
                params![topic, session, agent_id.clone(), group, now()],
            )
            .await
            .map_err(store_err)?;
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT cursor FROM subscriptions WHERE topic = ?1 AND (subscriber_session = ?2 OR \
                 (?3 IS NOT NULL AND subscriber_agent_id = ?3))",
                params![topic, session, agent_id],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(get_opt_int(&row, 0)?.unwrap_or(0)),
            None => Ok(0),
        }
    }

    /// Unsubscribe a session from a topic.
    pub async fn unsubscribe(&self, topic: &str, session: &str) -> Result<(), NexusError> {
        let agent_id = AgentRuntimes::new(self.store)
            .find_by_runtime_id(session)
            .await?
            .map(|runtime| runtime.agent_id);
        self.store
            .conn
            .execute(
                "DELETE FROM subscriptions WHERE topic = ?1 AND (subscriber_session = ?2 OR \
                 (?3 IS NOT NULL AND subscriber_agent_id = ?3))",
                params![topic, session, agent_id],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// The subscriber session snapshots of a topic for counts and compatibility views.
    ///
    /// Routing code should use [`Self::subscriber_refs`] so it can carry
    /// `subscriber_agent_id` into durable delivery rows even when the session snapshot is stale.
    pub async fn subscribers(&self, topic: &str) -> Result<Vec<String>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT subscriber_session, subscriber_agent_id FROM subscriptions \
             WHERE topic = ?1 ORDER BY subscribed_at",
                params![topic],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            let fallback = get_text(&row, 0)?;
            let runtime_id = match get_opt_text(&row, 1)? {
                Some(agent_id) => AgentRuntimes::new(self.store)
                    .active_for_agent(&agent_id)
                    .await?
                    .map(|runtime| runtime.runtime_id)
                    .unwrap_or(fallback),
                None => fallback,
            };
            out.push(runtime_id);
        }
        Ok(out)
    }

    /// The stored subscriber refs of a topic for routing fan-out.
    ///
    /// New rows carry `subscriber_agent_id`; fossil rows may still have only
    /// `subscriber_session`, so callers must retain session fallback until the compatibility
    /// window closes.
    pub async fn subscriber_refs(
        &self,
        topic: &str,
    ) -> Result<Vec<TopicSubscriberRef>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT subscriber_session, subscriber_agent_id FROM subscriptions \
                 WHERE topic = ?1 ORDER BY subscribed_at",
                params![topic],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(TopicSubscriberRef {
                subscriber_session: get_text(&row, 0)?,
                subscriber_agent_id: get_opt_text(&row, 1)?,
            });
        }
        Ok(out)
    }

    /// Advance a subscriber's cursor to `cursor`.
    pub async fn advance_cursor(
        &self,
        topic: &str,
        session: &str,
        cursor: i64,
    ) -> Result<(), NexusError> {
        let agent_id = AgentRuntimes::new(self.store)
            .find_by_runtime_id(session)
            .await?
            .map(|runtime| runtime.agent_id);
        self.store
            .conn
            .execute(
                "UPDATE subscriptions SET cursor = ?3 WHERE topic = ?1 AND (subscriber_session = ?2 \
                 OR (?4 IS NOT NULL AND subscriber_agent_id = ?4))",
                params![topic, session, cursor, agent_id],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// List every topic in a project.
    pub async fn list(&self, project: &str) -> Result<Vec<String>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT topic FROM topics WHERE project = ?1 ORDER BY created_at",
                params![project],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(get_text(&row, 0)?);
        }
        Ok(out)
    }
}
