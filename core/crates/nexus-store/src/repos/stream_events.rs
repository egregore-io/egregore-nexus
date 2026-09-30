//! The `stream_events` repo — the volatile ACP/session stream lane.
//!
//! The daemon appends one row per `agent.update` (text / thinking / tool_call / plan / commands /
//! turn_end) as the turn streams. The rows live in the attached volatile `mem.stream_events`
//! database (tmpfs for local stores, anonymous memory for pure test stores) so token deltas never
//! fsync into the durable Nexus file. Final `/agent` history is materialized into
//! `agent_session_messages` at turn end.

use libsql::params;

use nexus_common::{now, NexusError};
use nexus_contracts::ids::SessionId;

use crate::error::store_err;
use crate::repos::{DeveloperEvents, Sessions};
use crate::state::Store;

/// One volatile stream event (as read back from `mem.stream_events`).
#[derive(Debug, Clone, PartialEq)]
pub struct StreamEventRow {
    pub id: i64,
    pub session_id: String,
    pub kind: String,
    pub data: String,
    pub created_at: i64,
}

/// Accessor for the attached volatile `mem.stream_events` table.
pub struct StreamEvents<'a> {
    store: &'a Store,
}

impl<'a> StreamEvents<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        StreamEvents { store }
    }

    /// Append one `agent.update` to the volatile lane (`created_at = now()`) and return its row id.
    ///
    /// The durable `main` database must never receive these token-level rows; completed turns are
    /// recorded separately in `agent_session_messages`.
    /// `kind` is the wire `AgentUpdateKind` (`text`/`thinking`/…/`turn_end`); `data` is the
    /// JSON-encoded payload.
    pub async fn append(
        &self,
        session_id: &SessionId,
        kind: &str,
        data: &str,
    ) -> Result<i64, NexusError> {
        let ts = now();
        self.store
            .stream_conn()
            .execute(
                "INSERT INTO mem.stream_events (session_id, kind, data, created_at) \
                 VALUES (?1, ?2, ?3, ?4)",
                params![session_id.0.clone(), kind.to_string(), data.to_string(), ts],
            )
            .await
            .map_err(store_err)?;
        let id = self.store.stream_conn().last_insert_rowid();
        self.store.events().stream_row_appended().signal();
        if kind == "turn_end" {
            self.append_turn_end_lifecycle(session_id, ts).await?;
            self.store.events().turn_end_appended().signal();
        }
        Ok(id)
    }

    /// Read a session's volatile events with `id` greater than `after_id`, in order. `after_id = 0`
    /// returns the whole active in-memory turn buffer.
    pub async fn since(
        &self,
        session_id: &SessionId,
        after_id: i64,
    ) -> Result<Vec<StreamEventRow>, NexusError> {
        let mut rows = self
            .store
            .stream_conn()
            .query(
                "SELECT id, session_id, kind, data, created_at FROM mem.stream_events \
                 WHERE session_id = ?1 AND id > ?2 ORDER BY id ASC",
                params![session_id.0.clone(), after_id],
            )
            .await
            .map_err(store_err)?;

        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(StreamEventRow {
                id: row.get(0).map_err(store_err)?,
                session_id: row.get(1).map_err(store_err)?,
                kind: row.get(2).map_err(store_err)?,
                data: row.get(3).map_err(store_err)?,
                created_at: row.get(4).map_err(store_err)?,
            });
        }
        Ok(out)
    }

    /// Evict a session's volatile rows up through `through_id`, normally after `turn_end` has
    /// committed the finalized `/agent` turn to the durable projection.
    pub async fn evict_session_through(
        &self,
        session_id: &SessionId,
        through_id: i64,
    ) -> Result<u64, NexusError> {
        self.store
            .stream_conn()
            .execute(
                "DELETE FROM mem.stream_events WHERE session_id = ?1 AND id <= ?2",
                params![session_id.0.clone(), through_id],
            )
            .await
            .map_err(store_err)
    }

    async fn append_turn_end_lifecycle(
        &self,
        session_id: &SessionId,
        created_at: i64,
    ) -> Result<(), NexusError> {
        let Some(row) = Sessions::new(self.store)
            .find_by_session_id(session_id)
            .await?
        else {
            return Ok(());
        };
        if let Some(name) = row.name.as_deref() {
            DeveloperEvents::new(self.store)
                .append_agent_lifecycle(name, session_id, "turn_end", None, created_at)
                .await?;
        }
        Ok(())
    }
}
