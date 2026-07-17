//! Materialized `/agent` session history.
//!
//! Volatile `mem.stream_events` are optimized for live/debug streaming. This repo owns the
//! reconciled turn and message projection that the gateway can read without replaying every
//! token-level event.

use libsql::params;

use nexus_common::{now, NexusError};
use nexus_contracts::ids::SessionId;

use crate::error::store_err;
use crate::repos::sessions::{get_opt_int, get_opt_text, get_text};
use crate::state::Store;

/// One materialized turn for a session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSessionTurnRow {
    pub id: String,
    pub session_id: String,
    pub status: String,
    pub first_stream_event_id: i64,
    pub last_stream_event_id: i64,
    pub started_at: i64,
    pub updated_at: i64,
    pub finalized_at: Option<i64>,
}

/// One materialized message row inside a session turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSessionMessageRow {
    pub id: String,
    pub session_id: String,
    pub turn_id: String,
    pub ordinal: i64,
    pub role: String,
    pub author: Option<String>,
    pub content_json: String,
    pub status: String,
    pub first_stream_event_id: i64,
    pub last_stream_event_id: i64,
    pub created_at: i64,
    pub updated_at: i64,
    pub finalized_at: Option<i64>,
}

/// Fields needed to upsert a materialized message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewAgentSessionMessage {
    pub id: String,
    pub session_id: String,
    pub turn_id: String,
    pub ordinal: i64,
    pub role: String,
    pub author: Option<String>,
    pub content_json: String,
    pub status: String,
    pub first_stream_event_id: i64,
    pub last_stream_event_id: i64,
    pub created_at: i64,
    pub updated_at: i64,
    pub finalized_at: Option<i64>,
}

/// Persistence for materialized agent-session turns, messages, and stream cursors.
pub struct AgentSessionMessages<'a> {
    store: &'a Store,
}

impl<'a> AgentSessionMessages<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        AgentSessionMessages { store }
    }

    /// Return the current streaming turn for `session_id`, or create one starting at `event_id`.
    ///
    /// The generated turn id is deterministic from the session and first raw event id so replaying
    /// the same raw event converges on the same turn row.
    pub async fn begin_or_get_open_turn(
        &self,
        session_id: &SessionId,
        event_id: i64,
        ts: i64,
    ) -> Result<AgentSessionTurnRow, NexusError> {
        if let Some(turn) = self.open_turn(session_id).await? {
            self.store
                .conn
                .execute(
                    "UPDATE agent_session_turns \
                     SET last_stream_event_id = MAX(last_stream_event_id, ?2), updated_at = ?3 \
                     WHERE id = ?1",
                    params![turn.id.clone(), event_id, ts],
                )
                .await
                .map_err(store_err)?;
            return self
                .turn_by_id(&turn.id)
                .await?
                .ok_or_else(|| NexusError::NotFound(format!("agent_session_turn:{}", turn.id)));
        }

        let id = turn_id(session_id, event_id);
        self.store
            .conn
            .execute(
                "INSERT OR IGNORE INTO agent_session_turns (id, session_id, status, \
                 first_stream_event_id, last_stream_event_id, started_at, updated_at, \
                 finalized_at) VALUES (?1, ?2, 'streaming', ?3, ?3, ?4, ?4, NULL)",
                params![id.clone(), session_id.0.clone(), event_id, ts],
            )
            .await
            .map_err(store_err)?;
        self.turn_by_id(&id)
            .await?
            .ok_or_else(|| NexusError::NotFound(format!("agent_session_turn:{id}")))
    }

    /// Insert or update a materialized message. The `(turn_id, ordinal)` key is the replay
    /// idempotency boundary; content/status is replaced with the latest reconciliation result.
    pub async fn upsert_message(&self, row: NewAgentSessionMessage) -> Result<(), NexusError> {
        let tx = self
            .store
            .begin_write_txn("agent_session_message_upsert")
            .await?;
        tx.execute(
            "INSERT INTO agent_session_messages (id, session_id, turn_id, ordinal, role, author, \
             content_json, status, first_stream_event_id, last_stream_event_id, created_at, \
             updated_at, finalized_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, \
             ?12, ?13) ON CONFLICT(turn_id, ordinal) DO UPDATE SET role = excluded.role, \
             author = excluded.author, content_json = excluded.content_json, status = \
             excluded.status, first_stream_event_id = MIN(agent_session_messages.first_stream_event_id, \
             excluded.first_stream_event_id), last_stream_event_id = \
             MAX(agent_session_messages.last_stream_event_id, excluded.last_stream_event_id), \
             updated_at = excluded.updated_at, finalized_at = excluded.finalized_at",
            params![
                row.id,
                row.session_id.clone(),
                row.turn_id.clone(),
                row.ordinal,
                row.role,
                row.author,
                row.content_json,
                row.status,
                row.first_stream_event_id,
                row.last_stream_event_id,
                row.created_at,
                row.updated_at,
                row.finalized_at
            ],
        )
        .await?;
        tx.execute(
            "UPDATE agent_session_turns \
             SET last_stream_event_id = MAX(last_stream_event_id, ?2), updated_at = ?3 \
             WHERE id = ?1",
            params![row.turn_id, row.last_stream_event_id, row.updated_at],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Finalize the open turn for a session and any still-streaming messages in that turn.
    pub async fn finalize_turn(
        &self,
        session_id: &SessionId,
        event_id: i64,
        ts: i64,
        status: &str,
    ) -> Result<Option<AgentSessionTurnRow>, NexusError> {
        let Some(open) = self.open_turn(session_id).await? else {
            return self.latest_turn(session_id).await;
        };

        let tx = self
            .store
            .begin_write_txn("agent_session_turn_finalize")
            .await?;
        tx.execute(
            "UPDATE agent_session_turns SET status = ?2, last_stream_event_id = \
             MAX(last_stream_event_id, ?3), updated_at = ?4, finalized_at = ?4 WHERE id = ?1",
            params![open.id.clone(), status, event_id, ts],
        )
        .await?;
        tx.execute(
            "UPDATE agent_session_messages SET status = ?2, last_stream_event_id = \
             MAX(last_stream_event_id, ?3), updated_at = ?4, finalized_at = ?4 \
             WHERE turn_id = ?1 AND status = 'streaming'",
            params![open.id.clone(), status, event_id, ts],
        )
        .await?;
        tx.commit().await?;
        self.turn_by_id(&open.id).await
    }

    /// Abort open turns whose session/runtime has been marked offline by daemon reconcile.
    ///
    /// This is the materialized-history half of stale-presence repair: once the daemon decides a
    /// session is dead, any still-streaming `/agent` turn for that session must become a finalized
    /// aborted turn instead of hanging forever.
    pub async fn abort_open_turns_for_offline_sessions(&self, ts: i64) -> Result<u64, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT t.id FROM agent_session_turns t \
                 WHERE t.status = 'streaming' AND ( \
                   EXISTS ( \
                     SELECT 1 FROM sessions s \
                     WHERE s.session_id = t.session_id \
                       AND COALESCE(s.presence, 'offline') = 'offline' \
                   ) \
                   OR EXISTS ( \
                     SELECT 1 FROM agent_runtimes r \
                     WHERE r.runtime_id = t.session_id \
                       AND (r.active = 0 OR COALESCE(r.presence, 'offline') = 'offline') \
                   ) \
                 )",
                params![],
            )
            .await
            .map_err(store_err)?;
        let mut turn_ids = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            turn_ids.push(get_text(&row, 0)?);
        }
        drop(rows);

        if turn_ids.is_empty() {
            return Ok(0);
        }

        let tx = self
            .store
            .begin_write_txn("agent_session_turn_abort_offline")
            .await?;
        for turn_id in &turn_ids {
            tx.execute(
                "UPDATE agent_session_turns \
                 SET status = 'aborted', updated_at = ?2, finalized_at = ?2 \
                 WHERE id = ?1 AND status = 'streaming'",
                params![turn_id.clone(), ts],
            )
            .await?;
            tx.execute(
                "UPDATE agent_session_messages \
                 SET status = 'aborted', updated_at = ?2, finalized_at = ?2 \
                 WHERE turn_id = ?1 AND status = 'streaming'",
                params![turn_id.clone(), ts],
            )
            .await?;
        }
        tx.commit().await?;
        Ok(turn_ids.len() as u64)
    }

    /// Return the newest `limit` materialized messages for a session in chronological order.
    pub async fn messages_for_session(
        &self,
        session_id: &SessionId,
        limit: i64,
    ) -> Result<Vec<AgentSessionMessageRow>, NexusError> {
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let mut rows = self
            .store
            .conn
            .query(
                &format!(
                    "SELECT * FROM ({SELECT_MESSAGE} WHERE session_id = ?1 \
                     ORDER BY created_at DESC, ordinal DESC LIMIT ?2) \
                     ORDER BY created_at ASC, ordinal ASC"
                ),
                params![session_id.0.clone(), limit],
            )
            .await
            .map_err(store_err)?;

        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_message(&row)?);
        }
        Ok(out)
    }

    /// Return one materialized message by turn ordinal.
    pub async fn message_for_turn_ordinal(
        &self,
        turn_id: &str,
        ordinal: i64,
    ) -> Result<Option<AgentSessionMessageRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                &format!("{SELECT_MESSAGE} WHERE turn_id = ?1 AND ordinal = ?2"),
                params![turn_id, ordinal],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_message(&row)?)),
            None => Ok(None),
        }
    }

    /// Return the last materialized raw stream event id for a session.
    pub async fn cursor_for_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<i64>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT last_materialized_stream_event_id FROM agent_session_stream_cursors \
                 WHERE session_id = ?1",
                params![session_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => get_opt_int(&row, 0),
            None => Ok(None),
        }
    }

    /// Upsert the materialized cursor for a session.
    pub async fn set_cursor(
        &self,
        session_id: &SessionId,
        event_id: i64,
    ) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "INSERT INTO agent_session_stream_cursors (session_id, \
                 last_materialized_stream_event_id, updated_at) VALUES (?1, ?2, ?3) \
                 ON CONFLICT(session_id) DO UPDATE SET last_materialized_stream_event_id = \
                 MAX(last_materialized_stream_event_id, excluded.last_materialized_stream_event_id), \
                 updated_at = excluded.updated_at",
                params![session_id.0.clone(), event_id, now()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Delete volatile stream events older than `keep_after_id` for one session.
    ///
    /// This is intentionally unused until retention is wired after materialization/backfill.
    pub async fn prune_stream_events_before(
        &self,
        session_id: &SessionId,
        keep_after_id: i64,
    ) -> Result<u64, NexusError> {
        self.store
            .conn
            .execute(
                "DELETE FROM mem.stream_events WHERE session_id = ?1 AND id < ?2",
                params![session_id.0.clone(), keep_after_id],
            )
            .await
            .map_err(store_err)
    }

    async fn open_turn(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<AgentSessionTurnRow>, NexusError> {
        self.query_turn(
            "WHERE session_id = ?1 AND status = 'streaming' ORDER BY started_at DESC LIMIT 1",
            params![session_id.0.clone()],
        )
        .await
    }

    async fn latest_turn(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<AgentSessionTurnRow>, NexusError> {
        self.query_turn(
            "WHERE session_id = ?1 ORDER BY updated_at DESC LIMIT 1",
            params![session_id.0.clone()],
        )
        .await
    }

    async fn turn_by_id(&self, id: &str) -> Result<Option<AgentSessionTurnRow>, NexusError> {
        self.query_turn("WHERE id = ?1", params![id]).await
    }

    async fn query_turn(
        &self,
        where_clause: &str,
        p: impl libsql::params::IntoParams,
    ) -> Result<Option<AgentSessionTurnRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(&format!("{SELECT_TURN} {where_clause}"), p)
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_turn(&row)?)),
            None => Ok(None),
        }
    }
}

const SELECT_TURN: &str = "SELECT id, session_id, status, first_stream_event_id, \
     last_stream_event_id, started_at, updated_at, finalized_at FROM agent_session_turns";

const SELECT_MESSAGE: &str =
    "SELECT id, session_id, turn_id, ordinal, role, author, content_json, \
     status, first_stream_event_id, last_stream_event_id, created_at, updated_at, finalized_at \
     FROM agent_session_messages";

fn turn_id(session_id: &SessionId, event_id: i64) -> String {
    format!("turn_{}_{}", session_id.0, event_id)
}

fn row_to_turn(row: &libsql::Row) -> Result<AgentSessionTurnRow, NexusError> {
    Ok(AgentSessionTurnRow {
        id: get_text(row, 0)?,
        session_id: get_text(row, 1)?,
        status: get_text(row, 2)?,
        first_stream_event_id: get_opt_int(row, 3)?.unwrap_or(0),
        last_stream_event_id: get_opt_int(row, 4)?.unwrap_or(0),
        started_at: get_opt_int(row, 5)?.unwrap_or(0),
        updated_at: get_opt_int(row, 6)?.unwrap_or(0),
        finalized_at: get_opt_int(row, 7)?,
    })
}

fn row_to_message(row: &libsql::Row) -> Result<AgentSessionMessageRow, NexusError> {
    Ok(AgentSessionMessageRow {
        id: get_text(row, 0)?,
        session_id: get_text(row, 1)?,
        turn_id: get_text(row, 2)?,
        ordinal: get_opt_int(row, 3)?.unwrap_or(0),
        role: get_text(row, 4)?,
        author: get_opt_text(row, 5)?,
        content_json: get_text(row, 6)?,
        status: get_text(row, 7)?,
        first_stream_event_id: get_opt_int(row, 8)?.unwrap_or(0),
        last_stream_event_id: get_opt_int(row, 9)?.unwrap_or(0),
        created_at: get_opt_int(row, 10)?.unwrap_or(0),
        updated_at: get_opt_int(row, 11)?.unwrap_or(0),
        finalized_at: get_opt_int(row, 12)?,
    })
}
