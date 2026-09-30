//! Metadata-only developer event rows.
//!
//! The developer event surface is intentionally separate from Message Post fan-out and realtime
//! bells. Repos here allocate per-topic sequence numbers and store no-body envelopes for reserved
//! `sys.*` topics; they never wake an agent loop or enqueue a turn.

use libsql::params;

use nexus_common::NexusError;
use nexus_contracts::ids::SessionId;

use crate::error::store_err;
use crate::repos::sessions::{get_opt_int, get_opt_text, get_text};
use crate::state::Store;

/// Reserved lifecycle topic exposed to developer tooling.
pub const AGENT_LIFECYCLE_TOPIC: &str = "sys.agent.lifecycle";

/// One persisted developer event envelope row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeveloperEventRow {
    pub topic: String,
    pub seq: i64,
    pub kind: String,
    pub message_id: Option<String>,
    pub thread_name: Option<String>,
    pub dm_name: Option<String>,
    pub from_name: Option<String>,
    pub agent_name: Option<String>,
    pub session_id: Option<String>,
    pub lifecycle: Option<String>,
    pub current_work: Option<String>,
    pub data_json: Option<String>,
    pub created_at: i64,
}

/// Fields for creating a developer event row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewDeveloperEvent {
    pub topic: String,
    pub kind: String,
    pub message_id: Option<String>,
    pub thread_name: Option<String>,
    pub dm_name: Option<String>,
    pub from_name: Option<String>,
    pub agent_name: Option<String>,
    pub session_id: Option<String>,
    pub lifecycle: Option<String>,
    pub current_work: Option<String>,
    pub data_json: Option<String>,
    pub created_at: i64,
}

/// Persistence for `developer_event_topics` and `developer_events`.
pub struct DeveloperEvents<'a> {
    store: &'a Store,
}

impl<'a> DeveloperEvents<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    /// Append a metadata-only event and return its per-topic sequence number.
    pub async fn append(&self, event: NewDeveloperEvent) -> Result<i64, NexusError> {
        // Keep topic-sequence allocation and event insertion in one daemon-owned transaction.
        let txn = self.store.begin_write_txn("developer_event_append").await?;
        let result = async {
            txn.execute(
                "INSERT INTO developer_event_topics (topic, latest_seq, updated_at) \
                 VALUES (?1, 0, ?2) \
                 ON CONFLICT(topic) DO UPDATE SET updated_at = excluded.updated_at",
                params![event.topic.clone(), event.created_at],
            )
            .await?;
            txn.execute(
                "UPDATE developer_event_topics \
                 SET latest_seq = latest_seq + 1, updated_at = ?2 WHERE topic = ?1",
                params![event.topic.clone(), event.created_at],
            )
            .await?;
            let mut rows = txn
                .query(
                    "SELECT latest_seq FROM developer_event_topics WHERE topic = ?1",
                    params![event.topic.clone()],
                )
                .await?;
            let seq = match rows.next().await.map_err(store_err)? {
                Some(row) => required_int(&row, 0)?,
                None => 0,
            };
            drop(rows);
            txn.execute(
                "INSERT INTO developer_events (topic, seq, kind, message_id, thread_name, \
                 dm_name, from_name, agent_name, session_id, lifecycle, current_work, \
                 data_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, \
                 ?11, ?12, ?13)",
                params![
                    event.topic,
                    seq,
                    event.kind,
                    event.message_id,
                    event.thread_name,
                    event.dm_name,
                    event.from_name,
                    event.agent_name,
                    event.session_id,
                    event.lifecycle,
                    event.current_work,
                    event.data_json,
                    event.created_at
                ],
            )
            .await?;
            Ok(seq)
        }
        .await;
        match result {
            Ok(seq) => {
                txn.commit().await?;
                self.store.events().developer_event_appended().signal();
                Ok(seq)
            }
            Err(error) => {
                txn.rollback(&error).await?;
                Err(error)
            }
        }
    }

    /// Append an agent lifecycle/work event to [`AGENT_LIFECYCLE_TOPIC`].
    ///
    /// Lifecycle envelopes are metadata-only: they carry the agent/session identity and phase
    /// (`register`, `resume`, `attach`, `started`, `stopped`, `offline`, `turn_end`, or
    /// `current_work`) without message bodies or turn payloads. They are observational cursors for
    /// tooling and never feed the turn injector.
    pub async fn append_agent_lifecycle(
        &self,
        agent_name: &str,
        session_id: &SessionId,
        lifecycle: &str,
        current_work: Option<&str>,
        created_at: i64,
    ) -> Result<i64, NexusError> {
        self.append_agent_lifecycle_with_data(
            agent_name,
            session_id,
            lifecycle,
            current_work,
            None,
            created_at,
        )
        .await
    }

    /// Append an agent lifecycle event with optional metadata JSON.
    pub async fn append_agent_lifecycle_with_data(
        &self,
        agent_name: &str,
        session_id: &SessionId,
        lifecycle: &str,
        current_work: Option<&str>,
        data_json: Option<&str>,
        created_at: i64,
    ) -> Result<i64, NexusError> {
        self.append(NewDeveloperEvent {
            topic: AGENT_LIFECYCLE_TOPIC.to_string(),
            kind: "agent_lifecycle".to_string(),
            message_id: None,
            thread_name: None,
            dm_name: None,
            from_name: None,
            agent_name: Some(agent_name.to_string()),
            session_id: Some(session_id.0.clone()),
            lifecycle: Some(lifecycle.to_string()),
            current_work: current_work.map(str::to_string),
            data_json: data_json.map(str::to_string),
            created_at,
        })
        .await
    }

    /// Append a metadata-only command/action event.
    ///
    /// Action events cover state-changing commands that are not messages, agent lifecycle, or
    /// tool-call observations. The caller owns best-effort policy: this method returns append
    /// errors so command handlers can log and continue without failing the state change.
    pub async fn append_action(
        &self,
        topic: &str,
        action: &str,
        from_name: Option<&str>,
        thread_name: Option<&str>,
        agent_name: Option<&str>,
        session_id: Option<&str>,
        mut data: serde_json::Value,
        created_at: i64,
    ) -> Result<i64, NexusError> {
        match &mut data {
            serde_json::Value::Object(map) => {
                map.entry("action")
                    .or_insert_with(|| serde_json::Value::String(action.to_string()));
            }
            _ => {
                data = serde_json::json!({ "action": action, "value": data });
            }
        }
        let data_json =
            serde_json::to_string(&data).map_err(|e| NexusError::Store(e.to_string()))?;
        self.append(NewDeveloperEvent {
            topic: topic.to_string(),
            kind: "action".to_string(),
            message_id: None,
            thread_name: thread_name.map(str::to_string),
            dm_name: None,
            from_name: from_name.map(str::to_string),
            agent_name: agent_name.map(str::to_string),
            session_id: session_id.map(str::to_string),
            lifecycle: None,
            current_work: None,
            data_json: Some(data_json),
            created_at,
        })
        .await
    }

    /// Return the latest sequence number for a reserved developer topic.
    pub async fn latest_seq(&self, topic: &str) -> Result<i64, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT latest_seq FROM developer_event_topics WHERE topic = ?1",
                params![topic],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => required_int(&row, 0),
            None => Ok(0),
        }
    }

    /// Return developer events after a per-topic sequence cursor.
    pub async fn since(
        &self,
        topic: &str,
        after_seq: i64,
    ) -> Result<Vec<DeveloperEventRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT topic, seq, kind, message_id, thread_name, dm_name, from_name, \
                 agent_name, session_id, lifecycle, current_work, data_json, created_at \
                 FROM developer_events WHERE topic = ?1 AND seq > ?2 ORDER BY seq",
                params![topic, after_seq],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_event(&row)?);
        }
        Ok(out)
    }
}

fn row_to_event(row: &libsql::Row) -> Result<DeveloperEventRow, NexusError> {
    Ok(DeveloperEventRow {
        topic: get_text(row, 0)?,
        seq: required_int(row, 1)?,
        kind: get_text(row, 2)?,
        message_id: get_opt_text(row, 3)?,
        thread_name: get_opt_text(row, 4)?,
        dm_name: get_opt_text(row, 5)?,
        from_name: get_opt_text(row, 6)?,
        agent_name: get_opt_text(row, 7)?,
        session_id: get_opt_text(row, 8)?,
        lifecycle: get_opt_text(row, 9)?,
        current_work: get_opt_text(row, 10)?,
        data_json: get_opt_text(row, 11)?,
        created_at: required_int(row, 12)?,
    })
}

fn required_int(row: &libsql::Row, idx: i32) -> Result<i64, NexusError> {
    Ok(get_opt_int(row, idx)?.unwrap_or_default())
}
