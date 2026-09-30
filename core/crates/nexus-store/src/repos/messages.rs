//! The `messages` repo — the durable, immutable message log (one row per send, even on
//! fan-out). Reads are project-scoped. Provenance is stored as JSON in `messages.provenance`.

use libsql::params;

use nexus_common::NexusError;
use nexus_contracts::enums::Scope;
use nexus_contracts::ids::{MessageId, ProjectId, ThreadId, TopicId};
use nexus_contracts::message::{Message, Provenance};
use nexus_contracts::{GatewayProjectionEffect, GatewayProjectionKind, Kind};

use crate::error::{store_err, store_msg};
use crate::repos::sessions::{get_opt_int, get_opt_text, get_text};
use crate::repos::Agents;
use crate::state::Store;

/// Persistence for the canonical `messages` table.
pub struct Messages<'a> {
    store: &'a Store,
}

/// Map a [`Scope`] to its `messages.kind` token (the spec's `'dm'|'thread'|'topic'`).
fn scope_token(s: Scope) -> &'static str {
    match s {
        Scope::Dm => "dm",
        Scope::Thread => "thread",
        Scope::Topic => "topic",
    }
}
fn token_scope(t: &str) -> Scope {
    match t {
        "thread" => Scope::Thread,
        "topic" => Scope::Topic,
        _ => Scope::Dm,
    }
}

impl<'a> Messages<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        Messages { store }
    }

    /// Insert the canonical message row + its external-content FTS5 entry, returning its id.
    pub async fn insert(&self, msg: &Message) -> Result<MessageId, NexusError> {
        self.insert_with_agents(msg, None, None).await
    }

    /// Insert a message while also storing stable sender/DM recipient identities when the caller
    /// has already resolved them. The legacy [`Message`] shape remains unchanged; reads still
    /// return the display-name contract.
    pub async fn insert_with_agents(
        &self,
        msg: &Message,
        from_agent_id: Option<&str>,
        to_agent_id: Option<&str>,
    ) -> Result<MessageId, NexusError> {
        let provenance_json = serde_json::to_string(&msg.provenance).map_err(store_msg)?;
        let resolved_from_agent_id = match from_agent_id {
            Some(agent_id) => Some(agent_id.to_string()),
            None => Agents::new(self.store)
                .find_by_name(&msg.from)
                .await?
                .map(|agent| agent.agent_id),
        };
        // `messages.to_name` is the display target used by UI/read models: thread/topic rows must
        // carry the named conversation, not the sender.
        let to_name = match msg.scope {
            Scope::Thread => msg.provenance.thread.clone(),
            Scope::Topic => msg.provenance.topic.clone(),
            Scope::Dm => None,
        }
        .unwrap_or_else(|| msg.provenance.from.clone());
        let txn = self.store.begin_write_txn("messages.insert").await?;
        let result = async {
            txn.execute(
                "INSERT INTO messages (message_id, from_name, kind, to_name, thread_id, topic, \
                     summary, body, provenance, project, created_at, from_agent_id, to_agent_id) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    msg.id.0.clone(),
                    msg.from.clone(),
                    scope_token(msg.scope),
                    to_name,
                    msg.thread.as_ref().map(|t| t.0.clone()),
                    msg.topic.as_ref().map(|t| t.0.clone()),
                    msg.summary.clone(),
                    msg.body.clone(),
                    provenance_json,
                    msg.project.0.clone(),
                    msg.created_at,
                    resolved_from_agent_id,
                    to_agent_id.map(str::to_string)
                ],
            )
            .await?;

            // Compatibility stores retain the legacy local FTS index. In split daemon mode,
            // search/history are Gateway-owned and `messages_fts` intentionally does not exist.
            if !self.store.has_split_authority() {
                let rowid = txn.last_insert_rowid();
                txn.execute(
                    "INSERT INTO messages_fts (rowid, summary, body) VALUES (?1, ?2, ?3)",
                    params![rowid, msg.summary.clone(), msg.body.clone()],
                )
                .await?;
            }
            Ok::<(), NexusError>(())
        }
        .await;
        match result {
            Ok(()) => txn.commit().await?,
            Err(error) => {
                txn.rollback(&error).await?;
                return Err(error);
            }
        }
        Ok(msg.id.clone())
    }

    /// Fetch a message by id, scoped to the project. `None` if missing or in another project.
    pub async fn get(&self, project: &str, id: &MessageId) -> Result<Option<Message>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT message_id, from_name, kind, thread_id, topic, summary, body, \
                 provenance, project, created_at FROM messages \
                 WHERE message_id = ?1 AND project = ?2",
                params![id.0.clone(), project],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_message(&row)?)),
            None => Ok(None),
        }
    }

    /// Build the immutable Gateway facts for one already-committed message.
    ///
    /// This is called synchronously from the domain commit boundary; it is not a poller or a
    /// recovery scan. The stable message id is the projection idempotency key, while `project`
    /// remains ordinary metadata rather than a routing or authorization partition.
    pub async fn gateway_projection_effects(
        &self,
        id: &MessageId,
    ) -> Result<Vec<GatewayProjectionEffect>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT message_id, from_name, from_agent_id, to_name, to_agent_id, kind, \
                 thread_id, topic, summary, body, provenance, project, created_at, metadata_json, \
                 mention_json \
                 FROM messages WHERE message_id = ?1 LIMIT 1",
                params![id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        let Some(row) = rows.next().await.map_err(store_err)? else {
            return Ok(Vec::new());
        };
        let message_id = get_text(&row, 0)?;
        let provenance_raw = get_text(&row, 10)?;
        let provenance: Provenance = serde_json::from_str(&provenance_raw).map_err(store_msg)?;
        let occurred_at = get_opt_int(&row, 12)?.unwrap_or(0);
        let metadata = get_opt_text(&row, 13)?
            .map(|raw| serde_json::from_str::<serde_json::Value>(&raw))
            .transpose()
            .map_err(store_msg)?
            .unwrap_or_else(|| serde_json::json!({}));
        let mention = get_opt_text(&row, 14)?
            .map(|raw| serde_json::from_str::<Vec<String>>(&raw))
            .transpose()
            .map_err(store_msg)?
            .unwrap_or_default();
        let payload = serde_json::json!({
            "messageId": message_id,
            "fromName": get_text(&row, 1)?,
            "fromAgentId": get_opt_text(&row, 2)?,
            "toName": get_opt_text(&row, 3)?,
            "toAgentId": get_opt_text(&row, 4)?,
            "scope": get_text(&row, 5)?,
            "threadId": get_opt_text(&row, 6)?,
            "topic": get_opt_text(&row, 7)?,
            "summary": get_opt_text(&row, 8)?,
            "body": get_text(&row, 9)?,
            "provenance": serde_json::from_str::<serde_json::Value>(&provenance_raw)
                .map_err(store_msg)?,
            "project": get_text(&row, 11)?,
            "createdAt": occurred_at,
            "metadata": metadata,
            "mention": mention,
        });
        let mut effects = vec![GatewayProjectionEffect {
            event_id: format!("message:{message_id}"),
            occurred_at,
            kind: GatewayProjectionKind::MessageAccepted,
            payload: payload.clone(),
        }];
        if provenance.kind == Kind::Notification {
            effects.push(GatewayProjectionEffect {
                event_id: format!("notification:{message_id}"),
                occurred_at,
                kind: GatewayProjectionKind::NotificationEmitted,
                payload,
            });
        }
        Ok(effects)
    }

    /// Map a row whose message columns start at index 1 (used by joined queries that select an
    /// extra column at index 0, e.g. the `in_flight` join in [`Inbox::pending_for`]). The column
    /// order is: `_extra, message_id, from_name, kind, thread_id, topic, summary, body,
    /// provenance, project, created_at`.
    ///
    /// [`Inbox::pending_for`]: crate::repos::inbox::Inbox::pending_for
    pub(crate) fn row_to_message_joined(row: &libsql::Row) -> Result<Message, NexusError> {
        let provenance: Provenance = serde_json::from_str(&get_text(row, 8)?).map_err(store_msg)?;
        Ok(Message {
            id: MessageId(get_text(row, 1)?),
            from: get_text(row, 2)?,
            scope: token_scope(&get_text(row, 3)?),
            thread: get_opt_text(row, 4)?.map(ThreadId),
            topic: get_opt_text(row, 5)?.map(TopicId),
            summary: get_opt_text(row, 6)?,
            body: get_text(row, 7)?,
            provenance,
            project: ProjectId(get_text(row, 9)?),
            created_at: get_opt_int(row, 10)?.unwrap_or(0),
        })
    }
}

fn row_to_message(row: &libsql::Row) -> Result<Message, NexusError> {
    let provenance: Provenance = serde_json::from_str(&get_text(row, 7)?).map_err(store_msg)?;
    Ok(Message {
        id: MessageId(get_text(row, 0)?),
        from: get_text(row, 1)?,
        scope: token_scope(&get_text(row, 2)?),
        thread: get_opt_text(row, 3)?.map(ThreadId),
        topic: get_opt_text(row, 4)?.map(TopicId),
        summary: get_opt_text(row, 5)?,
        body: get_text(row, 6)?,
        provenance,
        project: ProjectId(get_text(row, 8)?),
        created_at: get_opt_int(row, 9)?.unwrap_or(0),
    })
}
