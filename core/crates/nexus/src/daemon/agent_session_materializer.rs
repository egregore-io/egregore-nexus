//! Materialize volatile `agent.update` stream events into durable `/agent` history rows.
//!
//! `mem.stream_events` is the live-only lane. The materializer replays one active turn from that
//! volatile buffer when `turn_end` arrives, commits compact `agent_session_turns` /
//! `agent_session_messages` rows, advances the cursor, then evicts the buffered token deltas in one
//! pinned write-transaction store unit.

use libsql::params;
use nexus_common::{now, NexusError};
use nexus_contracts::{AgentUpdateKind, SessionId, WsEvent};
use nexus_store::repos::{
    AgentSessionMessages, NewAgentSessionMessage, StreamEventRow, StreamEvents,
};
use nexus_store::{Store, WriteTxn};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

#[cfg(test)]
mod test_hooks {
    use std::sync::{Arc, Mutex, OnceLock};

    use nexus_contracts::SessionId;
    use tokio::sync::Notify;
    use tokio::sync::Semaphore;

    #[derive(Clone)]
    struct TransactionPause {
        session_id: String,
        entered: Arc<Notify>,
        release: Arc<Semaphore>,
    }

    static TRANSACTION_PAUSES: OnceLock<Mutex<Vec<TransactionPause>>> = OnceLock::new();

    fn pause_slot() -> &'static Mutex<Vec<TransactionPause>> {
        TRANSACTION_PAUSES.get_or_init(|| Mutex::new(Vec::new()))
    }

    pub struct TransactionPauseGuard {
        session_id: String,
    }

    impl Drop for TransactionPauseGuard {
        fn drop(&mut self) {
            if let Ok(mut slot) = pause_slot().lock() {
                slot.retain(|hook| hook.session_id != self.session_id);
            }
        }
    }

    pub fn pause_after_next_transaction(
        session_id: &SessionId,
        entered: Arc<Notify>,
        release: Arc<Semaphore>,
    ) -> TransactionPauseGuard {
        let session_id = session_id.0.clone();
        pause_slot()
            .lock()
            .expect("transaction pause hook poisoned")
            .push(TransactionPause {
                session_id: session_id.clone(),
                entered,
                release,
            });
        TransactionPauseGuard { session_id }
    }

    pub async fn maybe_pause_after_transaction(session_id: &SessionId) {
        let hook = {
            let mut slot = pause_slot()
                .lock()
                .expect("transaction pause hook poisoned");
            let index = slot.iter().position(|hook| hook.session_id == session_id.0);
            index.map(|index| slot.remove(index))
        };
        if let Some(hook) = hook {
            hook.entered.notify_waiters();
            let _permit = hook
                .release
                .acquire()
                .await
                .expect("transaction pause release semaphore closed");
        }
    }
}

#[cfg(test)]
async fn maybe_pause_after_transaction_for_test(session_id: &SessionId) {
    test_hooks::maybe_pause_after_transaction(session_id).await;
}

#[cfg(not(test))]
async fn maybe_pause_after_transaction_for_test(_session_id: &SessionId) {}

/// Materialize a websocket event if it is an `agent.update`; all other event kinds are ignored.
pub async fn materialize_ws_event(
    store: &Store,
    stream_event_id: i64,
    event: &WsEvent,
) -> Result<(), NexusError> {
    if let WsEvent::AgentUpdate {
        session_id,
        kind,
        data,
    } = event
    {
        materialize_agent_update(store, session_id, stream_event_id, *kind, data).await?;
    }
    Ok(())
}

/// Fold a `turn_end` marker into the materialized agent-session projection.
///
/// Non-final `agent.update` rows stay in `mem.stream_events` for live observers. At turn end, this
/// function replays the active volatile rows, writes the compact finalized projection to the
/// durable lane, advances the cursor, and evicts the volatile rows. The fold is protected by a
/// pinned `WriteTxn`; its cancellation-safe cleanup cannot strand the daemon writer in an open
/// transaction.
pub async fn materialize_agent_update(
    store: &Store,
    session_id: &SessionId,
    stream_event_id: i64,
    kind: AgentUpdateKind,
    _data: &Value,
) -> Result<(), NexusError> {
    // Live deltas are already buffered in the local volatile stream database. Reading the durable
    // cursor for every token/tool delta turns one model turn into hundreds of durable reads and
    // can starve command ingress and delivery settlement. Only a terminal boundary folds or
    // evicts the accumulated stream page, so keep non-terminal traffic off the durable store.
    if kind != AgentUpdateKind::TurnEnd {
        return Ok(());
    }

    let repo = AgentSessionMessages::new(store);
    let streams = StreamEvents::new(store);
    let cursor = repo.cursor_for_session(session_id).await?.unwrap_or(0);
    if stream_event_id <= cursor {
        streams
            .evict_session_through(session_id, stream_event_id)
            .await?;
        return Ok(());
    }

    let events = streams.since(session_id, cursor).await?;
    let Some(first_event_id) = events.first().map(|row| row.id) else {
        repo.set_cursor(session_id, stream_event_id).await?;
        return Ok(());
    };
    let last_event_id = events
        .last()
        .map(|row| row.id)
        .unwrap_or(stream_event_id)
        .max(stream_event_id);
    let mut user = MaterializedLane::new();
    let mut assistant = MaterializedLane::new();
    for row in &events {
        replay_stream_event(row, &mut user, &mut assistant)?;
    }

    let ts = now();
    commit_turn_end_fold(
        store,
        session_id,
        first_event_id,
        last_event_id,
        ts,
        user,
        assistant,
    )
    .await?;
    Ok(())
}

async fn commit_turn_end_fold(
    store: &Store,
    session_id: &SessionId,
    first_event_id: i64,
    last_event_id: i64,
    ts: i64,
    user: MaterializedLane,
    assistant: MaterializedLane,
) -> Result<(), NexusError> {
    let txn_label = format!("nexus_materializer_{}", last_event_id.max(0));
    // Keep the whole fold on one daemon-owned transaction so turn, messages, and cursor settle
    // atomically.
    let txn = store.begin_write_txn(&txn_label).await?;
    maybe_pause_after_transaction_for_test(session_id).await;

    let result = async {
        let turn_id =
            begin_or_get_open_turn_in_write_txn(&txn, session_id, first_event_id, ts).await?;
        let mut ordinal = 0;
        if !user.is_empty() {
            upsert_materialized_message_in_write_txn(
                &txn,
                materialized_row(
                    session_id,
                    &turn_id,
                    ordinal,
                    "user",
                    user.content,
                    user.first_event_id.unwrap_or(first_event_id),
                    user.last_event_id.unwrap_or(first_event_id),
                    ts,
                )?,
            )
            .await?;
            ordinal += 1;
        }
        if !assistant.is_empty() {
            upsert_materialized_message_in_write_txn(
                &txn,
                materialized_row(
                    session_id,
                    &turn_id,
                    ordinal,
                    "assistant",
                    assistant.content,
                    assistant.first_event_id.unwrap_or(last_event_id),
                    assistant.last_event_id.unwrap_or(last_event_id),
                    ts,
                )?,
            )
            .await?;
        }

        finalize_turn_in_write_txn(&txn, session_id, &turn_id, last_event_id, ts).await?;
        set_cursor_in_write_txn(&txn, session_id, last_event_id, ts).await?;
        Ok::<(), NexusError>(())
    }
    .await;

    match result {
        Ok(()) => {
            txn.commit().await?;
            // Evict AFTER commit: in server mode the volatile lane lives on a separate local
            // connection, so it cannot join the durable transaction. The delete is idempotent
            // (`id <= through_id`); a crash between commit and evict just re-evicts next pass.
            evict_stream_events(store, session_id, last_event_id).await?;
            Ok(())
        }
        Err(err) => {
            txn.rollback(&err).await?;
            Err(err)
        }
    }
}

async fn begin_or_get_open_turn_in_write_txn(
    txn: &WriteTxn,
    session_id: &SessionId,
    event_id: i64,
    ts: i64,
) -> Result<String, NexusError> {
    let mut rows = txn
        .query(
            "SELECT id FROM agent_session_turns \
             WHERE session_id = ?1 AND status = 'streaming' \
             ORDER BY started_at DESC LIMIT 1",
            params![session_id.0.clone()],
        )
        .await?;
    if let Some(row) = rows.next().await.map_err(store_err)? {
        let id: String = row.get(0).map_err(store_err)?;
        drop(rows);
        txn.execute(
            "UPDATE agent_session_turns \
                 SET last_stream_event_id = MAX(last_stream_event_id, ?2), updated_at = ?3 \
                 WHERE id = ?1",
            params![id.clone(), event_id, ts],
        )
        .await?;
        return Ok(id);
    }
    drop(rows);

    let id = format!("turn_{}_{}", session_id.0, event_id);
    txn.execute(
        "INSERT OR IGNORE INTO agent_session_turns (id, session_id, status, \
             first_stream_event_id, last_stream_event_id, started_at, updated_at, finalized_at) \
             VALUES (?1, ?2, 'streaming', ?3, ?3, ?4, ?4, NULL)",
        params![id.clone(), session_id.0.clone(), event_id, ts],
    )
    .await?;
    Ok(id)
}

async fn upsert_materialized_message_in_write_txn(
    txn: &WriteTxn,
    row: NewAgentSessionMessage,
) -> Result<(), NexusError> {
    txn
        .execute(
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
    txn.execute(
        "UPDATE agent_session_turns \
             SET last_stream_event_id = MAX(last_stream_event_id, ?2), updated_at = ?3 \
             WHERE id = ?1",
        params![row.turn_id, row.last_stream_event_id, row.updated_at],
    )
    .await?;
    Ok(())
}

async fn finalize_turn_in_write_txn(
    txn: &WriteTxn,
    session_id: &SessionId,
    turn_id: &str,
    event_id: i64,
    ts: i64,
) -> Result<(), NexusError> {
    txn.execute(
        "UPDATE agent_session_turns SET status = 'final', last_stream_event_id = \
             MAX(last_stream_event_id, ?3), updated_at = ?4, finalized_at = ?4 \
             WHERE id = ?1 AND session_id = ?2",
        params![turn_id, session_id.0.clone(), event_id, ts],
    )
    .await?;
    txn.execute(
        "UPDATE agent_session_messages SET status = 'final', last_stream_event_id = \
             MAX(last_stream_event_id, ?3), updated_at = ?4, finalized_at = ?4 \
             WHERE turn_id = ?1 AND session_id = ?2 AND status = 'streaming'",
        params![turn_id, session_id.0.clone(), event_id, ts],
    )
    .await?;
    Ok(())
}

async fn set_cursor_in_write_txn(
    txn: &WriteTxn,
    session_id: &SessionId,
    event_id: i64,
    ts: i64,
) -> Result<(), NexusError> {
    txn.execute(
        "INSERT INTO agent_session_stream_cursors (session_id, \
             last_materialized_stream_event_id, updated_at) VALUES (?1, ?2, ?3) \
             ON CONFLICT(session_id) DO UPDATE SET last_materialized_stream_event_id = \
             MAX(last_materialized_stream_event_id, excluded.last_materialized_stream_event_id), \
             updated_at = excluded.updated_at",
        params![session_id.0.clone(), event_id, ts],
    )
    .await?;
    Ok(())
}

async fn evict_stream_events(
    store: &Store,
    session_id: &SessionId,
    through_id: i64,
) -> Result<(), NexusError> {
    store
        .stream_conn()
        .execute(
            "DELETE FROM mem.stream_events WHERE session_id = ?1 AND id <= ?2",
            params![session_id.0.clone(), through_id],
        )
        .await
        .map_err(store_err)?;
    Ok(())
}

fn store_err(err: libsql::Error) -> NexusError {
    NexusError::Store(err.to_string())
}

fn replay_stream_event(
    row: &StreamEventRow,
    user: &mut MaterializedLane,
    assistant: &mut MaterializedLane,
) -> Result<(), NexusError> {
    let kind: AgentUpdateKind = serde_json::from_value(Value::String(row.kind.clone()))
        .map_err(|e| NexusError::Store(format!("invalid stream event kind {}: {e}", row.kind)))?;
    if kind == AgentUpdateKind::Commands || kind == AgentUpdateKind::TurnEnd {
        return Ok(());
    }
    let data: Value = serde_json::from_str(&row.data)
        .map_err(|e| NexusError::Store(format!("invalid stream event data: {e}")))?;

    if kind == AgentUpdateKind::UserInput {
        let text = text_field(&data);
        user.mark(row.id);
        push_user_input_block(&mut user.content, &data, text);
        return Ok(());
    }

    match kind {
        AgentUpdateKind::Text => push_text_block(
            &mut assistant.content,
            "text",
            text_field(&data),
            native_item_id(&data),
        ),
        AgentUpdateKind::Thinking => push_text_block(
            &mut assistant.content,
            "thinking",
            text_field(&data),
            native_item_id(&data),
        ),
        AgentUpdateKind::ToolCall => merge_tool_call(&mut assistant.content, &data, row.id),
        AgentUpdateKind::Plan => push_plan_block(&mut assistant.content, &data),
        AgentUpdateKind::Commands | AgentUpdateKind::TurnEnd | AgentUpdateKind::UserInput => {}
    }
    assistant.mark(row.id);
    Ok(())
}

/// Abort-finalize open `/agent` turns whose session/runtime is now offline.
///
/// Presence reconcile is the single daemon pass that decides a runtime is dead. This helper lets
/// that same pass close materialized session history so stale streaming turns do not hang forever
/// after session death or daemon boot.
pub async fn abort_open_turns_for_offline_sessions(
    store: &Store,
    ts: i64,
) -> Result<u64, NexusError> {
    AgentSessionMessages::new(store)
        .abort_open_turns_for_offline_sessions(ts)
        .await
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MaterializedContent {
    schema: i64,
    blocks: Vec<Value>,
    #[serde(default)]
    metadata: Map<String, Value>,
}

fn empty_content() -> MaterializedContent {
    MaterializedContent {
        schema: 1,
        blocks: Vec::new(),
        metadata: Map::new(),
    }
}

struct MaterializedLane {
    content: MaterializedContent,
    first_event_id: Option<i64>,
    last_event_id: Option<i64>,
}

impl MaterializedLane {
    fn new() -> Self {
        MaterializedLane {
            content: empty_content(),
            first_event_id: None,
            last_event_id: None,
        }
    }

    fn mark(&mut self, event_id: i64) {
        self.first_event_id = Some(self.first_event_id.unwrap_or(event_id).min(event_id));
        self.last_event_id = Some(self.last_event_id.unwrap_or(event_id).max(event_id));
    }

    fn is_empty(&self) -> bool {
        self.content.blocks.is_empty()
    }
}

fn materialized_row(
    session_id: &SessionId,
    turn_id: &str,
    ordinal: i64,
    role: &str,
    content: MaterializedContent,
    first_stream_event_id: i64,
    last_stream_event_id: i64,
    ts: i64,
) -> Result<NewAgentSessionMessage, NexusError> {
    let content_json = serde_json::to_string(&content)
        .map_err(|e| NexusError::Store(format!("serialize materialized content_json: {e}")))?;
    Ok(NewAgentSessionMessage {
        id: format!("msg_{turn_id}_{ordinal}"),
        session_id: session_id.0.clone(),
        turn_id: turn_id.to_string(),
        ordinal,
        role: role.to_string(),
        author: None,
        content_json,
        status: "streaming".to_string(),
        first_stream_event_id,
        last_stream_event_id,
        created_at: ts,
        updated_at: ts,
        finalized_at: None,
    })
}

fn text_field(data: &Value) -> String {
    data.get("text")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn native_item_id(data: &Value) -> Option<&str> {
    data.get("itemId")
        .or_else(|| data.get("item_id"))
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
}

fn push_text_block(
    content: &mut MaterializedContent,
    block_type: &str,
    text: String,
    item_id: Option<&str>,
) {
    let matching_index = match item_id {
        Some(item_id) => content.blocks.iter().position(|block| {
            block.get("type").and_then(Value::as_str) == Some(block_type)
                && block.get("itemId").and_then(Value::as_str) == Some(item_id)
        }),
        None => content.blocks.len().checked_sub(1).filter(|index| {
            let block = &content.blocks[*index];
            block.get("type").and_then(Value::as_str) == Some(block_type)
                && block.get("itemId").is_none()
        }),
    };
    if let Some(index) = matching_index {
        if let Some(last) = content.blocks.get_mut(index) {
            let current = last.get("text").and_then(Value::as_str).unwrap_or_default();
            last["text"] = Value::String(format!("{current}{text}"));
            return;
        }
    }
    let mut block = json!({ "type": block_type, "text": text });
    if let Some(item_id) = item_id {
        block["itemId"] = Value::String(item_id.to_string());
    }
    content.blocks.push(block);
}

fn push_user_input_block(content: &mut MaterializedContent, data: &Value, text: String) {
    let client_message_id = data
        .get("clientMessageId")
        .or_else(|| data.get("client_message_id"))
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty());
    for block in &mut content.blocks {
        if block.get("type").and_then(Value::as_str) == Some("text")
            && block.get("text").and_then(Value::as_str) == Some(text.as_str())
        {
            let existing_id = block.get("id").and_then(Value::as_str);
            if existing_id == client_message_id
                || existing_id.is_none()
                || client_message_id.is_none()
            {
                if existing_id.is_none() {
                    if let Some(id) = client_message_id {
                        block["id"] = Value::String(id.to_string());
                        block["clientMessageId"] = Value::String(id.to_string());
                    }
                }
                copy_user_input_metadata(block, data);
                return;
            }
        }
    }
    let mut block = json!({ "type": "text", "text": text });
    if let Some(id) = client_message_id {
        block["id"] = Value::String(id.to_string());
        block["clientMessageId"] = Value::String(id.to_string());
    }
    copy_user_input_metadata(&mut block, data);
    content.blocks.push(block);
}

fn copy_user_input_metadata(block: &mut Value, data: &Value) {
    copy_string_field(block, data, "source", "source");
    copy_string_field(block, data, "name", "name");
    copy_string_field(block, data, "kind", "kind");
    copy_string_field(block, data, "harness", "harness");
    if !copy_string_field(block, data, "runtimeId", "runtimeId") {
        copy_string_field(block, data, "runtime_id", "runtimeId");
    }
}

fn copy_string_field(block: &mut Value, data: &Value, from: &str, to: &str) -> bool {
    let Some(value) = data
        .get(from)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    else {
        return false;
    };
    if block.get(to).and_then(Value::as_str).is_none() {
        block[to] = Value::String(value.to_string());
    }
    true
}

fn push_plan_block(content: &mut MaterializedContent, data: &Value) {
    if let Some(entries) = data.get("entries") {
        content
            .blocks
            .push(json!({ "type": "plan", "entries": entries }));
    } else {
        content
            .blocks
            .push(json!({ "type": "plan", "text": text_field(data) }));
    }
}

fn merge_tool_call(content: &mut MaterializedContent, data: &Value, stream_event_id: i64) {
    let id = data
        .get("id")
        .or_else(|| data.get("toolCallId"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| format!("tool_{stream_event_id}"));
    let index = content.blocks.iter().position(|block| {
        block.get("type").and_then(Value::as_str) == Some("tool_call")
            && block.get("id").and_then(Value::as_str) == Some(id.as_str())
    });

    let block = match index {
        Some(i) => &mut content.blocks[i],
        None => {
            let name = data
                .get("title")
                .or_else(|| data.get("name"))
                .or_else(|| data.get("kind"))
                .and_then(Value::as_str)
                .unwrap_or("tool");
            content.blocks.push(json!({
                "type": "tool_call",
                "id": id,
                "name": name,
            }));
            content.blocks.last_mut().expect("tool block inserted")
        }
    };

    if let Some(name) = data
        .get("title")
        .or_else(|| data.get("name"))
        .or_else(|| data.get("kind"))
        .and_then(Value::as_str)
    {
        block["name"] = Value::String(name.to_string());
    }
    // C-TOOL v1: persist the canonical machine name and the STRUCTURED input alongside the
    // display fields — replay must hand back the same object the live stream carried
    // (docs/tool-call-contract.md; the argsJson-only store was where replay fidelity died).
    if let Some(tool) = data.get("tool").and_then(Value::as_str) {
        block["tool"] = Value::String(tool.to_string());
    }
    if let Some(status) = data.get("status").and_then(Value::as_str) {
        block["status"] = Value::String(status.to_string());
    }
    if let Some(input) = data.get("input") {
        block["input"] = input.clone();
        block["argsJson"] = Value::String(display_json(input));
    }
    if let Some(output) = data.get("content").or_else(|| data.get("output")) {
        let output = display_json(output);
        let should_append = data.get("content").is_none()
            && data.get("status").and_then(Value::as_str) == Some("in_progress");
        if should_append {
            let current = block
                .get("output")
                .and_then(Value::as_str)
                .unwrap_or_default();
            block["output"] = Value::String(format!("{current}{output}"));
        } else {
            block["output"] = Value::String(output);
        }
    }
}

fn display_json(value: &Value) -> String {
    value.as_str().map(str::to_string).unwrap_or_else(|| {
        serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
    })
}

#[cfg(test)]
#[path = "../../tests/unit/agent_session_materializer.rs"]
mod tests;
