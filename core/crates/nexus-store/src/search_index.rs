//! Search-index helpers over the durable `messages` log. Core Nexus owns FTS5 full-text only;
//! semantic vectors now live behind a plugin, so the legacy vector helpers degrade to no-ops when
//! `message_vectors` is absent. Every query takes a caller-supplied `scope_sql` — a `WHERE`
//! fragment (without the `WHERE` keyword) that `nexus-search` injects to enforce the per-agent
//! scope (own DMs ∪ member threads ∪ subscribed topics, context hygiene §9).

use libsql::params;

use nexus_common::NexusError;
use nexus_contracts::ids::{MessageId, ProjectId};

use crate::error::{store_err, store_msg};
use crate::repos::sessions::{get_opt_int, get_text};
use crate::state::Store;

/// One search result row: the message id, project, a snippet, and a relevance score
/// (FTS rank or vector distance — lower distance / higher rank is more relevant).
#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub id: MessageId,
    pub project: ProjectId,
    pub snippet: String,
    pub score: f64,
}

/// Run an FTS5 match over `messages_fts`, joined back to `messages` for scope + project filtering.
/// `scope_sql` is a caller-supplied boolean fragment over the `messages` alias `m` (e.g.
/// `m.from_name = 'ana'`); pass `"1=1"` for the whole project (web console).
pub async fn fts_query(
    store: &Store,
    project: &str,
    scope_sql: &str,
    query: &str,
    limit: u32,
) -> Result<Vec<SearchHit>, NexusError> {
    let sql = format!(
        "SELECT m.message_id, m.project, snippet(messages_fts, 1, '[', ']', '…', 12) AS snip, \
         bm25(messages_fts) AS score \
         FROM messages_fts \
         JOIN messages m ON m.rowid = messages_fts.rowid \
         WHERE messages_fts MATCH ?1 AND m.project = ?2 AND ({scope_sql}) \
         ORDER BY score ASC LIMIT ?3"
    );
    let mut rows = store
        .conn
        .query(&sql, params![query, project, limit as i64])
        .await
        .map_err(store_err)?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await.map_err(store_err)? {
        out.push(SearchHit {
            id: MessageId(get_text(&row, 0)?),
            project: ProjectId(get_text(&row, 1)?),
            snippet: get_text(&row, 2)?,
            score: get_f64(&row, 3)?,
        });
    }
    Ok(out)
}

/// Top-k nearest message vectors to `embedding`, scope- and project-filtered.
/// Core does not create `message_vectors`; when a semantic plugin has not
/// installed that table, this returns an empty list. If the table exists, this
/// uses libSQL's `vector_distance_cos` joined to `messages`; `scope_sql` is the
/// same caller-supplied fragment as [`fts_query`].
pub async fn vector_top_k(
    store: &Store,
    project: &str,
    scope_sql: &str,
    embedding: &[f32],
    k: u32,
) -> Result<Vec<SearchHit>, NexusError> {
    if !table_exists(store, "message_vectors").await? {
        return Ok(Vec::new());
    }
    let vec_literal = embedding_literal(embedding);
    let sql = format!(
        "SELECT m.message_id, m.project, m.body, \
         vector_distance_cos(v.embedding, vector32(?1)) AS dist \
         FROM message_vectors v \
         JOIN messages m ON m.message_id = v.message_id \
         WHERE m.project = ?2 AND ({scope_sql}) \
         ORDER BY dist ASC LIMIT ?3"
    );
    let mut rows = store
        .conn
        .query(&sql, params![vec_literal, project, k as i64])
        .await
        .map_err(store_err)?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await.map_err(store_err)? {
        out.push(SearchHit {
            id: MessageId(get_text(&row, 0)?),
            project: ProjectId(get_text(&row, 1)?),
            snippet: get_text(&row, 2)?,
            score: get_f64(&row, 3)?,
        });
    }
    Ok(out)
}

/// Re-index a message's FTS entry (delete-then-insert for external-content FTS5). `Messages::insert`
/// already seeds the index on first write; this is for updates/back-fill.
pub async fn upsert_fts(
    store: &Store,
    id: &MessageId,
    summary: Option<&str>,
    body: &str,
) -> Result<(), NexusError> {
    let mut rows = store
        .conn
        .query(
            "SELECT rowid FROM messages WHERE message_id = ?1",
            params![id.0.clone()],
        )
        .await
        .map_err(store_err)?;
    let rowid = match rows.next().await.map_err(store_err)? {
        Some(row) => get_opt_int(&row, 0)?.ok_or_else(|| store_msg("missing rowid"))?,
        None => return Err(NexusError::NotFound(id.0.clone())),
    };
    // external-content FTS5: the special 'delete' command removes the old row, then re-insert.
    store
        .conn
        .execute(
            "INSERT INTO messages_fts(messages_fts, rowid, summary, body) \
             VALUES('delete', ?1, (SELECT summary FROM messages WHERE rowid = ?1), \
             (SELECT body FROM messages WHERE rowid = ?1))",
            params![rowid],
        )
        .await
        .map_err(store_err)?;
    store
        .conn
        .execute(
            "INSERT INTO messages_fts (rowid, summary, body) VALUES (?1, ?2, ?3)",
            params![rowid, summary, body],
        )
        .await
        .map_err(store_err)?;
    Ok(())
}

/// Upsert a message's embedding vector (libSQL `F32_BLOB` via `vector32()`).
pub async fn upsert_vector(
    store: &Store,
    id: &MessageId,
    embedding: &[f32],
) -> Result<(), NexusError> {
    if !table_exists(store, "message_vectors").await? {
        return Ok(());
    }
    let vec_literal = embedding_literal(embedding);
    store
        .conn
        .execute(
            "INSERT INTO message_vectors (message_id, embedding) VALUES (?1, vector32(?2)) \
             ON CONFLICT(message_id) DO UPDATE SET embedding = vector32(?2)",
            params![id.0.clone(), vec_literal],
        )
        .await
        .map_err(store_err)?;
    Ok(())
}

async fn table_exists(store: &Store, table: &str) -> Result<bool, NexusError> {
    let mut rows = store
        .conn
        .query(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            params![table],
        )
        .await
        .map_err(store_err)?;
    Ok(rows.next().await.map_err(store_err)?.is_some())
}

/// Render an embedding into libSQL's vector text literal: `[0.1,0.2,...]`.
fn embedding_literal(embedding: &[f32]) -> String {
    let parts: Vec<String> = embedding.iter().map(|v| v.to_string()).collect();
    format!("[{}]", parts.join(","))
}

fn get_f64(row: &libsql::Row, idx: i32) -> Result<f64, NexusError> {
    match row.get_value(idx).map_err(store_err)? {
        libsql::Value::Real(r) => Ok(r),
        libsql::Value::Integer(i) => Ok(i as f64),
        _ => Ok(0.0),
    }
}
