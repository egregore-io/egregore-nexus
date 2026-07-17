//! [`Search`] — the [`SearchPort`] implementation. Wires the [`Store`], an [`Embedder`], and an
//! `Arc<dyn IdentityPort>` (caller scope) into the three operations:
//!
//! - `search` — `fts` | `semantic` | `hybrid`, each scoped via [`Scope`](crate::scope::Scope).
//!   Core always serves FTS; semantic returns no hits until plugin vector storage is installed.
//! - `history` — chronological, scoped recall of a conversation (`thread` / `with` / `topic`).
//!
//! Every query injects the caller scope; nothing here can return a message outside the caller's own
//! DMs ∪ member threads ∪ subscribed topics (context hygiene §9).

use std::sync::Arc;

use async_trait::async_trait;
use libsql::params;

use nexus_contracts::ids::MessageId;
use nexus_contracts::ports::{Caller, ContractError, IdentityPort, PortResult, SearchPort};
use nexus_contracts::search::{
    HistoryEntry, HistoryRequest, HistoryResponse, SearchHit, SearchMode, SearchRequest,
    SearchResponse,
};
use nexus_store::repos::messages::Messages;
use nexus_store::search_index::SearchHit as StoreHit;
use nexus_store::Store;

use crate::embed::{Embedder, StubEmbedder};
use crate::error::SearchError;
use crate::filter::scoped_where;
use crate::{fts, semantic};

/// Default result cap when a request omits `limit`.
const DEFAULT_LIMIT: u32 = 20;

/// The scoped search service (backend §9). Holds the shared store, the embedder, and the identity
/// port (reserved for caller-scope resolution at the edge; the resolved [`Caller`] is threaded in
/// per call).
pub struct Search {
    store: Arc<Store>,
    embedder: Arc<dyn Embedder>,
    #[allow(dead_code)]
    identity: Arc<dyn IdentityPort>,
}

impl Search {
    /// Construct with the default deterministic [`StubEmbedder`].
    pub fn new(store: Arc<Store>, identity: Arc<dyn IdentityPort>) -> Self {
        Search {
            store,
            embedder: Arc::new(StubEmbedder::new()),
            identity,
        }
    }

    /// Construct with a custom [`Embedder`] (e.g. a real model server).
    pub fn with_embedder(
        store: Arc<Store>,
        identity: Arc<dyn IdentityPort>,
        embedder: Arc<dyn Embedder>,
    ) -> Self {
        Search {
            store,
            embedder,
            identity,
        }
    }

    /// Map a store-level hit to the wire hit, filling `from`/`when` from the message row. A hit whose
    /// message vanished between query and lookup is dropped (it can no longer be rendered).
    async fn to_wire_hit(&self, hit: StoreHit) -> Result<Option<SearchHit>, SearchError> {
        let StoreHit {
            id,
            project,
            snippet,
            score,
        } = hit;
        let msg = Messages::new(&self.store).get(&project.0, &id).await?;
        Ok(msg.map(|m| SearchHit {
            message_id: MessageId(id.0),
            from: m.from,
            when: m.created_at,
            snippet,
            score: score as f32,
        }))
    }

    async fn to_wire_hits(&self, hits: Vec<StoreHit>) -> Result<Vec<SearchHit>, SearchError> {
        let mut out = Vec::with_capacity(hits.len());
        for h in hits {
            if let Some(w) = self.to_wire_hit(h).await? {
                out.push(w);
            }
        }
        Ok(out)
    }

    async fn run(
        &self,
        caller: &Caller,
        req: &SearchRequest,
    ) -> Result<SearchResponse, SearchError> {
        let limit = req.limit.unwrap_or(DEFAULT_LIMIT);
        let store_hits = match req.mode {
            SearchMode::Fts => fts::run(&self.store, caller, req, limit).await?,
            SearchMode::Semantic => {
                semantic::run(&self.store, self.embedder.as_ref(), caller, req, limit).await?
            }
            SearchMode::Hybrid => {
                let fts_hits = fts::run(&self.store, caller, req, limit).await?;
                let sem_hits =
                    semantic::run(&self.store, self.embedder.as_ref(), caller, req, limit).await?;
                merge_hybrid(fts_hits, sem_hits, limit)
            }
        };
        let hits = self.to_wire_hits(store_hits).await?;
        Ok(SearchResponse { hits })
    }

    async fn run_history(
        &self,
        caller: &Caller,
        req: &HistoryRequest,
    ) -> Result<HistoryResponse, SearchError> {
        let limit = req.limit.unwrap_or(DEFAULT_LIMIT);
        // Base scope, ANDed with the request's conversation selector (thread/with/topic) and the
        // optional `before` cursor. `topic` narrows to a subscribed topic; the base scope already
        // requires subscription, so a non-subscribed topic yields nothing.
        let mut scope_sql = scoped_where(
            &self.store,
            caller,
            req.thread.as_deref(),
            req.with.as_deref(),
            None,
        )
        .await?;
        if let Some(topic) = &req.topic {
            scope_sql.push_str(&format!(" AND m.topic = {}", sql_quote(topic)));
        }
        if let Some(before) = req.before {
            scope_sql.push_str(&format!(" AND m.created_at < {before}"));
        }

        // Newest-first selection (honors `before` + `limit`), re-sorted ascending so the returned
        // slice reads chronologically.
        let sql = format!(
            "SELECT m.from_name, m.created_at, m.summary, m.body FROM messages m \
             WHERE m.project = ?1 AND ({scope_sql}) \
             ORDER BY m.created_at DESC LIMIT ?2"
        );
        let mut rows = self
            .store
            .conn
            .query(&sql, params![caller.project.clone(), limit as i64])
            .await
            .map_err(|e| SearchError::Store(nexus_common::NexusError::Store(e.to_string())))?;
        let mut entries = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| SearchError::Store(nexus_common::NexusError::Store(e.to_string())))?
        {
            entries.push(HistoryEntry {
                from: text(&row, 0),
                when: int(&row, 1),
                summary: opt_text(&row, 2),
                body: text(&row, 3),
            });
        }
        entries.reverse(); // DESC select → ascending (chronological) output.
        Ok(HistoryResponse { entries })
    }
}

#[async_trait]
impl SearchPort for Search {
    async fn search(&self, caller: &Caller, req: SearchRequest) -> PortResult<SearchResponse> {
        self.run(caller, &req).await.map_err(to_contract)
    }

    async fn history(&self, caller: &Caller, req: HistoryRequest) -> PortResult<HistoryResponse> {
        self.run_history(caller, &req).await.map_err(to_contract)
    }
}

/// Merge FTS + semantic hits, dedup by message id (first occurrence wins, FTS-first), cap at `limit`.
fn merge_hybrid(fts: Vec<StoreHit>, sem: Vec<StoreHit>, limit: u32) -> Vec<StoreHit> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for h in fts.into_iter().chain(sem.into_iter()) {
        if seen.insert(h.id.0.clone()) {
            out.push(h);
            if out.len() >= limit as usize {
                break;
            }
        }
    }
    out
}

fn to_contract(e: SearchError) -> ContractError {
    nexus_common::NexusError::from(e).to_contract_error()
}

fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn text(row: &libsql::Row, idx: i32) -> String {
    match row.get_value(idx) {
        Ok(libsql::Value::Text(t)) => t,
        _ => String::new(),
    }
}
fn opt_text(row: &libsql::Row, idx: i32) -> Option<String> {
    match row.get_value(idx) {
        Ok(libsql::Value::Text(t)) => Some(t),
        _ => None,
    }
}
fn int(row: &libsql::Row, idx: i32) -> i64 {
    match row.get_value(idx) {
        Ok(libsql::Value::Integer(i)) => i,
        _ => 0,
    }
}
