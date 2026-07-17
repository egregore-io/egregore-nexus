//! Semantic (vector) search, scoped. Core no longer owns the vector table; this
//! seam embeds the query with the pluggable [`Embedder`] and asks whatever vector
//! storage is available via [`nexus_store::search_index::vector_top_k`]. Without a
//! semantic-search plugin/table installed, semantic returns no hits and hybrid
//! keeps the FTS fallback.

use nexus_contracts::ports::Caller;
use nexus_contracts::search::SearchRequest;
use nexus_store::search_index::{vector_top_k, SearchHit};
use nexus_store::Store;

use crate::embed::Embedder;
use crate::error::SearchError;
use crate::filter::scoped_where;

/// Run a scoped vector top-k for `caller`: embed `req.query`, then nearest-neighbour
/// over plugin-provided vector storage filtered by the composed scope. Returns
/// store-level hits, or an empty list when no vector store is installed.
pub async fn run(
    store: &Store,
    embedder: &dyn Embedder,
    caller: &Caller,
    req: &SearchRequest,
    k: u32,
) -> Result<Vec<SearchHit>, SearchError> {
    if req.query.trim().is_empty() {
        return Err(SearchError::Invalid("empty semantic query".into()));
    }
    let scope_sql = scoped_where(
        store,
        caller,
        req.thread.as_deref(),
        req.with.as_deref(),
        req.since,
    )
    .await?;
    let embedding = embedder.embed(&req.query);
    let hits = vector_top_k(store, &caller.project, &scope_sql, &embedding, k).await?;
    Ok(hits)
}
