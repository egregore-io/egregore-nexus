//! FTS5 full-text search, scoped. Thin layer over [`nexus_store::search_index::fts_query`]: it
//! composes the caller's [`Scope::where_sql`](crate::scope::Scope::where_sql) with the request's
//! optional narrowing filters (`thread` / `with` / `since`) into the single `scope_sql` fragment the
//! store helper ANDs into every query. The scope is always present, so a non-scoped message is never
//! matched.

use nexus_contracts::ports::Caller;
use nexus_contracts::search::SearchRequest;
use nexus_store::search_index::{fts_query, SearchHit};
use nexus_store::Store;

use crate::error::SearchError;
use crate::filter::scoped_where;

/// Run a scoped FTS5 match for `caller` over `req.query`, honoring `req.thread`/`with`/`since` and
/// `req.limit`. Returns store-level hits (id/project/snippet/score); the service maps them to the
/// wire [`nexus_contracts::search::SearchHit`].
pub async fn run(
    store: &Store,
    caller: &Caller,
    req: &SearchRequest,
    limit: u32,
) -> Result<Vec<SearchHit>, SearchError> {
    if req.query.trim().is_empty() {
        return Err(SearchError::Invalid("empty fts query".into()));
    }
    let scope_sql = scoped_where(
        store,
        caller,
        req.thread.as_deref(),
        req.with.as_deref(),
        req.since,
    )
    .await?;
    let hits = fts_query(store, &caller.project, &scope_sql, &req.query, limit).await?;
    Ok(hits)
}
