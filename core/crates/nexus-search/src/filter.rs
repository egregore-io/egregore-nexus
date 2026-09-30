//! Composes the caller scope ([`Scope::where_sql`]) with a request's optional narrowing filters
//! (`thread` by name, `with` DM-partner, `since` epoch-millis) into the single `scope_sql` fragment
//! the store's `fts_query` / `vector_top_k` AND into every query. The scope is ALWAYS the base
//! clause — narrowing filters only ever subtract from it, so they can never widen a search past the
//! caller's own DMs ∪ member threads ∪ subscribed topics.

use nexus_contracts::ports::Caller;
use nexus_store::repos::threads::Threads;
use nexus_store::Store;

use crate::error::SearchError;
use crate::scope::Scope;

/// Build the final `scope_sql` over alias `m`: the caller scope, ANDed with any of `thread` (by
/// global name, resolved to a hidden `thread_id`), `with` (a DM partner — both endpoints), and
/// `since` (`created_at >= since`).
pub async fn scoped_where(
    store: &Store,
    caller: &Caller,
    thread: Option<&str>,
    with: Option<&str>,
    since: Option<i64>,
) -> Result<String, SearchError> {
    let mut sql = Scope::where_sql(caller);

    if let Some(name) = thread {
        // Resolve by global thread identity; an unknown thread yields a fragment that matches
        // nothing (rather than erroring), keeping the membership scope intact.
        let tid = Threads::new(store)
            .find_active_any_by_name(name)
            .await?
            .map(|t| t.thread_id.0);
        match tid {
            Some(id) => sql.push_str(&format!(" AND m.thread_id = {}", sql_quote(&id))),
            None => sql.push_str(" AND 1=0"),
        }
    }

    if let Some(partner) = with {
        let p = sql_quote(partner);
        sql.push_str(&format!(
            " AND m.kind = 'dm' AND (m.from_name = {p} OR m.to_name = {p})"
        ));
    }

    if let Some(since) = since {
        sql.push_str(&format!(" AND m.created_at >= {since}"));
    }

    Ok(sql)
}

/// SQL single-quote with standard doubling escape.
fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}
