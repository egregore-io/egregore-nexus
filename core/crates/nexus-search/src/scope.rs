//! The per-agent search scope (context hygiene, backend §9). EVERY search/history query injects
//! [`Scope::where_sql`] — a boolean `WHERE` fragment over the `messages` alias `m` — so an agent
//! only ever sees:
//!
//! - **its own DMs** — direct messages it sent or received, and
//! - **active threads it belongs to** — via `thread_members` joined to non-archived `threads`, and
//! - **topics it subscribes to** — via `subscriptions`.
//!
//! A message in a thread the caller is *not* a member of (or a DM between two other agents) can never
//! be returned: the fragment is always ANDed into the store's `fts_query` / `vector_top_k`, which do
//! no global scan of their own. This is the load-bearing invariant of the crate.

use nexus_contracts::ports::Caller;

/// Builds the scoped `WHERE` fragment for one caller.
///
/// The schema seam (`core/migrations/0001_init.sql`):
/// - `messages.from_name` / `messages.to_name` — DM endpoints are agent **names**.
/// - `thread_members.session_name` — compatibility membership display/fallback key. Newer rows also
///   carry `agent_id` for id-aware routing paths.
/// - `threads.archived_at` — archived threads are hidden from active recall.
/// - `subscriptions.subscriber_session` — subscriptions are by **session id**.
///
/// This search scope still keys DMs and thread membership off [`Caller::name`], while topic
/// subscription keys off [`Caller::session`]. Names are quoted with SQL single-quote escaping; the
/// fragment carries no caller-controlled free text beyond those identifiers.
pub struct Scope;

impl Scope {
    /// The boolean `WHERE` fragment (no leading `WHERE`) over the `messages` alias `m` enforcing the
    /// caller's scope: own DMs ∪ member threads ∪ subscribed topics. Pass straight into
    /// [`nexus_store::search_index::fts_query`] / [`nexus_store::search_index::vector_top_k`] as
    /// `scope_sql`. Never returns `1=1` — the scope is always concrete, never a global scan.
    pub fn where_sql(caller: &Caller) -> String {
        let name = sql_quote(&caller.name);
        let session = sql_quote(&caller.session.0);
        format!(
            "(\
             (m.kind = 'dm' AND (m.from_name = {name} OR m.to_name = {name})) \
             OR m.thread_id IN (\
                SELECT tm.thread_id FROM thread_members tm \
                JOIN threads t ON t.thread_id = tm.thread_id \
                WHERE tm.session_name = {name} AND t.archived_at IS NULL\
             ) \
             OR m.topic IN (SELECT topic FROM subscriptions WHERE subscriber_session = {session})\
             )"
        )
    }
}

/// Wrap a value in SQL single quotes, doubling any embedded single quote (standard SQL escaping).
fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_contracts::enums::Tier;
    use nexus_contracts::ids::SessionId;

    fn caller(name: &str, session: &str) -> Caller {
        Caller {
            agent_id: None,
            session: SessionId(session.into()),
            name: name.into(),
            project: "p_demo".into(),
            tier: Tier::Agent,
        }
    }

    #[test]
    fn fragment_covers_dms_threads_and_topics_for_the_caller() {
        let sql = Scope::where_sql(&caller("ana", "s_ana"));
        assert!(sql.contains("m.kind = 'dm'"));
        assert!(sql.contains("m.from_name = 'ana'"));
        assert!(sql.contains("m.to_name = 'ana'"));
        assert!(sql.contains("thread_members tm"));
        assert!(sql.contains("t.archived_at IS NULL"));
        assert!(sql.contains("tm.session_name = 'ana'"));
        assert!(sql.contains("subscriptions WHERE subscriber_session = 's_ana'"));
    }

    #[test]
    fn never_a_global_scan() {
        let sql = Scope::where_sql(&caller("ana", "s_ana"));
        assert!(!sql.contains("1=1"));
        assert!(sql.contains("ana"), "scope must be bound to the caller");
    }

    #[test]
    fn names_are_quote_escaped() {
        let sql = Scope::where_sql(&caller("o'brien", "s_1"));
        assert!(sql.contains("'o''brien'"), "single quotes are doubled");
    }
}
