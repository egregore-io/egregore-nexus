//! # nexus-search
//!
//! Scoped recall over the durable message log (backend spec §9). Core Nexus owns **FTS5**
//! full-text plus chronological **history**; semantic vectors are an optional plugin seam. Every
//! query injects [`Scope::where_sql`], the caller's own DMs ∪ member threads ∪ subscribed topics. A
//! message in a thread the caller is not a member of (or a DM between two other agents) is never
//! returned — search is never a global scan (context hygiene §9).
//!
//! - [`scope`] — the per-agent `WHERE` fragment.
//! - [`filter`] — composes scope with a request's `thread`/`with`/`since` narrowing.
//! - [`fts`] / [`semantic`] — scoped engines over the store's FTS helpers and optional vector seam.
//! - [`embed`] — the pluggable [`Embedder`] trait + a deterministic, dependency-free stub.
//! - [`service`] — [`Search`], the [`nexus_contracts::ports::SearchPort`] implementation.

pub mod embed;
pub mod error;
pub mod filter;
pub mod fts;
pub mod scope;
pub mod semantic;
pub mod service;

pub use embed::{Embedder, StubEmbedder};
pub use error::SearchError;
pub use scope::Scope;
pub use service::Search;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use nexus_contracts::enums::{Kind, Scope as MsgScope, Tier};
    use nexus_contracts::ids::{MessageId, ProjectId, ThreadId};
    use nexus_contracts::message::{Message, Provenance};
    use nexus_contracts::ports::{Caller, IdentityPort, PortResult, SearchPort};
    use nexus_contracts::register::{
        HeartbeatResponse, MemberListRequest, MemberListResponse, RegisterRequest,
        RegisterResponse, StatusRequest, StatusResponse, Whoami,
    };
    use nexus_contracts::search::{HistoryRequest, SearchMode, SearchRequest};
    use nexus_store::repos::messages::Messages;
    use nexus_store::repos::threads::Threads;
    use nexus_store::Store;

    use super::*;

    /// A no-op IdentityPort — search threads the resolved `Caller` in per-call, so it never invokes
    /// the port in these tests; it only needs to exist to satisfy the constructor.
    struct NoIdentity;

    #[async_trait]
    impl IdentityPort for NoIdentity {
        async fn register(&self, _req: RegisterRequest) -> PortResult<RegisterResponse> {
            unimplemented!()
        }
        async fn whoami(&self, _caller: &Caller) -> PortResult<Whoami> {
            unimplemented!()
        }
        async fn resolve(&self, _project: &str, _name: &str) -> PortResult<Caller> {
            unimplemented!()
        }
        async fn members(
            &self,
            _caller: &Caller,
            _req: MemberListRequest,
        ) -> PortResult<MemberListResponse> {
            unimplemented!()
        }
        async fn status(
            &self,
            _caller: &Caller,
            _req: StatusRequest,
        ) -> PortResult<StatusResponse> {
            unimplemented!()
        }
        async fn heartbeat(&self, _caller: &Caller) -> PortResult<HeartbeatResponse> {
            unimplemented!()
        }
        async fn assign_project(
            &self,
            _name: &str,
            _to_project: &str,
        ) -> PortResult<nexus_contracts::AssignProjectResponse> {
            unimplemented!()
        }
    }

    async fn migrated() -> Arc<Store> {
        let s = Store::open(":memory:").await.unwrap();
        s.migrate().await.unwrap();
        Arc::new(s)
    }

    fn caller(name: &str, session: &str) -> Caller {
        Caller {
            agent_id: None,
            session: nexus_contracts::ids::SessionId(session.into()),
            name: name.into(),
            project: "p_demo".into(),
            tier: Tier::Agent,
            locality: Default::default(),
            access: None,
            principal_id: None,
        }
    }

    /// Insert a DM. `from`/`to` are agent names; created_at lets tests order history.
    async fn insert_dm(store: &Store, id: &str, from: &str, to: &str, body: &str, at: i64) {
        let msg = Message {
            id: MessageId(id.into()),
            project: ProjectId("p_demo".into()),
            from: from.into(),
            scope: MsgScope::Dm,
            thread: None,
            topic: None,
            body: body.into(),
            summary: None,
            provenance: Provenance {
                from: from.into(),
                kind: Kind::Agent,
                locality: Default::default(),
                access: None,
                thread: None,
                topic: None,
                stamp: None,
            },
            created_at: at,
        };
        Messages::new(store).insert(&msg).await.unwrap();
        // `to_name` is not set by Messages::insert for DMs derived from Message (no `to`), so set it
        // directly to model the recipient endpoint the scope filters on.
        store
            .conn
            .execute(
                "UPDATE messages SET to_name = ?2 WHERE message_id = ?1",
                libsql::params![id, to],
            )
            .await
            .unwrap();
    }

    /// Insert a thread message.
    async fn insert_thread_msg(
        store: &Store,
        id: &str,
        from: &str,
        thread_id: &str,
        body: &str,
        at: i64,
    ) {
        let msg = Message {
            id: MessageId(id.into()),
            project: ProjectId("p_demo".into()),
            from: from.into(),
            scope: MsgScope::Thread,
            thread: Some(ThreadId(thread_id.into())),
            topic: None,
            body: body.into(),
            summary: None,
            provenance: Provenance {
                from: from.into(),
                kind: Kind::Agent,
                locality: Default::default(),
                access: None,
                thread: Some("backend".into()),
                topic: None,
                stamp: None,
            },
            created_at: at,
        };
        Messages::new(store).insert(&msg).await.unwrap();
    }

    fn search(store: Arc<Store>) -> Search {
        Search::new(store, Arc::new(NoIdentity) as Arc<dyn IdentityPort>)
    }

    #[tokio::test]
    async fn search_is_scoped_to_caller_dms_and_threads() {
        let store = migrated().await;
        // ana is a member of `backend`; dylan is not.
        let threads = Threads::new(&store);
        threads
            .create(&ThreadId("t_backend".into()), "backend", "p_demo", "ben")
            .await
            .unwrap();
        threads
            .add_member(&ThreadId("t_backend".into()), "ana")
            .await
            .unwrap();
        // a message in a thread ana is NOT a member of:
        threads
            .create(&ThreadId("t_secret".into()), "secret", "p_demo", "ben")
            .await
            .unwrap();
        insert_thread_msg(&store, "m_in", "ben", "t_backend", "token rotation plan", 1).await;
        insert_thread_msg(&store, "m_out", "ben", "t_secret", "token rotation plan", 2).await;

        let svc = search(store.clone());
        let req = SearchRequest {
            query: "token".into(),
            mode: SearchMode::Fts,
            limit: Some(10),
            thread: None,
            with: None,
            since: None,
        };
        let res = svc.search(&caller("ana", "s_ana"), req).await.unwrap();
        let ids: Vec<_> = res.hits.iter().map(|h| h.message_id.0.clone()).collect();
        assert!(
            ids.contains(&"m_in".to_string()),
            "member-thread hit returned"
        );
        assert!(
            !ids.contains(&"m_out".to_string()),
            "non-member thread message must NEVER be returned (context hygiene §9)"
        );
    }

    #[tokio::test]
    async fn fts_finds_term_in_own_dm() {
        let store = migrated().await;
        insert_dm(&store, "m_dm", "ben", "ana", "rebase the auth refactor", 1).await;
        // a DM ana is not part of must not match:
        insert_dm(
            &store,
            "m_other",
            "ben",
            "dylan",
            "rebase the auth refactor",
            2,
        )
        .await;

        let svc = search(store.clone());
        let req = SearchRequest {
            query: "rebase".into(),
            mode: SearchMode::Fts,
            limit: Some(10),
            thread: None,
            with: None,
            since: None,
        };
        let res = svc.search(&caller("ana", "s_ana"), req).await.unwrap();
        assert_eq!(res.hits.len(), 1);
        let hit = &res.hits[0];
        assert_eq!(hit.message_id, MessageId("m_dm".into()));
        assert_eq!(hit.from, "ben");
        assert!(!hit.snippet.is_empty(), "hit carries a snippet");
    }

    #[tokio::test]
    async fn semantic_empty_without_plugin_vectors_and_hybrid_keeps_scoped_fts() {
        use nexus_store::search_index::upsert_vector;
        let store = migrated().await;
        // Ana's own DMs plus one DM she cannot see. Core no longer owns
        // `message_vectors`, so vector writes are no-ops until a semantic plugin
        // supplies storage; hybrid must still keep the scoped FTS path useful.
        insert_dm(&store, "m_near", "ben", "ana", "auth token refactor", 1).await;
        insert_dm(
            &store,
            "m_far",
            "ben",
            "ana",
            "kubernetes cluster deploy",
            2,
        )
        .await;
        // a DM ana is not part of — must be excluded even if its vector is nearest.
        insert_dm(&store, "m_out", "ben", "dylan", "auth token refactor", 3).await;

        let embedder = StubEmbedder::new();
        for (id, text) in [
            ("m_near", "auth token refactor"),
            ("m_far", "kubernetes cluster deploy"),
            ("m_out", "auth token refactor"),
        ] {
            upsert_vector(&store, &MessageId(id.into()), &embedder.embed(text))
                .await
                .unwrap();
        }

        let svc = search(store.clone());
        let semantic = svc
            .search(
                &caller("ana", "s_ana"),
                SearchRequest {
                    query: "auth refactor".into(),
                    mode: SearchMode::Semantic,
                    limit: Some(10),
                    thread: None,
                    with: None,
                    since: None,
                },
            )
            .await
            .unwrap();
        assert!(semantic.hits.is_empty());

        let hybrid = svc
            .search(
                &caller("ana", "s_ana"),
                SearchRequest {
                    query: "auth refactor".into(),
                    mode: SearchMode::Hybrid,
                    limit: Some(10),
                    thread: None,
                    with: None,
                    since: None,
                },
            )
            .await
            .unwrap();
        let ids: Vec<_> = hybrid.hits.iter().map(|h| h.message_id.0.clone()).collect();
        assert!(ids.contains(&"m_near".to_string()));
        assert!(
            !ids.contains(&"m_out".to_string()),
            "non-scoped DM excluded from hybrid FTS fallback"
        );
    }

    #[tokio::test]
    async fn history_returns_chronological_scoped_entries() {
        let store = migrated().await;
        // ana<->ben DM thread, out of order inserts:
        insert_dm(&store, "m_2", "ana", "ben", "second", 200).await;
        insert_dm(&store, "m_1", "ben", "ana", "first", 100).await;
        insert_dm(&store, "m_3", "ana", "ben", "third", 300).await;
        // a DM with someone else must be excluded by with=ben:
        insert_dm(&store, "m_x", "ana", "dylan", "elsewhere", 250).await;

        let svc = search(store.clone());
        let req = HistoryRequest {
            thread: None,
            with: Some("ben".into()),
            topic: None,
            limit: Some(10),
            before: None,
        };
        let res = svc.history(&caller("ana", "s_ana"), req).await.unwrap();
        let bodies: Vec<_> = res.entries.iter().map(|e| e.body.clone()).collect();
        assert_eq!(
            bodies,
            vec!["first".to_string(), "second".into(), "third".into()],
            "chronological, scoped to the ben DM only"
        );
    }
}
