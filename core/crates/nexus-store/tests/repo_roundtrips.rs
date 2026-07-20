//! Store repository and search-index coverage moved out of production modules.
//!
//! These tests mirror the old inline `#[cfg(test)]` blocks so repository source files
//! stay focused on persistence logic while preserving coverage.

mod search_index {
    use nexus_contracts::enums::{Kind, Scope};
    use nexus_contracts::ids::{MessageId, ProjectId};
    use nexus_contracts::message::{Message, Provenance};
    use nexus_store::repos::messages::Messages;
    use nexus_store::search_index::*;
    use nexus_store::Store;

    async fn migrated() -> Store {
        let s = Store::open(":memory:").await.unwrap();
        s.migrate().await.unwrap();
        s
    }

    fn dm(id: &str, from: &str, body: &str) -> Message {
        Message {
            id: MessageId(id.into()),
            project: ProjectId("p_demo".into()),
            from: from.into(),
            scope: Scope::Dm,
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
            created_at: 1_700_000_000,
        }
    }

    #[tokio::test]
    async fn fts_finds_inserted_message() {
        let store = migrated().await;
        Messages::new(&store)
            .insert(&dm("m_1", "ana", "rebase the auth refactor"))
            .await
            .unwrap();
        let hits = fts_query(&store, "p_demo", "1=1", "rebase", 10)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, MessageId("m_1".into()));
    }

    #[tokio::test]
    async fn fts_rowid_points_at_inserted_message_body() {
        let store = migrated().await;
        let repo = Messages::new(&store);
        repo.insert(&dm("m_alpha", "ana", "alphaunique launch note"))
            .await
            .unwrap();
        repo.insert(&dm("m_beta", "ana", "betaunique launch note"))
            .await
            .unwrap();

        let mut rows = store
            .conn
            .query(
                "SELECT m.message_id, m.body FROM messages_fts f \
                 JOIN messages m ON m.rowid = f.rowid \
                 WHERE messages_fts MATCH 'betaunique'",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("fts row");
        assert_eq!(row.get::<String>(0).unwrap(), "m_beta");
        assert_eq!(row.get::<String>(1).unwrap(), "betaunique launch note");
        assert!(rows.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn scope_sql_excludes_non_scoped_message() {
        let store = migrated().await;
        let m = Messages::new(&store);
        m.insert(&dm("m_ana", "ana", "shared token plan"))
            .await
            .unwrap();
        m.insert(&dm("m_dyl", "dylan", "shared token plan"))
            .await
            .unwrap();
        // scope to only ana's messages
        let hits = fts_query(&store, "p_demo", "m.from_name = 'ana'", "token", 10)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, MessageId("m_ana".into()));
    }

    #[tokio::test]
    async fn fts_is_project_scoped() {
        let store = migrated().await;
        Messages::new(&store)
            .insert(&dm("m_1", "ana", "deploy pipeline"))
            .await
            .unwrap();
        assert!(fts_query(&store, "p_other", "1=1", "deploy", 10)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn vector_helpers_noop_without_core_vector_table() {
        let store = migrated().await;
        let m = Messages::new(&store);
        m.insert(&dm("m_near", "ana", "a")).await.unwrap();
        m.insert(&dm("m_far", "ana", "b")).await.unwrap();
        upsert_vector(&store, &MessageId("m_near".into()), &[1.0, 0.0, 0.0])
            .await
            .unwrap();
        upsert_vector(&store, &MessageId("m_far".into()), &[0.0, 1.0, 0.0])
            .await
            .unwrap();
        let hits = vector_top_k(&store, "p_demo", "1=1", &[1.0, 0.0, 0.0], 1)
            .await
            .unwrap();
        assert!(
            hits.is_empty(),
            "semantic vectors are plugin-owned outside core"
        );
    }
}

mod agents {
    use nexus_store::repos::agents::*;
    use nexus_store::Store;

    async fn migrated() -> Store {
        let store = Store::open(":memory:").await.unwrap();
        store.migrate().await.unwrap();
        store
    }

    fn new_agent(agent_id: &str, name: &str, project: &str) -> NewAgent {
        NewAgent {
            agent_id: agent_id.to_string(),
            project: project.to_string(),
            name: Some(name.to_string()),
            default_harness: Some("codex".to_string()),
            role: Some("backend".to_string()),
            tier: Some("agent".to_string()),
            owner: None,
        }
    }

    #[tokio::test]
    async fn agents_roundtrip_rename_disable_and_list() {
        let store = migrated().await;
        let repo = Agents::new(&store);

        let agent_id = repo
            .create(new_agent("a_blake", "blake", "p_demo"))
            .await
            .unwrap();
        assert_eq!(agent_id, "a_blake");

        let by_id = repo
            .find_by_id("a_blake")
            .await
            .unwrap()
            .expect("agent by id");
        assert_eq!(by_id.name.as_deref(), Some("blake"));
        assert_eq!(by_id.project, "p_demo");
        assert_eq!(by_id.default_harness.as_deref(), Some("codex"));
        assert_eq!(by_id.role.as_deref(), Some("backend"));
        assert_eq!(by_id.tier, "agent");
        assert_eq!(by_id.disabled_at, None);
        assert!(by_id.created_at > 0);

        let by_name = repo
            .find_by_name("blake")
            .await
            .unwrap()
            .expect("agent by name");
        assert_eq!(by_name.agent_id, "a_blake");

        repo.create(new_agent("a_bianca", "bianca", "p_other"))
            .await
            .unwrap();
        assert_eq!(repo.list(Some("p_demo"), false).await.unwrap().len(), 1);
        assert_eq!(repo.list(None, false).await.unwrap().len(), 2);

        repo.rename("a_blake", "blake-renamed").await.unwrap();
        assert!(repo.find_by_name("blake").await.unwrap().is_none());
        assert_eq!(
            repo.find_by_name("blake-renamed")
                .await
                .unwrap()
                .expect("renamed agent")
                .agent_id,
            "a_blake"
        );

        repo.disable("a_blake").await.unwrap();
        assert!(repo
            .find_by_id("a_blake")
            .await
            .unwrap()
            .expect("disabled agent")
            .disabled_at
            .is_some());
        assert!(repo.list(Some("p_demo"), false).await.unwrap().is_empty());
        assert_eq!(repo.list(Some("p_demo"), true).await.unwrap().len(), 1);
    }
}

mod developer_events {
    use nexus_contracts::ids::SessionId;
    use nexus_store::repos::{
        AgentRuntimes, DeveloperEvents, NewAgentRuntime, NewDeveloperEvent, NewSession, Sessions,
        StreamEvents, AGENT_LIFECYCLE_TOPIC,
    };
    use nexus_store::Store;

    async fn migrated() -> Store {
        let store = Store::open(":memory:").await.unwrap();
        store.migrate().await.unwrap();
        store
    }

    fn event(topic: &str, message_id: &str, created_at: i64) -> NewDeveloperEvent {
        NewDeveloperEvent {
            topic: topic.to_string(),
            kind: "message".to_string(),
            message_id: Some(message_id.to_string()),
            thread_name: Some("ops".to_string()),
            dm_name: None,
            from_name: Some("ada".to_string()),
            agent_name: None,
            session_id: None,
            lifecycle: None,
            current_work: None,
            data_json: None,
            created_at,
        }
    }

    #[tokio::test]
    async fn developer_event_sequences_are_per_topic() {
        let store = migrated().await;
        let repo = DeveloperEvents::new(&store);

        assert_eq!(
            repo.append(event("sys.message.thread.ops", "m_1", 10))
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            repo.append(event("sys.message.thread.ops", "m_2", 11))
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            repo.append(event("sys.inbox.ben", "m_2", 11))
                .await
                .unwrap(),
            1
        );

        assert_eq!(repo.latest_seq("sys.message.thread.ops").await.unwrap(), 2);
        assert_eq!(repo.latest_seq("sys.inbox.ben").await.unwrap(), 1);

        let rows = repo.since("sys.message.thread.ops", 1).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].seq, 2);
        assert_eq!(rows[0].message_id.as_deref(), Some("m_2"));
        assert_eq!(rows[0].thread_name.as_deref(), Some("ops"));
        assert_eq!(rows[0].from_name.as_deref(), Some("ada"));
    }

    #[tokio::test]
    async fn agent_lifecycle_event_can_carry_metadata_json() {
        let store = migrated().await;
        let repo = DeveloperEvents::new(&store);
        let session = SessionId("s_rename_ada".into());

        repo.append_agent_lifecycle_with_data(
            "ada-renamed",
            &session,
            "rename",
            None,
            Some(r#"{"oldName":"ada","newName":"ada-renamed"}"#),
            10,
        )
        .await
        .unwrap();

        let rows = repo.since(AGENT_LIFECYCLE_TOPIC, 0).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].agent_name.as_deref(), Some("ada-renamed"));
        assert_eq!(rows[0].session_id.as_deref(), Some("s_rename_ada"));
        assert_eq!(rows[0].lifecycle.as_deref(), Some("rename"));
        assert_eq!(
            rows[0].data_json.as_deref(),
            Some(r#"{"oldName":"ada","newName":"ada-renamed"}"#)
        );
    }

    #[tokio::test]
    async fn agent_lifecycle_events_follow_session_and_runtime_edges() {
        let store = migrated().await;
        let session = SessionId("s_lifecycle_ada".into());
        let sessions = Sessions::new(&store);

        sessions
            .create(NewSession {
                session_id: session.clone(),
                name: Some("ada".into()),
                agent: Some("codex".into()),
                kind: "agent".into(),
                role: None,
                tier: "agent".into(),
                harness_session_id: None,
                client_key: None,
                cwd: None,
                project: "default".into(),
                transport: Some("codex-appserver".into()),
            })
            .await
            .unwrap();
        sessions
            .set_current_work(&session, Some("ship R1.5"))
            .await
            .unwrap();
        StreamEvents::new(&store)
            .append(&session, "turn_end", "{}")
            .await
            .unwrap();
        sessions
            .set_presence(&session, nexus_contracts::enums::Presence::Offline)
            .await
            .unwrap();
        AgentRuntimes::new(&store)
            .create(NewAgentRuntime {
                runtime_id: session.0.clone(),
                agent_id: "a_ada".into(),
                harness: "codex".into(),
                cwd: None,
                transport: Some("codex-appserver".into()),
                presence: Some("online".into()),
                active: true,
            })
            .await
            .unwrap();
        AgentRuntimes::new(&store).stop(&session.0).await.unwrap();

        let rows = DeveloperEvents::new(&store)
            .since(AGENT_LIFECYCLE_TOPIC, 0)
            .await
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.lifecycle.as_deref().unwrap_or_default())
                .collect::<Vec<_>>(),
            vec!["started", "current_work", "turn_end", "offline", "stopped"]
        );
        assert!(rows.iter().all(|row| row.kind == "agent_lifecycle"));
        assert!(rows.iter().all(|row| row.topic == AGENT_LIFECYCLE_TOPIC));
        assert!(rows
            .iter()
            .all(|row| row.agent_name.as_deref() == Some("ada")));
        assert!(rows
            .iter()
            .all(|row| row.session_id.as_deref() == Some("s_lifecycle_ada")));
        assert_eq!(rows[1].current_work.as_deref(), Some("ship R1.5"));
    }
}

mod transcript_archive {
    use nexus_store::repos::transcript_archive::{
        NewTranscriptArchive, TranscriptArchive, TranscriptArchiveProgress,
    };
    use nexus_store::Store;

    async fn migrated() -> Store {
        let store = Store::open(":memory:").await.unwrap();
        store.migrate().await.unwrap();
        store
    }

    #[tokio::test]
    async fn transcript_archive_roundtrips_progress_and_seal() {
        let store = migrated().await;
        let repo = TranscriptArchive::new(&store);

        repo.create(NewTranscriptArchive {
            runtime_id: "s_runtime".to_string(),
            agent_id: Some("a_robin".to_string()),
            agent_name: "robin".to_string(),
            project: "default".to_string(),
            harness: "claude".to_string(),
            source_kind: "file".to_string(),
            source_path: "/tmp/source.jsonl".to_string(),
            archive_path: "/tmp/archive.jsonl".to_string(),
        })
        .await
        .unwrap();

        let created = repo
            .find_by_runtime_id("s_runtime")
            .await
            .unwrap()
            .expect("archive row");
        assert_eq!(created.archive_offset, 0);
        assert_eq!(created.bytes_archived, 0);
        assert_eq!(created.prefix_sha256, None);
        assert_eq!(created.seal_reason, None);

        repo.update_progress(TranscriptArchiveProgress {
            runtime_id: "s_runtime".to_string(),
            bytes_archived: 42,
            prefix_sha256: Some("abc123".to_string()),
            last_event: Some("SessionStart".to_string()),
        })
        .await
        .unwrap();

        let advanced = repo
            .find_by_runtime_id("s_runtime")
            .await
            .unwrap()
            .expect("advanced archive row");
        assert_eq!(advanced.bytes_archived, 42);
        assert_eq!(advanced.prefix_sha256.as_deref(), Some("abc123"));
        assert_eq!(advanced.last_event.as_deref(), Some("SessionStart"));

        repo.reset_source(
            "s_runtime",
            "/tmp/source-2.jsonl",
            "/tmp/archive.jsonl",
            42,
            Some("PostCompact".to_string()),
        )
        .await
        .unwrap();
        let reset = repo
            .find_by_runtime_id("s_runtime")
            .await
            .unwrap()
            .expect("reset archive row");
        assert_eq!(reset.source_path, "/tmp/source-2.jsonl");
        assert_eq!(reset.archive_offset, 42);
        assert_eq!(reset.bytes_archived, 0);
        assert_eq!(reset.prefix_sha256, None);
        assert_eq!(reset.last_event.as_deref(), Some("PostCompact"));

        repo.seal("s_runtime", "fork_or_tamper").await.unwrap();
        let sealed = repo
            .find_by_runtime_id("s_runtime")
            .await
            .unwrap()
            .expect("sealed archive row");
        assert!(sealed.sealed_at.is_some());
        assert_eq!(sealed.seal_reason.as_deref(), Some("fork_or_tamper"));
    }
}

mod producer_identities {
    use nexus_store::repos::ProducerIdentities;
    use nexus_store::Store;

    async fn migrated() -> Store {
        let store = Store::open(":memory:").await.unwrap();
        store.migrate().await.unwrap();
        store
    }

    #[tokio::test]
    async fn durable_producer_identity_matches_cross_pass_semantics() {
        let store = migrated().await;
        let repo = ProducerIdentities::new(&store);

        assert!(repo.admit_streamed("s_runtime", "msg_1").await.unwrap());
        assert!(
            !repo.admit_final("s_runtime", "msg_1").await.unwrap(),
            "final snapshot after streamed id is suppressed"
        );
        assert!(
            repo.admit_final("s_runtime", "msg_1").await.unwrap(),
            "suppression is one-shot after the pending id is consumed"
        );
        assert!(
            repo.admit_final("s_runtime", "msg_2").await.unwrap(),
            "different message id is a real final-only message"
        );
        assert!(
            repo.admit_final("s_other", "msg_1").await.unwrap(),
            "pending ids are scoped per runtime"
        );
    }

    #[tokio::test]
    async fn durable_producer_identity_is_bounded_per_runtime() {
        let store = migrated().await;
        let repo = ProducerIdentities::with_cap(&store, 2);

        repo.insert("s_runtime", "a").await.unwrap();
        repo.insert("s_runtime", "b").await.unwrap();
        repo.insert("s_runtime", "c").await.unwrap();

        let ids = repo
            .list_for_runtime("s_runtime")
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.producer_id)
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["b", "c"]);
        assert!(
            repo.admit_final("s_runtime", "a").await.unwrap(),
            "oldest evicted id no longer suppresses a late final"
        );
        assert!(!repo.admit_final("s_runtime", "b").await.unwrap());
        assert!(!repo.admit_final("s_runtime", "c").await.unwrap());
    }

    #[tokio::test]
    async fn durable_producer_identity_consumes_fifo_and_clears_on_rebind() {
        let store = migrated().await;
        let repo = ProducerIdentities::new(&store);

        repo.insert("s_runtime", "display-a").await.unwrap();
        repo.insert("s_runtime", "display-b").await.unwrap();

        assert_eq!(
            repo.consume_oldest("s_runtime")
                .await
                .unwrap()
                .map(|row| row.producer_id),
            Some("display-a".to_string())
        );
        assert_eq!(
            repo.consume_oldest("s_runtime")
                .await
                .unwrap()
                .map(|row| row.producer_id),
            Some("display-b".to_string())
        );
        assert!(repo.consume_oldest("s_runtime").await.unwrap().is_none());

        repo.insert("s_runtime", "stale-display").await.unwrap();
        repo.clear_runtime("s_runtime").await.unwrap();
        assert!(repo.list_for_runtime("s_runtime").await.unwrap().is_empty());
    }
}

mod topics {
    use nexus_store::repos::topics::*;
    use nexus_store::repos::{AgentRuntimes, Agents, NewAgent, NewAgentRuntime};
    use nexus_store::Store;

    async fn migrated() -> Store {
        let s = Store::open(":memory:").await.unwrap();
        s.migrate().await.unwrap();
        s
    }

    async fn create_agent(store: &Store, agent_id: &str, name: &str) {
        Agents::new(store)
            .create(NewAgent {
                agent_id: agent_id.to_string(),
                project: "p_demo".to_string(),
                name: Some(name.to_string()),
                default_harness: Some("codex".to_string()),
                role: None,
                tier: Some("agent".to_string()),
                owner: None,
            })
            .await
            .unwrap();
    }

    async fn create_runtime(store: &Store, agent_id: &str, runtime_id: &str) {
        AgentRuntimes::new(store)
            .create(NewAgentRuntime {
                runtime_id: runtime_id.to_string(),
                agent_id: agent_id.to_string(),
                harness: "codex".to_string(),
                cwd: Some("/repo".to_string()),
                transport: Some("codex-appserver".to_string()),
                presence: Some("online".to_string()),
                active: true,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn subscribe_returns_cursor_and_appears_in_subscribers() {
        let store = migrated().await;
        let repo = Topics::new(&store);
        repo.ensure("ci", "p_demo").await.unwrap();
        let cursor = repo.subscribe("ci", "s_ana", None).await.unwrap();
        assert_eq!(cursor, 0);
        assert_eq!(
            repo.subscribers("ci").await.unwrap(),
            vec!["s_ana".to_string()]
        );
    }

    #[tokio::test]
    async fn advance_cursor_and_unsubscribe() {
        let store = migrated().await;
        let repo = Topics::new(&store);
        repo.ensure("ci", "p_demo").await.unwrap();
        repo.subscribe("ci", "s_ana", Some("g1")).await.unwrap();
        repo.advance_cursor("ci", "s_ana", 5).await.unwrap();
        assert_eq!(repo.subscribe("ci", "s_ana", None).await.unwrap(), 5);
        repo.unsubscribe("ci", "s_ana").await.unwrap();
        assert!(repo.subscribers("ci").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn stable_subscription_follows_active_runtime() {
        let store = migrated().await;
        create_agent(&store, "a_ana", "ana").await;
        create_runtime(&store, "a_ana", "s_ana_old").await;
        let repo = Topics::new(&store);
        repo.ensure("ci", "p_demo").await.unwrap();
        repo.subscribe("ci", "s_ana_old", None).await.unwrap();

        create_runtime(&store, "a_ana", "s_ana_new").await;
        assert_eq!(
            repo.subscribers("ci").await.unwrap(),
            vec!["s_ana_new".to_string()]
        );

        repo.advance_cursor("ci", "s_ana_new", 7).await.unwrap();
        assert_eq!(repo.subscribe("ci", "s_ana_new", None).await.unwrap(), 7);
        repo.unsubscribe("ci", "s_ana_new").await.unwrap();
        assert!(repo.subscribers("ci").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_is_project_scoped() {
        let store = migrated().await;
        let repo = Topics::new(&store);
        repo.ensure("ci", "p_demo").await.unwrap();
        repo.ensure("deploy", "p_demo").await.unwrap();
        assert_eq!(repo.list("p_demo").await.unwrap().len(), 2);
        assert_eq!(repo.list("p_other").await.unwrap().len(), 0);
    }
}

mod inbox {
    use nexus_common::now;
    use nexus_contracts::enums::{Kind, Presence, Scope};
    use nexus_contracts::ids::{MessageId, ProjectId, SessionId};
    use nexus_contracts::message::{Message, Provenance};
    use nexus_store::repos::inbox::*;
    use nexus_store::repos::messages::Messages;
    use nexus_store::repos::{
        AgentRuntimes, Agents, NewAgent, NewAgentRuntime, NewSession, Sessions,
    };
    use nexus_store::Store;

    async fn migrated() -> Store {
        let s = Store::open(":memory:").await.unwrap();
        s.migrate().await.unwrap();
        s
    }

    async fn create_agent(store: &Store, agent_id: &str, name: &str) {
        Agents::new(store)
            .create(NewAgent {
                agent_id: agent_id.to_string(),
                project: "p_demo".to_string(),
                name: Some(name.to_string()),
                default_harness: Some("codex".to_string()),
                role: None,
                tier: Some("agent".to_string()),
                owner: None,
            })
            .await
            .unwrap();
    }

    async fn create_runtime(store: &Store, agent_id: &str, runtime_id: &str) {
        AgentRuntimes::new(store)
            .create(NewAgentRuntime {
                runtime_id: runtime_id.to_string(),
                agent_id: agent_id.to_string(),
                harness: "codex".to_string(),
                cwd: Some("/repo".to_string()),
                transport: Some("codex-appserver".to_string()),
                presence: Some("online".to_string()),
                active: true,
            })
            .await
            .unwrap();
    }

    async fn create_online_session(store: &Store, name: &str, session_id: &str) {
        Sessions::new(store)
            .create(NewSession {
                session_id: SessionId(session_id.to_string()),
                name: Some(name.to_string()),
                agent: Some("codex".to_string()),
                kind: "agent".to_string(),
                role: None,
                tier: "agent".to_string(),
                harness_session_id: Some(format!("hs_{session_id}")),
                client_key: Some(format!("ck_{session_id}")),
                cwd: Some("/repo".to_string()),
                project: "p_demo".to_string(),
                transport: Some("codex-appserver".to_string()),
            })
            .await
            .unwrap();
    }

    fn msg(id: &str, body: &str) -> Message {
        Message {
            id: MessageId(id.into()),
            project: ProjectId("p_demo".into()),
            from: "ben".into(),
            scope: Scope::Dm,
            thread: None,
            topic: None,
            body: body.into(),
            summary: None,
            provenance: Provenance {
                from: "ben".into(),
                kind: Kind::Agent,
                locality: Default::default(),
                access: None,
                thread: None,
                topic: None,
                stamp: None,
            },
            created_at: 1_700_000_000,
        }
    }

    #[tokio::test]
    async fn enqueue_pending_ack_lifecycle() {
        let store = migrated().await;
        let recipient = SessionId("s_ana".into());
        Messages::new(&store)
            .insert(&msg("m_1", "hi"))
            .await
            .unwrap();
        let inbox = Inbox::new(&store);
        inbox
            .enqueue(&MessageId("m_1".into()), &recipient)
            .await
            .unwrap();

        let pending = inbox.pending_for(&recipient, "p_demo", 50).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].1.id, MessageId("m_1".into()));

        inbox.mark_notified(&recipient).await.unwrap();
        assert_eq!(
            inbox
                .mark_injecting(&MessageId("m_1".into()), &recipient)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            inbox
                .mark_delivered(&MessageId("m_1".into()), &recipient)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            inbox
                .ack(&MessageId("m_1".into()), &recipient)
                .await
                .unwrap(),
            1
        );
        assert!(inbox
            .pending_for(&recipient, "p_demo", 50)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn enqueue_is_idempotent() {
        let store = migrated().await;
        let recipient = SessionId("s_ana".into());
        Messages::new(&store)
            .insert(&msg("m_1", "hi"))
            .await
            .unwrap();
        let inbox = Inbox::new(&store);
        inbox
            .enqueue(&MessageId("m_1".into()), &recipient)
            .await
            .unwrap();
        inbox
            .enqueue(&MessageId("m_1".into()), &recipient)
            .await
            .unwrap();
        assert_eq!(
            inbox
                .pending_for(&recipient, "p_demo", 50)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn pending_delivery_ignores_project_metadata() {
        let store = migrated().await;
        let recipient = SessionId("s_cross_project".into());
        Messages::new(&store)
            .insert(&msg("m_cross_project", "metadata must not gate delivery"))
            .await
            .unwrap();
        let inbox = Inbox::new(&store);
        inbox
            .enqueue(&MessageId("m_cross_project".into()), &recipient)
            .await
            .unwrap();

        let pending = inbox
            .pending_for(&recipient, "different-project-metadata", 50)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].1.id, MessageId("m_cross_project".into()));
    }

    #[tokio::test]
    async fn notified_rows_still_pending_for_drain() {
        let store = migrated().await;
        let recipient = SessionId("s_ana".into());
        Messages::new(&store)
            .insert(&msg("m_1", "hi"))
            .await
            .unwrap();
        let inbox = Inbox::new(&store);
        inbox
            .enqueue(&MessageId("m_1".into()), &recipient)
            .await
            .unwrap();
        inbox.mark_notified(&recipient).await.unwrap();
        // notified rows are still drainable (not yet acked)
        assert_eq!(
            inbox
                .pending_for(&recipient, "p_demo", 50)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn atomic_batch_claim_never_partially_claims_across_notification_boundary() {
        let store = migrated().await;
        let recipient = SessionId("s_atomic_batch".into());
        let first = MessageId("m_atomic_first".into());
        let late = MessageId("m_atomic_late".into());
        let messages = Messages::new(&store);
        let inbox = Inbox::new(&store);

        messages
            .insert(&msg(&first.0, "already notified"))
            .await
            .unwrap();
        messages
            .insert(&msg(&late.0, "arrived after notification boundary"))
            .await
            .unwrap();
        inbox.enqueue(&first, &recipient).await.unwrap();
        inbox.mark_notified(&recipient).await.unwrap();
        inbox.enqueue(&late, &recipient).await.unwrap();

        assert_eq!(
            inbox
                .mark_batch_injecting(&[first.clone(), late.clone()], &recipient)
                .await
                .unwrap(),
            0,
            "a mixed notified/pending batch must claim nothing"
        );

        let mut rows = store
            .conn
            .query(
                "SELECT message_id, state, attempt_count FROM in_flight \
                 WHERE message_id IN ('m_atomic_first','m_atomic_late') ORDER BY message_id",
                (),
            )
            .await
            .unwrap();
        let first_row = rows.next().await.unwrap().expect("first row");
        assert_eq!(first_row.get::<String>(0).unwrap(), "m_atomic_first");
        assert_eq!(first_row.get::<String>(1).unwrap(), "notified");
        assert_eq!(first_row.get::<i64>(2).unwrap(), 0);
        let late_row = rows.next().await.unwrap().expect("late row");
        assert_eq!(late_row.get::<String>(0).unwrap(), "m_atomic_late");
        assert_eq!(late_row.get::<String>(1).unwrap(), "pending");
        assert_eq!(late_row.get::<i64>(2).unwrap(), 0);

        inbox.mark_notified(&recipient).await.unwrap();
        assert_eq!(
            inbox
                .mark_batch_injecting(&[first, late], &recipient)
                .await
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn one_attempt_delivery_transitions_are_state_guarded_and_report_affected_rows() {
        let store = migrated().await;
        let recipient = SessionId("s_ana".into());
        let message = MessageId("m_once".into());
        Messages::new(&store)
            .insert(&msg(&message.0, "deliver once"))
            .await
            .unwrap();
        let inbox = Inbox::new(&store);
        inbox.enqueue(&message, &recipient).await.unwrap();

        assert_eq!(inbox.mark_injecting(&message, &recipient).await.unwrap(), 0);
        inbox.mark_notified(&recipient).await.unwrap();
        assert_eq!(inbox.mark_injecting(&message, &recipient).await.unwrap(), 1);
        assert_eq!(inbox.mark_injecting(&message, &recipient).await.unwrap(), 0);
        assert_eq!(
            inbox
                .mark_delivered(&message, &SessionId("s_wrong".into()))
                .await
                .unwrap(),
            0
        );
        assert_eq!(inbox.mark_delivered(&message, &recipient).await.unwrap(), 1);
        assert_eq!(inbox.mark_delivered(&message, &recipient).await.unwrap(), 0);
        assert_eq!(inbox.ack(&message, &recipient).await.unwrap(), 1);
        assert_eq!(inbox.ack(&message, &recipient).await.unwrap(), 0);

        let mut rows = store
            .conn
            .query(
                "SELECT state, attempt_count, attempt_started_at, delivered_at, acked_at,
                        failed_at, error_code, error_reason, error_details_json
                 FROM in_flight WHERE message_id = 'm_once'",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("delivery row");
        assert_eq!(row.get::<String>(0).unwrap(), "acked");
        assert_eq!(row.get::<i64>(1).unwrap(), 1);
        assert!(row.get::<Option<i64>>(2).unwrap().is_some());
        assert!(row.get::<Option<i64>>(3).unwrap().is_some());
        assert!(row.get::<Option<i64>>(4).unwrap().is_some());
        assert_eq!(row.get::<Option<i64>>(5).unwrap(), None);
        assert_eq!(row.get::<Option<String>>(6).unwrap(), None);
        assert_eq!(row.get::<Option<String>>(7).unwrap(), None);
        assert_eq!(row.get::<Option<String>>(8).unwrap(), None);
    }

    #[tokio::test]
    async fn rejected_steer_restores_unaccepted_claim_to_notified() {
        let store = migrated().await;
        let recipient = SessionId("s_steer_race".into());
        let message = MessageId("m_steer_race".into());
        Messages::new(&store)
            .insert(&msg(&message.0, "turn ended before steer"))
            .await
            .unwrap();
        let inbox = Inbox::new(&store);
        inbox.enqueue(&message, &recipient).await.unwrap();
        inbox.mark_notified(&recipient).await.unwrap();
        assert_eq!(inbox.mark_injecting(&message, &recipient).await.unwrap(), 1);

        assert_eq!(
            inbox
                .restore_rejected_steer(&message, &recipient)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            inbox
                .restore_rejected_steer(&message, &recipient)
                .await
                .unwrap(),
            0,
            "only the exact injecting claim may be restored"
        );

        let pending = inbox.pending_for(&recipient, "p_demo", 50).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].1.id, message);

        let mut rows = store
            .conn
            .query(
                "SELECT state, attempt_count, attempt_started_at, delivered_at, acked_at,
                        failed_at, error_code, error_reason, error_details_json
                 FROM in_flight WHERE message_id = 'm_steer_race'",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("delivery row");
        assert_eq!(row.get::<String>(0).unwrap(), "notified");
        assert_eq!(row.get::<i64>(1).unwrap(), 1);
        assert_eq!(row.get::<Option<i64>>(2).unwrap(), None);
        assert_eq!(row.get::<Option<i64>>(3).unwrap(), None);
        assert_eq!(row.get::<Option<i64>>(4).unwrap(), None);
        assert_eq!(row.get::<Option<i64>>(5).unwrap(), None);
        assert_eq!(row.get::<Option<String>>(6).unwrap(), None);
        assert_eq!(row.get::<Option<String>>(7).unwrap(), None);
        assert_eq!(row.get::<Option<String>>(8).unwrap(), None);
    }

    #[tokio::test]
    async fn structured_delivery_errors_are_terminal_before_or_after_attempt_claim() {
        let store = migrated().await;
        let recipient = SessionId("s_ana".into());
        let pending_message = MessageId("m_dead".into());
        let attempted_message = MessageId("m_limit".into());
        let messages = Messages::new(&store);
        messages
            .insert(&msg(&pending_message.0, "dead target"))
            .await
            .unwrap();
        messages
            .insert(&msg(&attempted_message.0, "accepted then limited"))
            .await
            .unwrap();
        let inbox = Inbox::new(&store);
        inbox.enqueue(&pending_message, &recipient).await.unwrap();
        inbox.enqueue(&attempted_message, &recipient).await.unwrap();

        assert_eq!(
            inbox
                .mark_delivery_error(
                    &pending_message,
                    &recipient,
                    TARGET_DEAD_ERROR_CODE,
                    "target process is dead",
                    Some("{\"harness\":\"codex\"}"),
                )
                .await
                .unwrap(),
            1
        );
        inbox.mark_notified(&recipient).await.unwrap();
        assert_eq!(
            inbox
                .mark_injecting(&attempted_message, &recipient)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            inbox
                .mark_delivery_error(
                    &attempted_message,
                    &recipient,
                    PROVIDER_LIMIT_ERROR_CODE,
                    "provider usage limit reached",
                    Some("{\"provider\":\"anthropic\",\"resetAt\":123}"),
                )
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            inbox
                .mark_delivery_error(
                    &attempted_message,
                    &recipient,
                    PROVIDER_LIMIT_ERROR_CODE,
                    "duplicate terminal signal",
                    None,
                )
                .await
                .unwrap(),
            0
        );

        let mut rows = store
            .conn
            .query(
                "SELECT message_id, state, attempt_count, failed_at, error_code, error_reason,
                        error_details_json, delivered_at, acked_at
                 FROM in_flight ORDER BY message_id",
                (),
            )
            .await
            .unwrap();
        let mut errors = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            errors.push((
                row.get::<String>(0).unwrap(),
                row.get::<String>(1).unwrap(),
                row.get::<i64>(2).unwrap(),
                row.get::<Option<i64>>(3).unwrap(),
                row.get::<String>(4).unwrap(),
                row.get::<String>(5).unwrap(),
                row.get::<Option<String>>(6).unwrap(),
                row.get::<Option<i64>>(7).unwrap(),
                row.get::<Option<i64>>(8).unwrap(),
            ));
        }
        assert_eq!(errors[0].0, "m_dead");
        assert_eq!(errors[0].1, "error");
        assert_eq!(errors[0].2, 0);
        assert!(errors[0].3.is_some());
        assert_eq!(errors[0].4, TARGET_DEAD_ERROR_CODE);
        assert_eq!(errors[0].5, "target process is dead");
        assert_eq!(errors[0].6.as_deref(), Some("{\"harness\":\"codex\"}"));
        assert_eq!(errors[0].7, None);
        assert_eq!(errors[0].8, None);

        assert_eq!(errors[1].0, "m_limit");
        assert_eq!(errors[1].1, "error");
        assert_eq!(errors[1].2, 1);
        assert!(errors[1].3.is_some());
        assert_eq!(errors[1].4, PROVIDER_LIMIT_ERROR_CODE);
        assert_eq!(errors[1].5, "provider usage limit reached");
        assert_eq!(
            errors[1].6.as_deref(),
            Some("{\"provider\":\"anthropic\",\"resetAt\":123}")
        );
        assert_eq!(errors[1].7, None);
        assert_eq!(errors[1].8, None);
        assert!(inbox.recipients_with_pending().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn restart_recovery_errors_injecting_and_only_explicit_requeue_makes_it_eligible() {
        let store = migrated().await;
        let recipient = SessionId("s_ana".into());
        let message = MessageId("m_ambiguous".into());
        Messages::new(&store)
            .insert(&msg(&message.0, "crash at harness boundary"))
            .await
            .unwrap();
        let inbox = Inbox::new(&store);
        inbox.enqueue(&message, &recipient).await.unwrap();
        inbox.mark_notified(&recipient).await.unwrap();
        assert_eq!(inbox.mark_injecting(&message, &recipient).await.unwrap(), 1);

        let recovered = inbox.recover_stale_injecting().await.unwrap();
        assert_eq!(recovered.count, 1);
        assert_eq!(recovered.in_flight_ids.len(), 1);
        assert!(inbox.recipients_with_pending().await.unwrap().is_empty());
        assert_eq!(
            inbox.recover_stale_injecting().await.unwrap(),
            DeadLetterMutation::default()
        );

        let requeued = inbox
            .requeue_dead_letters(DeadLetterSelector::InFlightId(
                recovered.in_flight_ids[0].clone(),
            ))
            .await
            .unwrap();
        assert_eq!(requeued.count, 1);
        assert_eq!(
            inbox
                .pending_for(&recipient, "p_demo", 10)
                .await
                .unwrap()
                .len(),
            1
        );

        let mut rows = store
            .conn
            .query(
                "SELECT state, attempt_count, attempt_started_at, failed_at, error_code,
                        error_reason, error_details_json, delivered_at, acked_at
                 FROM in_flight WHERE message_id = 'm_ambiguous'",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("requeued delivery");
        assert_eq!(row.get::<String>(0).unwrap(), "pending");
        assert_eq!(row.get::<i64>(1).unwrap(), 1);
        assert_eq!(row.get::<Option<i64>>(2).unwrap(), None);
        assert_eq!(row.get::<Option<i64>>(3).unwrap(), None);
        assert_eq!(row.get::<Option<String>>(4).unwrap(), None);
        assert_eq!(row.get::<Option<String>>(5).unwrap(), None);
        assert_eq!(row.get::<Option<String>>(6).unwrap(), None);
        assert_eq!(row.get::<Option<i64>>(7).unwrap(), None);
        assert_eq!(row.get::<Option<i64>>(8).unwrap(), None);

        inbox.mark_notified(&recipient).await.unwrap();
        assert_eq!(inbox.mark_injecting(&message, &recipient).await.unwrap(), 1);
        let mut rows = store
            .conn
            .query(
                "SELECT state, attempt_count FROM in_flight WHERE message_id = 'm_ambiguous'",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(row.get::<String>(0).unwrap(), "injecting");
        assert_eq!(row.get::<i64>(1).unwrap(), 2);
    }

    #[tokio::test]
    async fn restart_recovery_drains_more_than_one_bounded_candidate_page() {
        let store = migrated().await;
        store
            .conn
            .execute_batch(
                "WITH RECURSIVE sequence(value) AS ( \
                   SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value < 1201 \
                 ) \
                 INSERT INTO in_flight (in_flight_id, message_id, recipient_session, state) \
                 SELECT printf('if_recover_%04d', value), printf('m_%04d', value), \
                        's_recover', 'injecting' FROM sequence",
            )
            .await
            .unwrap();

        let mutation = Inbox::new(&store).recover_stale_injecting().await.unwrap();
        assert_eq!(mutation.count, 1201);
        assert_eq!(mutation.in_flight_ids.len(), 1201);
        let mut rows = store
            .conn
            .query(
                "SELECT COUNT(*) FROM in_flight WHERE state = 'error' \
                 AND error_code = 'delivery_outcome_unknown'",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            1201
        );
    }

    #[tokio::test]
    async fn recipients_with_pending_lists_distinct_undrained() {
        let store = migrated().await;
        let s_a = SessionId("s_a".into());
        let s_b = SessionId("s_b".into());
        let m = Messages::new(&store);
        m.insert(&msg("m1", "a")).await.unwrap();
        m.insert(&msg("m2", "b")).await.unwrap();
        m.insert(&msg("m3", "c")).await.unwrap();
        let inbox = Inbox::new(&store);
        // two pending rows for s_a (distinct collapses to one)
        inbox.enqueue(&MessageId("m1".into()), &s_a).await.unwrap();
        inbox.enqueue(&MessageId("m2".into()), &s_a).await.unwrap();
        // one delivered row for s_b — must NOT appear
        inbox.enqueue(&MessageId("m3".into()), &s_b).await.unwrap();
        inbox.mark_notified(&s_b).await.unwrap();
        inbox
            .mark_injecting(&MessageId("m3".into()), &s_b)
            .await
            .unwrap();
        inbox
            .mark_delivered(&MessageId("m3".into()), &s_b)
            .await
            .unwrap();

        let got = inbox.recipients_with_pending().await.unwrap();
        assert_eq!(got, vec![SessionId("s_a".into())]);
    }

    #[tokio::test]
    async fn boot_pending_recipients_exclude_messages_at_or_after_start_cutoff() {
        let store = migrated().await;
        let before = SessionId("s_before_boot".into());
        let after = SessionId("s_after_boot".into());
        let messages = Messages::new(&store);
        let mut before_message = msg("m_before_boot", "queued before boot");
        before_message.created_at = 999;
        messages.insert(&before_message).await.unwrap();
        let mut after_message = msg("m_after_boot", "queued after boot started");
        after_message.created_at = 1_000;
        messages.insert(&after_message).await.unwrap();
        let inbox = Inbox::new(&store);
        inbox.enqueue(&before_message.id, &before).await.unwrap();
        inbox.enqueue(&after_message.id, &after).await.unwrap();

        assert_eq!(
            inbox.recipients_with_pending_before(1_000).await.unwrap(),
            vec![before]
        );
    }

    #[tokio::test]
    async fn boot_dead_letter_excludes_messages_at_or_after_start_cutoff() {
        let store = migrated().await;
        let messages = Messages::new(&store);
        let mut before_message = msg("m_dead_before_boot", "orphaned before boot");
        before_message.created_at = 999;
        messages.insert(&before_message).await.unwrap();
        let mut after_message = msg("m_dead_after_boot", "orphaned after boot started");
        after_message.created_at = 1_000;
        messages.insert(&after_message).await.unwrap();
        store
            .conn
            .execute(
                "INSERT INTO in_flight (in_flight_id, message_id, recipient_session, state) \
                 VALUES ('if_dead_before_boot', 'm_dead_before_boot', 's_missing_before', 'pending'), \
                        ('if_dead_after_boot', 'm_dead_after_boot', 's_missing_after', 'pending')",
                (),
            )
            .await
            .unwrap();

        assert_eq!(
            Inbox::new(&store)
                .dead_letter_undeliverable_for_dead_recipients_before(1_000)
                .await
                .unwrap(),
            1
        );
        let mut rows = store
            .conn
            .query(
                "SELECT in_flight_id, state FROM in_flight ORDER BY in_flight_id",
                (),
            )
            .await
            .unwrap();
        let first = rows.next().await.unwrap().unwrap();
        assert_eq!(first.get::<String>(0).unwrap(), "if_dead_after_boot");
        assert_eq!(first.get::<String>(1).unwrap(), "pending");
        let second = rows.next().await.unwrap().unwrap();
        assert_eq!(second.get::<String>(0).unwrap(), "if_dead_before_boot");
        assert_eq!(second.get::<String>(1).unwrap(), "error");
    }

    #[tokio::test]
    async fn stable_agent_pending_drains_from_replaced_runtime() {
        let store = migrated().await;
        create_agent(&store, "a_ana", "ana").await;
        create_runtime(&store, "a_ana", "s_ana_old").await;
        Messages::new(&store)
            .insert(&msg("m_agent", "resume-safe"))
            .await
            .unwrap();
        let inbox = Inbox::new(&store);
        inbox
            .enqueue_for_agent(&MessageId("m_agent".into()), "a_ana")
            .await
            .unwrap();

        create_runtime(&store, "a_ana", "s_ana_new").await;
        assert_eq!(
            inbox.recipients_with_pending().await.unwrap(),
            vec![SessionId("s_ana_new".into())]
        );
        assert!(inbox
            .pending_for(&SessionId("s_ana_old".into()), "p_demo", 50)
            .await
            .unwrap()
            .is_empty());

        let pending = inbox
            .pending_for(&SessionId("s_ana_new".into()), "p_demo", 50)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].1.id, MessageId("m_agent".into()));

        let replacement = SessionId("s_ana_new".into());
        inbox.mark_notified(&replacement).await.unwrap();
        inbox
            .mark_injecting(&MessageId("m_agent".into()), &replacement)
            .await
            .unwrap();
        inbox
            .mark_delivered(&MessageId("m_agent".into()), &replacement)
            .await
            .unwrap();
        inbox
            .ack(&MessageId("m_agent".into()), &replacement)
            .await
            .unwrap();
        assert!(inbox
            .pending_for(&SessionId("s_ana_new".into()), "p_demo", 50)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn concrete_online_session_drains_even_when_runtime_row_is_inactive() {
        let store = migrated().await;
        create_agent(&store, "a_ana", "ana").await;
        create_online_session(&store, "ana", "s_ana").await;
        create_runtime(&store, "a_ana", "s_ana").await;
        AgentRuntimes::new(&store)
            .set_active("s_ana", false)
            .await
            .unwrap();

        Messages::new(&store)
            .insert_with_agents(&msg("m_direct", "still-live session"), None, Some("a_ana"))
            .await
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO in_flight (in_flight_id, message_id, recipient_session, \
                 recipient_agent_id, state) VALUES ('if_direct', 'm_direct', 's_ana', \
                 'a_ana', 'pending')",
                (),
            )
            .await
            .unwrap();

        let inbox = Inbox::new(&store);
        let pending = inbox
            .pending_for(&SessionId("s_ana".into()), "p_demo", 50)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].1.id, MessageId("m_direct".into()));

        let recipient = SessionId("s_ana".into());
        inbox.mark_notified(&recipient).await.unwrap();
        inbox
            .mark_injecting(&MessageId("m_direct".into()), &recipient)
            .await
            .unwrap();
        inbox
            .mark_delivered(&MessageId("m_direct".into()), &recipient)
            .await
            .unwrap();
        assert!(inbox
            .pending_for(&SessionId("s_ana".into()), "p_demo", 50)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn stopped_stable_runtime_with_concrete_pending_recipient_reappears_for_boot_respawn() {
        let store = migrated().await;
        create_agent(&store, "a_ana", "ana").await;
        create_online_session(&store, "ana", "s_ana").await;
        create_runtime(&store, "a_ana", "s_ana").await;
        Messages::new(&store)
            .insert_with_agents(&msg("m_after_stop", "wake after stop"), None, Some("a_ana"))
            .await
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO in_flight (in_flight_id, message_id, recipient_session, \
                 recipient_agent_id, state) VALUES ('if_after_stop', 'm_after_stop', \
                 's_ana', 'a_ana', 'pending')",
                (),
            )
            .await
            .unwrap();

        Sessions::new(&store)
            .set_presence(&SessionId("s_ana".into()), Presence::Offline)
            .await
            .unwrap();
        AgentRuntimes::new(&store).stop("s_ana").await.unwrap();

        assert_eq!(
            Inbox::new(&store).recipients_with_pending().await.unwrap(),
            vec![SessionId("s_ana".into())]
        );
    }

    #[tokio::test]
    async fn reap_undeliverable_removes_only_dead_recipient_backlog() {
        let store = migrated().await;
        let messages = Messages::new(&store);
        for id in ["m_orphan", "m_stale", "m_recent", "m_replaced", "m_online"] {
            messages.insert(&msg(id, id)).await.unwrap();
        }
        create_agent(&store, "a_replaced", "replaced").await;
        create_runtime(&store, "a_replaced", "s_replaced_active").await;
        create_agent(&store, "a_online", "online").await;
        create_online_session(&store, "online", "s_online").await;
        create_runtime(&store, "a_online", "s_online").await;
        AgentRuntimes::new(&store)
            .set_active("s_online", false)
            .await
            .unwrap();
        create_online_session(&store, "recent", "s_recent").await;
        Sessions::new(&store)
            .set_presence(&SessionId("s_recent".into()), Presence::Offline)
            .await
            .unwrap();
        Sessions::new(&store)
            .touch_heartbeat(&SessionId("s_recent".into()))
            .await
            .unwrap();
        create_online_session(&store, "stale", "s_stale").await;
        Sessions::new(&store)
            .set_presence(&SessionId("s_stale".into()), Presence::Offline)
            .await
            .unwrap();
        let old_heartbeat = now() - (7 * 60 * 60 * 1_000);
        store
            .conn
            .execute(
                "UPDATE sessions SET last_heartbeat = ?2 WHERE session_id = ?1",
                ("s_stale", old_heartbeat),
            )
            .await
            .unwrap();

        store
            .conn
            .execute(
                "INSERT INTO in_flight (in_flight_id, message_id, recipient_session, \
                 recipient_agent_id, state) VALUES \
                 ('if_orphan', 'm_orphan', 's_missing', NULL, 'pending'), \
                 ('if_stale', 'm_stale', 's_stale', NULL, 'notified'), \
                 ('if_recent', 'm_recent', 's_recent', NULL, 'pending'), \
                 ('if_replaced', 'm_replaced', 's_old', 'a_replaced', 'pending'), \
                 ('if_online', 'm_online', 's_online', 'a_online', 'pending')",
                (),
            )
            .await
            .unwrap();

        let reaped = Inbox::new(&store)
            .reap_undeliverable_for_dead_recipients()
            .await
            .unwrap();

        // Only the recipient whose session row no longer EXISTS is reaped. Stale/offline
        // sessions keep their pending mail: rows are durable and an existing session can still
        // be revived — the old stale-heartbeat clause deleted freshly-registered consumers'
        // mail (no heartbeat yet) the moment the boot respawn pass ran.
        assert_eq!(reaped, 1);
        let mut rows = store
            .conn
            .query(
                "SELECT in_flight_id FROM in_flight ORDER BY in_flight_id",
                (),
            )
            .await
            .unwrap();
        let mut ids = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            ids.push(row.get::<String>(0).unwrap());
        }
        assert_eq!(
            ids,
            vec!["if_online", "if_recent", "if_replaced", "if_stale"]
        );
    }

    #[tokio::test]
    async fn boot_settles_dead_and_unreachable_targets_without_deleting_mail() {
        let store = migrated().await;
        Messages::new(&store)
            .insert(&msg("m_missing_target", "preserve my failure"))
            .await
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO in_flight (in_flight_id, message_id, recipient_session, state) \
                 VALUES ('if_missing_target', 'm_missing_target', 's_gone', 'pending')",
                (),
            )
            .await
            .unwrap();

        let inbox = Inbox::new(&store);
        assert_eq!(
            inbox
                .dead_letter_undeliverable_for_dead_recipients()
                .await
                .unwrap(),
            1
        );
        let mut rows = store
            .conn
            .query(
                "SELECT state, error_code, failed_at FROM in_flight \
                 WHERE in_flight_id = 'if_missing_target'",
                (),
            )
            .await
            .unwrap();
        let row = rows
            .next()
            .await
            .unwrap()
            .expect("delivery failure retained");
        assert_eq!(row.get::<String>(0).unwrap(), "error");
        assert_eq!(row.get::<String>(1).unwrap(), "target_dead");
        assert!(row.get::<Option<i64>>(2).unwrap().is_some());

        create_online_session(&store, "ana", "s_ana_unreachable").await;
        Messages::new(&store)
            .insert(&msg("m_unreachable_target", "one revive attempt"))
            .await
            .unwrap();
        inbox
            .enqueue(
                &MessageId("m_unreachable_target".into()),
                &SessionId("s_ana_unreachable".into()),
            )
            .await
            .unwrap();
        assert_eq!(
            inbox
                .mark_recipient_pending_error(
                    &SessionId("s_ana_unreachable".into()),
                    "target_unreachable",
                    "adapter could not be revived",
                    None,
                )
                .await
                .unwrap(),
            1
        );
        assert!(inbox
            .pending_for(&SessionId("s_ana_unreachable".into()), "p_demo", 10)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn dead_letters_list_requeue_and_purge_are_operator_visible() {
        let store = migrated().await;
        create_agent(&store, "a_ana", "ana").await;
        create_online_session(&store, "ana", "s_ana").await;
        create_runtime(&store, "a_ana", "s_ana").await;
        Messages::new(&store)
            .insert_with_agents(
                &msg(
                    "m_dead",
                    "this body is long enough to prove previews are bounded for operator scans",
                ),
                None,
                Some("a_ana"),
            )
            .await
            .unwrap();
        Messages::new(&store)
            .insert_with_agents(
                &msg("m_agent_dead", "agent-only dead letter"),
                None,
                Some("a_ana"),
            )
            .await
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO in_flight (in_flight_id, message_id, recipient_session, \
                 recipient_agent_id, state, attempt_count, error_code, error_reason, \
                 error_details_json) VALUES \
                 ('if_dead', 'm_dead', 's_ana', 'a_ana', 'error', 2, 'provider_error', \
                 'provider server error', '{\"retryable\":true,\"source\":\"claude.acp.prompt_error\"}')",
                (),
            )
            .await
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO in_flight (in_flight_id, message_id, recipient_agent_id, \
                 state, error_reason) VALUES \
                 ('if_agent_dead', 'm_agent_dead', 'a_ana', 'error', 'agent-only failure')",
                (),
            )
            .await
            .unwrap();

        let inbox = Inbox::new(&store);
        let rows = inbox
            .dead_letters(DeadLetterFilter {
                target: Some("ana".into()),
                since: None,
                limit: 10,
            })
            .await
            .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].in_flight_id, "if_dead");
        assert_eq!(rows[0].message_id, MessageId("m_dead".into()));
        assert_eq!(rows[0].recipient_name.as_deref(), Some("ana"));
        assert_eq!(rows[0].recipient_agent_id.as_deref(), Some("a_ana"));
        assert_eq!(
            rows[0].error_reason.as_deref(),
            Some("provider server error")
        );
        assert_eq!(rows[0].attempt_count, 2);
        assert_eq!(rows[0].error_code.as_deref(), Some("provider_error"));
        assert_eq!(
            rows[0].error_details,
            Some(serde_json::json!({
                "retryable": true,
                "source": "claude.acp.prompt_error",
            }))
        );
        assert!(rows[0].body_preview.len() <= 80);
        let agent_only = rows
            .iter()
            .find(|row| row.in_flight_id == "if_agent_dead")
            .expect("agent-only dead-letter row");
        assert_eq!(agent_only.recipient_name.as_deref(), Some("ana"));
        assert_eq!(agent_only.recipient_agent_id.as_deref(), Some("a_ana"));
        assert_eq!(inbox.dead_letter_summary().await.unwrap().count, 2);

        let requeued = inbox
            .requeue_dead_letters(DeadLetterSelector::InFlightId("if_dead".into()))
            .await
            .unwrap();
        assert_eq!(requeued.count, 1);
        assert_eq!(inbox.dead_letter_summary().await.unwrap().count, 1);
        let pending = inbox
            .pending_for(&SessionId("s_ana".into()), "p_demo", 10)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, "if_dead");

        let purged_agent = inbox
            .purge_dead_letters(DeadLetterSelector::Filter(DeadLetterFilter {
                target: Some("ana".into()),
                since: None,
                limit: 10,
            }))
            .await
            .unwrap();
        assert_eq!(purged_agent.count, 1);
        assert_eq!(
            purged_agent.in_flight_ids,
            vec!["if_agent_dead".to_string()]
        );

        store
            .conn
            .execute(
                "UPDATE in_flight SET state = 'error', error_reason = 'operator purged' \
                 WHERE in_flight_id = 'if_dead'",
                (),
            )
            .await
            .unwrap();
        let purged = inbox
            .purge_dead_letters(DeadLetterSelector::InFlightId("if_dead".into()))
            .await
            .unwrap();
        assert_eq!(purged.count, 1);
        assert!(inbox
            .dead_letters(DeadLetterFilter {
                target: None,
                since: None,
                limit: 10,
            })
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn delivery_ttl_moves_old_pending_and_notified_rows_to_dlq() {
        let store = migrated().await;
        create_agent(&store, "a_ana", "ana").await;
        create_online_session(&store, "ana", "s_ana").await;
        create_runtime(&store, "a_ana", "s_ana").await;

        let now_ms = 1_800_000_000_i64;
        let ttl_ms = 30 * 60 * 1_000;
        let old = now_ms - ttl_ms - 1;
        let recent = now_ms - ttl_ms + 1;
        let messages = Messages::new(&store);
        for (id, created_at) in [
            ("m_old_pending", old),
            ("m_old_notified", old),
            ("m_recent_pending", recent),
            ("m_old_delivered", old),
            ("m_old_error", old),
        ] {
            let mut message = msg(id, id);
            message.created_at = created_at;
            messages.insert(&message).await.unwrap();
        }
        store
            .conn
            .execute(
                "INSERT INTO in_flight (in_flight_id, message_id, recipient_session, \
                 recipient_agent_id, state, error_reason) VALUES \
                 ('if_old_pending', 'm_old_pending', 's_ana', 'a_ana', 'pending', NULL), \
                 ('if_old_notified', 'm_old_notified', 's_ana', 'a_ana', 'notified', NULL), \
                 ('if_recent_pending', 'm_recent_pending', 's_ana', 'a_ana', 'pending', NULL), \
                 ('if_old_delivered', 'm_old_delivered', 's_ana', 'a_ana', 'delivered', NULL), \
                 ('if_old_error', 'm_old_error', 's_ana', 'a_ana', 'error', 'existing')",
                (),
            )
            .await
            .unwrap();

        let mutation = Inbox::new(&store)
            .dead_letter_expired_deliveries(now_ms, ttl_ms)
            .await
            .unwrap();

        assert_eq!(mutation.count, 2);
        assert_eq!(
            mutation.in_flight_ids,
            vec!["if_old_notified".to_string(), "if_old_pending".to_string()]
        );

        let mut rows = store
            .conn
            .query(
                "SELECT in_flight_id, state, error_reason FROM in_flight ORDER BY in_flight_id",
                (),
            )
            .await
            .unwrap();
        let mut states = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            states.push((
                row.get::<String>(0).unwrap(),
                row.get::<String>(1).unwrap(),
                row.get::<Option<String>>(2).unwrap(),
            ));
        }
        assert_eq!(
            states,
            vec![
                (
                    "if_old_delivered".to_string(),
                    "delivered".to_string(),
                    None
                ),
                (
                    "if_old_error".to_string(),
                    "error".to_string(),
                    Some("existing".to_string())
                ),
                (
                    "if_old_notified".to_string(),
                    "error".to_string(),
                    Some("delivery_timeout".to_string())
                ),
                (
                    "if_old_pending".to_string(),
                    "error".to_string(),
                    Some("delivery_timeout".to_string())
                ),
                ("if_recent_pending".to_string(), "pending".to_string(), None),
            ]
        );

        let dead_letters = Inbox::new(&store)
            .dead_letters(DeadLetterFilter {
                target: Some("ana".into()),
                since: None,
                limit: 10,
            })
            .await
            .unwrap();
        let mut timeout_ids = dead_letters
            .iter()
            .filter(|row| row.error_reason.as_deref() == Some("delivery_timeout"))
            .map(|row| row.in_flight_id.as_str())
            .collect::<Vec<_>>();
        timeout_ids.sort();
        assert_eq!(timeout_ids, vec!["if_old_notified", "if_old_pending"]);
    }

    #[tokio::test]
    async fn delivery_ttl_drains_more_than_one_bounded_candidate_page() {
        let store = migrated().await;
        store
            .conn
            .execute_batch(
                "WITH RECURSIVE sequence(value) AS ( \
                   SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value < 1201 \
                 ) \
                 INSERT INTO messages (message_id, body, created_at, project) \
                 SELECT printf('m_expire_%04d', value), 'expired', 1, 'metadata' FROM sequence; \
                 WITH RECURSIVE sequence(value) AS ( \
                   SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value < 1201 \
                 ) \
                 INSERT INTO in_flight (in_flight_id, message_id, recipient_session, state) \
                 SELECT printf('if_expire_%04d', value), printf('m_expire_%04d', value), \
                        's_expire', 'pending' FROM sequence",
            )
            .await
            .unwrap();

        let mutation = Inbox::new(&store)
            .dead_letter_expired_deliveries(10_000, 1_000)
            .await
            .unwrap();
        assert_eq!(mutation.count, 1201);
        assert_eq!(mutation.in_flight_ids.len(), 1201);
        let mut rows = store
            .conn
            .query(
                "SELECT COUNT(*) FROM in_flight WHERE state = 'error' \
                 AND error_code = 'delivery_timeout'",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            1201
        );
    }

    #[test]
    fn dead_letter_mutations_stay_on_pinned_write_txn_connection() {
        let source = include_str!("../src/repos/inbox.rs");
        for function in [
            "dead_letter_expired_deliveries",
            "recover_stale_injecting",
            "requeue_dead_letters",
            "purge_dead_letters",
            "selected_dead_letter_ids",
        ] {
            let body = function_body(source, function);
            let compact_body = body
                .chars()
                .filter(|ch| !ch.is_whitespace())
                .collect::<String>();
            assert!(
                !compact_body.contains("self.store.conn"),
                "{function} must not use StoreConnection inside a DLQ write transaction"
            );
        }
    }

    fn function_body<'a>(source: &'a str, name: &str) -> &'a str {
        let signature = format!("fn {name}");
        let start = source.find(&signature).expect("function signature");
        let after_signature = &source[start..];
        let body_start = after_signature.find('{').expect("function body start");
        let mut depth = 0usize;
        let mut body_end = None;
        for (idx, ch) in after_signature[body_start..].char_indices() {
            match ch {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        body_end = Some(body_start + idx + ch.len_utf8());
                        break;
                    }
                }
                _ => {}
            }
        }
        &after_signature[..body_end.expect("function body end")]
    }

    #[tokio::test]
    async fn ack_many_bulk_clears_thread() {
        let store = migrated().await;
        let recipient = SessionId("s_ana".into());
        let m = Messages::new(&store);
        m.insert(&msg("m_1", "a")).await.unwrap();
        m.insert(&msg("m_2", "b")).await.unwrap();
        let inbox = Inbox::new(&store);
        inbox
            .enqueue(&MessageId("m_1".into()), &recipient)
            .await
            .unwrap();
        inbox
            .enqueue(&MessageId("m_2".into()), &recipient)
            .await
            .unwrap();
        inbox.mark_notified(&recipient).await.unwrap();
        for message in [MessageId("m_1".into()), MessageId("m_2".into())] {
            inbox.mark_injecting(&message, &recipient).await.unwrap();
            inbox.mark_delivered(&message, &recipient).await.unwrap();
        }
        inbox
            .ack_many(
                &[MessageId("m_1".into()), MessageId("m_2".into())],
                &recipient,
            )
            .await
            .unwrap();
        assert!(inbox
            .pending_for(&recipient, "p_demo", 50)
            .await
            .unwrap()
            .is_empty());
    }
}

mod agent_credentials {
    use nexus_store::repos::agent_credentials::*;
    use nexus_store::repos::{Agents, NewAgent};
    use nexus_store::Store;

    async fn migrated() -> Store {
        let store = Store::open(":memory:").await.unwrap();
        store.migrate().await.unwrap();
        store
    }

    async fn create_agent(store: &Store) {
        Agents::new(store)
            .create(NewAgent {
                agent_id: "a_blake".to_string(),
                project: "p_demo".to_string(),
                name: Some("blake".to_string()),
                default_harness: Some("codex".to_string()),
                role: None,
                tier: Some("agent".to_string()),
                owner: None,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn credentials_create_find_touch_and_revoke() {
        let store = migrated().await;
        create_agent(&store).await;
        let repo = AgentCredentials::new(&store);

        let credential_id = repo
            .create_hash(NewAgentCredential {
                credential_id: "cred_1".to_string(),
                agent_id: "a_blake".to_string(),
                secret_hash: "sha256:test".to_string(),
                purpose: Some("runtime".to_string()),
                label: Some("local".to_string()),
                scopes_json: r#"["runtime:register"]"#.to_string(),
                metadata_json: Some(r#"{"createdBy":"test"}"#.to_string()),
            })
            .await
            .unwrap();
        assert_eq!(credential_id, "cred_1");

        let active = repo.find_active("a_blake").await.unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].credential_id, "cred_1");
        assert_eq!(active[0].secret_hash, "sha256:test");
        assert_eq!(active[0].purpose.as_deref(), Some("runtime"));
        assert_eq!(active[0].label.as_deref(), Some("local"));
        assert_eq!(active[0].scopes_json, r#"["runtime:register"]"#);
        assert_eq!(
            active[0].metadata_json.as_deref(),
            Some(r#"{"createdBy":"test"}"#)
        );
        assert_eq!(active[0].revoked_at, None);
        assert_eq!(active[0].last_used_at, None);
        assert!(active[0].created_at > 0);

        repo.touch_last_used("cred_1").await.unwrap();
        let touched = repo.find_active("a_blake").await.unwrap();
        assert!(touched[0].last_used_at.is_some());

        repo.revoke("cred_1").await.unwrap();
        assert!(repo.find_active("a_blake").await.unwrap().is_empty());
    }
}

mod sessions {
    use libsql::params;
    use nexus_contracts::enums::{Kind, Presence, Scope};
    use nexus_contracts::ids::{MessageId, ProjectId, SessionId};
    use nexus_contracts::message::{Message, Provenance};
    use nexus_store::repos::sessions::*;
    use nexus_store::repos::{AgentCredentials, Agents, Messages, NewAgent, NewAgentCredential};
    use nexus_store::Store;

    async fn migrated() -> Store {
        let s = Store::open(":memory:").await.unwrap();
        s.migrate().await.unwrap();
        s
    }

    fn new_session(name: &str, ck: &str) -> NewSession {
        NewSession {
            session_id: SessionId(format!("s_{name}")),
            name: Some(name.into()),
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some("h_1".into()),
            client_key: Some(ck.into()),
            cwd: None,
            project: "p_demo".into(),
            transport: None,
        }
    }

    fn msg(id: &str, from: &str) -> Message {
        Message {
            id: MessageId(id.to_string()),
            project: ProjectId("p_demo".to_string()),
            from: from.to_string(),
            scope: Scope::Dm,
            thread: None,
            topic: None,
            body: "hello".to_string(),
            summary: None,
            provenance: Provenance {
                from: from.to_string(),
                kind: Kind::Agent,
                locality: Default::default(),
                access: None,
                thread: None,
                topic: None,
                stamp: None,
            },
            created_at: 1_700_000_000,
        }
    }

    #[tokio::test]
    async fn create_then_find_by_client_key_resumes() {
        let store = migrated().await;
        let repo = Sessions::new(&store);
        let id = repo.create(new_session("ben", "ck1")).await.unwrap();
        let found = repo
            .find_by_client_key("p_demo", "ck1")
            .await
            .unwrap()
            .expect("session exists");
        assert_eq!(found.session_id, id);
        assert_eq!(found.name.as_deref(), Some("ben"));
        assert_eq!(found.presence.as_deref(), Some("online"));
    }

    #[tokio::test]
    async fn find_by_name_after_create_and_duplicate_returns_existing() {
        let store = migrated().await;
        let repo = Sessions::new(&store);
        repo.create(new_session("ana", "ck-a")).await.unwrap();
        let row = repo.find_by_name("p_demo", "ana").await.unwrap();
        assert!(row.is_some(), "duplicate detection: existing row returned");
        assert!(repo
            .find_by_name("p_demo", "nobody")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn find_by_name_is_project_scoped() {
        let store = migrated().await;
        let repo = Sessions::new(&store);
        repo.create(new_session("ben", "ck1")).await.unwrap();
        assert!(repo.find_by_name("p_other", "ben").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn presence_pause_and_heartbeat_update() {
        let store = migrated().await;
        let repo = Sessions::new(&store);
        let id = repo.create(new_session("ben", "ck1")).await.unwrap();
        repo.set_presence(&id, Presence::Busy).await.unwrap();
        repo.set_paused(&id, true, Some("self")).await.unwrap();
        repo.touch_heartbeat(&id).await.unwrap();
        let row = repo.find_by_name("p_demo", "ben").await.unwrap().unwrap();
        assert_eq!(row.presence.as_deref(), Some("busy"));
        assert!(row.paused);
        assert_eq!(row.paused_by.as_deref(), Some("self"));
        assert!(row.last_heartbeat.is_some());
    }

    #[tokio::test]
    async fn set_harness_session_id_persists_resume_key() {
        let store = migrated().await;
        let repo = Sessions::new(&store);
        let id = repo.create(new_session("cora", "ck-c")).await.unwrap();
        repo.set_harness_session_id(&id, "codex-thread-1")
            .await
            .unwrap();

        let row = repo.find_by_name("p_demo", "cora").await.unwrap().unwrap();
        assert_eq!(row.harness_session_id.as_deref(), Some("codex-thread-1"));

        repo.clear_harness_session_id(&id).await.unwrap();
        let row = repo.find_by_name("p_demo", "cora").await.unwrap().unwrap();
        assert_eq!(row.harness_session_id, None);
    }

    #[tokio::test]
    async fn list_is_project_scoped() {
        let store = migrated().await;
        let repo = Sessions::new(&store);
        repo.create(new_session("ben", "ck1")).await.unwrap();
        repo.create(new_session("ana", "ck2")).await.unwrap();
        assert_eq!(repo.list("p_demo").await.unwrap().len(), 2);
        assert_eq!(repo.list("p_empty").await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn create_with_transport_acp_roundtrips() {
        let store = migrated().await;
        let repo = Sessions::new(&store);
        let mut s = new_session("acp_agent", "ck-acp");
        s.transport = Some("acp".to_string());
        let id = repo.create(s).await.unwrap();
        let found = repo
            .find_by_session_id(&id)
            .await
            .unwrap()
            .expect("session exists");
        assert_eq!(found.transport.as_deref(), Some("acp"));
    }

    #[tokio::test]
    async fn set_transport_flips_value() {
        let store = migrated().await;
        let repo = Sessions::new(&store);
        let mut s = new_session("flipper", "ck-flip");
        s.transport = Some("pty".to_string());
        let id = repo.create(s).await.unwrap();
        repo.set_transport(&id, "acp").await.unwrap();
        let found = repo
            .find_by_session_id(&id)
            .await
            .unwrap()
            .expect("session exists");
        assert_eq!(found.transport.as_deref(), Some("acp"));
    }

    #[tokio::test]
    async fn legacy_row_transport_is_none() {
        let store = migrated().await;
        let repo = Sessions::new(&store);
        // new_session helper sets transport = None (the legacy / NULL path)
        let id = repo
            .create(new_session("legacy", "ck-legacy"))
            .await
            .unwrap();
        let found = repo
            .find_by_session_id(&id)
            .await
            .unwrap()
            .expect("session exists");
        assert_eq!(found.transport, None);
    }

    #[tokio::test]
    async fn purge_removes_stable_identity_graph() {
        let store = migrated().await;
        let repo = Sessions::new(&store);
        let session = repo.create(new_session("ben", "ck1")).await.unwrap();
        Agents::new(&store)
            .create(NewAgent {
                agent_id: "a_ben".to_string(),
                project: "p_demo".to_string(),
                name: Some("ben".to_string()),
                default_harness: Some("claude".to_string()),
                role: None,
                tier: Some("agent".to_string()),
                owner: None,
            })
            .await
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO agent_runtimes (runtime_id, agent_id, harness, active, \
                 started_at) VALUES (?1, 'a_ben', 'claude', 1, 1)",
                params![session.0.clone()],
            )
            .await
            .unwrap();
        AgentCredentials::new(&store)
            .create_hash(NewAgentCredential {
                credential_id: "cred_ben".to_string(),
                agent_id: "a_ben".to_string(),
                secret_hash: "sha256:test".to_string(),
                purpose: Some("runtime".to_string()),
                label: None,
                scopes_json: r#"["runtime:register"]"#.to_string(),
                metadata_json: None,
            })
            .await
            .unwrap();
        Messages::new(&store)
            .insert_with_agents(&msg("m_from", "ben"), Some("a_ben"), None)
            .await
            .unwrap();
        Messages::new(&store)
            .insert_with_agents(&msg("m_to", "ana"), None, Some("a_ben"))
            .await
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO in_flight (in_flight_id, message_id, recipient_session, \
                 recipient_agent_id, state) VALUES ('if_1', 'm_to', ?1, 'a_ben', 'pending')",
                params![session.0.clone()],
            )
            .await
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO thread_members (thread_id, session_name, agent_id, joined_at) \
                 VALUES ('t_1', 'ben', 'a_ben', 1)",
                (),
            )
            .await
            .unwrap();

        repo.purge(&session, "ben").await.unwrap();

        for (table, where_clause) in [
            ("sessions", "session_id = 's_ben'"),
            ("agents", "agent_id = 'a_ben'"),
            ("agent_runtimes", "agent_id = 'a_ben'"),
            ("agent_credentials", "agent_id = 'a_ben'"),
            ("in_flight", "recipient_agent_id = 'a_ben'"),
            ("thread_members", "agent_id = 'a_ben'"),
            (
                "messages",
                "from_agent_id = 'a_ben' OR to_agent_id = 'a_ben'",
            ),
        ] {
            let mut rows = store
                .conn
                .query(
                    &format!("SELECT COUNT(*) FROM {table} WHERE {where_clause}"),
                    (),
                )
                .await
                .unwrap();
            let count: i64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
            assert_eq!(count, 0, "{table} rows should be purged");
        }
    }
}

mod threads {
    use nexus_contracts::ids::ThreadId;
    use nexus_store::repos::threads::*;
    use nexus_store::repos::{Agents, NewAgent};
    use nexus_store::Store;

    async fn migrated() -> Store {
        let s = Store::open(":memory:").await.unwrap();
        s.migrate().await.unwrap();
        s
    }

    async fn create_agent(store: &Store, agent_id: &str, name: &str) {
        Agents::new(store)
            .create(NewAgent {
                agent_id: agent_id.to_string(),
                project: "p_demo".to_string(),
                name: Some(name.to_string()),
                default_harness: Some("codex".to_string()),
                role: None,
                tier: Some("agent".to_string()),
                owner: None,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn create_join_members_and_is_member() {
        let store = migrated().await;
        let repo = Threads::new(&store);
        let tid = ThreadId("t_backend".into());
        repo.create(&tid, "backend", "p_demo", "ben").await.unwrap();
        repo.add_member(&tid, "ben").await.unwrap();
        repo.add_member(&tid, "ana").await.unwrap();

        let mut members = repo.members(&tid).await.unwrap();
        members.sort();
        assert_eq!(members, vec!["ana".to_string(), "ben".to_string()]);
        assert!(repo.is_member(&tid, "ben").await.unwrap());
        assert!(
            !repo.is_member(&tid, "dylan").await.unwrap(),
            "non-member excluded"
        );

        let found = repo
            .find_by_name("p_demo", "backend")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.thread_id, tid);
    }

    #[tokio::test]
    async fn remove_member_and_list_active_threads() {
        let store = migrated().await;
        let repo = Threads::new(&store);
        let tid = ThreadId("t_backend".into());
        repo.create(&tid, "backend", "p_demo", "ben").await.unwrap();
        repo.add_member(&tid, "ben").await.unwrap();
        repo.remove_member(&tid, "ben").await.unwrap();
        assert!(!repo.is_member(&tid, "ben").await.unwrap());

        assert_eq!(repo.list().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn stable_agent_membership_survives_rename() {
        let store = migrated().await;
        create_agent(&store, "a_ana", "ana").await;
        let repo = Threads::new(&store);
        let tid = ThreadId("t_backend".into());
        repo.create(&tid, "backend", "p_demo", "ben").await.unwrap();
        repo.add_member(&tid, "ana").await.unwrap();

        Agents::new(&store).rename("a_ana", "anabel").await.unwrap();
        assert_eq!(
            repo.members(&tid).await.unwrap(),
            vec!["anabel".to_string()]
        );
        assert!(repo.is_member(&tid, "anabel").await.unwrap());

        repo.remove_member(&tid, "anabel").await.unwrap();
        assert!(repo.members(&tid).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn archive_hides_thread_from_active_reads_without_deleting_members() {
        let store = migrated().await;
        let repo = Threads::new(&store);
        let tid = ThreadId("t_backend".into());
        repo.create(&tid, "backend", "p_demo", "ben").await.unwrap();
        repo.add_member(&tid, "ben").await.unwrap();

        repo.archive("backend").await.unwrap();

        assert!(
            repo.find_active_any_by_name("backend")
                .await
                .unwrap()
                .is_none(),
            "archived threads are not active routing/read targets"
        );
        assert_eq!(repo.list().await.unwrap(), Vec::new());
        assert_eq!(
            repo.members(&tid).await.unwrap(),
            vec!["ben".to_string()],
            "archive keeps membership for audit/future unarchive"
        );
    }

    #[tokio::test]
    async fn delete_removes_thread_registry_and_memberships() {
        let store = migrated().await;
        let repo = Threads::new(&store);
        let tid = ThreadId("t_backend".into());
        repo.create(&tid, "backend", "p_demo", "ben").await.unwrap();
        repo.add_member(&tid, "ben").await.unwrap();

        repo.delete("backend").await.unwrap();

        assert!(repo.find_any_by_name("backend").await.unwrap().is_none());
        assert!(repo
            .find_active_any_by_name("backend")
            .await
            .unwrap()
            .is_none());
        assert_eq!(repo.members(&tid).await.unwrap(), Vec::<String>::new());

        repo.create(&ThreadId("t_backend_2".into()), "backend", "p_demo", "ana")
            .await
            .unwrap();
        assert!(
            repo.find_active_any_by_name("backend")
                .await
                .unwrap()
                .is_some(),
            "delete frees the thread name for reuse"
        );
    }
}

mod notifications {
    use nexus_store::repos::notifications::*;
    use nexus_store::Store;

    async fn migrated() -> Store {
        let s = Store::open(":memory:").await.unwrap();
        s.migrate().await.unwrap();
        s
    }

    #[tokio::test]
    async fn record_returns_id_and_roundtrips() {
        let store = migrated().await;
        let repo = Notifications::new(&store);
        let id = repo
            .record(Some("ci"), Some("ci"), true, "{\"ok\":true}", "ana,ben")
            .await
            .unwrap();
        let row = repo.get(&id).await.unwrap().unwrap();
        assert_eq!(row.source.as_deref(), Some("ci"));
        assert!(row.hmac_ok);
        assert_eq!(row.routed_to.as_deref(), Some("ana,ben"));
    }

    #[tokio::test]
    async fn bad_signature_recorded_with_hmac_ok_false() {
        let store = migrated().await;
        let repo = Notifications::new(&store);
        let id = repo
            .record(Some("ci"), None, false, "{}", "")
            .await
            .unwrap();
        let row = repo.get(&id).await.unwrap().unwrap();
        assert!(!row.hmac_ok);
        assert_eq!(row.routed_to.as_deref(), Some(""));
    }
}

mod sources {
    use nexus_common::NexusError;
    use nexus_store::repos::sources::*;
    use nexus_store::Store;

    async fn migrated() -> Store {
        let s = Store::open(":memory:").await.unwrap();
        s.migrate().await.unwrap();
        s
    }

    #[tokio::test]
    async fn create_then_find_returns_row() {
        let store = migrated().await;
        let repo = Sources::new(&store);
        repo.create("webhook_a", "hash_abc", "alerts", 1000)
            .await
            .unwrap();
        let row = repo
            .find("webhook_a")
            .await
            .unwrap()
            .expect("source should exist");
        assert_eq!(row.name, "webhook_a");
        assert_eq!(row.token, "hash_abc");
        assert_eq!(row.topic, "alerts");
        assert!(row.enabled);
        assert_eq!(row.created_at, 1000);
        assert_eq!(row.last_fired_at, None);
    }

    #[tokio::test]
    async fn create_duplicate_name_errors() {
        let store = migrated().await;
        let repo = Sources::new(&store);
        repo.create("dup", "hash1", "topic1", 1000).await.unwrap();
        let result = repo.create("dup", "hash2", "topic2", 2000).await;
        assert!(result.is_err(), "second create with same name must error");
        // Verify it's the DuplicateName variant.
        match result.unwrap_err() {
            NexusError::DuplicateName(n) => assert_eq!(n, "dup"),
            other => panic!("expected DuplicateName, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn set_enabled_toggles() {
        let store = migrated().await;
        let repo = Sources::new(&store);
        repo.create("src", "h", "t", 1000).await.unwrap();

        repo.set_enabled("src", false).await.unwrap();
        let row = repo.find("src").await.unwrap().unwrap();
        assert!(!row.enabled, "should be disabled");

        repo.set_enabled("src", true).await.unwrap();
        let row = repo.find("src").await.unwrap().unwrap();
        assert!(row.enabled, "should be re-enabled");
    }

    #[tokio::test]
    async fn set_token_rotates_hash() {
        let store = migrated().await;
        let repo = Sources::new(&store);
        repo.create("src", "old_hash", "t", 1000).await.unwrap();
        repo.set_token("src", "new_hash").await.unwrap();
        let row = repo.find("src").await.unwrap().unwrap();
        assert_eq!(row.token, "new_hash");
    }

    #[tokio::test]
    async fn touch_fired_sets_timestamp() {
        let store = migrated().await;
        let repo = Sources::new(&store);
        repo.create("src", "h", "t", 1000).await.unwrap();
        repo.touch_fired("src", 9999).await.unwrap();
        let row = repo.find("src").await.unwrap().unwrap();
        assert_eq!(row.last_fired_at, Some(9999));
    }

    #[tokio::test]
    async fn delete_then_find_returns_none() {
        let store = migrated().await;
        let repo = Sources::new(&store);
        repo.create("src", "h", "t", 1000).await.unwrap();
        repo.delete("src").await.unwrap();
        assert!(repo.find("src").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_returns_all_rows() {
        let store = migrated().await;
        let repo = Sources::new(&store);
        repo.create("a", "h1", "t1", 1000).await.unwrap();
        repo.create("b", "h2", "t2", 1001).await.unwrap();
        repo.create("c", "h3", "t3", 1002).await.unwrap();
        let rows = repo.list().await.unwrap();
        assert_eq!(rows.len(), 3);
        let names: Vec<&str> = rows.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"a"));
        assert!(names.contains(&"b"));
        assert!(names.contains(&"c"));
    }
}

mod agent_runtimes {
    use nexus_common::RuntimeProcessIds;
    use nexus_contracts::enums::Presence;
    use nexus_contracts::SessionId;
    use nexus_store::repos::agent_runtimes::*;
    use nexus_store::repos::{Agents, NewAgent, NewSession, Sessions};
    use nexus_store::Store;

    async fn migrated() -> Store {
        let store = Store::open(":memory:").await.unwrap();
        store.migrate().await.unwrap();
        store
    }

    async fn create_agent(store: &Store) {
        Agents::new(store)
            .create(NewAgent {
                agent_id: "a_blake".to_string(),
                project: "p_demo".to_string(),
                name: Some("blake".to_string()),
                default_harness: Some("codex".to_string()),
                role: None,
                tier: Some("agent".to_string()),
                owner: None,
            })
            .await
            .unwrap();
    }

    fn runtime(runtime_id: &str, active: bool) -> NewAgentRuntime {
        NewAgentRuntime {
            runtime_id: runtime_id.to_string(),
            agent_id: "a_blake".to_string(),
            harness: "codex".to_string(),
            cwd: Some("/repo".to_string()),
            transport: Some("codex-appserver".to_string()),
            presence: Some("online".to_string()),
            active,
        }
    }

    fn session(session_id: &str) -> NewSession {
        NewSession {
            session_id: SessionId(session_id.to_string()),
            name: Some("blake".to_string()),
            agent: Some("codex".to_string()),
            kind: "agent".to_string(),
            role: None,
            tier: "agent".to_string(),
            harness_session_id: None,
            client_key: Some(format!("ck_{session_id}")),
            cwd: Some("/repo".to_string()),
            project: "p_demo".to_string(),
            transport: Some("codex-appserver".to_string()),
        }
    }

    #[tokio::test]
    async fn runtimes_keep_one_active_runtime_per_agent() {
        let store = migrated().await;
        create_agent(&store).await;
        let repo = AgentRuntimes::new(&store);

        assert_eq!(repo.create(runtime("s_1", true)).await.unwrap(), "s_1");
        let first = repo
            .active_for_agent("a_blake")
            .await
            .unwrap()
            .expect("first active runtime");
        assert_eq!(first.runtime_id, "s_1");
        assert!(first.active);
        assert_eq!(first.cwd.as_deref(), Some("/repo"));
        assert_eq!(first.transport.as_deref(), Some("codex-appserver"));

        repo.create(runtime("s_2", true)).await.unwrap();
        let active = repo
            .active_for_agent("a_blake")
            .await
            .unwrap()
            .expect("second active runtime");
        assert_eq!(active.runtime_id, "s_2");

        let old = repo
            .find_by_runtime_id("s_1")
            .await
            .unwrap()
            .expect("old runtime");
        assert!(!old.active);
        assert!(old.stopped_at.is_some());

        repo.set_presence("s_2", Presence::Busy).await.unwrap();
        let busy = repo
            .find_by_runtime_id("s_2")
            .await
            .unwrap()
            .expect("busy runtime");
        assert_eq!(busy.presence.as_deref(), Some("busy"));
        assert!(busy.last_heartbeat.is_some());

        repo.set_active("s_1", true).await.unwrap();
        assert_eq!(
            repo.active_for_agent("a_blake")
                .await
                .unwrap()
                .expect("reactivated runtime")
                .runtime_id,
            "s_1"
        );
        assert!(
            !repo
                .find_by_runtime_id("s_2")
                .await
                .unwrap()
                .expect("second runtime stopped")
                .active
        );

        repo.create(runtime("s_3", false)).await.unwrap();
        assert_eq!(
            repo.list_for_agent("a_blake", false)
                .await
                .unwrap()
                .into_iter()
                .map(|row| row.runtime_id)
                .collect::<Vec<_>>(),
            vec!["s_1".to_string()]
        );
        assert_eq!(repo.list_for_agent("a_blake", true).await.unwrap().len(), 3);

        repo.stop("s_1").await.unwrap();
        assert!(repo.active_for_agent("a_blake").await.unwrap().is_none());

        let all = repo.list_for_agent("a_blake", true).await.unwrap();
        assert_eq!(all.len(), 3);
        assert!(repo
            .list_for_agent("a_blake", false)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn stop_stale_uses_freshest_runtime_or_session_heartbeat() {
        let store = migrated().await;
        create_agent(&store).await;
        let sessions = Sessions::new(&store);
        let session_id = sessions.create(session("s_acp_live")).await.unwrap();
        sessions.set_agent_id(&session_id, "a_blake").await.unwrap();
        sessions.touch_heartbeat(&session_id).await.unwrap();

        let repo = AgentRuntimes::new(&store);
        repo.create(runtime("s_acp_live", true)).await.unwrap();
        store
            .conn
            .execute(
                "UPDATE agent_runtimes SET last_heartbeat = ?2 WHERE runtime_id = ?1",
                ("s_acp_live".to_string(), 1_700_000_000_000_i64),
            )
            .await
            .unwrap();

        repo.stop_stale(nexus_common::now(), 30_000).await.unwrap();

        let runtime = repo
            .find_by_runtime_id("s_acp_live")
            .await
            .unwrap()
            .expect("runtime remains");
        assert!(
            runtime.active,
            "a fresh session heartbeat must keep a daemon-owned ACP runtime active even if the runtime heartbeat column is older"
        );
        assert_eq!(runtime.presence.as_deref(), Some("online"));
        assert_eq!(runtime.stopped_at, None);
    }

    #[tokio::test]
    async fn process_ids_roundtrip_and_stop_clears_them() {
        let store = migrated().await;
        create_agent(&store).await;
        let repo = AgentRuntimes::new(&store);
        repo.create(runtime("s_proc", true)).await.unwrap();

        let entry = RuntimeProcessIds {
            os_pid: 1234,
            os_pgid: 1234,
        };
        repo.set_process_ids("s_proc", entry).await.unwrap();
        let stored = repo
            .find_by_runtime_id("s_proc")
            .await
            .unwrap()
            .expect("runtime");
        assert_eq!(stored.runtime_process_ids(), Some(entry));

        repo.stop("s_proc").await.unwrap();
        let stopped = repo
            .find_by_runtime_id("s_proc")
            .await
            .unwrap()
            .expect("stopped runtime");
        assert_eq!(stopped.runtime_process_ids(), None);
    }

    #[tokio::test]
    async fn boot_reap_candidates_include_acp_and_stopped_rows_only() {
        let store = migrated().await;
        create_agent(&store).await;
        let repo = AgentRuntimes::new(&store);
        let entry = RuntimeProcessIds {
            os_pid: 1,
            os_pgid: 1,
        };
        Agents::new(&store)
            .create(NewAgent {
                agent_id: "a_acp".to_string(),
                project: "p_demo".to_string(),
                name: Some("acp".to_string()),
                default_harness: Some("claude".to_string()),
                role: None,
                tier: Some("agent".to_string()),
                owner: None,
            })
            .await
            .unwrap();

        repo.create(NewAgentRuntime {
            agent_id: "a_acp".to_string(),
            harness: "claude".to_string(),
            transport: Some("acp".to_string()),
            ..runtime("s_acp", true)
        })
        .await
        .unwrap();
        repo.set_process_ids("s_acp", entry).await.unwrap();

        repo.create(NewAgentRuntime {
            transport: Some("pty".to_string()),
            ..runtime("s_pty", true)
        })
        .await
        .unwrap();
        repo.set_process_ids("s_pty", entry).await.unwrap();

        repo.create(NewAgentRuntime {
            transport: Some("pty".to_string()),
            ..runtime("s_stopped", false)
        })
        .await
        .unwrap();
        repo.set_process_ids("s_stopped", entry).await.unwrap();

        let candidates = repo
            .list_boot_process_reap_candidates()
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.runtime_id)
            .collect::<Vec<_>>();

        assert!(candidates.contains(&"s_acp".to_string()));
        assert!(candidates.contains(&"s_stopped".to_string()));
        assert!(
            !candidates.contains(&"s_pty".to_string()),
            "active headed/app-server rows are adoptable after daemon restart"
        );
    }

    #[tokio::test]
    async fn mark_live_does_not_steal_active_runtime_from_newer_sibling() {
        let store = migrated().await;
        create_agent(&store).await;
        let repo = AgentRuntimes::new(&store);
        repo.create(runtime("s_old", true)).await.unwrap();
        repo.create(runtime("s_new", true)).await.unwrap();

        repo.mark_live("s_old").await.unwrap();

        let active = repo
            .active_for_agent("a_blake")
            .await
            .unwrap()
            .expect("new sibling remains active");
        assert_eq!(active.runtime_id, "s_new");

        let old = repo
            .find_by_runtime_id("s_old")
            .await
            .unwrap()
            .expect("old runtime remains for audit");
        assert!(
            !old.active,
            "keeper restamp must not let an old loop steal activeRuntime from a newer sibling"
        );
        assert!(old.stopped_at.is_some());
    }
}

mod native_thread_bindings {
    use nexus_store::repos::{NativeThreadBindings, NewNativeThreadBinding};
    use nexus_store::Store;

    async fn migrated() -> Store {
        let store = Store::open(":memory:").await.unwrap();
        store.migrate().await.unwrap();
        store
    }

    #[tokio::test]
    async fn native_thread_binding_claim_roundtrips_by_provider_thread() {
        let store = migrated().await;
        let repo = NativeThreadBindings::new(&store);

        repo.claim(NewNativeThreadBinding {
            provider: "codex".into(),
            kind: "harness".into(),
            native_thread_id: "codex-thread-1".into(),
            agent_id: "a_otto".into(),
            project: "default".into(),
            runtime_id: Some("s_otto_1".into()),
        })
        .await
        .unwrap();

        let row = repo
            .find("codex", "codex-thread-1")
            .await
            .unwrap()
            .expect("binding exists");
        assert_eq!(row.agent_id, "a_otto");
        assert_eq!(row.first_runtime_id.as_deref(), Some("s_otto_1"));
        assert_eq!(row.last_runtime_id.as_deref(), Some("s_otto_1"));
        assert_eq!(row.released_at, None);
    }

    #[tokio::test]
    async fn native_thread_binding_same_owner_refreshes_last_runtime() {
        let store = migrated().await;
        let repo = NativeThreadBindings::new(&store);

        repo.claim(NewNativeThreadBinding {
            provider: "codex".into(),
            kind: "harness".into(),
            native_thread_id: "codex-thread-1".into(),
            agent_id: "a_otto".into(),
            project: "default".into(),
            runtime_id: Some("s_otto_1".into()),
        })
        .await
        .unwrap();
        repo.mark_released_for_runtime("codex", "s_otto_1")
            .await
            .unwrap();
        repo.claim(NewNativeThreadBinding {
            provider: "codex".into(),
            kind: "harness".into(),
            native_thread_id: "codex-thread-1".into(),
            agent_id: "a_otto".into(),
            project: "default".into(),
            runtime_id: Some("s_otto_2".into()),
        })
        .await
        .unwrap();

        let row = repo.find("codex", "codex-thread-1").await.unwrap().unwrap();
        assert_eq!(row.first_runtime_id.as_deref(), Some("s_otto_1"));
        assert_eq!(row.last_runtime_id.as_deref(), Some("s_otto_2"));
        assert_eq!(row.released_at, None);
    }

    #[tokio::test]
    async fn native_thread_binding_rejects_different_owner() {
        let store = migrated().await;
        let repo = NativeThreadBindings::new(&store);

        repo.claim(NewNativeThreadBinding {
            provider: "codex".into(),
            kind: "harness".into(),
            native_thread_id: "codex-thread-1".into(),
            agent_id: "a_otto".into(),
            project: "default".into(),
            runtime_id: Some("s_otto".into()),
        })
        .await
        .unwrap();

        let err = repo
            .claim(NewNativeThreadBinding {
                provider: "codex".into(),
                kind: "harness".into(),
                native_thread_id: "codex-thread-1".into(),
                agent_id: "a_celia".into(),
                project: "default".into(),
                runtime_id: Some("s_celia".into()),
            })
            .await
            .expect_err("second owner must be rejected");
        assert!(err.to_string().contains("already bound"));
    }

    #[tokio::test]
    async fn native_thread_binding_is_provider_scoped_for_same_native_key_text() {
        let store = migrated().await;
        let repo = NativeThreadBindings::new(&store);

        repo.claim(NewNativeThreadBinding {
            provider: "claude".into(),
            kind: "harness".into(),
            native_thread_id: "shared-native-key".into(),
            agent_id: "a_claude".into(),
            project: "default".into(),
            runtime_id: Some("s_claude".into()),
        })
        .await
        .unwrap();
        repo.claim(NewNativeThreadBinding {
            provider: "codex".into(),
            kind: "harness".into(),
            native_thread_id: "shared-native-key".into(),
            agent_id: "a_codex".into(),
            project: "default".into(),
            runtime_id: Some("s_codex".into()),
        })
        .await
        .unwrap();

        assert_eq!(
            repo.find("claude", "shared-native-key")
                .await
                .unwrap()
                .unwrap()
                .agent_id,
            "a_claude"
        );
        assert_eq!(
            repo.find("codex", "shared-native-key")
                .await
                .unwrap()
                .unwrap()
                .agent_id,
            "a_codex"
        );
    }

    #[tokio::test]
    async fn native_thread_binding_finds_latest_unreleased_key_for_agent() {
        let store = migrated().await;
        let repo = NativeThreadBindings::new(&store);

        repo.claim(NewNativeThreadBinding {
            provider: "claude".into(),
            kind: "harness".into(),
            native_thread_id: "claude-old".into(),
            agent_id: "a_hugo".into(),
            project: "default".into(),
            runtime_id: Some("s_hugo_old".into()),
        })
        .await
        .unwrap();
        repo.mark_released_for_runtime("claude", "s_hugo_old")
            .await
            .unwrap();
        repo.claim(NewNativeThreadBinding {
            provider: "claude".into(),
            kind: "harness".into(),
            native_thread_id: "claude-live".into(),
            agent_id: "a_hugo".into(),
            project: "default".into(),
            runtime_id: Some("s_hugo_live".into()),
        })
        .await
        .unwrap();

        let row = repo
            .find_latest_for_agent("claude", "a_hugo")
            .await
            .unwrap()
            .expect("binding");
        assert_eq!(row.native_thread_id, "claude-live");
        assert_eq!(row.last_runtime_id.as_deref(), Some("s_hugo_live"));
        assert_eq!(row.released_at, None);
    }
}

mod messages {
    use nexus_contracts::enums::{Kind, Scope};
    use nexus_contracts::ids::{MessageId, ProjectId, ThreadId};
    use nexus_contracts::message::{Message, Provenance};
    use nexus_store::repos::messages::*;
    use nexus_store::Store;

    async fn migrated() -> Store {
        let s = Store::open(":memory:").await.unwrap();
        s.migrate().await.unwrap();
        s
    }

    fn sample_dm() -> Message {
        Message {
            id: MessageId("m_01".into()),
            project: ProjectId("p_demo".into()),
            from: "etan".into(),
            scope: Scope::Dm,
            thread: None,
            topic: None,
            body: "take the auth refactor?".into(),
            summary: None,
            provenance: Provenance {
                from: "etan".into(),
                kind: Kind::Human,
                locality: Default::default(),
                access: None,
                thread: None,
                topic: None,
                stamp: None,
            },
            created_at: 1_700_000_000,
        }
    }

    #[tokio::test]
    async fn insert_then_get_roundtrips() {
        let store = migrated().await;
        let repo = Messages::new(&store);
        let id = repo.insert(&sample_dm()).await.unwrap();
        let got = repo.get("p_demo", &id).await.unwrap().expect("present");
        assert_eq!(got, sample_dm());
    }

    #[tokio::test]
    async fn get_is_project_scoped() {
        let store = migrated().await;
        let repo = Messages::new(&store);
        repo.insert(&sample_dm()).await.unwrap();
        assert!(repo
            .get("p_other", &MessageId("m_01".into()))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn thread_message_roundtrips_thread_id() {
        let store = migrated().await;
        let repo = Messages::new(&store);
        let mut m = sample_dm();
        m.id = MessageId("m_t".into());
        m.scope = Scope::Thread;
        m.thread = Some(ThreadId("t_backend".into()));
        m.provenance.thread = Some("backend".into());
        m.provenance.kind = Kind::Agent;
        repo.insert(&m).await.unwrap();
        let got = repo.get("p_demo", &m.id).await.unwrap().unwrap();
        assert_eq!(got.scope, Scope::Thread);
        assert_eq!(got.thread, Some(ThreadId("t_backend".into())));
    }

    #[tokio::test]
    async fn accepted_message_projection_carries_canonical_hook_fields() {
        let store = migrated().await;
        let repo = Messages::new(&store);
        let message = sample_dm();
        repo.insert(&message).await.unwrap();
        store
            .conn
            .execute(
                "UPDATE messages
                 SET metadata_json = ?2, mention_json = ?3
                 WHERE message_id = ?1",
                libsql::params![
                    message.id.0.clone(),
                    r#"{"reviewed":true,"nested":{"source":"hook"}}"#,
                    r#"["fable"]"#
                ],
            )
            .await
            .unwrap();

        let effects = repo.gateway_projection_effects(&message.id).await.unwrap();
        assert_eq!(
            effects[0].payload["metadata"],
            serde_json::json!({"reviewed": true, "nested": {"source": "hook"}})
        );
        assert_eq!(effects[0].payload["mention"], serde_json::json!(["fable"]));
    }
}

mod stream_raw {
    use std::sync::{Mutex, OnceLock};

    use nexus_contracts::SessionId;
    use nexus_store::repos::StreamRaw;
    use nexus_store::Store;

    async fn migrated() -> Store {
        let s = Store::open(":memory:").await.unwrap();
        s.migrate().await.unwrap();
        s
    }

    #[tokio::test]
    async fn append_reads_and_caps_per_session() {
        let _guard = env_guard().lock().unwrap();
        std::env::set_var("NEXUS_STREAM_RAW_CAP", "2");
        std::env::remove_var("NEXUS_STREAM_RAW_BYTE_CAP");
        let store = migrated().await;
        let repo = StreamRaw::new(&store);
        let session = SessionId("s_raw".to_string());
        let other = SessionId("s_other_raw".to_string());

        repo.append(&session, b"one").await.unwrap();
        let second = repo.append(&session, b"two").await.unwrap();
        let third = repo.append(&session, b"three").await.unwrap();
        repo.append(&other, b"other").await.unwrap();

        let rows = repo.since(&session, 0).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, second);
        assert_eq!(rows[0].chunk, b"two");
        assert_eq!(rows[1].id, third);
        assert_eq!(rows[1].chunk, b"three");
        assert_eq!(
            repo.since(&session, second).await.unwrap()[0].chunk,
            b"three"
        );
        assert_eq!(repo.since(&other, 0).await.unwrap()[0].chunk, b"other");

        std::env::remove_var("NEXUS_STREAM_RAW_CAP");
    }

    #[tokio::test]
    async fn append_caps_per_session_by_retained_bytes() {
        let _guard = env_guard().lock().unwrap();
        std::env::set_var("NEXUS_STREAM_RAW_CAP", "2000");
        std::env::set_var("NEXUS_STREAM_RAW_BYTE_CAP", "10");
        let store = migrated().await;
        let repo = StreamRaw::new(&store);
        let session = SessionId("s_raw_bytes".to_string());
        let other = SessionId("s_other_raw_bytes".to_string());

        repo.append(&session, b"1234").await.unwrap();
        repo.append(&session, b"5678").await.unwrap();
        repo.append(&session, b"abcdefghijklmnopqrstuvwxyz")
            .await
            .unwrap();
        repo.append(&other, b"untrimmed-other").await.unwrap();

        let rows = repo.since(&session, 0).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].chunk, b"qrstuvwxyz");
        let retained: usize = rows.iter().map(|row| row.chunk.len()).sum();
        assert!(
            retained <= 10,
            "raw lane should retain at most the configured byte cap"
        );
        assert_eq!(
            repo.since(&other, 0).await.unwrap()[0].chunk,
            b"mmed-other",
            "byte cap is enforced independently per session"
        );

        std::env::remove_var("NEXUS_STREAM_RAW_CAP");
        std::env::remove_var("NEXUS_STREAM_RAW_BYTE_CAP");
    }

    fn env_guard() -> &'static Mutex<()> {
        static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
        GUARD.get_or_init(|| Mutex::new(()))
    }
}
