use std::collections::HashSet;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use nexus_contracts::enums::{Kind, Scope};
use nexus_contracts::ids::{MessageId, ProjectId, SessionId, ThreadId};
use nexus_contracts::message::{Message, Provenance};
use nexus_contracts::AgentId;
use nexus_store::daemon_store::{
    DaemonStore, GATEWAY_ONLY_PRODUCT_TABLES, PERSISTENT_CONTINUITY_TABLES,
    VOLATILE_TRANSPORT_TABLES,
};
use nexus_store::repos::{
    AgentGroups, AgentRuntimes, Agents, CommandIntents, DeliveryObligations, IdentitySessions,
    Inbox, LiveSessions, Messages, Metadata, MetadataEntity, NewAgent, NewAgentRuntime,
    NewCommandIntent, NewDeliveryObligation, NewIdentitySession, NewLiveSession, NewSession,
    Sessions, Threads, Topics,
};

fn unique_store_path(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "nexus-daemon-store-{label}-{}-{nonce}.db",
        std::process::id()
    ))
}

async fn table_exists(store: &nexus_store::Store, table: &str) -> bool {
    let mut rows = store
        .conn
        .query(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?1",
            libsql::params![table],
        )
        .await
        .expect("schema query");
    rows.next().await.expect("schema row").is_some()
}

#[tokio::test]
async fn fresh_identity_baseline_carries_the_runtime_client_key() {
    let path = unique_store_path("runtime-client-key");
    let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("open split daemon store");
    let mut columns = daemon
        .identity()
        .conn
        .query("PRAGMA table_info(identity_sessions)", ())
        .await
        .expect("identity schema query");
    let mut names = Vec::new();
    while let Some(row) = columns.next().await.expect("identity schema row") {
        names.push(row.get::<String>(1).expect("column name"));
    }
    assert!(
        names.iter().any(|name| name == "client_key"),
        "runtime authentication must survive the same restart as its resurrection descriptor"
    );

    drop(daemon);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn legacy_identity_marker_ladder_reopens_and_is_canonicalized() {
    let path = unique_store_path("legacy-identity-marker");
    {
        let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
            .await
            .expect("create split daemon store");
        daemon
            .identity()
            .conn
            .execute_batch(
                "DELETE FROM schema_migrations;
                 INSERT INTO schema_migrations(version, name, applied_at)
                   VALUES (1, 'v0.1.0_identity', 1),
                          (3, 'v0.1.0_identity', 3);",
            )
            .await
            .expect("seed identity marker ladder from the pre-release upgrader");
    }

    let reopened = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("reopen legacy identity marker ladder");
    let mut rows = reopened
        .identity()
        .conn
        .query(
            "SELECT version, name FROM schema_migrations ORDER BY version",
            (),
        )
        .await
        .expect("query canonical identity marker");
    let mut markers = Vec::new();
    while let Some(row) = rows.next().await.expect("identity marker row") {
        markers.push((
            row.get::<i64>(0).expect("identity marker version"),
            row.get::<String>(1).expect("identity marker name"),
        ));
    }
    assert_eq!(markers, vec![(1, "v0.1.0_identity".into())]);

    drop(rows);
    drop(reopened);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[test]
fn every_daemon_table_has_exactly_one_authoritative_class() {
    let persistent = PERSISTENT_CONTINUITY_TABLES
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let volatile = VOLATILE_TRANSPORT_TABLES
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let gateway = GATEWAY_ONLY_PRODUCT_TABLES
        .iter()
        .copied()
        .collect::<HashSet<_>>();

    assert!(persistent.is_disjoint(&volatile));
    assert!(persistent.is_disjoint(&gateway));
    assert!(volatile.is_disjoint(&gateway));

    for required in [
        "agents",
        "agent_runtimes",
        "identity_sessions",
        "delivery_obligations",
        "sessions",
        "messages",
        "in_flight",
        "developer_events",
        "notifications",
        "agent_session_messages",
        "messages_fts",
    ] {
        let count = persistent.contains(required) as u8
            + volatile.contains(required) as u8
            + gateway.contains(required) as u8;
        assert_eq!(count, 1, "{required} must have one authoritative owner");
    }
}

#[tokio::test]
async fn restart_preserves_identity_and_unsettled_delivery_but_drops_live_transport() {
    let path = unique_store_path("restart");
    {
        let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
            .await
            .expect("open split daemon store");
        IdentitySessions::new(daemon.identity())
            .upsert(NewIdentitySession {
                runtime_id: "s_headed".into(),
                agent_id: "a_ada".into(),
                project: "default".into(),
                harness: "codex".into(),
                mode: "headed".into(),
                backend: Some("tmux".into()),
                cwd: Some("/work/repo".into()),
                native_resume_key: Some("thread_native_1".into()),
                client_key: Some("stable-runtime-key".into()),
            })
            .await
            .expect("persist identity session");
        DeliveryObligations::new(daemon.identity())
            .insert(NewDeliveryObligation {
                message_id: "m_pending".into(),
                recipient_agent_id: "a_ada".into(),
                recipient_runtime_id: Some("s_headed".into()),
                payload_json: r#"{"body":"resume me"}"#.into(),
                dedupe_key: "delivery:m_pending:a_ada".into(),
                attempt: 1,
                state: "pending".into(),
                created_at: 10,
            })
            .await
            .expect("persist delivery obligation");
        LiveSessions::new(daemon.transport())
            .upsert(NewLiveSession {
                runtime_id: "s_headed".into(),
                presence: "online".into(),
                connection_id: Some("conn_1".into()),
                boot_epoch: "boot_one".into(),
                updated_at: 11,
            })
            .await
            .expect("record live session");
    }

    let reopened = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("reopen split daemon store");
    let identity = IdentitySessions::new(reopened.identity())
        .find("s_headed")
        .await
        .expect("identity lookup")
        .expect("identity survives");
    assert_eq!(identity.agent_id, "a_ada");
    assert_eq!(identity.mode, "headed");
    assert_eq!(identity.backend.as_deref(), Some("tmux"));
    assert_eq!(
        identity.native_resume_key.as_deref(),
        Some("thread_native_1")
    );
    assert_eq!(identity.client_key.as_deref(), Some("stable-runtime-key"));
    assert_eq!(
        DeliveryObligations::new(reopened.identity())
            .pending()
            .await
            .expect("pending obligations")
            .len(),
        1
    );
    assert!(LiveSessions::new(reopened.transport())
        .find("s_headed")
        .await
        .expect("live lookup")
        .is_none());

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn runtime_client_key_revocation_clears_volatile_and_persistent_identity() {
    let path = unique_store_path("runtime-key-revocation");
    let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("open split daemon store");
    let store = daemon.compatibility_store();
    let session = SessionId("s_revoked".into());
    IdentitySessions::new(&store)
        .upsert(NewIdentitySession {
            runtime_id: session.0.clone(),
            agent_id: "a_revoked".into(),
            project: "default".into(),
            harness: "opencode".into(),
            mode: "headed".into(),
            backend: Some("pty".into()),
            cwd: Some("/work/revoked".into()),
            native_resume_key: None,
            client_key: Some("revoke-me".into()),
        })
        .await
        .expect("identity capsule");
    Sessions::new(&store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some("revoked".into()),
            agent: Some("opencode".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some("revoke-me".into()),
            cwd: Some("/work/revoked".into()),
            project: "default".into(),
            transport: Some("opencode-plugin".into()),
        })
        .await
        .expect("volatile runtime");

    Sessions::new(&store)
        .clear_client_key(&session)
        .await
        .expect("revoke runtime key");
    assert!(Sessions::new(&store)
        .find_by_session_id(&session)
        .await
        .expect("runtime lookup")
        .expect("runtime row")
        .client_key
        .is_none());
    assert!(
        IdentitySessions::new(&store)
            .find(&session.0)
            .await
            .expect("identity lookup")
            .expect("identity capsule")
            .client_key
            .is_none(),
        "revoked credentials must not return on the next daemon boot"
    );

    drop(store);
    drop(daemon);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn product_history_tables_exist_in_neither_daemon_store() {
    let path = unique_store_path("authority");
    let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("open split daemon store");

    for table in GATEWAY_ONLY_PRODUCT_TABLES {
        assert!(
            !table_exists(daemon.identity(), table).await,
            "{table} leaked into persistent daemon identity"
        );
        assert!(
            !table_exists(daemon.transport(), table).await,
            "{table} leaked into volatile daemon transport"
        );
    }
    assert!(table_exists(daemon.identity(), "identity_sessions").await);
    assert!(table_exists(daemon.identity(), "delivery_obligations").await);
    assert!(table_exists(daemon.transport(), "live_sessions").await);

    drop(daemon);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn split_open_rejects_pre_release_store_without_pruning_rows() {
    let path = unique_store_path("pre-release-rejected");
    let pre_release = nexus_store::Store::open(path.to_string_lossy().as_ref())
        .await
        .expect("pre-release store");
    pre_release
        .conn
        .execute_batch(
            "CREATE TABLE schema_migrations (
               version INTEGER PRIMARY KEY,
               applied_at INTEGER
             );
             INSERT INTO schema_migrations VALUES (1, 1);
             INSERT INTO schema_migrations VALUES (17, 17);
             CREATE TABLE notifications (
               notif_id TEXT PRIMARY KEY,
               payload TEXT NOT NULL
             );
             INSERT INTO notifications VALUES ('n_pre_release', 'preserve-me');",
        )
        .await
        .expect("pre-release fixture");
    drop(pre_release);

    let error = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .err()
        .expect("pre-release store must fail closed")
        .to_string();
    assert!(error.contains("pre-release"), "unexpected error: {error}");

    let untouched = nexus_store::Store::open(path.to_string_lossy().as_ref())
        .await
        .expect("reopen untouched store");
    let mut rows = untouched
        .conn
        .query(
            "SELECT payload FROM notifications WHERE notif_id = 'n_pre_release'",
            (),
        )
        .await
        .expect("preserved row query");
    assert_eq!(
        rows.next()
            .await
            .expect("pre-release row")
            .expect("preserved notification")
            .get::<String>(0)
            .expect("payload"),
        "preserve-me"
    );
    drop(rows);
    drop(untouched);

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn compatibility_handle_routes_identity_and_transport_repositories_to_separate_authorities() {
    let path = unique_store_path("compatibility");
    {
        let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
            .await
            .expect("open split daemon store");
        let store = daemon.compatibility_store();
        Agents::new(&store)
            .create(NewAgent {
                agent_id: "a_split".into(),
                project: "default".into(),
                name: Some("split-agent".into()),
                default_harness: Some("codex".into()),
                role: None,
                tier: Some("agent".into()),
                owner: None,
            })
            .await
            .expect("identity repository uses persistent authority");
        Sessions::new(&store)
            .create(NewSession {
                session_id: SessionId("s_split".into()),
                name: Some("split-agent".into()),
                agent: Some("codex".into()),
                kind: "agent".into(),
                role: None,
                tier: "agent".into(),
                harness_session_id: None,
                client_key: Some("split-client".into()),
                cwd: None,
                project: "default".into(),
                transport: Some("acp".into()),
            })
            .await
            .expect("session repository uses volatile transport authority");
        assert!(Agents::new(&store)
            .find_by_id("a_split")
            .await
            .expect("agent lookup")
            .is_some());
        assert!(Sessions::new(&store)
            .find_by_name("default", "split-agent")
            .await
            .expect("session lookup")
            .is_some());
    }

    let reopened = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("reopen split daemon store");
    let store = reopened.compatibility_store();
    assert!(Agents::new(&store)
        .find_by_id("a_split")
        .await
        .expect("agent survives")
        .is_some());
    assert!(Sessions::new(&store)
        .find_by_name("default", "split-agent")
        .await
        .expect("session resets")
        .is_none());

    drop(store);
    drop(reopened);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn command_claim_resolves_volatile_runtime_without_cross_database_sql() {
    let path = unique_store_path("command-claim");
    let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("open split daemon store");
    let store = daemon.compatibility_store();
    Agents::new(&store)
        .create(NewAgent {
            agent_id: "a_target".into(),
            project: "default".into(),
            name: Some("target".into()),
            default_harness: Some("codex".into()),
            role: None,
            tier: Some("agent".into()),
            owner: None,
        })
        .await
        .expect("identity");
    let session_id = SessionId("s_target".into());
    Sessions::new(&store)
        .create(NewSession {
            session_id: session_id.clone(),
            name: Some("target".into()),
            agent: Some("codex".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some("target-client".into()),
            cwd: None,
            project: "default".into(),
            transport: Some("codex-appserver".into()),
        })
        .await
        .expect("volatile runtime");
    Sessions::new(&store)
        .set_agent_id(&session_id, "a_target")
        .await
        .expect("runtime identity binding");

    let commands = CommandIntents::new(&store);
    commands
        .insert_pending(NewCommandIntent {
            command_id: "cmd_prompt".into(),
            kind: nexus_store::command_kinds::harness::PROMPT.into(),
            project: "default".into(),
            caller_name: "operator".into(),
            caller_session_id: Some("local-operator".into()),
            caller_agent_id: None,
            caller_runtime_id: None,
            caller_client_key: None,
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: Some("split-claim".into()),
            request_json: r#"{"agentId":"a_target","name":"target","text":"hello"}"#.into(),
            created_at: 10,
        })
        .await
        .expect("persistent command");
    let claimed = commands
        .claim_next_ready_harness_prompt(20, 1_000, &[])
        .await
        .expect("claim across explicit authorities")
        .expect("ready prompt");
    assert_eq!(claimed.command_id, "cmd_prompt");
    assert_eq!(claimed.status, "claimed");

    drop(store);
    drop(daemon);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn routing_repositories_resolve_identity_without_cross_database_sql() {
    let path = unique_store_path("routing-repositories");
    let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("open split daemon store");
    let store = daemon.compatibility_store();

    for (agent_id, name, runtime_id) in [
        ("a_sender", "sender", "s_sender"),
        ("a_target", "target", "s_target"),
    ] {
        Agents::new(&store)
            .create(NewAgent {
                agent_id: agent_id.into(),
                project: "default".into(),
                name: Some(name.into()),
                default_harness: Some("codex".into()),
                role: None,
                tier: Some("agent".into()),
                owner: None,
            })
            .await
            .expect("create identity");
        AgentRuntimes::new(&store)
            .create(NewAgentRuntime {
                runtime_id: runtime_id.into(),
                agent_id: agent_id.into(),
                harness: "codex".into(),
                cwd: Some("/work".into()),
                transport: Some("acp".into()),
                presence: Some("online".into()),
                active: true,
            })
            .await
            .expect("create runtime");
        Sessions::new(&store)
            .create(NewSession {
                session_id: SessionId(runtime_id.into()),
                name: Some(name.into()),
                agent: Some("codex".into()),
                kind: "agent".into(),
                role: None,
                tier: "agent".into(),
                harness_session_id: None,
                client_key: Some(format!("{name}-client")),
                cwd: Some("/work".into()),
                project: "default".into(),
                transport: Some("acp".into()),
            })
            .await
            .expect("create live session");
        Sessions::new(&store)
            .set_agent_id(&SessionId(runtime_id.into()), agent_id)
            .await
            .expect("bind live session to its stable agent identity");
    }

    let message = Message {
        id: MessageId("m_split_route".into()),
        project: ProjectId("default".into()),
        from: "sender".into(),
        scope: Scope::Dm,
        thread: None,
        topic: None,
        body: "split route".into(),
        summary: None,
        provenance: Provenance {
            from: "sender".into(),
            kind: Kind::Agent,
            locality: Default::default(),
            access: None,
            thread: None,
            topic: None,
            stamp: None,
        },
        created_at: 100,
    };
    Messages::new(&store)
        .insert(&message)
        .await
        .expect("message resolves durable sender identity without local FTS");
    Inbox::new(&store)
        .enqueue(&message.id, &SessionId("s_target".into()))
        .await
        .expect("inbox resolves runtime identity");
    assert_eq!(
        Inbox::new(&store)
            .pending_for(&SessionId("s_target".into()), "default", 10)
            .await
            .expect("pending inbox")
            .len(),
        1
    );

    let thread_id = ThreadId("t_split".into());
    let threads = Threads::new(&store);
    threads
        .create(&thread_id, "split-thread", "default", "sender")
        .await
        .expect("thread");
    threads
        .add_member(&thread_id, "target")
        .await
        .expect("thread membership resolves identity");
    assert_eq!(
        threads.members(&thread_id).await.expect("thread roster"),
        vec!["target".to_string()]
    );

    let topics = Topics::new(&store);
    topics
        .ensure("split-topic", "default")
        .await
        .expect("topic");
    topics
        .subscribe("split-topic", "s_target", None)
        .await
        .expect("topic subscription resolves identity");
    assert_eq!(
        topics
            .subscribers("split-topic")
            .await
            .expect("subscribers"),
        vec!["s_target".to_string()]
    );

    let groups = AgentGroups::new(&store);
    groups
        .assign(
            "default",
            "split-group",
            &AgentId("a_target".into()),
            "target",
        )
        .await
        .expect("group assignment");
    assert_eq!(
        groups
            .members_any_project("split-group")
            .await
            .expect("group members"),
        vec![(AgentId("a_target".into()), Some("target".into()))]
    );

    Metadata::new(&store)
        .set(
            "default",
            MetadataEntity::Agent,
            "a_target",
            &serde_json::json!({"split": true}),
        )
        .await
        .expect("agent metadata uses identity authority");
    assert!(Sessions::new(&store)
        .active_runtime_session_for_agent("a_target")
        .await
        .expect("active runtime session")
        .is_some());
    assert_eq!(
        Inbox::new(&store)
            .mark_delivery_error(
                &message.id,
                &SessionId("s_target".into()),
                "test_terminal",
                "terminal split test",
                None,
            )
            .await
            .expect("terminal settlement"),
        1
    );
    assert!(Inbox::new(&store)
        .gateway_delivery_effect(&message.id, &SessionId("s_target".into()))
        .await
        .expect("projection effect")
        .is_some());
    assert!(Inbox::new(&store)
        .discard_settled_message_if_complete(&message.id)
        .await
        .expect("discard settled residue"));
    assert!(Messages::new(&store)
        .get("default", &message.id)
        .await
        .expect("message lookup after discard")
        .is_none());

    drop(store);
    drop(daemon);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}
