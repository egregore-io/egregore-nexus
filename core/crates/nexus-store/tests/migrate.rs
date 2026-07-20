use nexus_store::Store;

#[tokio::test]
async fn fresh_store_creates_one_complete_named_baseline_and_reopens_idempotently() {
    let store = Store::open(":memory:").await.unwrap();

    store.migrate().await.unwrap();
    store.migrate().await.unwrap();

    for table in [
        "sessions",
        "agents",
        "agent_credentials",
        "agent_acl_grants",
        "agent_runtimes",
        "agent_groups",
        "agent_group_members",
        "messages",
        "in_flight",
        "inbox_subscriptions",
        "inbox_subscription_batches",
        "threads",
        "thread_members",
        "topics",
        "subscriptions",
        "notifications",
        "developer_event_topics",
        "developer_events",
        "command_intents",
        "command_intent_events",
        "command_queue_mutations",
        "daemon_state",
        "initial_prompt_deliveries",
        "sources",
        "agent_session_turns",
        "agent_session_messages",
        "agent_session_stream_cursors",
        "transcript_archive",
        "native_thread_bindings",
        "producer_identities",
        "reply_contexts",
        "messages_fts",
    ] {
        assert!(
            object_exists(&store, "table", table).await,
            "missing table {table}"
        );
    }
    for (kind, name) in [
        ("view", "nexus_broadcast_ingress"),
        ("trigger", "nexus_broadcast_ingress_insert"),
        ("trigger", "trg_command_intent_queue_insert"),
        ("trigger", "trg_command_intent_queue_update"),
        ("trigger", "in_flight_reply_context_on_injecting"),
    ] {
        assert!(
            object_exists(&store, kind, name).await,
            "missing {kind} {name}"
        );
    }
    assert!(!object_exists(&store, "table", "projects").await);
    assert!(!object_exists(&store, "table", "message_vectors").await);
    assert!(!object_exists(&store, "table", "stream_events").await);
    assert!(attached_table_exists(&store, "mem", "stream_events").await);
    assert!(attached_table_exists(&store, "mem", "stream_raw").await);

    assert_eq!(
        schema_rows(&store).await,
        vec![
            (1, "v0.1.0_baseline".into()),
            (2, "v0.1.5_message_hooks".into()),
            (3, "v0.1.5_delivery_timing".into()),
        ]
    );
}

#[tokio::test]
async fn baseline_contains_current_identity_routing_and_delivery_columns() {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();

    for (table, column) in [
        ("sessions", "agent_id"),
        ("sessions", "metadata_json"),
        ("sessions", "transport"),
        ("agents", "owner_agent_id"),
        ("agents", "lifecycle_state"),
        ("agents", "dead_reason"),
        ("agent_runtimes", "os_pid"),
        ("agent_runtimes", "os_pgid"),
        ("messages", "from_agent_id"),
        ("messages", "to_agent_id"),
        ("messages", "sender_session_id"),
        ("messages", "idempotency_key"),
        ("messages", "metadata_json"),
        ("messages", "mention_json"),
        ("in_flight", "recipient_agent_id"),
        ("in_flight", "attempt_count"),
        ("in_flight", "attempt_started_at"),
        ("in_flight", "error_code"),
        ("in_flight", "error_reason"),
        ("in_flight", "error_details_json"),
        ("in_flight", "delivery_timing"),
        ("threads", "archived_at"),
        ("threads", "metadata_json"),
        ("threads", "topic"),
        ("threads", "description"),
        ("thread_members", "agent_id"),
        ("subscriptions", "subscriber_agent_id"),
        ("command_intents", "caller_agent_id"),
        ("command_intents", "caller_runtime_id"),
        ("command_intents", "idempotency_key"),
        ("command_intents", "revision"),
        ("developer_events", "data_json"),
        ("agent_acl_grants", "principal_agent_id"),
        ("agent_acl_grants", "granted_by_agent_id"),
        ("initial_prompt_deliveries", "client_message_id"),
        ("native_thread_bindings", "provider"),
        ("native_thread_bindings", "kind"),
        ("inbox_subscription_batches", "message_signature"),
        ("native_thread_bindings", "native_thread_id"),
        ("producer_identities", "producer_id"),
        ("transcript_archive", "archive_offset"),
        ("reply_contexts", "recipient_key"),
    ] {
        assert!(
            column_exists(&store, table, column).await,
            "missing {table}.{column}"
        );
    }
}

#[tokio::test]
async fn public_v010_baseline_upgrades_to_message_hook_schema_without_losing_rows() {
    const PUBLIC_V010_SCHEMA: &str = include_str!("../../../migrations/0001_init.sql");

    let store = Store::open(":memory:").await.unwrap();
    store
        .conn
        .execute_batch(
            "CREATE TABLE schema_migrations (
               version INTEGER PRIMARY KEY,
               name TEXT NOT NULL,
               applied_at INTEGER NOT NULL
             );",
        )
        .await
        .unwrap();
    store.conn.execute_batch(PUBLIC_V010_SCHEMA).await.unwrap();
    store
        .conn
        .execute(
            "INSERT INTO schema_migrations(version, name, applied_at)
             VALUES (1, 'v0.1.0_baseline', 1)",
            (),
        )
        .await
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO messages(
               message_id, from_name, kind, to_name, body, provenance, project, created_at
             ) VALUES (
               'm_existing', 'ana', 'agent', 'ben', 'preserve-me', '{}', 'default', 1
             )",
            (),
        )
        .await
        .unwrap();

    store.migrate().await.unwrap();

    assert_eq!(
        schema_rows(&store).await,
        vec![
            (1, "v0.1.0_baseline".into()),
            (2, "v0.1.5_message_hooks".into()),
            (3, "v0.1.5_delivery_timing".into()),
        ]
    );
    assert!(column_exists(&store, "messages", "mention_json").await);
    assert!(view_column_exists(&store, "nexus_broadcast_ingress", "metadata_json").await);
    assert!(view_column_exists(&store, "nexus_broadcast_ingress", "mention_json").await);
    assert!(view_column_exists(&store, "nexus_broadcast_ingress", "delivery_timing").await);
    assert_eq!(
        single_text(
            &store,
            "SELECT body FROM messages WHERE message_id = 'm_existing'"
        )
        .await,
        "preserve-me"
    );
}

#[tokio::test]
async fn in_flight_state_sweeps_use_partial_indexes() {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();

    for (index, sql) in [
        (
            "idx_in_flight_undrained",
            "SELECT f.in_flight_id FROM in_flight f JOIN messages m \
             ON m.message_id = f.message_id WHERE f.state IN ('pending','notified') \
             AND m.created_at <= 10",
        ),
        (
            "idx_in_flight_injecting",
            "SELECT in_flight_id FROM in_flight WHERE state = 'injecting' ORDER BY in_flight_id",
        ),
        (
            "idx_in_flight_error",
            "SELECT f.in_flight_id FROM in_flight f JOIN messages m \
             ON m.message_id = f.message_id WHERE f.state = 'error'",
        ),
    ] {
        assert!(
            object_exists(&store, "index", index).await,
            "missing {index}"
        );
        let mut rows = store
            .conn
            .query(&format!("EXPLAIN QUERY PLAN {sql}"), ())
            .await
            .unwrap();
        let mut plan = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            plan.push(row.get::<String>(3).unwrap());
        }
        assert!(
            plan.iter().any(|detail| detail.contains(index)),
            "expected {index} in plan {plan:?}"
        );
    }
}

#[tokio::test]
async fn pre_release_upgrade_ladder_is_rejected_without_mutation() {
    let store = Store::open(":memory:").await.unwrap();
    store
        .conn
        .execute_batch(
            "CREATE TABLE schema_migrations (
               version INTEGER PRIMARY KEY,
               applied_at INTEGER
             );
             INSERT INTO schema_migrations(version, applied_at)
               VALUES (1, 1), (2, 2), (17, 17);
             CREATE TABLE pre_release_sentinel(value TEXT NOT NULL);
             INSERT INTO pre_release_sentinel VALUES ('preserve-me');",
        )
        .await
        .unwrap();

    let error = store.migrate().await.unwrap_err().to_string();
    assert!(error.contains("pre-release"), "unexpected error: {error}");
    assert!(
        error.contains("published forward migrations"),
        "unexpected error: {error}"
    );
    assert_eq!(
        single_text(&store, "SELECT value FROM pre_release_sentinel").await,
        "preserve-me"
    );
    assert_eq!(pre_release_schema_versions(&store).await, vec![1, 2, 17]);
}

#[tokio::test]
async fn unmarked_nonempty_store_is_rejected_without_mutation() {
    let store = Store::open(":memory:").await.unwrap();
    store
        .conn
        .execute_batch(
            "CREATE TABLE operator_data(value TEXT NOT NULL);
             INSERT INTO operator_data VALUES ('preserve-me');",
        )
        .await
        .unwrap();

    let error = store.migrate().await.unwrap_err().to_string();
    assert!(
        error.contains("unrecognized non-empty"),
        "unexpected error: {error}"
    );
    assert!(!object_exists(&store, "table", "schema_migrations").await);
    assert_eq!(
        single_text(&store, "SELECT value FROM operator_data").await,
        "preserve-me"
    );
}

#[tokio::test]
async fn named_baseline_with_missing_required_object_fails_closed() {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    store.conn.execute("DROP TABLE messages", ()).await.unwrap();

    let error = store.migrate().await.unwrap_err().to_string();
    assert!(
        error.contains("incomplete v0.1.0"),
        "unexpected error: {error}"
    );
    assert!(error.contains("messages"), "unexpected error: {error}");
}

async fn object_exists(store: &Store, kind: &str, name: &str) -> bool {
    let mut rows = store
        .conn
        .query(
            "SELECT 1 FROM sqlite_master WHERE type = ?1 AND name = ?2 LIMIT 1",
            libsql::params![kind, name],
        )
        .await
        .unwrap();
    rows.next().await.unwrap().is_some()
}

async fn attached_table_exists(store: &Store, schema: &str, table: &str) -> bool {
    let sql = format!("SELECT 1 FROM {schema}.sqlite_master WHERE type = 'table' AND name = ?1");
    let mut rows = store
        .conn
        .query(&sql, libsql::params![table])
        .await
        .unwrap();
    rows.next().await.unwrap().is_some()
}

async fn column_exists(store: &Store, table: &str, column: &str) -> bool {
    let mut rows = store
        .conn
        .query(&format!("PRAGMA table_info({table})"), ())
        .await
        .unwrap();
    while let Some(row) = rows.next().await.unwrap() {
        if row.get::<String>(1).unwrap() == column {
            return true;
        }
    }
    false
}

async fn view_column_exists(store: &Store, view: &str, column: &str) -> bool {
    column_exists(store, view, column).await
}

async fn schema_rows(store: &Store) -> Vec<(i64, String)> {
    let mut rows = store
        .conn
        .query(
            "SELECT version, name FROM schema_migrations ORDER BY version",
            (),
        )
        .await
        .unwrap();
    let mut values = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        values.push((row.get(0).unwrap(), row.get(1).unwrap()));
    }
    values
}

async fn pre_release_schema_versions(store: &Store) -> Vec<i64> {
    let mut rows = store
        .conn
        .query("SELECT version FROM schema_migrations ORDER BY version", ())
        .await
        .unwrap();
    let mut values = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        values.push(row.get(0).unwrap());
    }
    values
}

async fn single_text(store: &Store, sql: &str) -> String {
    let mut rows = store.conn.query(sql, ()).await.unwrap();
    rows.next()
        .await
        .unwrap()
        .unwrap()
        .get::<String>(0)
        .unwrap()
}
