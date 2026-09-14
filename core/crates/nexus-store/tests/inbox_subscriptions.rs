use nexus_contracts::{
    BatchCounts, BatchMessage, Kind, Message, MessageId, NexusBatch, ProjectId, Provenance, Scope,
    SessionId,
};
use nexus_store::daemon_store::DaemonStore;
use nexus_store::repos::{
    caller_subscription_id, AgentRuntimes, Agents, DeliveryObligations, DeveloperEvents, Inbox,
    InboxSubscriptions, Messages, NewAgent, NewAgentRuntime, NewDeliveryObligation,
    NewInboxSubscription, NewSession, Sessions,
};
use nexus_store::Store;

async fn migrated() -> Store {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    store
}

fn subscription() -> NewInboxSubscription {
    NewInboxSubscription {
        subscription_id: caller_subscription_id("default", "s_ada"),
        project: "default".to_string(),
        caller_name: "ada".to_string(),
        caller_session_id: "s_ada".to_string(),
        caller_agent_id: Some("a_ada".to_string()),
        caller_client_key: Some("ck_ada".to_string()),
        timeout_ms: Some(500),
        max: Some(10),
        created_at: 100,
    }
}

fn batch() -> NexusBatch {
    NexusBatch {
        counts: BatchCounts {
            dms: 1,
            thread: 0,
            total: 1,
        },
        dms: vec![BatchMessage {
            id: MessageId("m_1".to_string()),
            from: "ben".to_string(),
            kind: Kind::Agent,
            scope: Scope::Dm,
            thread: None,
            topic: None,
            body: "hello".to_string(),
            truncated: false,
        }],
        threads: vec![],
        dm_message_ids: vec![MessageId("m_1".to_string())],
        thread_message_ids: vec![],
        message_ids: vec![MessageId("m_1".to_string())],
        auto_reply_note: None,
    }
}

fn message() -> Message {
    Message {
        id: MessageId("m_1".into()),
        project: ProjectId("default".into()),
        from: "ben".into(),
        scope: Scope::Dm,
        thread: None,
        topic: None,
        body: "hello".into(),
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
        created_at: 250,
    }
}

#[tokio::test]
async fn active_subscriptions_are_durable_and_refreshable() {
    let store = migrated().await;
    let repo = InboxSubscriptions::new(&store);

    repo.upsert_active(subscription()).await.unwrap();
    assert!(repo.has_active_for_session("s_ada").await.unwrap());
    assert!(!repo.has_active_for_session("s_ben").await.unwrap());
    let row = repo
        .get(&caller_subscription_id("default", "s_ada"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, "active");
    assert_eq!(row.caller_session_id, "s_ada");
    assert_eq!(row.caller_agent_id.as_deref(), Some("a_ada"));
    assert_eq!(row.timeout_ms, Some(500));

    let mut refreshed = subscription();
    refreshed.timeout_ms = Some(1_000);
    refreshed.max = Some(20);
    refreshed.created_at = 200;
    repo.upsert_active(refreshed).await.unwrap();
    let rows = repo.list_active().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].timeout_ms, Some(1_000));
    assert_eq!(rows[0].max, Some(20));
}

#[tokio::test]
async fn inbox_subscribe_and_unsubscribe_write_action_events() {
    let store = migrated().await;
    let repo = InboxSubscriptions::new(&store);
    let sub = caller_subscription_id("default", "s_ada");

    repo.upsert_active(subscription()).await.unwrap();
    assert!(repo.mark_inactive(&sub, 200).await.unwrap());
    assert!(!repo.has_active_for_session("s_ada").await.unwrap());

    let rows = DeveloperEvents::new(&store)
        .since("sys.inbox.ada", 0)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].kind, "action");
    assert_eq!(rows[0].from_name.as_deref(), Some("ada"));
    assert_eq!(rows[0].session_id.as_deref(), Some("s_ada"));
    let first: serde_json::Value =
        serde_json::from_str(rows[0].data_json.as_deref().unwrap()).unwrap();
    let second: serde_json::Value =
        serde_json::from_str(rows[1].data_json.as_deref().unwrap()).unwrap();
    assert_eq!(first["action"], "inbox.subscribe");
    assert_eq!(first["subscriptionId"], sub);
    assert_eq!(second["action"], "inbox.unsubscribe");
    assert_eq!(second["subscriptionId"], sub);
}

#[tokio::test]
async fn inbox_action_events_are_best_effort_when_append_fails() {
    let store = migrated().await;
    store
        .conn
        .execute(
            "CREATE TRIGGER fail_inbox_action_events
             BEFORE INSERT ON developer_events
             WHEN NEW.kind = 'action'
             BEGIN
               SELECT RAISE(ABORT, 'forced inbox telemetry failure');
             END",
            (),
        )
        .await
        .unwrap();
    let repo = InboxSubscriptions::new(&store);
    let sub = caller_subscription_id("default", "s_ada");

    repo.upsert_active(subscription()).await.unwrap();
    assert!(repo.mark_inactive(&sub, 200).await.unwrap());

    assert_eq!(repo.get(&sub).await.unwrap().unwrap().status, "inactive");
    assert!(DeveloperEvents::new(&store)
        .since("sys.inbox.ada", 0)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn pending_batches_are_replayed_until_consumed() {
    let store = migrated().await;
    let repo = InboxSubscriptions::new(&store);
    let sub = caller_subscription_id("default", "s_ada");
    repo.upsert_active(subscription()).await.unwrap();

    let first = repo
        .insert_pending_batch(&sub, &batch(), 300)
        .await
        .unwrap()
        .expect("first batch inserted");
    assert_eq!(first.subscription_id, sub);
    assert_eq!(first.status, "pending");

    let duplicate = repo
        .insert_pending_batch(&sub, &batch(), 301)
        .await
        .unwrap();
    assert!(
        duplicate.is_none(),
        "same pending message signature must not create duplicate durable batches"
    );

    let pending = repo.pending_batch(&sub).await.unwrap().unwrap();
    assert_eq!(pending.batch_id, first.batch_id);
    assert!(pending.batch_json.contains("hello"));

    assert!(repo
        .mark_batch_consumed(&sub, &first.batch_id, 400)
        .await
        .unwrap());
    assert!(repo.pending_batch(&sub).await.unwrap().is_none());
    assert!(!repo
        .mark_batch_consumed(&sub, &first.batch_id, 401)
        .await
        .unwrap());
}

#[tokio::test]
async fn acknowledged_pull_batch_settles_the_persistent_delivery_obligation() {
    let daemon = DaemonStore::open(":memory:").await.unwrap();
    let store = daemon.compatibility_store();
    let session = SessionId("s_ada".into());

    Agents::new(&store)
        .create(NewAgent {
            agent_id: "a_ada".into(),
            project: "default".into(),
            name: Some("ada".into()),
            default_harness: Some("other".into()),
            role: None,
            tier: Some("agent".into()),
            owner: None,
        })
        .await
        .unwrap();
    AgentRuntimes::new(&store)
        .create(NewAgentRuntime {
            runtime_id: session.0.clone(),
            agent_id: "a_ada".into(),
            harness: "other".into(),
            cwd: None,
            transport: None,
            presence: Some("online".into()),
            active: true,
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some("ada".into()),
            agent: Some("other".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some("ck_ada".into()),
            cwd: None,
            project: "default".into(),
            transport: None,
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .set_agent_id(&session, "a_ada")
        .await
        .unwrap();
    Messages::new(&store).insert(&message()).await.unwrap();
    Inbox::new(&store)
        .enqueue(&MessageId("m_1".into()), &session)
        .await
        .unwrap();
    DeliveryObligations::new(&store)
        .insert(NewDeliveryObligation {
            message_id: "m_1".into(),
            recipient_agent_id: "a_ada".into(),
            recipient_runtime_id: Some(session.0.clone()),
            payload_json: serde_json::to_string(&message()).unwrap(),
            dedupe_key: "delivery:m_1:a_ada".into(),
            attempt: 0,
            state: "pending".into(),
            created_at: 250,
        })
        .await
        .unwrap();

    let repo = InboxSubscriptions::new(&store);
    repo.upsert_active(subscription()).await.unwrap();
    let durable = repo
        .insert_pending_batch(&caller_subscription_id("default", "s_ada"), &batch(), 300)
        .await
        .unwrap()
        .unwrap();
    assert!(repo
        .acknowledge_pull_batch(&durable.subscription_id, &durable.batch_id, &session, 400,)
        .await
        .unwrap());

    assert!(
        DeliveryObligations::new(&store)
            .pending()
            .await
            .unwrap()
            .is_empty(),
        "a rendered+acknowledged pull batch must not leave a phantom pending obligation to replay"
    );
}

#[tokio::test]
async fn durable_pull_ack_settlement_survives_a_lost_volatile_commit_and_reopen() {
    let identity_path = std::env::temp_dir().join(format!(
        "nexus-pull-ack-settlement-{}-{}.db",
        std::process::id(),
        nexus_common::now()
    ));
    let daemon = DaemonStore::open(identity_path.to_string_lossy().as_ref())
        .await
        .unwrap();
    let store = daemon.compatibility_store();
    DeliveryObligations::new(&store)
        .insert(NewDeliveryObligation {
            message_id: "m_crash_window".into(),
            recipient_agent_id: "a_ada".into(),
            recipient_runtime_id: Some("s_ada".into()),
            payload_json: serde_json::to_string(&message()).unwrap(),
            dedupe_key: "delivery:m_crash_window:a_ada".into(),
            attempt: 0,
            state: "pending".into(),
            created_at: 250,
        })
        .await
        .unwrap();

    // This is the durable half of pull ACK. Production performs it after validating the pending
    // transport batch but before committing its boot-scoped rows. Dropping the daemon here models
    // a crash that loses that volatile commit entirely.
    assert_eq!(
        DeliveryObligations::new(&store)
            .settle_pull_batch_for_runtime(&[MessageId("m_crash_window".into())], "s_ada",)
            .await
            .unwrap(),
        1
    );
    drop(store);
    drop(daemon);

    let reopened = DaemonStore::open(identity_path.to_string_lossy().as_ref())
        .await
        .unwrap();
    assert!(
        DeliveryObligations::new(&reopened.compatibility_store())
            .pending()
            .await
            .unwrap()
            .is_empty(),
        "a pull ACK accepted at the durable boundary must not replay after daemon boot"
    );
    drop(reopened);
    let _ = std::fs::remove_file(identity_path);
}
