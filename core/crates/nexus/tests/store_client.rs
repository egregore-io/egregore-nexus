use std::sync::Arc;
use std::time::{Duration, Instant};

use nexus::cli::ambient::with_test_env_vars;
use nexus::cli::store_client::StoreClient;
use nexus::daemon::AppState;
use nexus_common::{now, Config};
use nexus_contracts::{
    codes, Ack, BatchCounts, ConsumeRequest, ContractError, Kind, MessageId, NexusBatch, Presence,
    RegisterRequest, SendRequest, SendTarget, SpawnRequest, SpawnResponse, Tier,
};
use nexus_store::command_kinds;
use nexus_store::repos::{CommandIntents, DaemonState, Inbox, NewSession, Sessions};
use nexus_store::Store;

async fn migrated_store() -> Arc<Store> {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    store
}

async fn first_command(store: &Store) -> nexus_store::repos::CommandIntentRow {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let mut rows = store
            .conn
            .query("SELECT command_id FROM command_intents LIMIT 1", ())
            .await
            .unwrap();
        if let Some(row) = rows.next().await.unwrap() {
            let command_id = row.get::<String>(0).unwrap();
            drop(rows);
            return CommandIntents::new(store)
                .get(&command_id)
                .await
                .unwrap()
                .unwrap();
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for command row"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_kind_count(store: &Store, kind: &str, count: i64) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let mut rows = store
            .conn
            .query(
                "SELECT COUNT(*) FROM command_intents WHERE kind = ?1",
                libsql::params![kind],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let actual = row.get::<i64>(0).unwrap();
        if actual >= count {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {count} {kind} command rows; saw {actual}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_kind_status(store: &Store, kind: &str, status: &str) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let mut rows = store
            .conn
            .query(
                "SELECT status FROM command_intents WHERE kind = ?1 ORDER BY created_at ASC LIMIT 1",
                libsql::params![kind],
            )
            .await
            .unwrap();
        if let Some(row) = rows.next().await.unwrap() {
            let actual = row.get::<String>(0).unwrap();
            if actual == status {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {kind} command row to become {status}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_first_kind_row(
    store: &Store,
    kind: &str,
) -> nexus_store::repos::CommandIntentRow {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(row) = command_rows_for_kind(store, kind).await.into_iter().next() {
            return row;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {kind} command row"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn command_rows_for_kind(
    store: &Store,
    kind: &str,
) -> Vec<nexus_store::repos::CommandIntentRow> {
    let mut rows = store
        .conn
        .query(
            "SELECT command_id FROM command_intents WHERE kind = ?1 ORDER BY created_at ASC",
            libsql::params![kind],
        )
        .await
        .unwrap();
    let mut command_ids = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        command_ids.push(row.get::<String>(0).unwrap());
    }
    drop(rows);
    let repo = CommandIntents::new(store);
    let mut out = Vec::new();
    for command_id in command_ids {
        out.push(repo.get(&command_id).await.unwrap().unwrap());
    }
    out
}

async fn wait_for_new_kind_row(
    store: &Store,
    kind: &str,
    before_count: usize,
) -> nexus_store::repos::CommandIntentRow {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let rows = command_rows_for_kind(store, kind).await;
        if rows.len() > before_count {
            return rows[before_count].clone();
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for new {kind} command row after {before_count} existing rows"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn process_until_command_done(state: &AppState, command_id: &str) {
    let row = process_until_command_terminal(state, command_id).await;
    if row.status == "error" {
        panic!(
            "command {command_id} errored: {}",
            row.error_json.as_deref().unwrap_or("<missing error_json>")
        );
    }
}

async fn process_until_command_terminal(
    state: &AppState,
    command_id: &str,
) -> nexus_store::repos::CommandIntentRow {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let _ = nexus::daemon::command_worker::process_next(state)
            .await
            .unwrap();
        let row = CommandIntents::new(&state.store)
            .get(command_id)
            .await
            .unwrap()
            .expect("command row");
        match row.status.as_str() {
            "done" | "error" => return row,
            _ => {}
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for command {command_id} done; status={}, error_json={:?}",
            row.status,
            row.error_json
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn agent_register(name: &str, client_key: &str) -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some(name.into()),
        harness: hid("other"),
        harness_session_id: format!("hs_{client_key}"),
        project: "default".into(),
        client_key: client_key.into(),
        runtime_credential: None,
        tier: Tier::Agent,
        kind: Some(Kind::Agent),
        role: None,
        cwd: None,
    }
}

fn app_register(name: &str, client_key: &str) -> RegisterRequest {
    RegisterRequest {
        kind: Some(Kind::App),
        ..agent_register(name, client_key)
    }
}

async fn mark_batch_delivery_completed(
    store: &Store,
    session: &nexus_contracts::SessionId,
    batch: &NexusBatch,
) {
    let inbox = Inbox::new(store);
    inbox.mark_notified(session).await.unwrap();
    for message_id in &batch.message_ids {
        assert_eq!(inbox.mark_injecting(message_id, session).await.unwrap(), 1);
        assert_eq!(inbox.mark_delivered(message_id, session).await.unwrap(), 1);
    }
}

fn dm_request() -> SendRequest {
    SendRequest {
        to: SendTarget::dm_name("ben"),
        summary: Some("hello".into()),
        body: "hi".into(),
        mention: vec![],
        idempotency_key: None,
    }
}

async fn consume_via_store_client(client: &StoreClient) -> Result<NexusBatch, ContractError> {
    client
        .command(
            command_kinds::inbox::CONSUME,
            &ConsumeRequest {
                timeout_ms: Some(1),
                max: Some(10),
            },
        )
        .await
}

#[tokio::test]
async fn listen_ack_activity_keeps_consumer_heartbeat_fresh() {
    let store = migrated_store().await;
    let config = Config::default();
    let state = AppState::wire(store.clone(), &config);
    let target = state
        .identity
        .register(app_register("listen-target", "ck_listen_target"))
        .await
        .unwrap();
    state
        .identity
        .register(agent_register("listen-sender", "ck_listen_sender"))
        .await
        .unwrap();

    let old_heartbeat = nexus_common::now() - 120_000;
    store
        .conn
        .execute(
            "UPDATE sessions SET last_heartbeat = ?2 WHERE session_id = ?1",
            libsql::params![target.session_id.0.clone(), old_heartbeat],
        )
        .await
        .unwrap();

    let sender = state
        .identity
        .resolve("default", "listen-sender")
        .await
        .unwrap();
    state
        .bus
        .send(
            &sender,
            SendRequest {
                to: SendTarget::dm_name("listen-target".to_string()),
                summary: Some("listen heartbeat proof".to_string()),
                body: "hello from listen-sender".to_string(),
                mention: vec![],
                idempotency_key: None,
            },
        )
        .await
        .unwrap();

    let consumer = StoreClient::from_store_with_caller_for_tests(
        store.clone(),
        "listen-target",
        "default",
        Some("ck_listen_target".into()),
        Tier::Agent,
        Kind::App,
    );
    let before_consume = command_rows_for_kind(&state.store, command_kinds::inbox::CONSUME)
        .await
        .len();
    let drain_task = tokio::spawn({
        let consumer = consumer.clone();
        async move { nexus::cli::commands::listen::drain_once(&consumer, Some(1), Some(10)).await }
    });
    let consume_row =
        wait_for_new_kind_row(&state.store, command_kinds::inbox::CONSUME, before_consume).await;
    process_until_command_done(&state, &consume_row.command_id).await;
    let batch = drain_task.await.unwrap().unwrap();
    assert_eq!(batch.counts.dms, 1);
    assert_eq!(batch.dm_message_ids.len(), 1);
    assert_eq!(batch.dms[0].body, "hello from listen-sender");
    mark_batch_delivery_completed(&store, &target.session_id, &batch).await;

    let row_after_consume = Sessions::new(&store)
        .find_by_session_id(&target.session_id)
        .await
        .unwrap()
        .expect("target session");
    assert!(row_after_consume.last_heartbeat.unwrap_or_default() > old_heartbeat);

    store
        .conn
        .execute(
            "UPDATE sessions SET last_heartbeat = ?2 WHERE session_id = ?1",
            libsql::params![target.session_id.0.clone(), old_heartbeat],
        )
        .await
        .unwrap();

    let row_before_ack = Sessions::new(&store)
        .find_by_session_id(&target.session_id)
        .await
        .unwrap()
        .expect("target session");
    assert_eq!(row_before_ack.last_heartbeat, Some(old_heartbeat));

    let before_ack = command_rows_for_kind(&state.store, command_kinds::inbox::ACK)
        .await
        .len();
    let ack_task = tokio::spawn({
        let consumer = consumer.clone();
        let batch = batch.clone();
        async move { nexus::cli::commands::listen::split_ack(&consumer, &batch).await }
    });
    let ack_row = wait_for_new_kind_row(&state.store, command_kinds::inbox::ACK, before_ack).await;
    process_until_command_done(&state, &ack_row.command_id).await;
    ack_task.await.unwrap().unwrap();

    let row = Sessions::new(&store)
        .find_by_session_id(&target.session_id)
        .await
        .unwrap()
        .expect("target session");
    assert!(row.last_heartbeat.unwrap_or_default() > old_heartbeat);
}

#[tokio::test]
async fn durable_listen_subscription_replays_pending_batch_until_subscription_ack() {
    let store = migrated_store().await;
    let config = Config::default();
    let state = AppState::wire(store.clone(), &config);
    let target = state
        .identity
        .register(app_register("sub-target", "ck_sub_target"))
        .await
        .unwrap();
    state
        .identity
        .register(agent_register("sub-sender", "ck_sub_sender"))
        .await
        .unwrap();
    let sender = state
        .identity
        .resolve("default", "sub-sender")
        .await
        .unwrap();
    state
        .bus
        .send(
            &sender,
            SendRequest {
                to: SendTarget::dm_name("sub-target".to_string()),
                summary: Some("durable subscription replay".to_string()),
                body: "hello durable listen".to_string(),
                mention: vec![],
                idempotency_key: None,
            },
        )
        .await
        .unwrap();

    let consumer = StoreClient::from_store_with_caller_for_tests(
        store.clone(),
        "sub-target",
        "default",
        Some("ck_sub_target".into()),
        Tier::Agent,
        Kind::App,
    );

    let before_subscribe = command_rows_for_kind(&state.store, command_kinds::inbox::SUBSCRIBE)
        .await
        .len();
    let subscribe_task = tokio::spawn({
        let consumer = consumer.clone();
        async move { nexus::cli::commands::listen::subscribe(&consumer, Some(0), Some(10)).await }
    });
    let row = wait_for_new_kind_row(
        &state.store,
        command_kinds::inbox::SUBSCRIBE,
        before_subscribe,
    )
    .await;
    process_until_command_done(&state, &row.command_id).await;
    let subscription_id = subscribe_task.await.unwrap().unwrap().subscription_id;
    assert!(subscription_id.contains(&target.session_id.0));

    let first = next_subscription_batch_for_test(&state, &consumer, &subscription_id)
        .await
        .expect("first durable batch");
    assert_eq!(first.batch.counts.dms, 1);
    assert_eq!(first.batch.dms[0].body, "hello durable listen");

    let replayed = next_subscription_batch_for_test(&state, &consumer, &subscription_id)
        .await
        .expect("pending durable batch must replay before subscription ack");
    assert_eq!(replayed.batch_id, first.batch_id);
    assert_eq!(replayed.batch.message_ids, first.batch.message_ids);

    let before_ack = command_rows_for_kind(&state.store, command_kinds::inbox::ACK)
        .await
        .len();
    let ack_task = tokio::spawn({
        let consumer = consumer.clone();
        let batch = first.batch.clone();
        async move { nexus::cli::commands::listen::split_ack(&consumer, &batch).await }
    });
    let ack_row = wait_for_new_kind_row(&state.store, command_kinds::inbox::ACK, before_ack).await;
    process_until_command_done(&state, &ack_row.command_id).await;
    ack_task.await.unwrap().unwrap();

    let before_batch_ack =
        command_rows_for_kind(&state.store, command_kinds::inbox::SUBSCRIPTION_ACK)
            .await
            .len();
    let batch_ack_task = tokio::spawn({
        let consumer = consumer.clone();
        let first = first.clone();
        async move { nexus::cli::commands::listen::ack_subscription_batch(&consumer, &first).await }
    });
    let batch_ack_row = wait_for_new_kind_row(
        &state.store,
        command_kinds::inbox::SUBSCRIPTION_ACK,
        before_batch_ack,
    )
    .await;
    process_until_command_done(&state, &batch_ack_row.command_id).await;
    batch_ack_task.await.unwrap().unwrap();

    let mut delivery_rows = store
        .conn
        .query(
            "SELECT state FROM in_flight WHERE message_id = ?1 AND recipient_session = ?2",
            libsql::params![
                first.batch.message_ids[0].0.clone(),
                target.session_id.0.clone()
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        delivery_rows
            .next()
            .await
            .unwrap()
            .expect("delivery row")
            .get::<String>(0)
            .unwrap(),
        "acked",
        "the client's durable batch acknowledgement must settle a pull-owned delivery"
    );

    assert!(
        next_subscription_batch_for_test(&state, &consumer, &subscription_id)
            .await
            .is_none(),
        "after split-ack + subscription ack, no pending durable batch should replay"
    );
}

#[tokio::test]
async fn durable_subscription_ack_does_not_resurrect_a_terminal_delivery() {
    let store = migrated_store().await;
    let state = AppState::wire(store.clone(), &Config::default());
    let target = state
        .identity
        .register(app_register(
            "terminal-sub-target",
            "ck_terminal_sub_target",
        ))
        .await
        .unwrap();
    state
        .identity
        .register(agent_register(
            "terminal-sub-sender",
            "ck_terminal_sub_sender",
        ))
        .await
        .unwrap();
    let sender = state
        .identity
        .resolve("default", "terminal-sub-sender")
        .await
        .unwrap();
    let sent = state
        .bus
        .send(
            &sender,
            SendRequest {
                to: SendTarget::dm_name("terminal-sub-target"),
                summary: None,
                body: "terminal pull delivery".to_string(),
                mention: vec![],
                idempotency_key: None,
            },
        )
        .await
        .unwrap();
    let consumer = StoreClient::from_store_with_caller_for_tests(
        store.clone(),
        "terminal-sub-target",
        "default",
        Some("ck_terminal_sub_target".into()),
        Tier::Agent,
        Kind::App,
    );
    let subscribe_task = tokio::spawn({
        let consumer = consumer.clone();
        async move { nexus::cli::commands::listen::subscribe(&consumer, Some(0), Some(10)).await }
    });
    let subscribe_row =
        wait_for_new_kind_row(&state.store, command_kinds::inbox::SUBSCRIBE, 0).await;
    process_until_command_done(&state, &subscribe_row.command_id).await;
    let subscription_id = subscribe_task.await.unwrap().unwrap().subscription_id;
    let batch = next_subscription_batch_for_test(&state, &consumer, &subscription_id)
        .await
        .expect("durable batch");

    Inbox::new(&store)
        .mark_delivery_error(
            &sent.message_id,
            &target.session_id,
            nexus_store::repos::inbox::TARGET_DEAD_ERROR_CODE,
            "target died before pull acknowledgement",
            None,
        )
        .await
        .unwrap();

    let before_ack = command_rows_for_kind(&state.store, command_kinds::inbox::SUBSCRIPTION_ACK)
        .await
        .len();
    let ack_task = tokio::spawn({
        let consumer = consumer.clone();
        let batch = batch.clone();
        async move { nexus::cli::commands::listen::ack_subscription_batch(&consumer, &batch).await }
    });
    let ack_row = wait_for_new_kind_row(
        &state.store,
        command_kinds::inbox::SUBSCRIPTION_ACK,
        before_ack,
    )
    .await;
    let terminal = process_until_command_terminal(&state, &ack_row.command_id).await;
    assert_eq!(terminal.status, "error");
    let error = ack_task
        .await
        .unwrap()
        .expect_err("terminal delivery must reject pull acknowledgement");
    assert!(error.message.contains("terminal delivery"), "{error:?}");

    let replay = next_subscription_batch_for_test(&state, &consumer, &subscription_id)
        .await
        .expect("rejected terminal batch must remain pending for inspection");
    assert_eq!(replay.batch_id, batch.batch_id);
    let mut rows = store
        .conn
        .query(
            "SELECT state, error_code FROM in_flight WHERE message_id = ?1",
            [sent.message_id.0.as_str()],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("terminal delivery row");
    assert_eq!(row.get::<String>(0).unwrap(), "error");
    assert_eq!(
        row.get::<Option<String>>(1).unwrap().as_deref(),
        Some(nexus_store::repos::inbox::TARGET_DEAD_ERROR_CODE)
    );
}

#[tokio::test]
async fn listen_retries_an_initial_subscription_timeout() {
    let store = migrated_store().await;
    let state = AppState::wire(store.clone(), &Config::default());
    state
        .identity
        .register(app_register("retrying-listener", "ck_retrying_listener"))
        .await
        .unwrap();
    let consumer = StoreClient::from_store_with_caller_for_tests(
        store.clone(),
        "retrying-listener",
        "default",
        Some("ck_retrying_listener".into()),
        Tier::Agent,
        Kind::App,
    )
    .with_timeout(Duration::from_millis(40));
    let listen_task = tokio::spawn(async move {
        nexus::cli::commands::listen::listen(
            &consumer,
            nexus::cli::commands::listen::ListenArgs {
                timeout_ms: Some(0),
                max: Some(10),
                no_ack: false,
                once: true,
            },
            true,
        )
        .await
    });

    wait_for_kind_count(&store, command_kinds::inbox::SUBSCRIBE, 1).await;
    wait_for_kind_count(&store, command_kinds::inbox::SUBSCRIBE, 2).await;
    let subscribe_rows = command_rows_for_kind(&store, command_kinds::inbox::SUBSCRIBE).await;
    assert_eq!(subscribe_rows[0].status, "pending");
    process_until_command_done(&state, &subscribe_rows[1].command_id).await;

    let next_row = wait_for_new_kind_row(&store, command_kinds::inbox::SUBSCRIPTION_NEXT, 0).await;
    process_until_command_done(&state, &next_row.command_id).await;
    assert_eq!(
        listen_task.await.unwrap(),
        std::process::ExitCode::SUCCESS,
        "a transient initial-subscribe timeout must not kill the long-running listener"
    );
}

#[tokio::test]
async fn listen_once_keeps_one_subscription_until_requested_empty_window_expires() {
    let store = migrated_store().await;
    let consumer = StoreClient::from_store_with_caller_for_tests(
        store.clone(),
        "held-window-listener",
        "default",
        Some("ck_held_window_listener".into()),
        Tier::Agent,
        Kind::App,
    );
    let mut listen_task = tokio::spawn(async move {
        nexus::cli::commands::listen::listen(
            &consumer,
            nexus::cli::commands::listen::ListenArgs {
                timeout_ms: Some(200),
                max: Some(10),
                no_ack: false,
                once: true,
            },
            true,
        )
        .await
    });

    let subscribe_row = wait_for_new_kind_row(&store, command_kinds::inbox::SUBSCRIBE, 0).await;
    CommandIntents::new(&store)
        .mark_done(
            &subscribe_row.command_id,
            &serde_json::to_string(&nexus_contracts::InboxSubscribeResponse {
                subscription_id: "sub_held_window".into(),
                active: true,
            })
            .unwrap(),
            now(),
        )
        .await
        .unwrap();
    let first_next =
        wait_for_new_kind_row(&store, command_kinds::inbox::SUBSCRIPTION_NEXT, 0).await;
    CommandIntents::new(&store)
        .mark_done(
            &first_next.command_id,
            &serde_json::to_string(&nexus_contracts::InboxSubscriptionNextResponse { batch: None })
                .unwrap(),
            now(),
        )
        .await
        .unwrap();

    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut listen_task)
            .await
            .is_err(),
        "--once returned after the first internal slice instead of retaining the held window"
    );

    let second_next =
        wait_for_new_kind_row(&store, command_kinds::inbox::SUBSCRIPTION_NEXT, 1).await;
    tokio::time::sleep(Duration::from_millis(175)).await;
    CommandIntents::new(&store)
        .mark_done(
            &second_next.command_id,
            &serde_json::to_string(&nexus_contracts::InboxSubscriptionNextResponse { batch: None })
                .unwrap(),
            now(),
        )
        .await
        .unwrap();
    let exit = tokio::time::timeout(Duration::from_secs(1), listen_task)
        .await
        .expect("held-window listen should complete")
        .unwrap();

    assert_eq!(exit, std::process::ExitCode::SUCCESS);
    assert_eq!(
        command_rows_for_kind(&store, command_kinds::inbox::SUBSCRIBE)
            .await
            .len(),
        1,
        "one held window must retain one durable subscription"
    );
    assert!(
        command_rows_for_kind(&store, command_kinds::inbox::SUBSCRIPTION_NEXT)
            .await
            .len()
            >= 2,
        "the requested window must span multiple bounded daemon slices"
    );
}

async fn next_subscription_batch_for_test(
    state: &AppState,
    consumer: &StoreClient,
    subscription_id: &str,
) -> Option<nexus_contracts::InboxSubscriptionBatch> {
    let before_next = command_rows_for_kind(&state.store, command_kinds::inbox::SUBSCRIPTION_NEXT)
        .await
        .len();
    let next_task = tokio::spawn({
        let consumer = consumer.clone();
        let subscription_id = subscription_id.to_string();
        async move {
            nexus::cli::commands::listen::next_batch(&consumer, &subscription_id, Some(0)).await
        }
    });
    let row = wait_for_new_kind_row(
        &state.store,
        command_kinds::inbox::SUBSCRIPTION_NEXT,
        before_next,
    )
    .await;
    process_until_command_done(state, &row.command_id).await;
    next_task.await.unwrap().unwrap().batch
}

#[tokio::test]
async fn inbox_consume_reissues_after_daemon_boot_epoch_changes() {
    let daemon_store = migrated_store().await;
    let client_store = daemon_store.clone();
    DaemonState::new(&daemon_store)
        .set_boot_epoch("boot_before_restart", now())
        .await
        .unwrap();

    let config = Config::default();
    let state_before = AppState::wire(daemon_store.clone(), &config);
    let target = state_before
        .identity
        .register(app_register("restart-target", "ck_restart_target"))
        .await
        .unwrap();
    Sessions::new(&daemon_store)
        .set_presence(&target.session_id, Presence::Online)
        .await
        .unwrap();
    state_before
        .identity
        .register(agent_register("restart-sender", "ck_restart_sender"))
        .await
        .unwrap();

    let consumer = StoreClient::from_store_with_caller_for_tests(
        client_store.clone(),
        "restart-target",
        "default",
        Some("ck_restart_target".into()),
        Tier::Agent,
        Kind::App,
    )
    .with_timeout(Duration::from_secs(3))
    .with_poll_interval(Duration::from_millis(500));
    let consume_task = tokio::spawn({
        let consumer = consumer.clone();
        async move {
            consumer
                .command::<_, NexusBatch>(
                    command_kinds::inbox::CONSUME,
                    &ConsumeRequest {
                        timeout_ms: Some(60_000),
                        max: None,
                    },
                )
                .await
        }
    });

    wait_for_first_kind_row(&daemon_store, command_kinds::inbox::CONSUME).await;
    let claimed = CommandIntents::new(&daemon_store)
        .claim_next_kind(now(), 15_000, command_kinds::inbox::CONSUME)
        .await
        .unwrap()
        .expect("first consume row should be claimable");
    assert_eq!(claimed.status, "claimed");
    wait_for_kind_status(&daemon_store, command_kinds::inbox::CONSUME, "claimed").await;

    // Simulate a daemon restart while the old long-poll command is claimed: the old worker dies,
    // mail arrives durably while no worker is alive, the new daemon records a fresh boot epoch, and
    // callers must reissue the held receive without operator intervention.
    let ack = state_before
        .bus
        .send(
            &state_before
                .identity
                .resolve("default", "restart-sender")
                .await
                .unwrap(),
            SendRequest {
                to: SendTarget::dm_name("restart-target"),
                summary: None,
                body: "restart-safe delivery".into(),
                mention: vec![],
                idempotency_key: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(ack.fanout, Some(1));
    DaemonState::new(&daemon_store)
        .set_boot_epoch("boot_after_restart", now())
        .await
        .unwrap();
    let state_after = AppState::wire(daemon_store.clone(), &config);

    wait_for_kind_count(&daemon_store, command_kinds::inbox::CONSUME, 2).await;
    let caller = state_after
        .identity
        .resolve("default", "restart-target")
        .await
        .unwrap();
    Sessions::new(&daemon_store)
        .set_presence(&caller.session, Presence::Online)
        .await
        .unwrap();
    let drained = state_after
        .realtime
        .consume(
            &caller,
            ConsumeRequest {
                timeout_ms: Some(0),
                max: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(drained.counts.total, 1);
    assert_eq!(drained.dms[0].body, "restart-safe delivery");
    let consume_rows = command_rows_for_kind(&daemon_store, command_kinds::inbox::CONSUME).await;
    CommandIntents::new(&daemon_store)
        .mark_done(
            &consume_rows[1].command_id,
            &serde_json::to_string(&drained).unwrap(),
            now(),
        )
        .await
        .unwrap();

    let batch = tokio::time::timeout(Duration::from_secs(3), consume_task)
        .await
        .expect("consume task must finish after restart")
        .unwrap()
        .unwrap();
    assert_eq!(batch.counts.total, 1);
    assert_eq!(batch.dms[0].body, "restart-safe delivery");
    let consume_rows = command_rows_for_kind(&daemon_store, command_kinds::inbox::CONSUME).await;
    assert_eq!(consume_rows.len(), 2);
    assert_eq!(consume_rows[0].status, "claimed");
    assert_eq!(consume_rows[1].status, "done");
}

#[tokio::test]
async fn store_client_consume_helper_reissues_after_boot_epoch_change() {
    let daemon_store = migrated_store().await;
    DaemonState::new(&daemon_store)
        .set_boot_epoch("boot_before_restart", now())
        .await
        .unwrap();

    let config = Config::default();
    let state = AppState::wire(daemon_store.clone(), &config);
    state
        .identity
        .register(agent_register("mcp-target", "ck_mcp_target"))
        .await
        .unwrap();

    let consumer = StoreClient::from_store_with_caller_for_tests(
        daemon_store.clone(),
        "mcp-target",
        "default",
        Some("ck_mcp_target".into()),
        Tier::Agent,
        Kind::Agent,
    )
    .with_timeout(Duration::from_secs(3))
    .with_poll_interval(Duration::from_millis(100));

    let consume_task = tokio::spawn({
        let consumer = consumer.clone();
        async move { consume_via_store_client(&consumer).await }
    });

    wait_for_first_kind_row(&daemon_store, command_kinds::inbox::CONSUME).await;
    let claimed = CommandIntents::new(&daemon_store)
        .claim_next_kind(now(), 15_000, command_kinds::inbox::CONSUME)
        .await
        .unwrap()
        .expect("first consume row should be claimable");
    assert_eq!(claimed.status, "claimed");
    wait_for_kind_status(&daemon_store, command_kinds::inbox::CONSUME, "claimed").await;

    DaemonState::new(&daemon_store)
        .set_boot_epoch("boot_after_restart", now())
        .await
        .unwrap();

    wait_for_kind_count(&daemon_store, command_kinds::inbox::CONSUME, 2).await;
    let empty_batch = NexusBatch {
        counts: BatchCounts {
            dms: 0,
            thread: 0,
            total: 0,
        },
        dms: vec![],
        threads: vec![],
        dm_message_ids: vec![],
        thread_message_ids: vec![],
        message_ids: vec![],
    };
    let consume_rows = command_rows_for_kind(&daemon_store, command_kinds::inbox::CONSUME).await;
    CommandIntents::new(&daemon_store)
        .mark_done(
            &consume_rows[1].command_id,
            &serde_json::to_string(&empty_batch).unwrap(),
            now(),
        )
        .await
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(3), consume_task)
        .await
        .expect("consume task must finish after restart")
        .unwrap()
        .unwrap();
    assert_eq!(result.counts.total, 0);
    let consume_rows = command_rows_for_kind(&daemon_store, command_kinds::inbox::CONSUME).await;
    assert_eq!(consume_rows.len(), 2);
    assert_eq!(consume_rows[0].status, "claimed");
    assert_eq!(consume_rows[1].status, "done");
}

#[tokio::test]
async fn store_client_submits_command_intents_with_caller_metadata() {
    let store = migrated_store().await;
    let client = StoreClient::from_store_with_caller_for_tests(
        store.clone(),
        "ada",
        "demo",
        Some("ck_ada".into()),
        Tier::Admin,
        Kind::Human,
    )
    .with_timeout(Duration::from_secs(2));

    let pending = tokio::spawn({
        let client = client.clone();
        async move { client.message_post_send(&dm_request()).await }
    });

    let row = first_command(&store).await;
    assert_eq!(row.kind, command_kinds::message_post::SEND);
    assert_eq!(row.project, "demo");
    assert_eq!(row.caller_name, "ada");
    assert_eq!(row.caller_client_key.as_deref(), Some("ck_ada"));
    assert_eq!(row.caller_kind.as_deref(), Some("human"));
    assert_eq!(row.caller_tier.as_deref(), Some("admin"));

    CommandIntents::new(&store)
        .mark_error(
            &row.command_id,
            &serde_json::to_string(&ContractError {
                code: codes::INTERNAL_ERROR,
                message: "stop test".into(),
            })
            .unwrap(),
            2,
        )
        .await
        .unwrap();
    let _ = pending.await.unwrap();
}

#[tokio::test]
async fn ambient_store_client_does_not_invent_session_metadata_from_client_key() {
    let store = migrated_store().await;
    let client = with_test_env_vars(
        &[
            ("NEXUS_NAME", Some("bianca")),
            ("NEXUS_CLIENT_KEY", Some("nexus_ck_bianca")),
            ("NEXUS_PROJECT", Some("default")),
            ("NEXUS_AGENT", Some("codex")),
            ("NEXUS_SESSION_ID", None),
            ("NEXUS_AGENT_ID", None),
            ("NEXUS_RUNTIME_ID", None),
        ],
        || StoreClient::from_store_for_tests(store.clone()).with_timeout(Duration::from_secs(2)),
    );

    let pending = tokio::spawn({
        let client = client.clone();
        async move { client.message_post_send(&dm_request()).await }
    });

    let row = first_command(&store).await;
    assert_eq!(row.caller_name, "bianca");
    assert_eq!(row.caller_client_key.as_deref(), Some("nexus_ck_bianca"));
    assert_eq!(row.caller_session_id, None);

    CommandIntents::new(&store)
        .mark_error(
            &row.command_id,
            &serde_json::to_string(&ContractError {
                code: codes::INTERNAL_ERROR,
                message: "stop test".into(),
            })
            .unwrap(),
            2,
        )
        .await
        .unwrap();
    let _ = pending.await.unwrap();
}

#[tokio::test]
async fn ambient_store_client_follows_registered_client_key_after_rename() {
    let store = migrated_store().await;
    Sessions::new(&store)
        .create(NewSession {
            session_id: nexus_contracts::SessionId("s_renamed".into()),
            name: Some("after-rename".into()),
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some("hs_rename".into()),
            client_key: Some("ck_rename".into()),
            cwd: None,
            project: "default".into(),
            transport: Some("pty".into()),
        })
        .await
        .unwrap();

    let client = StoreClient::from_store_with_identity_for_tests_resolving(
        store.clone(),
        RegisterRequest {
            agent_id: None,
            name: Some("before-rename".into()),
            harness: hid("claude"),
            harness_session_id: "hs_rename".into(),
            project: "default".into(),
            client_key: "ck_rename".into(),
            runtime_credential: None,
            tier: Tier::Agent,
            kind: Some(Kind::Agent),
            role: None,
            cwd: None,
        },
    )
    .await
    .unwrap()
    .with_timeout(Duration::from_secs(2));

    let pending = tokio::spawn({
        let client = client.clone();
        async move { client.message_post_send(&dm_request()).await }
    });

    let row = first_command(&store).await;
    assert_eq!(row.caller_name, "after-rename");
    assert_eq!(row.caller_client_key.as_deref(), Some("ck_rename"));
    assert_eq!(row.caller_kind.as_deref(), Some("agent"));
    assert_eq!(row.caller_tier.as_deref(), Some("agent"));
    assert_eq!(row.caller_session_id, None);

    CommandIntents::new(&store)
        .mark_error(
            &row.command_id,
            &serde_json::to_string(&ContractError {
                code: codes::INTERNAL_ERROR,
                message: "stop test".into(),
            })
            .unwrap(),
            2,
        )
        .await
        .unwrap();
    let _ = pending.await.unwrap();
}

#[tokio::test]
async fn store_client_polls_done_result_json_into_typed_ack() {
    let store = migrated_store().await;
    let client = StoreClient::from_store_with_caller_for_tests(
        store.clone(),
        "ada",
        "demo",
        Some("ck_ada".into()),
        Tier::Agent,
        Kind::Agent,
    );

    let pending = tokio::spawn({
        let client = client.clone();
        async move { client.message_post_send(&dm_request()).await }
    });

    let row = first_command(&store).await;
    let internal_ack = Ack {
        message_id: MessageId("m_done".into()),
        fanout: Some(2),
    };
    CommandIntents::new(&store)
        .mark_done(
            &row.command_id,
            &serde_json::to_string(&internal_ack).unwrap(),
            3,
        )
        .await
        .unwrap();

    assert_eq!(
        pending.await.unwrap().unwrap(),
        Ack {
            message_id: MessageId("m_done".into()),
            fanout: None,
        },
        "the store client must preserve the public ack shape without internal fanout"
    );
}

#[tokio::test]
async fn store_client_maps_error_json_to_contract_error() {
    let store = migrated_store().await;
    let client = StoreClient::from_store_with_caller_for_tests(
        store.clone(),
        "ada",
        "demo",
        Some("ck_ada".into()),
        Tier::Agent,
        Kind::Agent,
    );

    let pending = tokio::spawn({
        let client = client.clone();
        async move { client.message_post_send(&dm_request()).await }
    });

    let row = first_command(&store).await;
    let err = ContractError {
        code: codes::NOT_FOUND,
        message: "no such recipient".into(),
    };
    CommandIntents::new(&store)
        .mark_error(&row.command_id, &serde_json::to_string(&err).unwrap(), 4)
        .await
        .unwrap();

    assert_eq!(pending.await.unwrap().unwrap_err(), err);
}

#[tokio::test]
async fn default_harness_launch_wait_outlives_the_generic_ten_second_budget() {
    let store = migrated_store().await;
    let client = StoreClient::from_store_with_caller_for_tests(
        store.clone(),
        "operator",
        "demo",
        None,
        Tier::Admin,
        Kind::Human,
    );
    let pending = tokio::spawn(async move {
        client
            .command::<_, SpawnResponse>(
                command_kinds::harness::LAUNCH,
                &SpawnRequest {
                    kind: hid("claude"),
                    name: Some("slow-cold-launch".into()),
                    identity_policy: None,
                    cwd: None,
                    project: Some("demo".into()),
                    role: None,
                    initial_prompt: None,
                    resume: None,
                    harness_args: Vec::new(),
                    headless: true,
                    backend: None,
                },
            )
            .await
    });
    let row = first_command(&store).await;

    tokio::time::sleep(Duration::from_millis(10_250)).await;
    let response = SpawnResponse {
        session_id: nexus_contracts::SessionId("s_slow_launch".into()),
    };
    CommandIntents::new(&store)
        .mark_done(
            &row.command_id,
            &serde_json::to_string(&response).unwrap(),
            now(),
        )
        .await
        .unwrap();

    assert_eq!(pending.await.unwrap().unwrap(), response);
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
