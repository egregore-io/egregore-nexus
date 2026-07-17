use std::io::Cursor;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nexus::cli::commands::admin::{self, AdminCmd, AdminDlqCmd};
use nexus::cli::commands::listen::{self, ListenArgs};
use nexus::cli::commands::send::{self, DmArgs, PostArgs, PublishArgs, ReplyArgs, SendArgs};
use nexus::cli::commands::threads::{self, ThreadCmd};
use nexus::cli::read_client::ReadClient;
use nexus::cli::store_client::StoreClient;
use nexus_contracts::{
    Ack, AdminRenameResponse, ContractError, InboxSubscribeResponse, InboxSubscriptionNextResponse,
    InboxSubscriptionStatusResponse, Kind, MessageId, RemoveResponse, SendRequest, SendTarget,
    SessionId, Tier,
};
use nexus_store::command_kinds;
use nexus_store::repos::CommandIntents;
use nexus_store::Store;

async fn migrated_store() -> Arc<Store> {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    store
}

fn client(store: Arc<Store>) -> StoreClient {
    StoreClient::from_store_with_caller_for_tests(
        store,
        "ada",
        "demo",
        Some("ck_ada".into()),
        Tier::Agent,
        Kind::Agent,
    )
}

async fn wait_for_nth_command(store: &Store, count: usize) -> nexus_store::repos::CommandIntentRow {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let mut rows = store
            .conn
            .query(
                "SELECT command_id FROM command_intents ORDER BY created_at, command_id",
                (),
            )
            .await
            .unwrap();
        let mut ids = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            ids.push(row.get::<String>(0).unwrap());
        }
        drop(rows);
        if ids.len() >= count {
            return CommandIntents::new(store)
                .get(&ids[count - 1])
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

async fn complete_next(store: Arc<Store>, count: usize, fanout: Option<u32>) {
    let row = wait_for_nth_command(&store, count).await;
    let ack = Ack {
        message_id: MessageId(format!("m_{count}")),
        fanout,
    };
    CommandIntents::new(&store)
        .mark_done(
            &row.command_id,
            &serde_json::to_string(&ack).unwrap(),
            count as i64,
        )
        .await
        .unwrap();
}

async fn complete_next_null(store: Arc<Store>, count: usize) {
    let row = wait_for_nth_command(&store, count).await;
    CommandIntents::new(&store)
        .mark_done(&row.command_id, "null", count as i64)
        .await
        .unwrap();
}

async fn request_at(store: &Store, count: usize) -> SendRequest {
    let row = wait_for_nth_command(store, count).await;
    assert_eq!(row.kind, command_kinds::message_post::SEND);
    serde_json::from_str(&row.request_json).unwrap()
}

async fn command_count(store: &Store) -> usize {
    let mut rows = store
        .conn
        .query("SELECT COUNT(*) FROM command_intents", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    row.get::<i64>(0).unwrap() as usize
}

#[tokio::test]
async fn cli_listen_once_relinquishes_its_durable_subscription_before_exit() {
    let store = migrated_store().await;
    let cli = client(store.clone());

    let task = tokio::spawn({
        let cli = cli.clone();
        async move {
            listen::listen(
                &cli,
                ListenArgs {
                    timeout_ms: Some(0),
                    max: Some(10),
                    no_ack: false,
                    once: true,
                },
                true,
            )
            .await
        }
    });

    let subscribe = wait_for_nth_command(&store, 1).await;
    assert_eq!(subscribe.kind, command_kinds::inbox::SUBSCRIBE);
    CommandIntents::new(&store)
        .mark_done(
            &subscribe.command_id,
            &serde_json::to_string(&InboxSubscribeResponse {
                subscription_id: "sub_test".into(),
                active: true,
            })
            .unwrap(),
            1,
        )
        .await
        .unwrap();

    let next = wait_for_nth_command(&store, 2).await;
    assert_eq!(next.kind, command_kinds::inbox::SUBSCRIPTION_NEXT);
    CommandIntents::new(&store)
        .mark_done(
            &next.command_id,
            &serde_json::to_string(&InboxSubscriptionNextResponse { batch: None }).unwrap(),
            2,
        )
        .await
        .unwrap();

    let unsubscribe = wait_for_nth_command(&store, 3).await;
    assert_eq!(
        unsubscribe.kind,
        command_kinds::inbox::UNSUBSCRIBE,
        "one-shot listen must release daemon delivery ownership before returning"
    );
    CommandIntents::new(&store)
        .mark_done(
            &unsubscribe.command_id,
            &serde_json::to_string(&InboxSubscriptionStatusResponse {
                subscription_id: "sub_test".into(),
                active: false,
            })
            .unwrap(),
            3,
        )
        .await
        .unwrap();

    assert_eq!(task.await.unwrap(), ExitCode::SUCCESS);
}

fn read_client(store: Arc<Store>) -> ReadClient {
    ReadClient::from_store_with_caller_for_tests(
        store,
        "ada",
        "demo",
        Some("ck_ada".into()),
        Some("ck_ada".into()),
        Tier::Agent,
    )
}

#[tokio::test]
async fn cli_dm_submits_message_post_send_through_store_ingress() {
    let store = migrated_store().await;
    let cli = client(store.clone());

    let task = tokio::spawn({
        let cli = cli.clone();
        async move {
            send::dm(
                &cli,
                DmArgs {
                    name: "ben".into(),
                    message: Some("hello".into()),
                    stdin: false,
                    summary: None,
                },
                true,
            )
            .await
        }
    });
    complete_next(store.clone(), 1, None).await;

    assert_eq!(task.await.unwrap(), ExitCode::SUCCESS);
    let req = request_at(&store, 1).await;
    assert!(matches!(req.to, SendTarget::Dm { ref name, .. } if name.as_deref() == Some("ben")));
    assert_eq!(req.body, "hello");
}

#[tokio::test]
async fn cli_post_reply_publish_and_generic_send_preserve_request_shapes() {
    let store = migrated_store().await;
    let cli = client(store.clone());

    let post = tokio::spawn({
        let cli = cli.clone();
        async move {
            send::post(
                &cli,
                PostArgs {
                    thread: "backend".into(),
                    message: Some("plan".into()),
                    stdin: false,
                    mention: vec!["ben".into()],
                    summary: Some("s".into()),
                },
                true,
            )
            .await
        }
    });
    complete_next(store.clone(), 1, Some(1)).await;
    assert_eq!(post.await.unwrap(), ExitCode::SUCCESS);

    let reply = tokio::spawn({
        let cli = cli.clone();
        async move {
            send::reply(
                &cli,
                ReplyArgs {
                    message: Some("on it".into()),
                    stdin: false,
                    mention: vec![],
                    summary: None,
                },
                true,
            )
            .await
        }
    });
    complete_next(store.clone(), 2, None).await;
    assert_eq!(reply.await.unwrap(), ExitCode::SUCCESS);

    let publish = tokio::spawn({
        let cli = cli.clone();
        async move {
            send::publish(
                &cli,
                PublishArgs {
                    topic: "ci".into(),
                    message: Some("green".into()),
                    stdin: false,
                    summary: None,
                },
                true,
            )
            .await
        }
    });
    complete_next(store.clone(), 3, Some(3)).await;
    assert_eq!(publish.await.unwrap(), ExitCode::SUCCESS);

    let generic = tokio::spawn({
        let cli = cli.clone();
        async move {
            send::send(
                &cli,
                SendArgs {
                    to: "ben".into(),
                    message: Some("generic".into()),
                    stdin: false,
                    summary: None,
                    mention: vec!["ada".into()],
                },
                true,
            )
            .await
        }
    });
    complete_next(store.clone(), 4, None).await;
    assert_eq!(generic.await.unwrap(), ExitCode::SUCCESS);

    assert!(matches!(
        request_at(&store, 1).await.to,
        SendTarget::Post { ref thread } if thread == "backend"
    ));
    assert!(matches!(request_at(&store, 2).await.to, SendTarget::Reply));
    assert!(matches!(
        request_at(&store, 3).await.to,
        SendTarget::Publish { ref topic } if topic == "ci"
    ));
    assert!(matches!(
        request_at(&store, 4).await.to,
        SendTarget::Dm { ref name, .. } if name.as_deref() == Some("ben")
    ));
}

#[tokio::test]
async fn cli_admin_delete_submits_admin_delete_intent() {
    let store = migrated_store().await;
    let cli = client(store.clone());

    let task = tokio::spawn({
        let cli = cli.clone();
        async move {
            admin::run(
                &cli,
                AdminCmd::Delete {
                    name: "old-agent".into(),
                },
                true,
            )
            .await
        }
    });
    let row = wait_for_nth_command(&store, 1).await;
    assert_eq!(row.kind, nexus_store::command_kinds::admin::DELETE);
    CommandIntents::new(&store)
        .mark_done(
            &row.command_id,
            &serde_json::to_string(&RemoveResponse {
                name: Some("old-agent".into()),
                status: "deleted".into(),
            })
            .unwrap(),
            1,
        )
        .await
        .unwrap();

    assert_eq!(task.await.unwrap(), ExitCode::SUCCESS);
    let req: nexus_contracts::RemoveRequest = serde_json::from_str(&row.request_json).unwrap();
    assert_eq!(req.name, "old-agent");
    assert!(
        !req.kill,
        "admin.delete never exposes the remove --kill flag"
    );
}

#[tokio::test]
async fn cli_admin_rename_submits_admin_rename_intent() {
    let store = migrated_store().await;
    let cli = client(store.clone());

    let task = tokio::spawn({
        let cli = cli.clone();
        async move {
            admin::run(
                &cli,
                AdminCmd::Rename {
                    source: "s_staged".into(),
                    target: "nora".into(),
                },
                true,
            )
            .await
        }
    });
    let row = wait_for_nth_command(&store, 1).await;
    assert_eq!(row.kind, nexus_store::command_kinds::admin::RENAME);
    CommandIntents::new(&store)
        .mark_done(
            &row.command_id,
            &serde_json::to_string(&AdminRenameResponse {
                agent_id: nexus_contracts::AgentId("a_staged".into()),
                session_id: Some(SessionId("s_staged".into())),
                name: "nora".into(),
                previous: None,
            })
            .unwrap(),
            1,
        )
        .await
        .unwrap();

    assert_eq!(task.await.unwrap(), ExitCode::SUCCESS);
    let req: nexus_contracts::AdminRenameRequest = serde_json::from_str(&row.request_json).unwrap();
    assert_eq!(req.source, "s_staged");
    assert_eq!(req.target, "nora");
}

#[tokio::test]
async fn cli_admin_dlq_commands_submit_store_backed_admin_intents() {
    let store = migrated_store().await;
    let cli = client(store.clone());

    let list = tokio::spawn({
        let cli = cli.clone();
        async move {
            admin::run(
                &cli,
                AdminCmd::Dlq {
                    cmd: AdminDlqCmd::List {
                        for_target: Some("ana".into()),
                        since: Some("1h".into()),
                        limit: Some(25),
                    },
                },
                true,
            )
            .await
        }
    });
    let list_row = wait_for_nth_command(&store, 1).await;
    assert_eq!(list_row.kind, nexus_store::command_kinds::admin::DLQ_LIST);
    CommandIntents::new(&store)
        .mark_done(
            &list_row.command_id,
            &serde_json::to_string(&nexus_contracts::DlqListResponse {
                rows: vec![],
                total: 0,
            })
            .unwrap(),
            1,
        )
        .await
        .unwrap();
    assert_eq!(list.await.unwrap(), ExitCode::SUCCESS);
    let list_req: nexus_contracts::DlqListRequest =
        serde_json::from_str(&list_row.request_json).unwrap();
    assert_eq!(list_req.for_target.as_deref(), Some("ana"));
    assert_eq!(list_req.since.as_deref(), Some("1h"));
    assert_eq!(list_req.limit, Some(25));

    let requeue = tokio::spawn({
        let cli = cli.clone();
        async move {
            admin::run(
                &cli,
                AdminCmd::Dlq {
                    cmd: AdminDlqCmd::Requeue {
                        in_flight_id: Some("if_dead".into()),
                        for_target: None,
                        since: None,
                    },
                },
                true,
            )
            .await
        }
    });
    let requeue_row = wait_for_nth_command(&store, 2).await;
    assert_eq!(
        requeue_row.kind,
        nexus_store::command_kinds::admin::DLQ_REQUEUE
    );
    CommandIntents::new(&store)
        .mark_done(
            &requeue_row.command_id,
            &serde_json::to_string(&nexus_contracts::DlqMutationResponse {
                count: 1,
                in_flight_ids: vec!["if_dead".into()],
            })
            .unwrap(),
            2,
        )
        .await
        .unwrap();
    assert_eq!(requeue.await.unwrap(), ExitCode::SUCCESS);
    let requeue_req: nexus_contracts::DlqRequeueRequest =
        serde_json::from_str(&requeue_row.request_json).unwrap();
    assert_eq!(requeue_req.in_flight_id.as_deref(), Some("if_dead"));

    let purge = tokio::spawn({
        let cli = cli.clone();
        async move {
            admin::run(
                &cli,
                AdminCmd::Dlq {
                    cmd: AdminDlqCmd::Purge {
                        in_flight_id: None,
                        for_target: Some("ana".into()),
                        since: Some("24h".into()),
                        yes: true,
                    },
                },
                true,
            )
            .await
        }
    });
    let purge_row = wait_for_nth_command(&store, 3).await;
    assert_eq!(purge_row.kind, nexus_store::command_kinds::admin::DLQ_PURGE);
    CommandIntents::new(&store)
        .mark_done(
            &purge_row.command_id,
            &serde_json::to_string(&nexus_contracts::DlqMutationResponse {
                count: 2,
                in_flight_ids: vec!["if_a".into(), "if_b".into()],
            })
            .unwrap(),
            3,
        )
        .await
        .unwrap();
    assert_eq!(purge.await.unwrap(), ExitCode::SUCCESS);
    let purge_req: nexus_contracts::DlqPurgeRequest =
        serde_json::from_str(&purge_row.request_json).unwrap();
    assert_eq!(purge_req.for_target.as_deref(), Some("ana"));
    assert!(purge_req.yes);
}

#[tokio::test]
async fn cli_thread_archive_and_delete_submit_thread_lifecycle_intents() {
    let store = migrated_store().await;
    let cli = client(store.clone());
    let read = read_client(store.clone());

    let archive = tokio::spawn({
        let cli = cli.clone();
        let read = read.clone();
        async move {
            threads::run_store(
                &cli,
                &read,
                ThreadCmd::Archive {
                    name: "backend".into(),
                },
                true,
            )
            .await
        }
    });
    complete_next_null(store.clone(), 1).await;
    assert_eq!(archive.await.unwrap(), ExitCode::SUCCESS);

    let delete = tokio::spawn({
        let cli = cli.clone();
        let read = read.clone();
        async move {
            threads::run_store(
                &cli,
                &read,
                ThreadCmd::Delete {
                    name: "backend".into(),
                },
                true,
            )
            .await
        }
    });
    complete_next_null(store.clone(), 2).await;
    assert_eq!(delete.await.unwrap(), ExitCode::SUCCESS);

    let archive_row = wait_for_nth_command(&store, 1).await;
    assert_eq!(archive_row.kind, command_kinds::thread::ARCHIVE);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&archive_row.request_json).unwrap(),
        serde_json::json!({"name": "backend"})
    );

    let delete_row = wait_for_nth_command(&store, 2).await;
    assert_eq!(delete_row.kind, command_kinds::thread::DELETE);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&delete_row.request_json).unwrap(),
        serde_json::json!({"name": "backend"})
    );
}

#[tokio::test]
async fn cli_send_commands_stdin_preserve_shell_sensitive_body_verbatim() {
    let store = migrated_store().await;
    let cli = client(store.clone());
    let body = "literal `backticks` $vars \"quotes\" 'single quotes' $(no_subshell)";

    let dm = tokio::spawn({
        let cli = cli.clone();
        let body = body.to_string();
        async move {
            send::dm_with_reader(
                &cli,
                DmArgs {
                    name: "ben".into(),
                    message: None,
                    stdin: true,
                    summary: None,
                },
                true,
                Cursor::new(body),
            )
            .await
        }
    });
    complete_next(store.clone(), 1, None).await;
    assert_eq!(dm.await.unwrap(), ExitCode::SUCCESS);

    let post = tokio::spawn({
        let cli = cli.clone();
        let body = body.to_string();
        async move {
            send::post_with_reader(
                &cli,
                PostArgs {
                    thread: "backend".into(),
                    message: None,
                    stdin: true,
                    mention: vec![],
                    summary: None,
                },
                true,
                Cursor::new(body),
            )
            .await
        }
    });
    complete_next(store.clone(), 2, Some(1)).await;
    assert_eq!(post.await.unwrap(), ExitCode::SUCCESS);

    let reply = tokio::spawn({
        let cli = cli.clone();
        let body = body.to_string();
        async move {
            send::reply_with_reader(
                &cli,
                ReplyArgs {
                    message: None,
                    stdin: true,
                    mention: vec![],
                    summary: None,
                },
                true,
                Cursor::new(body),
            )
            .await
        }
    });
    complete_next(store.clone(), 3, None).await;
    assert_eq!(reply.await.unwrap(), ExitCode::SUCCESS);

    let publish = tokio::spawn({
        let cli = cli.clone();
        let body = body.to_string();
        async move {
            send::publish_with_reader(
                &cli,
                PublishArgs {
                    topic: "ci".into(),
                    message: None,
                    stdin: true,
                    summary: None,
                },
                true,
                Cursor::new(body),
            )
            .await
        }
    });
    complete_next(store.clone(), 4, Some(1)).await;
    assert_eq!(publish.await.unwrap(), ExitCode::SUCCESS);

    let generic = tokio::spawn({
        let cli = cli.clone();
        let body = body.to_string();
        async move {
            send::send_with_reader(
                &cli,
                SendArgs {
                    to: "ben".into(),
                    message: None,
                    stdin: true,
                    summary: None,
                    mention: vec![],
                },
                true,
                Cursor::new(body),
            )
            .await
        }
    });
    complete_next(store.clone(), 5, None).await;
    assert_eq!(generic.await.unwrap(), ExitCode::SUCCESS);

    assert_eq!(request_at(&store, 1).await.body, body);
    assert_eq!(request_at(&store, 2).await.body, body);
    assert_eq!(request_at(&store, 3).await.body, body);
    assert_eq!(request_at(&store, 4).await.body, body);
    assert_eq!(request_at(&store, 5).await.body, body);
}

#[tokio::test]
async fn cli_send_commands_reject_empty_or_whitespace_bodies_before_enqueue() {
    let store = migrated_store().await;
    let cli = client(store.clone());

    assert_ne!(
        send::dm_with_reader(
            &cli,
            DmArgs {
                name: "ben".into(),
                message: None,
                stdin: true,
                summary: None,
            },
            true,
            Cursor::new(""),
        )
        .await,
        ExitCode::SUCCESS
    );
    assert_ne!(
        send::post_with_reader(
            &cli,
            PostArgs {
                thread: "backend".into(),
                message: None,
                stdin: true,
                mention: vec![],
                summary: None,
            },
            true,
            Cursor::new(" \n\t "),
        )
        .await,
        ExitCode::SUCCESS
    );
    assert_ne!(
        send::reply_with_reader(
            &cli,
            ReplyArgs {
                message: Some("\n\t ".into()),
                stdin: false,
                mention: vec![],
                summary: None,
            },
            true,
            Cursor::new("ignored"),
        )
        .await,
        ExitCode::SUCCESS
    );
    let fast_cli = cli.clone().with_timeout(Duration::from_millis(1));
    assert_eq!(
        fast_cli
            .message_post_send(&SendRequest {
                to: SendTarget::Post {
                    thread: "backend".into(),
                },
                summary: None,
                body: " \n\t ".into(),
                mention: vec![],
                idempotency_key: None,
            })
            .await
            .unwrap_err()
            .code,
        nexus_contracts::codes::INVALID_PARAMS
    );

    assert_eq!(command_count(&store).await, 0);
}

#[tokio::test]
async fn cli_message_post_contract_errors_exit_nonzero() {
    let store = migrated_store().await;
    let cli = client(store.clone());

    let task = tokio::spawn({
        let cli = cli.clone();
        async move {
            send::dm(
                &cli,
                DmArgs {
                    name: "nobody".into(),
                    message: Some("?".into()),
                    stdin: false,
                    summary: None,
                },
                true,
            )
            .await
        }
    });
    let row = wait_for_nth_command(&store, 1).await;
    CommandIntents::new(&store)
        .mark_error(
            &row.command_id,
            &serde_json::to_string(&ContractError {
                code: nexus_contracts::codes::NOT_FOUND,
                message: "unknown recipient".into(),
            })
            .unwrap(),
            9,
        )
        .await
        .unwrap();

    assert_ne!(task.await.unwrap(), ExitCode::SUCCESS);
}
