//! Atomic daemon-owned queue mutation behavior used by the gateway IPC surface.

use nexus_contracts::{
    AgentId, CommandExpectedRevision, CommandQueueAction, CommandQueueMutationRequest, SessionId,
};
use nexus_store::repos::{CommandIntents, CommandQueue, NewCommandIntent};
use nexus_store::Store;

async fn store(active_turn: bool) -> Store {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    store
        .conn
        .execute(
            "INSERT INTO sessions (session_id, agent_id, name, agent, kind, transport, project, created_at) \
             VALUES ('s_otto', 'a_otto', 'otto', 'codex', 'agent', 'codex-appserver', 'default', 1)",
            (),
        )
        .await
        .unwrap();
    if active_turn {
        store
            .conn
            .execute(
                "INSERT INTO agent_session_turns VALUES \
                 ('turn_active', 's_otto', 'streaming', 1, 1, 1, 1, NULL)",
                (),
            )
            .await
            .unwrap();
    }
    store
}

async fn prompt(store: &Store, command_id: &str, created_at: i64) {
    prompt_json(
        store,
        command_id,
        created_at,
        format!(
            r#"{{"name":"otto","text":"text {command_id}","clientMessageId":"cm_{command_id}"}}"#
        ),
    )
    .await;
}

async fn prompt_json(store: &Store, command_id: &str, created_at: i64, request_json: String) {
    CommandIntents::new(store)
        .insert_pending(NewCommandIntent {
            command_id: command_id.into(),
            kind: nexus_store::command_kinds::harness::PROMPT.into(),
            project: "default".into(),
            caller_name: "Alex".into(),
            caller_session_id: Some("local-operator".into()),
            caller_agent_id: None,
            caller_runtime_id: Some("local-operator".into()),
            caller_client_key: None,
            caller_principal_id: None,
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: Some(format!("cm_{command_id}")),
            request_json,
            created_at,
        })
        .await
        .unwrap();
}

async fn exact_store() -> Store {
    let store = store(true).await;
    store.identity_conn().execute_batch(
        "INSERT INTO agents (agent_id, project, name, tier, created_at) VALUES ('a_otto', 'default', 'otto', 'agent', 1);
         INSERT INTO agent_runtimes (runtime_id, agent_id, harness, transport, presence, active, started_at) VALUES ('s_otto', 'a_otto', 'codex', 'codex-appserver', 'busy', 1, 1);"
    ).await.unwrap();
    store
}

fn exact_mutation(
    action: CommandQueueAction,
    id: &str,
    command_id: &str,
    session: &str,
) -> CommandQueueMutationRequest {
    let mut wire = serde_json::to_value(mutation(action, id, Some(command_id))).unwrap();
    wire["expectedSessionId"] = serde_json::json!(session);
    serde_json::from_value(wire).unwrap()
}

#[tokio::test]
async fn exact_redirect_requires_matching_immutable_original_selector() {
    for selector in [None, Some("s_foreign"), Some("s_otto")] {
        let store = exact_store().await;
        let mut wire = serde_json::json!({"agentId":"a_otto", "name":"otto", "text":"preserve", "clientMessageId":"cm_exact"});
        if let Some(selector) = selector {
            wire["expectedSessionId"] = serde_json::json!(selector);
        }
        let original = wire.to_string();
        prompt_json(&store, "cmd_exact", 1, original.clone()).await;
        let request = exact_mutation(
            CommandQueueAction::RedirectNow,
            "mut_exact",
            "cmd_exact",
            "s_otto",
        );
        let outcome = CommandQueue::new(&store)
            .mutate("metadata", &request, 2)
            .await
            .unwrap();
        assert_eq!(
            outcome.status,
            if selector == Some("s_otto") { 200 } else { 409 },
            "{selector:?}: {outcome:?}"
        );
        let row = CommandIntents::new(&store)
            .get("cmd_exact")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.request_json, original);
        assert_eq!(
            row.kind,
            if selector == Some("s_otto") {
                nexus_store::command_kinds::harness::STEER
            } else {
                nexus_store::command_kinds::harness::PROMPT
            }
        );
    }
}

#[tokio::test]
async fn exact_mutation_replays_committed_receipt_after_rebind_but_conflicts_on_changed_selector() {
    let store = exact_store().await;
    prompt(&store, "cmd_exact_cancel", 1).await;
    let request = exact_mutation(
        CommandQueueAction::Cancel,
        "mut_exact_cancel",
        "cmd_exact_cancel",
        "s_otto",
    );
    let first = CommandQueue::new(&store)
        .mutate("metadata", &request, 2)
        .await
        .unwrap();
    assert_eq!(first.status, 200);
    store
        .identity_conn()
        .execute(
            "UPDATE agent_runtimes SET active = 0, stopped_at = 3 WHERE agent_id = 'a_otto'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        CommandQueue::new(&store)
            .mutate("other metadata", &request, 4)
            .await
            .unwrap(),
        first
    );
    let changed = exact_mutation(
        CommandQueueAction::Cancel,
        "mut_exact_cancel",
        "cmd_exact_cancel",
        "s_other",
    );
    assert_eq!(
        CommandQueue::new(&store)
            .mutate("metadata", &changed, 5)
            .await
            .unwrap()
            .status,
        409
    );
    let fresh = exact_mutation(
        CommandQueueAction::Cancel,
        "mut_exact_absent",
        "cmd_exact_cancel",
        "s_otto",
    );
    let stale = CommandQueue::new(&store)
        .mutate("metadata", &fresh, 6)
        .await
        .unwrap();
    assert_eq!(stale.status, 409);
    assert!(stale.body["error"].as_str().unwrap().contains("session"));
}

#[tokio::test]
async fn exact_mutation_reads_runtime_only_after_waiting_for_identity_transaction() {
    use std::future::Future;
    use std::task::Poll;
    let store = exact_store().await;
    prompt_json(&store, "cmd_wait", 1, serde_json::json!({"agentId":"a_otto", "expectedSessionId":"s_otto", "name":"otto", "text":"do not retarget"}).to_string()).await;
    let tx = store
        .begin_identity_write_txn("test_rebind_before_mutation_lock")
        .await
        .unwrap();
    let request = exact_mutation(CommandQueueAction::Cancel, "mut_wait", "cmd_wait", "s_otto");
    let queue = CommandQueue::new(&store);
    let mut mutation = Box::pin(queue.mutate("metadata", &request, 4));
    // Embedded reads complete synchronously. Poll through them to the held write
    // gate: spawning an unpolled task would not exercise the old pre-lock read.
    std::future::poll_fn(|cx| {
        assert!(matches!(mutation.as_mut().poll(cx), Poll::Pending));
        Poll::Ready(())
    })
    .await;
    tx.execute_batch("UPDATE agent_runtimes SET active = 0, stopped_at = 2 WHERE agent_id = 'a_otto';
        INSERT INTO agent_runtimes (runtime_id, agent_id, harness, transport, presence, active, started_at) VALUES ('s_rebound', 'a_otto', 'codex', 'codex-appserver', 'busy', 1, 3);
        INSERT INTO sessions (session_id, agent_id, name, agent, kind, transport, project, created_at) VALUES ('s_rebound', 'a_otto', 'rebound', 'codex', 'agent', 'codex-appserver', 'default', 3);"
    ).await.unwrap();
    tx.commit().await.unwrap();
    let outcome = mutation.await.unwrap();
    assert_eq!(
        outcome.status, 409,
        "the queued mutation must see committed S2, not its old pre-lock S1 read"
    );
    assert_eq!(
        CommandIntents::new(&store)
            .get("cmd_wait")
            .await
            .unwrap()
            .unwrap()
            .status,
        "pending"
    );
}

fn mutation(
    action: CommandQueueAction,
    mutation_id: &str,
    command_id: Option<&str>,
) -> CommandQueueMutationRequest {
    CommandQueueMutationRequest {
        expected_session_id: None,
        name: Some("otto".into()),
        agent_id: Some(AgentId("a_otto".into())),
        action,
        client_mutation_id: mutation_id.into(),
        command_id: command_id.map(str::to_owned),
        expected_revision: command_id.map(|_| 1),
        text: None,
        command_ids: Vec::new(),
        expected_revisions: Vec::new(),
    }
}

#[tokio::test]
async fn split_exact_mutation_uses_identity_runtime_and_transport_owned_descriptor() {
    let daemon = nexus_store::DaemonStore::open(":memory:").await.unwrap();
    let store = daemon.compatibility_store();
    daemon.identity().conn.execute_batch(
        "INSERT INTO agents (agent_id, project, name, tier, created_at) VALUES ('a_otto', 'identity', 'otto', 'agent', 1);
         INSERT INTO agent_runtimes (runtime_id, agent_id, harness, transport, presence, active, started_at) VALUES ('s_otto', 'a_otto', 'codex', 'codex-appserver', 'busy', 1, 1);"
    ).await.unwrap();
    store.conn.execute_batch(
        "INSERT INTO sessions (session_id, agent_id, name, agent, kind, transport, project, created_at) VALUES ('s_otto', 'a_otto', 'otto', 'codex', 'agent', 'codex-appserver', 'transport', 1);"
    ).await.unwrap();
    let original =
        r#"{ "agentId":"a_otto", "expectedSessionId":"s_otto", "name":"otto", "text":"split" }"#
            .to_string();
    prompt_json(&store, "cmd_split_exact", 1, original.clone()).await;
    let request = exact_mutation(
        CommandQueueAction::RedirectNow,
        "mut_split_exact",
        "cmd_split_exact",
        "s_otto",
    );
    let outcome = CommandQueue::new(&store)
        .mutate_with_active_sessions("metadata", &request, 2, &[SessionId("s_otto".into())])
        .await
        .unwrap();
    assert_eq!(outcome.status, 200, "{outcome:?}");
    assert_eq!(outcome.body["sessionId"], "s_otto");
    assert_eq!(
        CommandIntents::new(&store)
            .get("cmd_split_exact")
            .await
            .unwrap()
            .unwrap()
            .request_json,
        original
    );
    store
        .conn
        .execute(
            "UPDATE sessions SET agent_id = 'a_foreign' WHERE session_id = 's_otto'",
            (),
        )
        .await
        .unwrap();
    let cancel = exact_mutation(
        CommandQueueAction::Cancel,
        "mut_split_foreign",
        "cmd_split_exact",
        "s_otto",
    );
    assert!(
        CommandQueue::new(&store)
            .mutate("metadata", &cancel, 3)
            .await
            .is_err(),
        "transport description cannot assert ownership of another identity's runtime"
    );
}

#[tokio::test]
async fn legacy_mutation_canonical_json_does_not_acquire_a_null_selector() {
    let store = store(false).await;
    prompt(&store, "cmd_legacy", 1).await;
    let request = mutation(CommandQueueAction::Cancel, "mut_legacy", Some("cmd_legacy"));
    assert_eq!(
        CommandQueue::new(&store)
            .mutate("metadata", &request, 2)
            .await
            .unwrap()
            .status,
        200
    );
    let mut rows = store.identity_conn().query("SELECT request_json FROM command_queue_mutations WHERE client_mutation_id = 'mut_legacy'", ()).await.unwrap();
    let raw: String = rows.next().await.unwrap().unwrap().get(0).unwrap();
    assert!(serde_json::from_str::<serde_json::Value>(&raw)
        .unwrap()
        .get("expectedSessionId")
        .is_none());
}

#[tokio::test]
async fn legacy_mutation_retains_fallback_when_active_runtime_has_no_transport_row() {
    let store = exact_store().await;
    store
        .identity_conn()
        .execute(
            "UPDATE agent_runtimes SET runtime_id = 's_missing' WHERE runtime_id = 's_otto'",
            (),
        )
        .await
        .unwrap();
    prompt(&store, "cmd_legacy_fallback", 1).await;
    let request = mutation(
        CommandQueueAction::RedirectNow,
        "mut_legacy_fallback",
        Some("cmd_legacy_fallback"),
    );
    let outcome = CommandQueue::new(&store)
        .mutate("metadata", &request, 2)
        .await
        .unwrap();
    assert_eq!(
        outcome.status, 200,
        "omitted selector preserves existing revive-era lookup compatibility"
    );
    assert_eq!(outcome.body["sessionId"], "s_otto");
}

#[tokio::test]
async fn authoritative_runtime_rebind_waits_for_identity_transaction_reader_to_commit() {
    use std::future::Future;
    use std::task::Poll;
    let store = exact_store().await;
    let tx = store
        .begin_identity_write_txn("queue_identity_read_exclusion")
        .await
        .unwrap();
    let mut rows = tx
        .query(
            "SELECT runtime_id FROM agent_runtimes WHERE agent_id = 'a_otto' AND active = 1",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next()
            .await
            .unwrap()
            .unwrap()
            .get::<String>(0)
            .unwrap(),
        "s_otto"
    );
    drop(rows);
    let runtimes = nexus_store::repos::AgentRuntimes::new(&store);
    let mut rebind = Box::pin(runtimes.create(nexus_store::repos::NewAgentRuntime {
        runtime_id: "s_next".into(),
        agent_id: "a_otto".into(),
        harness: "codex".into(),
        cwd: None,
        transport: Some("codex-appserver".into()),
        presence: Some("busy".into()),
        active: true,
    }));
    std::future::poll_fn(|cx| {
        assert!(matches!(rebind.as_mut().poll(cx), Poll::Pending));
        Poll::Ready(())
    })
    .await;
    let mut rows = tx
        .query(
            "SELECT runtime_id FROM agent_runtimes WHERE agent_id = 'a_otto' AND active = 1",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next()
            .await
            .unwrap()
            .unwrap()
            .get::<String>(0)
            .unwrap(),
        "s_otto"
    );
    drop(rows);
    tx.commit().await.unwrap();
    rebind.await.unwrap();
    assert_eq!(
        runtimes
            .active_for_agent("a_otto")
            .await
            .unwrap()
            .unwrap()
            .runtime_id,
        "s_next"
    );
}

#[tokio::test]
async fn redirect_promotes_one_pending_row_and_replays_the_receipt_after_ack_loss() {
    let store = store(true).await;
    prompt(&store, "cmd_redirect", 10).await;
    let request = mutation(
        CommandQueueAction::RedirectNow,
        "mut_redirect",
        Some("cmd_redirect"),
    );

    let first = CommandQueue::new(&store)
        .mutate("default", &request, 9_000)
        .await
        .unwrap();
    let replay = CommandQueue::new(&store)
        .mutate("default", &request, 9_001)
        .await
        .unwrap();

    assert_eq!(first, replay);
    assert_eq!(first.status, 200);
    assert_eq!(first.body["clientMutationId"], "mut_redirect");
    assert_eq!(first.body["commandId"], "cmd_redirect");
    assert_eq!(first.body["state"], "queued");
    assert_eq!(first.body["steerCapability"], "native_steer");
    assert_eq!(first.body["revision"], 2);
    let row = CommandIntents::new(&store)
        .get("cmd_redirect")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.kind, nexus_store::command_kinds::harness::STEER);
    assert_eq!(row.revision, 2);
}

#[tokio::test]
async fn reused_mutation_id_with_different_payload_is_a_durable_conflict() {
    let store = store(true).await;
    prompt(&store, "cmd_conflict", 10).await;
    let redirect = mutation(
        CommandQueueAction::RedirectNow,
        "mut_conflict",
        Some("cmd_conflict"),
    );
    let cancel = mutation(
        CommandQueueAction::Cancel,
        "mut_conflict",
        Some("cmd_conflict"),
    );
    assert_eq!(
        CommandQueue::new(&store)
            .mutate("default", &redirect, 9_000)
            .await
            .unwrap()
            .status,
        200
    );
    let conflict = CommandQueue::new(&store)
        .mutate("default", &cancel, 9_001)
        .await
        .unwrap();
    assert_eq!(conflict.status, 409);
    assert_eq!(
        conflict.body["error"],
        "clientMutationId was already used for a different mutation"
    );
}

#[tokio::test]
async fn stable_agent_id_is_a_complete_queue_mutation_target() {
    let store = store(false).await;
    prompt(&store, "cmd_id_only", 10).await;
    let mut request = mutation(
        CommandQueueAction::Cancel,
        "mut_id_only",
        Some("cmd_id_only"),
    );
    request.name = None;

    let outcome = CommandQueue::new(&store)
        .mutate("display-only-project", &request, 9_000)
        .await
        .unwrap();

    assert_eq!(outcome.status, 200);
    assert_eq!(outcome.body["commandId"], "cmd_id_only");
    assert_eq!(outcome.body["state"], "cancelled");
}

#[tokio::test]
async fn stable_agent_id_prefers_its_active_runtime_over_a_newer_stale_session_for_redirect() {
    let store = store(true).await;
    store
        .conn
        .execute(
            "INSERT INTO agents (agent_id, project, name, tier, created_at) \
             VALUES ('a_otto', 'identity-metadata', 'otto', 'agent', 1)",
            (),
        )
        .await
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO agent_runtimes (runtime_id, agent_id, harness, transport, presence, active, started_at) \
             VALUES ('s_otto', 'a_otto', 'codex', 'codex-appserver', 'busy', 1, 1)",
            (),
        )
        .await
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO sessions (session_id, agent_id, name, agent, kind, transport, project, created_at) \
             VALUES ('s_otto_stale', 'a_otto', 'otto-stale', 'claude', 'agent', 'acp', 'other', 2)",
            (),
        )
        .await
        .unwrap();
    prompt(&store, "cmd_active_runtime", 10).await;
    let active_sessions = [SessionId("s_otto".into())];

    let snapshot = CommandQueue::new(&store)
        .snapshot_with_active_sessions(
            Some("stale-display-name"),
            Some(&AgentId("a_otto".into())),
            &active_sessions,
        )
        .await
        .unwrap();
    assert_eq!(snapshot.session_id.as_deref(), Some("s_otto"));
    assert!(snapshot.turn_active);
    assert_eq!(
        snapshot.steer_capability,
        nexus_contracts::SteerCapability::NativeSteer
    );

    let outcome = CommandQueue::new(&store)
        .mutate_with_active_sessions(
            "display-metadata-only",
            &mutation(
                CommandQueueAction::RedirectNow,
                "mut_active_runtime",
                Some("cmd_active_runtime"),
            ),
            9_000,
            &active_sessions,
        )
        .await
        .unwrap();

    assert_eq!(outcome.status, 200);
    assert_eq!(outcome.body["sessionId"], "s_otto");
    assert_eq!(outcome.body["steerCapability"], "native_steer");
    assert_eq!(
        CommandIntents::new(&store)
            .get("cmd_active_runtime")
            .await
            .unwrap()
            .unwrap()
            .kind,
        nexus_store::command_kinds::harness::STEER,
    );
}

#[tokio::test]
async fn cancel_edit_and_reorder_are_revision_guarded_and_atomic() {
    let store = store(false).await;
    prompt(&store, "cmd_cancel", 10).await;
    prompt(&store, "cmd_edit", 20).await;
    prompt(&store, "cmd_first", 30).await;
    prompt(&store, "cmd_second", 40).await;

    let cancelled = CommandQueue::new(&store)
        .mutate(
            "default",
            &mutation(CommandQueueAction::Cancel, "mut_cancel", Some("cmd_cancel")),
            9_000,
        )
        .await
        .unwrap();
    assert_eq!(cancelled.status, 200);
    assert_eq!(cancelled.body["state"], "cancelled");

    let mut edit = mutation(CommandQueueAction::Edit, "mut_edit", Some("cmd_edit"));
    edit.text = Some("edited text".into());
    let edited = CommandQueue::new(&store)
        .mutate("default", &edit, 9_001)
        .await
        .unwrap();
    assert_eq!(edited.status, 200);
    let edited_row = CommandIntents::new(&store)
        .get("cmd_edit")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&edited_row.request_json).unwrap()["text"],
        "edited text"
    );

    let mut reorder = mutation(CommandQueueAction::Reorder, "mut_reorder", None);
    reorder.command_ids = vec!["cmd_second".into(), "cmd_first".into()];
    reorder.expected_revisions = vec![
        CommandExpectedRevision {
            command_id: "cmd_first".into(),
            revision: 1,
        },
        CommandExpectedRevision {
            command_id: "cmd_second".into(),
            revision: 1,
        },
    ];
    let reordered = CommandQueue::new(&store)
        .mutate("default", &reorder, 9_002)
        .await
        .unwrap();
    assert_eq!(reordered.status, 200);
    let mut rows = store
        .conn
        .query(
            "SELECT command_id, revision FROM command_intents \
             WHERE command_id IN ('cmd_first', 'cmd_second') ORDER BY created_at",
            (),
        )
        .await
        .unwrap();
    let first = rows.next().await.unwrap().unwrap();
    assert_eq!(first.get::<String>(0).unwrap(), "cmd_second");
    assert_eq!(first.get::<i64>(1).unwrap(), 2);
    let second = rows.next().await.unwrap().unwrap();
    assert_eq!(second.get::<String>(0).unwrap(), "cmd_first");
    assert_eq!(second.get::<i64>(1).unwrap(), 2);
}

#[tokio::test]
async fn redirect_without_an_active_turn_is_rejected_without_changing_the_prompt() {
    let store = store(false).await;
    prompt(&store, "cmd_inactive", 10).await;
    let result = CommandQueue::new(&store)
        .mutate(
            "default",
            &mutation(
                CommandQueueAction::RedirectNow,
                "mut_inactive",
                Some("cmd_inactive"),
            ),
            9_000,
        )
        .await
        .unwrap();
    assert_eq!(result.status, 409);
    assert_eq!(
        result.body["error"],
        "target has no active turn to redirect"
    );
    let row = CommandIntents::new(&store)
        .get("cmd_inactive")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.kind, nexus_store::command_kinds::harness::PROMPT);
    assert_eq!(row.revision, 1);
}
