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
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: Some(format!("cm_{command_id}")),
            request_json: format!(
                r#"{{"name":"otto","text":"text {command_id}","clientMessageId":"cm_{command_id}"}}"#
            ),
            created_at,
        })
        .await
        .unwrap();
}

fn mutation(
    action: CommandQueueAction,
    mutation_id: &str,
    command_id: Option<&str>,
) -> CommandQueueMutationRequest {
    CommandQueueMutationRequest {
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
