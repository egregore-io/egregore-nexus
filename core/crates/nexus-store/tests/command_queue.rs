//! Durable session-command queue transitions and atomic redirect promotion.

use nexus_store::command_kinds;
use nexus_store::repos::{CommandIntents, NewCommandIntent};
use nexus_store::Store;

async fn store() -> Store {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    store
        .conn
        .execute(
            "INSERT INTO sessions (session_id, agent_id, name, agent, project, created_at) \
             VALUES ('s_otto', 'a_otto', 'otto', 'codex', 'default', 1)",
            (),
        )
        .await
        .unwrap();
    store
}

fn prompt(command_id: &str, created_at: i64) -> NewCommandIntent {
    NewCommandIntent {
        command_id: command_id.into(),
        kind: command_kinds::harness::PROMPT.into(),
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
            r#"{{"name":"otto","text":"continue","clientMessageId":"cm_{command_id}"}}"#
        ),
        created_at,
    }
}

#[tokio::test]
async fn pending_prompt_promotes_in_place_with_stable_ids() {
    let store = store().await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(prompt("cmd_promote", 1)).await.unwrap();

    assert!(repo
        .promote_pending_prompt_to_steer("cmd_promote")
        .await
        .unwrap());

    let row = repo.get("cmd_promote").await.unwrap().unwrap();
    assert_eq!(row.command_id, "cmd_promote");
    assert_eq!(row.idempotency_key.as_deref(), Some("cm_cmd_promote"));
    assert_eq!(row.kind, command_kinds::harness::STEER);
    assert_eq!(row.status, "pending");
    assert_eq!(row.revision, 2);
    let mut retry = prompt("cmd_promote", 2);
    retry.command_id = "cmd_retry".into();
    assert_eq!(
        repo.insert_pending_idempotent(retry).await.unwrap(),
        "cmd_promote"
    );
    let mut count = store
        .conn
        .query("SELECT COUNT(*) FROM command_intents", ())
        .await
        .unwrap();
    assert_eq!(
        count.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        1
    );
}

#[tokio::test]
async fn claim_wins_promotion_without_duplicate_or_loss() {
    let store = store().await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(prompt("cmd_claimed", 1)).await.unwrap();
    let claimed = repo
        .claim_next_kind(10, 1_000, command_kinds::harness::PROMPT)
        .await
        .unwrap()
        .unwrap();

    assert!(!repo
        .promote_pending_prompt_to_steer("cmd_claimed")
        .await
        .unwrap());
    assert!(repo
        .mark_started_for_claim("cmd_claimed", claimed.claimed_at.unwrap(), 11)
        .await
        .unwrap());

    let row = repo.get("cmd_claimed").await.unwrap().unwrap();
    assert_eq!(row.kind, command_kinds::harness::PROMPT);
    assert_eq!(row.status, "claimed");
    assert_eq!(row.started_at, Some(11));
    assert_eq!(row.revision, 3);
}

#[tokio::test]
async fn cancel_is_compare_and_set_against_pending_state() {
    let store = store().await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(prompt("cmd_cancel", 1)).await.unwrap();

    assert!(repo.cancel_pending_prompt("cmd_cancel", 20).await.unwrap());
    assert!(!repo.cancel_pending_prompt("cmd_cancel", 21).await.unwrap());
    let row = repo.get("cmd_cancel").await.unwrap().unwrap();
    assert_eq!(row.status, "cancelled");
    assert_eq!(row.completed_at, Some(20));
    assert_eq!(row.revision, 2);
}

#[tokio::test]
async fn command_event_projection_records_every_revision_in_order() {
    let store = store().await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(prompt("cmd_lifecycle", 1))
        .await
        .unwrap();
    // Later transitions retain the session resolved at enqueue even if its display name changes
    // before the daemon claims the command.
    store
        .conn
        .execute(
            "UPDATE sessions SET name = 'otto-renamed' WHERE session_id = 's_otto'",
            (),
        )
        .await
        .unwrap();
    let claimed = repo
        .claim_next_kind(10, 1_000, command_kinds::harness::PROMPT)
        .await
        .unwrap()
        .unwrap();
    let claimed_at = claimed.claimed_at.unwrap();
    assert!(repo
        .mark_started_for_claim("cmd_lifecycle", claimed_at, 11)
        .await
        .unwrap());
    assert!(repo
        .mark_done_for_claim("cmd_lifecycle", claimed_at, r#"{"accepted":true}"#, 12)
        .await
        .unwrap());

    let mut rows = store
        .conn
        .query(
            "SELECT session_id, state, mode, revision FROM command_intent_events \
             WHERE command_id = 'cmd_lifecycle' ORDER BY seq",
            (),
        )
        .await
        .unwrap();
    let mut events = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        events.push((
            row.get::<String>(0).unwrap(),
            row.get::<String>(1).unwrap(),
            row.get::<String>(2).unwrap(),
            row.get::<i64>(3).unwrap(),
        ));
    }
    assert_eq!(
        events,
        vec![
            ("s_otto".into(), "queued".into(), "queue".into(), 1),
            ("s_otto".into(), "claimed".into(), "queue".into(), 2),
            ("s_otto".into(), "started".into(), "queue".into(), 3),
            ("s_otto".into(), "completed".into(), "queue".into(), 4),
        ]
    );
}

#[tokio::test]
async fn queue_event_cursor_is_visible_to_insert_returning() {
    let store = store().await;
    let mut rows = store
        .conn
        .query(
            "INSERT INTO command_intents (command_id, kind, status, project, caller_name, \
             request_json, attempts, revision, created_at) VALUES \
             ('cmd_returning', 'harness.prompt', 'pending', 'default', 'Alex', \
              '{\"name\":\"otto\",\"text\":\"continue\",\"clientMessageId\":\"cm_returning\"}', \
              0, 1, 1) RETURNING revision, \
             (SELECT session_id FROM command_intent_events WHERE command_id = 'cmd_returning' \
              ORDER BY seq DESC LIMIT 1), \
             (SELECT seq FROM command_intent_events WHERE command_id = 'cmd_returning' \
              ORDER BY seq DESC LIMIT 1)",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    assert_eq!(row.get::<String>(1).unwrap(), "s_otto");
    assert_eq!(row.get::<i64>(2).unwrap(), 1);
}

#[tokio::test]
async fn retention_reaps_only_completed_mutation_receipts() {
    let store = store().await;
    store
        .conn
        .execute(
            "INSERT INTO command_queue_mutations \
             (project, client_mutation_id, request_json, response_status, response_json, created_at) \
             VALUES ('default', 'mut_done', '{}', 200, '{}', 1), \
                    ('default', 'mut_open', '{}', NULL, NULL, 1)",
            (),
        )
        .await
        .unwrap();

    assert_eq!(
        CommandIntents::new(&store)
            .reap_terminal_older_than(10)
            .await
            .unwrap(),
        0
    );
    let mut rows = store
        .conn
        .query(
            "SELECT client_mutation_id FROM command_queue_mutations ORDER BY client_mutation_id",
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
        "mut_open"
    );
    assert!(rows.next().await.unwrap().is_none());
}
