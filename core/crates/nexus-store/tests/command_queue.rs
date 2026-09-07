//! Durable session-command queue transitions and atomic redirect promotion.

use nexus_common::NexusError;
use nexus_contracts::{AgentId, CommandQueueState, SessionId};
use nexus_store::command_kinds;
use nexus_store::repos::{
    CommandIntents, CommandQueue, NewCommandIntent, NewSession, PromptCommandOutcome, Sessions,
};
use nexus_store::{DaemonStore, Store};

async fn store() -> Store {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    store
        .conn
        .execute(
            "INSERT INTO sessions (session_id, agent_id, name, agent, kind, project, created_at) \
             VALUES ('s_otto', 'a_otto', 'otto', 'codex', 'agent', 'default', 1)",
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
        caller_principal_id: None,
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
        .mark_prompt_started_for_claim(&claimed, 11)
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
    assert!(repo
        .mark_prompt_started_for_claim(&claimed, 11)
        .await
        .unwrap());
    assert!(repo
        .settle_prompt_claim(
            &claimed,
            PromptCommandOutcome::Completed(r#"{"accepted":true}"#),
            12
        )
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
async fn typed_snapshot_projects_daemon_queue_truth_for_name_or_agent_id() {
    let store = store().await;
    let intents = CommandIntents::new(&store);
    intents
        .insert_pending(prompt("cmd_snapshot", 10))
        .await
        .unwrap();
    let queue = CommandQueue::new(&store);
    let active = vec![SessionId("s_otto".into())];

    let by_name = queue
        .snapshot_with_active_sessions(Some("otto"), None, &active)
        .await
        .unwrap();
    assert_eq!(by_name.target, "otto");
    assert_eq!(by_name.session_id.as_deref(), Some("s_otto"));
    assert!(by_name.turn_active);
    assert_eq!(
        by_name.steer_capability,
        nexus_contracts::SteerCapability::InterruptAndSend
    );
    assert_eq!(by_name.seq, 1);
    assert_eq!(by_name.revision, 1);
    assert_eq!(by_name.commands.len(), 1);
    assert_eq!(by_name.commands[0].command_id, "cmd_snapshot");
    assert_eq!(
        by_name.commands[0].client_message_id.as_deref(),
        Some("cm_cmd_snapshot")
    );
    assert_eq!(by_name.commands[0].state, CommandQueueState::Queued);
    assert_eq!(by_name.commands[0].text, "continue");

    let by_id = queue
        .snapshot_with_active_sessions(
            Some("stale-display-name"),
            Some(&AgentId("a_otto".into())),
            &active,
        )
        .await
        .unwrap();
    assert_eq!(by_id.target, "otto");
    assert_eq!(by_id.commands[0].command_id, "cmd_snapshot");

    let id_only = queue
        .snapshot_with_active_sessions(None, Some(&AgentId("a_otto".into())), &active)
        .await
        .unwrap();
    assert_eq!(id_only.target, "otto");
    assert_eq!(id_only.session_id.as_deref(), Some("s_otto"));
    assert_eq!(id_only.commands[0].command_id, "cmd_snapshot");
}

#[tokio::test]
async fn name_only_queue_target_rejects_ambiguous_legacy_sessions() {
    let store = store().await;
    store
        .conn
        .execute("DROP INDEX idx_sessions_name", ())
        .await
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO sessions (session_id, name, agent, kind, tier, project, created_at) \
             VALUES ('s_otto_duplicate', 'otto', 'claude', 'agent', 'agent', 'other', 2)",
            (),
        )
        .await
        .unwrap();

    let error = CommandQueue::new(&store)
        .snapshot_with_active_sessions(Some("otto"), None, &[])
        .await
        .unwrap_err();

    assert!(matches!(error, NexusError::Ambiguous(_)));
}

#[tokio::test]
async fn split_store_snapshot_reads_runtime_from_transport_and_queue_from_identity() {
    let daemon = DaemonStore::open(":memory:").await.unwrap();
    let store = daemon.compatibility_store();
    let session_id = SessionId("s_split".into());
    Sessions::new(&store)
        .create(NewSession {
            session_id: session_id.clone(),
            name: Some("split-agent".into()),
            agent: Some("codex".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some("split-client".into()),
            cwd: None,
            project: "other-metadata".into(),
            transport: Some("codex-appserver".into()),
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .set_agent_id(&session_id, "a_split")
        .await
        .unwrap();
    CommandIntents::new(&store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_split".into(),
            kind: command_kinds::harness::PROMPT.into(),
            project: "default".into(),
            caller_name: "Alex".into(),
            caller_session_id: Some("local-operator".into()),
            caller_agent_id: None,
            caller_runtime_id: Some("local-operator".into()),
            caller_client_key: None,
            caller_principal_id: None,
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: Some("cm_split".into()),
            request_json: serde_json::json!({
                "name": "split-agent",
                "agentId": "a_split",
                "text": "split truth",
                "clientMessageId": "cm_split"
            })
            .to_string(),
            created_at: 1,
        })
        .await
        .unwrap();

    let snapshot = CommandQueue::new(&store)
        .snapshot_with_active_sessions(None, Some(&AgentId("a_split".into())), &[session_id])
        .await
        .unwrap();
    assert!(snapshot.turn_active);
    assert_eq!(snapshot.session_id.as_deref(), Some("s_split"));
    assert_eq!(snapshot.commands.len(), 1);
    assert_eq!(snapshot.commands[0].command_id, "cmd_split");
    assert_eq!(snapshot.commands[0].text, "split truth");
    assert_eq!(snapshot.commands[0].state, CommandQueueState::Queued);
}

#[tokio::test]
async fn typed_transition_read_is_bounded_and_reports_cursor_gaps() {
    let store = store().await;
    let intents = CommandIntents::new(&store);
    intents
        .insert_pending(prompt("cmd_events", 1))
        .await
        .unwrap();
    let mut interleaved = prompt("cmd_other_metadata", 2);
    interleaved.project = "other-metadata".into();
    intents.insert_pending(interleaved).await.unwrap();
    let claimed = intents
        .claim_next_kind(10, 1_000, command_kinds::harness::PROMPT)
        .await
        .unwrap()
        .unwrap();
    assert!(intents
        .mark_prompt_started_for_claim(&claimed, 11)
        .await
        .unwrap());
    assert!(intents
        .settle_prompt_claim(
            &claimed,
            PromptCommandOutcome::Completed(r#"{"accepted":true}"#),
            12
        )
        .await
        .unwrap());
    let queue = CommandQueue::new(&store);

    let page = queue.events_after(1).await.unwrap();
    assert_eq!(page.next_seq, 2);
    assert_eq!(page.latest_seq, 5);
    assert!(!page.gap);
    assert_eq!(
        page.events
            .iter()
            .map(|event| event.state)
            .collect::<Vec<_>>(),
        vec![
            CommandQueueState::Queued,
            CommandQueueState::Claimed,
            CommandQueueState::Started,
            CommandQueueState::Completed,
        ]
    );

    store
        .conn
        .execute("DELETE FROM command_intent_events WHERE seq = 2", ())
        .await
        .unwrap();
    let gap = queue.events_after(1).await.unwrap();
    assert!(gap.gap);
    assert!(gap.events.is_empty());
    assert_eq!(gap.next_seq, 3);
    assert_eq!(gap.latest_seq, 5);
}

#[tokio::test]
async fn terminal_command_events_preserve_kind_and_authenticated_caller_for_every_session_action() {
    let store = store().await;
    let intents = CommandIntents::new(&store);

    for (offset, kind) in [
        command_kinds::harness::PROMPT,
        command_kinds::harness::STEER,
        command_kinds::harness::INTERRUPT,
        command_kinds::harness::COMPACT,
    ]
    .into_iter()
    .enumerate()
    {
        let command_id = format!("cmd_receipt_{offset}");
        let client_message_id = format!("cm_receipt_{offset}");
        intents
            .insert_pending(NewCommandIntent {
                command_id: command_id.clone(),
                kind: kind.into(),
                project: "presentation-only".into(),
                caller_name: "Operator".into(),
                caller_session_id: Some("s_human".into()),
                caller_agent_id: None,
                caller_runtime_id: Some("s_human".into()),
                caller_client_key: Some("nexus_ck_human".into()),
                caller_principal_id: None,
                caller_kind: Some("human".into()),
                caller_tier: Some("admin".into()),
                idempotency_key: Some(client_message_id.clone()),
                request_json: serde_json::json!({
                    "name": "otto",
                    "agentId": "a_otto",
                    "clientMessageId": client_message_id,
                    "text": "only prompt and steer require text"
                })
                .to_string(),
                created_at: 100 + offset as i64,
            })
            .await
            .unwrap();
        let claimed = intents
            .claim_next_kind(200 + offset as i64, 1_000, kind)
            .await
            .unwrap()
            .unwrap();
        intents
            .mark_done_for_claim(
                &command_id,
                claimed.claimed_at.unwrap(),
                r#"{"ok":true}"#,
                300 + offset as i64,
            )
            .await
            .unwrap();
    }

    let events = CommandQueue::new(&store)
        .events_after(0)
        .await
        .unwrap()
        .events
        .into_iter()
        .filter(|event| event.state == CommandQueueState::Completed)
        .collect::<Vec<_>>();

    assert_eq!(events.len(), 4);
    assert_eq!(
        events
            .iter()
            .map(|event| event.command_kind.as_str())
            .collect::<Vec<_>>(),
        vec![
            command_kinds::harness::PROMPT,
            command_kinds::harness::STEER,
            command_kinds::harness::INTERRUPT,
            command_kinds::harness::COMPACT,
        ]
    );
    for event in events {
        assert_eq!(event.session_id.as_deref(), Some("s_otto"));
        assert_eq!(event.caller_name, "Operator");
        assert_eq!(event.caller_session_id.as_deref(), Some("s_human"));
        assert_eq!(event.caller_agent_id, None);
        assert_eq!(event.caller_kind.as_deref(), Some("local.human"));
        assert!(event
            .client_message_id
            .as_deref()
            .unwrap()
            .starts_with("cm_receipt_"));
    }
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
