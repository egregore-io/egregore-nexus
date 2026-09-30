use nexus_store::command_kinds;
use nexus_store::repos::{
    CommandIntents, DeliveryObligations, NewCommandIntent, NewDeliveryObligation,
};
use nexus_store::{DaemonStore, Store};

async fn migrated() -> Store {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    store
}

fn pending(id: &str, created_at: i64) -> NewCommandIntent {
    NewCommandIntent {
        command_id: id.to_string(),
        kind: command_kinds::message_post::SEND.to_string(),
        project: "default".to_string(),
        caller_name: "alex".to_string(),
        caller_session_id: Some("s_human".to_string()),
        caller_agent_id: Some("a_human".to_string()),
        caller_runtime_id: Some("s_human".to_string()),
        caller_client_key: Some("ck_human".to_string()),
        caller_principal_id: None,
        caller_kind: Some("human".to_string()),
        caller_tier: Some("admin".to_string()),
        idempotency_key: None,
        request_json:
            r#"{"to":{"verb":"dm","name":"blake"},"summary":null,"body":"hi","mention":[]}"#
                .to_string(),
        created_at,
    }
}

fn pending_kind(
    id: &str,
    kind: &str,
    caller_session_id: Option<&str>,
    created_at: i64,
) -> NewCommandIntent {
    let mut row = pending(id, created_at);
    row.kind = kind.to_string();
    row.caller_session_id = caller_session_id.map(str::to_string);
    row
}

fn prompt_pending(id: &str, target: &str, created_at: i64) -> NewCommandIntent {
    let mut row = pending_kind(
        id,
        command_kinds::harness::PROMPT,
        Some("s_human"),
        created_at,
    );
    row.request_json =
        format!(r#"{{"name":"{target}","text":"hello {target}","clientMessageId":null}}"#);
    row
}

async fn insert_session(store: &Store, session_id: &str, name: &str) {
    store
        .conn
        .execute(
            "INSERT INTO sessions (session_id, name, kind, tier, project, presence, paused, \
             last_heartbeat, created_at) VALUES (?1, ?2, 'agent', 'agent', 'default', \
             'online', 0, 100, 1)",
            libsql::params![session_id, name],
        )
        .await
        .unwrap();
}

async fn insert_session_with_agent_id(store: &Store, session_id: &str, name: &str, agent_id: &str) {
    insert_session(store, session_id, name).await;
    store
        .conn
        .execute(
            "UPDATE sessions SET agent_id = ?2 WHERE session_id = ?1",
            libsql::params![session_id, agent_id],
        )
        .await
        .unwrap();
}

async fn insert_durable_agent_with_active_runtime(
    store: &Store,
    agent_id: &str,
    name: &str,
    active_session_id: &str,
) {
    store
        .identity_conn()
        .execute(
            "INSERT INTO agents (agent_id, project, name, tier, created_at) \
             VALUES (?1, 'identity-metadata', ?2, 'agent', 1)",
            libsql::params![agent_id, name],
        )
        .await
        .unwrap();
    store
        .identity_conn()
        .execute(
            "INSERT INTO agent_runtimes \
             (runtime_id, agent_id, harness, transport, presence, active, started_at) \
             VALUES (?1, ?2, 'codex', 'codex-appserver', 'online', 1, 1)",
            libsql::params![active_session_id, agent_id],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn insert_and_claim_oldest_pending_command() {
    let store = migrated().await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(pending("cmd_new", 20)).await.unwrap();
    repo.insert_pending(pending("cmd_old", 10)).await.unwrap();

    let claimed = repo.claim_next(100, 5_000).await.unwrap().unwrap();

    assert_eq!(claimed.command_id, "cmd_old");
    assert_eq!(claimed.kind, command_kinds::message_post::SEND);
    assert_eq!(claimed.caller_agent_id.as_deref(), Some("a_human"));
    assert_eq!(claimed.caller_runtime_id.as_deref(), Some("s_human"));
    assert_eq!(claimed.status, "claimed");
    assert_eq!(claimed.attempts, 1);
    assert_eq!(claimed.claimed_at, Some(100));
    assert_eq!(claimed.lease_until, Some(5_100));
}

#[tokio::test]
async fn concurrent_claimers_have_single_winner_before_lease_expiry() {
    let store = migrated().await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(pending("cmd_overlap", 10))
        .await
        .unwrap();

    let (first, second) = tokio::join!(repo.claim_next(100, 5_000), repo.claim_next(100, 5_000));
    let mut claimed = [first.unwrap(), second.unwrap()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    assert_eq!(claimed.len(), 1);
    let row = claimed.pop().unwrap();
    assert_eq!(row.command_id, "cmd_overlap");
    assert_eq!(row.status, "claimed");
    assert_eq!(row.claimed_at, Some(100));
    assert_eq!(row.lease_until, Some(5_100));
}

#[test]
fn command_kind_constants_are_the_published_ingress_contract() {
    assert_eq!(command_kinds::message_post::SEND, "message.post.send");
    assert_eq!(command_kinds::notification::NOTIFY, "notification.notify");
    assert_eq!(command_kinds::notification::SEND, "notification.send");
    assert_eq!(command_kinds::inbox::CONSUME, "inbox.consume");
    assert_eq!(command_kinds::inbox::SUBSCRIBE, "inbox.subscribe");
    assert_eq!(
        command_kinds::inbox::SUBSCRIPTION_NEXT,
        "inbox.subscription_next"
    );
    assert_eq!(
        command_kinds::inbox::SUBSCRIPTION_ACK,
        "inbox.subscription_ack"
    );
    assert_eq!(command_kinds::inbox::UNSUBSCRIBE, "inbox.unsubscribe");
    assert_eq!(command_kinds::inbox::ACK, "inbox.ack");
    assert_eq!(command_kinds::inbox::ACK_THREADS, "inbox.ack_threads");
    assert_eq!(command_kinds::identity::REGISTER, "identity.register");
    assert_eq!(command_kinds::identity::ATTACH, "identity.attach");
    assert_eq!(command_kinds::harness::PROMPT, "harness.prompt");
    assert_eq!(command_kinds::harness::STEER, "harness.steer");
    assert_eq!(command_kinds::harness::INTERRUPT, "harness.interrupt");
    assert_eq!(command_kinds::harness::COMPACT, "harness.compact");
    assert_eq!(command_kinds::harness::WARM, "harness.warm");
    assert_eq!(command_kinds::thread::CREATE, "thread.create");
    assert_eq!(command_kinds::thread::JOIN, "thread.join");
    assert_eq!(command_kinds::thread::LEAVE, "thread.leave");
    assert_eq!(command_kinds::thread::ARCHIVE, "thread.archive");
    assert_eq!(command_kinds::thread::DELETE, "thread.delete");
    assert_eq!(command_kinds::thread::RENAME, "thread.rename");
    assert_eq!(command_kinds::thread::ADD_MEMBER, "thread.add_member");
    assert_eq!(command_kinds::admin::GRANT_TIER, "admin.grant_tier");
    assert_eq!(command_kinds::admin::RENAME, "admin.rename");
    assert_eq!(command_kinds::admin::DLQ_LIST, "admin.dlq.list");
    assert_eq!(command_kinds::admin::DLQ_REQUEUE, "admin.dlq.requeue");
    assert_eq!(command_kinds::admin::DLQ_PURGE, "admin.dlq.purge");
    assert_eq!(
        command_kinds::agent::credential::CREATE,
        "agent.credential.create"
    );
}

#[tokio::test]
async fn complete_and_error_updates_terminal_state() {
    let store = migrated().await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(pending("cmd_ok", 1)).await.unwrap();
    repo.insert_pending(pending("cmd_err", 2)).await.unwrap();

    repo.mark_done("cmd_ok", r#"{"messageId":"m_1"}"#, 200)
        .await
        .unwrap();
    repo.mark_error("cmd_err", r#"{"message":"bad"}"#, 300)
        .await
        .unwrap();

    let ok = repo.get("cmd_ok").await.unwrap().unwrap();
    assert_eq!(ok.status, "done");
    assert_eq!(ok.result_json.as_deref(), Some(r#"{"messageId":"m_1"}"#));
    assert_eq!(ok.completed_at, Some(200));
    assert_eq!(ok.lease_until, None);

    let err = repo.get("cmd_err").await.unwrap().unwrap();
    assert_eq!(err.status, "error");
    assert_eq!(err.error_json.as_deref(), Some(r#"{"message":"bad"}"#));
    assert_eq!(err.completed_at, Some(300));
}

#[tokio::test]
async fn terminal_reap_deletes_only_old_done_and_error_rows() {
    let store = migrated().await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(pending("cmd_old_done", 1))
        .await
        .unwrap();
    repo.insert_pending(pending("cmd_old_error", 2))
        .await
        .unwrap();
    repo.insert_pending(pending("cmd_recent_done", 3))
        .await
        .unwrap();
    repo.insert_pending(pending("cmd_claimed_old", 4))
        .await
        .unwrap();
    repo.insert_pending(pending("cmd_pending_old", 5))
        .await
        .unwrap();
    repo.insert_pending(pending("cmd_legacy_done_null_completed", 6))
        .await
        .unwrap();

    repo.mark_done("cmd_old_done", r#"{"ok":true}"#, 100)
        .await
        .unwrap();
    repo.mark_error("cmd_old_error", r#"{"message":"bad"}"#, 101)
        .await
        .unwrap();
    repo.mark_done("cmd_recent_done", r#"{"ok":true}"#, 500)
        .await
        .unwrap();
    repo.mark_done("cmd_legacy_done_null_completed", r#"{"legacy":true}"#, 102)
        .await
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE command_intents SET completed_at = NULL \
             WHERE command_id = 'cmd_legacy_done_null_completed'",
            (),
        )
        .await
        .unwrap();
    let claimed = repo
        .claim_next_kind(120, 1_000, command_kinds::message_post::SEND)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.command_id, "cmd_claimed_old");

    assert_eq!(repo.reap_terminal_older_than(200).await.unwrap(), 2);

    assert!(repo.get("cmd_old_done").await.unwrap().is_none());
    assert!(repo.get("cmd_old_error").await.unwrap().is_none());
    assert_eq!(
        repo.get("cmd_recent_done").await.unwrap().unwrap().status,
        "done"
    );
    assert_eq!(
        repo.get("cmd_pending_old").await.unwrap().unwrap().status,
        "pending"
    );
    assert_eq!(
        repo.get("cmd_claimed_old").await.unwrap().unwrap().status,
        "claimed"
    );
    assert_eq!(
        repo.get("cmd_legacy_done_null_completed")
            .await
            .unwrap()
            .unwrap()
            .status,
        "done"
    );
    assert_eq!(repo.reap_terminal_older_than(200).await.unwrap(), 0);
}

#[tokio::test]
async fn migration_installs_command_intent_reap_and_json_claim_indexes() {
    let store = migrated().await;
    let mut rows = store
        .conn
        .query("PRAGMA index_list(command_intents)", ())
        .await
        .unwrap();
    let mut names = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }

    assert!(names
        .iter()
        .any(|name| name == "idx_command_intents_terminal_completed"));
    assert!(names
        .iter()
        .any(|name| name == "idx_command_intents_harness_claim_agent"));
    assert!(names
        .iter()
        .any(|name| name == "idx_command_intents_harness_claim_name"));
}

#[tokio::test]
async fn idempotent_insert_returns_existing_command_for_same_key() {
    let store = migrated().await;
    let repo = CommandIntents::new(&store);
    let mut first = pending("cmd_first", 1);
    first.idempotency_key = Some("retry-1".into());
    assert_eq!(
        repo.insert_pending_idempotent(first).await.unwrap(),
        "cmd_first"
    );

    let mut retry = pending("cmd_retry", 2);
    retry.idempotency_key = Some("retry-1".into());
    assert_eq!(
        repo.insert_pending_idempotent(retry).await.unwrap(),
        "cmd_first"
    );

    let mut rows = store
        .conn
        .query("SELECT COUNT(*) FROM command_intents", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
}

#[tokio::test]
async fn idempotent_insert_dedupes_local_operator_without_client_key() {
    let store = migrated().await;
    let repo = CommandIntents::new(&store);
    let mut first = pending("cmd_first", 1);
    first.caller_name = "operator".to_string();
    first.caller_session_id = None;
    first.caller_agent_id = None;
    first.caller_runtime_id = None;
    first.caller_client_key = None;
    first.idempotency_key = Some("web:post:backend:retry-1".into());
    assert_eq!(
        repo.insert_pending_idempotent(first).await.unwrap(),
        "cmd_first"
    );

    let mut retry = pending("cmd_retry", 2);
    retry.caller_name = "operator".to_string();
    retry.caller_session_id = None;
    retry.caller_agent_id = None;
    retry.caller_runtime_id = None;
    retry.caller_client_key = None;
    retry.idempotency_key = Some("web:post:backend:retry-1".into());
    assert_eq!(
        repo.insert_pending_idempotent(retry).await.unwrap(),
        "cmd_first"
    );

    let mut rows = store
        .conn
        .query(
            "SELECT command_id, caller_client_key FROM command_intents",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), "cmd_first");
    assert!(row.get::<Option<String>>(1).unwrap().is_none());
    assert!(rows.next().await.unwrap().is_none());
}

#[tokio::test]
async fn expired_claim_can_be_reclaimed() {
    let store = migrated().await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(pending("cmd_retry", 1)).await.unwrap();

    let first = repo.claim_next(100, 10).await.unwrap().unwrap();
    assert_eq!(first.lease_until, Some(110));

    assert!(repo.claim_next(109, 10).await.unwrap().is_none());

    let second = repo.claim_next(111, 10).await.unwrap().unwrap();
    assert_eq!(second.command_id, "cmd_retry");
    assert_eq!(second.attempts, 2);
    assert_eq!(second.claimed_at, Some(111));
    assert_eq!(second.lease_until, Some(121));
}

fn auto_prompt_pending(id: &str) -> NewCommandIntent {
    let mut row = prompt_pending(id, "recipient", 1);
    row.request_json = serde_json::json!({
        "agentId": "a_recipient", "name": "recipient", "text": "hello",
        "clientMessageId": id, "expectedSessionId": "s_recipient", "delivery": "auto"
    })
    .to_string();
    row
}

#[tokio::test]
async fn persisted_started_steer_survives_restart_without_replay() {
    let path = std::env::temp_dir().join(format!(
        "steer-restart-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    {
        let daemon = DaemonStore::open(path.to_str().unwrap()).await.unwrap();
        let store = daemon.compatibility_store();
        let repo = CommandIntents::new(&store);
        let mut input = prompt_pending("started-steer", "recipient", 1);
        input.kind = command_kinds::harness::STEER.into();
        repo.insert_pending(input).await.unwrap();
        let claim = repo.claim_next(100, 10).await.unwrap().unwrap();
        assert_eq!(claim.attempts, 1);
        // Reproduce an already-persisted started row from the existing steer worker.
        // The native effect may have happened before the daemon stopped.
        store
            .identity_conn()
            .execute(
                "UPDATE command_intents SET started_at = 101 WHERE command_id = 'started-steer'",
                (),
            )
            .await
            .unwrap();
    }
    let daemon = DaemonStore::open(path.to_str().unwrap()).await.unwrap();
    let store = daemon.compatibility_store();
    let repo = CommandIntents::new(&store);
    let replay = repo
        .claim_next_kind(111, 10, command_kinds::harness::STEER)
        .await
        .unwrap();
    assert!(
        replay.is_none(),
        "a possibly admitted steer must not be claimed again after restart: {replay:?}"
    );
    assert_eq!(
        repo.get("started-steer").await.unwrap().unwrap().attempts,
        1
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn ordinary_prompt_attempt_survives_split_restart_without_replay() {
    let path = std::env::temp_dir().join(format!(
        "ordinary-restart-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    {
        let daemon = DaemonStore::open(path.to_str().unwrap()).await.unwrap();
        let store = daemon.compatibility_store();
        let repo = CommandIntents::new(&store);
        let mut input = prompt_pending("ordinary", "recipient", 1);
        input.idempotency_key = Some("original-client-key".into());
        repo.insert_pending(input).await.unwrap();
        let claim = repo.claim_next(100, 10).await.unwrap().unwrap();
        assert!(
            repo.mark_prompt_started_for_claim(&claim, 101)
                .await
                .unwrap(),
            "ordinary sends need the durable attempt fence too"
        );
    }
    let daemon = DaemonStore::open(path.to_str().unwrap()).await.unwrap();
    let store = daemon.compatibility_store();
    let repo = CommandIntents::new(&store);
    assert!(repo.claim_next(111, 10).await.unwrap().is_none());
    assert!(repo
        .claim_next_kind(111, 10, command_kinds::harness::PROMPT)
        .await
        .unwrap()
        .is_none());
    assert!(repo
        .claim_next_ready_harness_prompt(111, 10, &[])
        .await
        .unwrap()
        .is_none());
    assert!(repo
        .claim_next_ready_harness_command(111, 10, command_kinds::harness::PROMPT)
        .await
        .unwrap()
        .is_none());
    assert!(repo
        .claim_next_ready_harness_prompt_for_session(111, 10, "s_recipient")
        .await
        .unwrap()
        .is_none());
    assert_eq!(repo.expire_started_prompt_claims(111).await.unwrap(), 1);
    let before = repo.get("ordinary").await.unwrap().unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(before.error_json.as_deref().unwrap()).unwrap()
            ["code"],
        -32011
    );
    repo.mark_done("ordinary", "{}", 112).await.unwrap();
    repo.mark_error("ordinary", "{}", 112).await.unwrap();
    assert_eq!(repo.reap_terminal_older_than(1000).await.unwrap(), 0);
    assert_eq!(repo.get("ordinary").await.unwrap().unwrap(), before);
    let mut retry = prompt_pending("replacement-id", "recipient", 1);
    retry.idempotency_key = Some("original-client-key".into());
    assert_eq!(
        repo.insert_pending_or_resume(retry).await.unwrap(),
        "ordinary"
    );
    assert!(repo.claim_next(1001, 10).await.unwrap().is_none());
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn ordinary_prompt_shutdown_and_generic_setters_cannot_clear_attempt() {
    for kind in [
        command_kinds::harness::PROMPT,
        command_kinds::harness::STEER,
    ] {
        let store = migrated().await;
        let repo = CommandIntents::new(&store);
        let mut input = prompt_pending("ordinary", "recipient", 1);
        input.kind = kind.into();
        repo.insert_pending(input).await.unwrap();
        let claim = repo.claim_next(100, 10).await.unwrap().unwrap();
        assert!(repo
            .mark_prompt_started_for_claim(&claim, 101)
            .await
            .unwrap());
        let before = repo.get("ordinary").await.unwrap().unwrap();
        assert!(!repo
            .release_claim_for_shutdown_retry("ordinary", 100)
            .await
            .unwrap());
        assert!(!repo
            .mark_done_for_claim("ordinary", 100, "{}", 102)
            .await
            .unwrap());
        assert!(!repo
            .mark_error_for_claim("ordinary", 100, "{}", 102)
            .await
            .unwrap());
        repo.mark_done("ordinary", "{}", 102).await.unwrap();
        repo.mark_error("ordinary", "{}", 102).await.unwrap();
        assert_eq!(repo.get("ordinary").await.unwrap().unwrap(), before);
        repo.reap_claimed_for_shutdown(103).await.unwrap();
        let after = repo.get("ordinary").await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(after.error_json.as_deref().unwrap())
                .unwrap()["code"],
            -32011
        );
        assert!(!repo
            .settle_prompt_claim(
                &claim,
                nexus_store::repos::PromptCommandOutcome::Completed("{}"),
                104
            )
            .await
            .unwrap());
        assert_eq!(repo.reap_terminal_older_than(200).await.unwrap(), 0);
    }
}

#[tokio::test]
async fn prompt_attempt_cas_rejects_changed_lease_even_with_same_timestamp_and_attempt() {
    use nexus_store::repos::PromptCommandOutcome;
    for mode in ["auto", "prompt", "steer"] {
        let store = migrated().await;
        let repo = CommandIntents::new(&store);
        let mut input = if mode == "auto" {
            auto_prompt_pending("cas")
        } else {
            prompt_pending("cas", "recipient", 1)
        };
        if mode == "steer" {
            input.kind = command_kinds::harness::STEER.into();
        }
        repo.insert_pending(input).await.unwrap();
        let claim = repo.claim_next(100, 20).await.unwrap().unwrap();
        assert!(
            !repo.mark_started_for_claim("cas", 100, 101).await.unwrap(),
            "arming requires the complete captured claim, not a timestamp-only helper"
        );
        let mut wrong = claim.clone();
        wrong.lease_until = Some(121);
        assert!(
            !repo
                .mark_prompt_started_for_claim(&wrong, 101)
                .await
                .unwrap(),
            "captured lease is part of exact claim identity"
        );
        assert!(repo
            .mark_prompt_started_for_claim(&claim, 101)
            .await
            .unwrap());
        assert!(!repo
            .settle_prompt_claim(&wrong, PromptCommandOutcome::Completed("{}"), 102)
            .await
            .unwrap());
        assert!(!repo
            .defer_prompt_claim_known_not_accepted(&wrong, 102)
            .await
            .unwrap());
        assert!(!repo
            .defer_prompt_claim_known_not_accepted(&claim, 120)
            .await
            .unwrap());
        assert!(
            !repo
                .settle_prompt_claim(&claim, PromptCommandOutcome::Completed("{}"), 120)
                .await
                .unwrap(),
            "expired owners cannot settle"
        );
        assert!(repo
            .settle_prompt_claim(&claim, PromptCommandOutcome::Completed("{}"), 103)
            .await
            .unwrap());
        assert!(!repo
            .settle_prompt_claim(&claim, PromptCommandOutcome::Uncertain, 104)
            .await
            .unwrap());
        assert_eq!(repo.reap_terminal_older_than(200).await.unwrap(), 1);
    }
}

#[tokio::test]
async fn ordinary_same_timestamp_reclaim_rejects_old_attempt_and_known_results_expire() {
    use nexus_store::repos::PromptCommandOutcome;
    for kind in [
        command_kinds::harness::PROMPT,
        command_kinds::harness::STEER,
    ] {
        for rejected in [false, true] {
            let store = migrated().await;
            let repo = CommandIntents::new(&store);
            let mut input = prompt_pending("ordinary", "recipient", 1);
            input.kind = kind.into();
            repo.insert_pending(input).await.unwrap();
            let first = repo.claim_next(100, 20).await.unwrap().unwrap();
            assert!(repo
                .mark_prompt_started_for_claim(&first, 101)
                .await
                .unwrap());
            assert!(repo
                .defer_prompt_claim_known_not_accepted(&first, 102)
                .await
                .unwrap());
            // Deliberately identical timestamp and lease: attempts must still distinguish owners.
            let second = repo.claim_next(100, 20).await.unwrap().unwrap();
            assert_eq!(first.claimed_at, second.claimed_at);
            assert_eq!(first.lease_until, second.lease_until);
            assert_ne!(first.attempts, second.attempts);
            assert!(!repo
                .mark_prompt_started_for_claim(&first, 103)
                .await
                .unwrap());
            assert!(repo
                .mark_prompt_started_for_claim(&second, 103)
                .await
                .unwrap());
            assert!(!repo
                .defer_prompt_claim_known_not_accepted(&first, 104)
                .await
                .unwrap());
            assert!(!repo
                .settle_prompt_claim(&first, PromptCommandOutcome::Completed("{}"), 104)
                .await
                .unwrap());
            let outcome = if rejected {
                PromptCommandOutcome::Rejected(
                    r#"{"code":-32602,"message":"invalid exact target"}"#,
                )
            } else {
                PromptCommandOutcome::Completed("{}")
            };
            assert!(repo
                .settle_prompt_claim(&second, outcome, 104)
                .await
                .unwrap());
            assert!(!repo
                .settle_prompt_claim(&second, PromptCommandOutcome::Uncertain, 105)
                .await
                .unwrap());
            assert_eq!(repo.reap_terminal_older_than(200).await.unwrap(), 1);
        }
    }
}

async fn arm_auto(repo: &CommandIntents<'_>, id: &str, now: i64) -> bool {
    let claim = repo.get(id).await.unwrap().unwrap();
    repo.mark_prompt_started_for_claim(&claim, now)
        .await
        .unwrap()
}

#[tokio::test]
async fn started_auto_prompt_is_not_reclaimed_after_lease_expiry_by_any_claim_path() {
    let store = migrated().await;
    insert_session_with_agent_id(&store, "s_recipient", "recipient", "a_recipient").await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(auto_prompt_pending("cmd_auto"))
        .await
        .unwrap();
    let claim = repo.claim_next(100, 10).await.unwrap().unwrap();
    assert!(repo
        .mark_prompt_started_for_claim(&claim, 101)
        .await
        .unwrap());

    assert!(
        repo.claim_next(111, 10).await.unwrap().is_none(),
        "lease expiry is not evidence that native delivery had no effect"
    );
    assert!(repo
        .claim_next_kind(111, 10, command_kinds::harness::PROMPT)
        .await
        .unwrap()
        .is_none());
    assert!(repo
        .claim_next_ready_harness_prompt(111, 10, &[])
        .await
        .unwrap()
        .is_none());
    assert!(repo
        .claim_next_ready_harness_prompt_for_session(111, 10, "s_recipient")
        .await
        .unwrap()
        .is_none());
    assert!(repo
        .claim_next_ready_harness_command(111, 10, command_kinds::harness::PROMPT)
        .await
        .unwrap()
        .is_none());
    let unchanged = repo.get("cmd_auto").await.unwrap().unwrap();
    assert_eq!(unchanged.attempts, 1);
    assert_eq!(unchanged.started_at, Some(101));
}

#[tokio::test]
async fn auto_prompt_uncertainty_survives_a_split_daemon_store_restart() {
    let path = std::env::temp_dir().join(format!(
        "nexus-auto-restart-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    {
        let daemon = DaemonStore::open(path.to_str().unwrap()).await.unwrap();
        let store = daemon.compatibility_store();
        let repo = CommandIntents::new(&store);
        repo.insert_pending(auto_prompt_pending("cmd_restart_auto"))
            .await
            .unwrap();
        repo.claim_next(100, 10).await.unwrap().unwrap();
        assert!(arm_auto(&repo, "cmd_restart_auto", 101).await);
        // Simulate process death after admission was armed but before its result was recorded.
    }
    {
        let daemon = DaemonStore::open(path.to_str().unwrap()).await.unwrap();
        let store = daemon.compatibility_store();
        let repo = CommandIntents::new(&store);
        assert!(
            repo.claim_next(1_000, 10).await.unwrap().is_none(),
            "restart must not redeliver a potentially accepted automatic send"
        );
        assert_eq!(
            repo.get("cmd_restart_auto")
                .await
                .unwrap()
                .unwrap()
                .attempts,
            1
        );
    }
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn auto_prompt_without_a_started_boundary_remains_reclaimable() {
    let store = migrated().await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(auto_prompt_pending("cmd_unstarted_auto"))
        .await
        .unwrap();
    repo.claim_next(100, 10).await.unwrap().unwrap();
    let reclaimed = repo.claim_next(111, 10).await.unwrap().unwrap();
    assert_eq!(reclaimed.command_id, "cmd_unstarted_auto");
    assert_eq!(reclaimed.attempts, 2);
}

#[tokio::test]
async fn shutdown_retry_does_not_clear_an_armed_auto_prompt() {
    let store = migrated().await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(auto_prompt_pending("cmd_auto_shutdown"))
        .await
        .unwrap();
    repo.claim_next(100, 10).await.unwrap().unwrap();
    assert!(arm_auto(&repo, "cmd_auto_shutdown", 101).await);
    assert!(
        !repo
            .release_claim_for_shutdown_retry("cmd_auto_shutdown", 100)
            .await
            .unwrap(),
        "generic shutdown retry has no native evidence permitting an armed auto send to replay"
    );
    assert!(repo.claim_next(200, 10).await.unwrap().is_none());
}

#[tokio::test]
async fn expired_auto_owner_cannot_arm_admission() {
    let store = migrated().await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(auto_prompt_pending("cmd_expired_auto"))
        .await
        .unwrap();
    repo.claim_next(100, 10).await.unwrap().unwrap();
    assert!(
        !arm_auto(&repo, "cmd_expired_auto", 110).await,
        "an owner whose lease already expired cannot cross native admission"
    );
    assert_eq!(repo.claim_next(111, 10).await.unwrap().unwrap().attempts, 2);
}

#[tokio::test]
async fn armed_auto_prompt_blocks_a_later_prompt_in_its_lane_after_lease_expiry() {
    let store = migrated().await;
    insert_session_with_agent_id(&store, "s_recipient", "recipient", "a_recipient").await;
    insert_session(&store, "s_other", "other").await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(auto_prompt_pending("cmd_auto_first"))
        .await
        .unwrap();
    repo.claim_next(100, 10).await.unwrap().unwrap();
    assert!(arm_auto(&repo, "cmd_auto_first", 101).await);
    repo.insert_pending(prompt_pending("cmd_same_lane", "recipient", 2))
        .await
        .unwrap();
    repo.insert_pending(prompt_pending("cmd_other_lane", "other", 3))
        .await
        .unwrap();
    let other = repo
        .claim_next_ready_harness_prompt(111, 10, &[])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        other.command_id, "cmd_other_lane",
        "an expired native attempt is still unresolved; unrelated lanes must keep progressing"
    );
}

#[tokio::test]
async fn shutdown_reports_armed_auto_delivery_as_uncertain_and_retains_its_replay_fence() {
    let store = migrated().await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(auto_prompt_pending("cmd_uncertain_shutdown"))
        .await
        .unwrap();
    repo.claim_next(100, 10).await.unwrap().unwrap();
    assert!(arm_auto(&repo, "cmd_uncertain_shutdown", 101).await);
    assert_eq!(repo.reap_claimed_for_shutdown(111).await.unwrap(), 1);
    let row = repo.get("cmd_uncertain_shutdown").await.unwrap().unwrap();
    let error: serde_json::Value =
        serde_json::from_str(row.error_json.as_deref().unwrap()).unwrap();
    assert_eq!(
        error["code"], -32011,
        "uncertainty must not look like a known rejected send"
    );
    assert!(
        !repo
            .mark_done_for_claim("cmd_uncertain_shutdown", 100, "{}", 112)
            .await
            .unwrap(),
        "late native callbacks cannot replace an uncertainty terminal"
    );
    repo.reap_terminal_older_than(1_000).await.unwrap();
    assert!(
        repo.get("cmd_uncertain_shutdown").await.unwrap().is_some(),
        "unresolved uncertainty is an outstanding delivery obligation, not expirable history"
    );
    assert!(repo.claim_next(1_001, 10).await.unwrap().is_none());
}

#[tokio::test]
async fn auto_admission_requires_the_current_attempt_and_has_one_winner() {
    let store = migrated().await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(auto_prompt_pending("cmd_auto_cas"))
        .await
        .unwrap();
    let old = repo.claim_next(100, 10).await.unwrap().unwrap();
    let current = repo.claim_next(111, 10).await.unwrap().unwrap();
    let mut forged = old.clone();
    forged.claimed_at = current.claimed_at;
    forged.lease_until = current.lease_until;
    assert!(
        !repo
            .mark_prompt_started_for_claim(&forged, 112)
            .await
            .unwrap(),
        "the claim timestamp alone must not authorize an older attempt"
    );
    let (left, right) = tokio::join!(
        repo.mark_prompt_started_for_claim(&current, 112),
        repo.mark_prompt_started_for_claim(&current, 112)
    );
    assert_ne!(
        left.unwrap(),
        right.unwrap(),
        "only one worker may arm native admission"
    );
}

#[tokio::test]
async fn auto_only_current_known_nonacceptance_may_defer_and_old_callbacks_cannot_settle() {
    use nexus_store::repos::PromptCommandOutcome;
    let store = migrated().await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(auto_prompt_pending("cmd_auto_deferred"))
        .await
        .unwrap();
    let first = repo.claim_next(100, 10).await.unwrap().unwrap();
    assert!(repo
        .mark_prompt_started_for_claim(&first, 101)
        .await
        .unwrap());
    assert!(repo
        .defer_prompt_claim_known_not_accepted(&first, 101)
        .await
        .unwrap());
    let second = repo.claim_next(102, 10).await.unwrap().unwrap();
    assert!(repo
        .mark_prompt_started_for_claim(&second, 103)
        .await
        .unwrap());
    assert!(!repo
        .defer_prompt_claim_known_not_accepted(&first, 101)
        .await
        .unwrap());
    assert!(!repo
        .settle_prompt_claim(&first, PromptCommandOutcome::Completed("{}"), 104)
        .await
        .unwrap());
    assert_eq!(repo.expire_started_prompt_claims(112).await.unwrap(), 1);
    assert!(
        !repo
            .settle_prompt_claim(&second, PromptCommandOutcome::Completed("{}"), 113)
            .await
            .unwrap(),
        "an old detached task cannot overwrite the unknown terminal with a late response"
    );
    assert!(!repo
        .defer_prompt_claim_known_not_accepted(&second, 113)
        .await
        .unwrap());
    let row = repo.get("cmd_auto_deferred").await.unwrap().unwrap();
    assert_eq!(row.status, "error");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(row.error_json.as_deref().unwrap()).unwrap()
            ["code"],
        -32011
    );
    assert!(repo.claim_next(200, 10).await.unwrap().is_none());
    let resumed = repo
        .insert_pending_or_resume(auto_prompt_pending("cmd_auto_deferred"))
        .await
        .unwrap();
    assert_eq!(resumed, "cmd_auto_deferred");
    assert!(repo.claim_next(201, 10).await.unwrap().is_none());
}

#[tokio::test]
async fn auto_known_outcomes_settle_once_and_keep_normal_retention() {
    use nexus_store::repos::PromptCommandOutcome;
    for outcome in [
        PromptCommandOutcome::Completed(r#"{"delivered":true}"#),
        PromptCommandOutcome::Rejected(r#"{"code":-32602,"message":"wrong session"}"#),
    ] {
        let store = migrated().await;
        let repo = CommandIntents::new(&store);
        repo.insert_pending(auto_prompt_pending("cmd_auto_settle"))
            .await
            .unwrap();
        let claim = repo.claim_next(100, 10).await.unwrap().unwrap();
        assert!(repo
            .mark_prompt_started_for_claim(&claim, 101)
            .await
            .unwrap());
        assert!(repo
            .settle_prompt_claim(&claim, outcome, 102)
            .await
            .unwrap());
        assert!(!repo
            .settle_prompt_claim(&claim, PromptCommandOutcome::Uncertain, 103)
            .await
            .unwrap());
        assert_eq!(repo.reap_terminal_older_than(200).await.unwrap(), 1);
    }
}

#[tokio::test]
async fn auto_fence_cannot_be_bypassed_by_legacy_settlement_helpers() {
    let store = migrated().await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(auto_prompt_pending("cmd_auto_legacy"))
        .await
        .unwrap();
    let claim = repo.claim_next(100, 10).await.unwrap().unwrap();
    assert!(
        !repo
            .mark_started_for_claim("cmd_auto_legacy", 100, 101)
            .await
            .unwrap(),
        "automatic admission requires attempt-scoped ownership, not the legacy timestamp helper"
    );
    assert!(repo
        .mark_prompt_started_for_claim(&claim, 101)
        .await
        .unwrap());
    assert!(!repo
        .mark_done_for_claim("cmd_auto_legacy", 100, "{}", 102)
        .await
        .unwrap());
    assert!(!repo
        .mark_error_for_claim("cmd_auto_legacy", 100, "{}", 102)
        .await
        .unwrap());
    assert_eq!(repo.expire_started_prompt_claims(111).await.unwrap(), 1);
    let before = repo.get("cmd_auto_legacy").await.unwrap().unwrap();
    repo.mark_done("cmd_auto_legacy", "{}", 112).await.unwrap();
    repo.mark_error("cmd_auto_legacy", "{}", 113).await.unwrap();
    assert_eq!(
        repo.get("cmd_auto_legacy").await.unwrap().unwrap(),
        before,
        "unscoped legacy writes must not overwrite the auto uncertainty terminal"
    );
}

#[tokio::test]
async fn auto_guards_do_not_poison_legacy_claims_with_malformed_payloads() {
    let store = migrated().await;
    let repo = CommandIntents::new(&store);
    // Non-session commands can carry malformed payloads until their own dispatch validation.
    // Session prompts already have JSON-expression indexes that reject malformed insertion.
    let mut invalid = pending("cmd_bad_payload", 1);
    invalid.request_json = "{invalid json".into();
    repo.insert_pending(invalid).await.unwrap();
    let row = repo.claim_next(100, 10).await.unwrap().unwrap();
    assert_eq!(row.command_id, "cmd_bad_payload");
    assert!(repo
        .mark_started_for_claim(&row.command_id, 100, 101)
        .await
        .unwrap());
    repo.mark_error(&row.command_id, "legacy unstructured error", 102)
        .await
        .unwrap();
    assert_eq!(repo.reap_terminal_older_than(200).await.unwrap(), 1);
}

#[tokio::test]
async fn lane_depths_and_shutdown_reap_claimed_rows_only() {
    let store = migrated().await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(pending_kind(
        "cmd_send_claimed",
        command_kinds::message_post::SEND,
        Some("s_human"),
        1,
    ))
    .await
    .unwrap();
    repo.insert_pending(pending_kind(
        "cmd_notify_pending",
        command_kinds::notification::NOTIFY,
        Some("s_human"),
        2,
    ))
    .await
    .unwrap();

    let claimed = repo.claim_next(100, 1_000).await.unwrap().unwrap();
    assert_eq!(claimed.command_id, "cmd_send_claimed");

    let mut depths = repo.lane_depths().await.unwrap();
    depths.sort_by(|left, right| left.kind.cmp(&right.kind));
    let send_depth = depths
        .iter()
        .find(|depth| depth.kind == command_kinds::message_post::SEND)
        .unwrap();
    assert_eq!(send_depth.pending, 0);
    assert_eq!(send_depth.claimed, 1);
    let notify_depth = depths
        .iter()
        .find(|depth| depth.kind == command_kinds::notification::NOTIFY)
        .unwrap();
    assert_eq!(notify_depth.pending, 1);
    assert_eq!(notify_depth.claimed, 0);
    assert_eq!(repo.expired_claimed_count(1_099).await.unwrap(), 0);
    assert_eq!(repo.expired_claimed_count(1_100).await.unwrap(), 1);

    assert_eq!(repo.reap_claimed_for_shutdown(2_000).await.unwrap(), 1);
    let reaped = repo.get("cmd_send_claimed").await.unwrap().unwrap();
    assert_eq!(reaped.status, "error");
    assert_eq!(reaped.completed_at, Some(2_000));
    assert_eq!(reaped.lease_until, None);
    assert!(reaped
        .error_json
        .as_deref()
        .unwrap()
        .contains("daemon shutdown"));

    let pending = repo.get("cmd_notify_pending").await.unwrap().unwrap();
    assert_eq!(pending.status, "pending");
    assert_eq!(pending.completed_at, None);
}

#[tokio::test]
async fn stale_claim_cannot_complete_after_reclaim() {
    let store = migrated().await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(pending("cmd_retry", 1)).await.unwrap();

    let first = repo.claim_next(100, 10).await.unwrap().unwrap();
    assert_eq!(first.claimed_at, Some(100));
    let second = repo.claim_next(111, 10).await.unwrap().unwrap();
    assert_eq!(second.claimed_at, Some(111));

    assert!(!repo
        .mark_done_for_claim(
            "cmd_retry",
            first.claimed_at.unwrap(),
            r#"{"stale":true}"#,
            112
        )
        .await
        .unwrap());
    let still_claimed = repo.get("cmd_retry").await.unwrap().unwrap();
    assert_eq!(still_claimed.status, "claimed");
    assert_eq!(still_claimed.claimed_at, Some(111));
    assert_eq!(still_claimed.result_json, None);

    assert!(repo
        .mark_done_for_claim(
            "cmd_retry",
            second.claimed_at.unwrap(),
            r#"{"current":true}"#,
            113,
        )
        .await
        .unwrap());
    let done = repo.get("cmd_retry").await.unwrap().unwrap();
    assert_eq!(done.status, "done");
    assert_eq!(done.result_json.as_deref(), Some(r#"{"current":true}"#));
}

#[tokio::test]
async fn shutdown_retry_releases_only_the_current_unfinished_claim() {
    let store = migrated().await;
    let repo = CommandIntents::new(&store);
    let mut prompt = pending("cmd_shutdown_retry", 1);
    prompt.kind = command_kinds::harness::PROMPT.into();
    repo.insert_pending(prompt).await.unwrap();

    let claimed = repo.claim_next(100, 1_000).await.unwrap().unwrap();
    let claimed_at = claimed.claimed_at.expect("claim timestamp");
    assert!(repo
        .mark_prompt_started_for_claim(&claimed, 101)
        .await
        .unwrap());
    assert!(repo
        .defer_prompt_claim_known_not_accepted(&claimed, 102)
        .await
        .unwrap());

    let released = repo.get("cmd_shutdown_retry").await.unwrap().unwrap();
    assert_eq!(released.status, "pending");
    assert_eq!(released.claimed_at, None);
    assert_eq!(released.started_at, None);
    assert_eq!(released.lease_until, None);
    assert_eq!(released.attempts, 1);

    let reclaimed = repo.claim_next(200, 1_000).await.unwrap().unwrap();
    assert_eq!(reclaimed.claimed_at, Some(200));
    assert!(!repo
        .release_claim_for_shutdown_retry("cmd_shutdown_retry", claimed_at)
        .await
        .unwrap());
    let still_reclaimed = repo.get("cmd_shutdown_retry").await.unwrap().unwrap();
    assert_eq!(still_reclaimed.status, "claimed");
    assert_eq!(still_reclaimed.claimed_at, Some(200));
    assert_eq!(still_reclaimed.attempts, 2);
}

#[tokio::test]
async fn stale_heartbeat_inbox_consume_claims_are_expired() {
    let store = migrated().await;
    store
        .conn
        .execute(
            "INSERT INTO sessions (session_id, name, kind, tier, project, presence, paused, \
             last_heartbeat, created_at) VALUES ('s_dead', 'dead-agent', 'agent', 'agent', \
             'default', 'online', 0, 100, 1)",
            (),
        )
        .await
        .unwrap();
    let repo = CommandIntents::new(&store);
    repo.insert_pending(pending_kind(
        "cmd_dead_consume",
        command_kinds::inbox::CONSUME,
        Some("s_dead"),
        1,
    ))
    .await
    .unwrap();
    repo.claim_next_kind(120, 10, command_kinds::inbox::CONSUME)
        .await
        .unwrap()
        .unwrap();

    let expired = repo.expire_stale_inbox_consumes(250, 50).await.unwrap();
    assert_eq!(expired, 1);

    let row = repo.get("cmd_dead_consume").await.unwrap().unwrap();
    assert_eq!(row.status, "error");
    assert!(row
        .error_json
        .as_deref()
        .unwrap()
        .contains("stale inbox consumer"));
    assert_eq!(row.completed_at, Some(250));
    assert_eq!(row.lease_until, None);
}

#[tokio::test]
async fn missing_session_inbox_consume_claims_are_expired() {
    let store = migrated().await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(pending_kind(
        "cmd_missing_consume",
        command_kinds::inbox::CONSUME,
        Some("s_missing"),
        1,
    ))
    .await
    .unwrap();
    repo.claim_next_kind(120, 10, command_kinds::inbox::CONSUME)
        .await
        .unwrap()
        .unwrap();

    let expired = repo.expire_stale_inbox_consumes(250, 50).await.unwrap();
    assert_eq!(expired, 1);

    let row = repo.get("cmd_missing_consume").await.unwrap().unwrap();
    assert_eq!(row.status, "error");
    assert!(row
        .error_json
        .as_deref()
        .unwrap()
        .contains("stale inbox consumer"));
    assert_eq!(row.completed_at, Some(250));
    assert_eq!(row.lease_until, None);
}

#[tokio::test]
async fn null_session_inbox_consume_claims_are_expired() {
    let store = migrated().await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(pending_kind(
        "cmd_null_consume",
        command_kinds::inbox::CONSUME,
        None,
        1,
    ))
    .await
    .unwrap();
    repo.claim_next_kind(120, 10, command_kinds::inbox::CONSUME)
        .await
        .unwrap()
        .unwrap();

    let expired = repo.expire_stale_inbox_consumes(250, 50).await.unwrap();
    assert_eq!(expired, 1);

    let row = repo.get("cmd_null_consume").await.unwrap().unwrap();
    assert_eq!(row.status, "error");
    assert!(row
        .error_json
        .as_deref()
        .unwrap()
        .contains("stale inbox consumer"));
    assert_eq!(row.completed_at, Some(250));
    assert_eq!(row.lease_until, None);
}

#[tokio::test]
async fn stale_client_key_inbox_consume_without_session_id_is_expired() {
    let store = migrated().await;
    store
        .conn
        .execute(
            "INSERT INTO sessions (session_id, name, kind, tier, project, presence, paused, \
             client_key, last_heartbeat, created_at) VALUES ('s_dead_keyed', 'dead-keyed', \
             'agent', 'agent', 'default', 'online', 0, 'ck_human', 100, 1)",
            (),
        )
        .await
        .unwrap();
    let repo = CommandIntents::new(&store);
    repo.insert_pending(pending_kind(
        "cmd_dead_keyed_consume",
        command_kinds::inbox::CONSUME,
        None,
        1,
    ))
    .await
    .unwrap();
    repo.claim_next_kind(120, 10, command_kinds::inbox::CONSUME)
        .await
        .unwrap()
        .unwrap();

    let expired = repo.expire_stale_inbox_consumes(250, 50).await.unwrap();
    assert_eq!(expired, 1);

    let row = repo.get("cmd_dead_keyed_consume").await.unwrap().unwrap();
    assert_eq!(row.status, "error");
    assert!(row
        .error_json
        .as_deref()
        .unwrap()
        .contains("stale inbox consumer"));
    assert_eq!(row.completed_at, Some(250));
    assert_eq!(row.lease_until, None);
}

#[tokio::test]
async fn fresh_client_key_inbox_consume_without_session_id_is_not_expired() {
    let store = migrated().await;
    store
        .conn
        .execute(
            "INSERT INTO sessions (session_id, name, kind, tier, project, presence, paused, \
             client_key, last_heartbeat, created_at) VALUES ('s_live_keyed', 'live-keyed', \
             'agent', 'agent', 'default', 'online', 0, 'ck_human', 240, 1)",
            (),
        )
        .await
        .unwrap();
    let repo = CommandIntents::new(&store);
    repo.insert_pending(pending_kind(
        "cmd_live_keyed_consume",
        command_kinds::inbox::CONSUME,
        None,
        1,
    ))
    .await
    .unwrap();
    repo.claim_next_kind(245, 10, command_kinds::inbox::CONSUME)
        .await
        .unwrap()
        .unwrap();

    let expired = repo.expire_stale_inbox_consumes(250, 50).await.unwrap();
    assert_eq!(expired, 0);

    let row = repo.get("cmd_live_keyed_consume").await.unwrap().unwrap();
    assert_eq!(row.status, "claimed");
    assert_eq!(row.lease_until, Some(255));
}

#[tokio::test]
async fn fresh_client_key_in_another_project_is_not_expired() {
    let store = migrated().await;
    store
        .conn
        .execute(
            "INSERT INTO sessions (session_id, name, kind, tier, project, presence, paused, \
             client_key, last_heartbeat, created_at) VALUES ('s_cross_project_keyed', \
             'cross-project-keyed', 'agent', 'agent', 'actual-project', 'online', 0, \
             'ck_cross_project', 240, 1)",
            (),
        )
        .await
        .unwrap();
    let repo = CommandIntents::new(&store);
    let mut intent = pending_kind(
        "cmd_cross_project_keyed_consume",
        command_kinds::inbox::CONSUME,
        None,
        1,
    );
    intent.project = "stale-project-metadata".into();
    intent.caller_client_key = Some("ck_cross_project".into());
    repo.insert_pending(intent).await.unwrap();
    repo.claim_next_kind(245, 10, command_kinds::inbox::CONSUME)
        .await
        .unwrap()
        .unwrap();

    let expired = repo.expire_stale_inbox_consumes(250, 50).await.unwrap();
    assert_eq!(expired, 0);
    assert_eq!(
        repo.get("cmd_cross_project_keyed_consume")
            .await
            .unwrap()
            .unwrap()
            .status,
        "claimed"
    );
}

#[tokio::test]
async fn fresh_heartbeat_inbox_consume_claims_are_not_expired() {
    let store = migrated().await;
    store
        .conn
        .execute(
            "INSERT INTO sessions (session_id, name, kind, tier, project, presence, paused, \
             last_heartbeat, created_at) VALUES ('s_live', 'live-agent', 'agent', 'agent', \
             'default', 'online', 0, 240, 1)",
            (),
        )
        .await
        .unwrap();
    let repo = CommandIntents::new(&store);
    repo.insert_pending(pending_kind(
        "cmd_live_consume",
        command_kinds::inbox::CONSUME,
        Some("s_live"),
        1,
    ))
    .await
    .unwrap();
    repo.claim_next_kind(245, 10, command_kinds::inbox::CONSUME)
        .await
        .unwrap()
        .unwrap();

    let expired = repo.expire_stale_inbox_consumes(250, 50).await.unwrap();
    assert_eq!(expired, 0);

    let row = repo.get("cmd_live_consume").await.unwrap().unwrap();
    assert_eq!(row.status, "claimed");
    assert_eq!(row.lease_until, Some(255));
}

#[tokio::test]
async fn harness_prompt_claim_skips_target_with_live_claim() {
    let store = migrated().await;
    insert_session(&store, "s_ada", "ada").await;
    insert_session(&store, "s_blake", "blake").await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(prompt_pending("cmd_ada_1", "ada", 1))
        .await
        .unwrap();
    repo.insert_pending(prompt_pending("cmd_ada_2", "ada", 2))
        .await
        .unwrap();
    repo.insert_pending(prompt_pending("cmd_blake", "blake", 3))
        .await
        .unwrap();

    let first = repo
        .claim_next_ready_harness_prompt(100, 5_000, &[])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.command_id, "cmd_ada_1");

    let second = repo
        .claim_next_ready_harness_prompt(101, 5_000, &[])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        second.command_id, "cmd_blake",
        "same-session cmd_ada_2 must be skipped while cmd_ada_1 is claimed"
    );

    let blocked = repo.get("cmd_ada_2").await.unwrap().unwrap();
    assert_eq!(blocked.status, "pending");
}

#[tokio::test]
async fn harness_prompt_claim_skips_turn_tracker_active_sessions() {
    let store = migrated().await;
    insert_session(&store, "s_ada", "ada").await;
    insert_session(&store, "s_blake", "blake").await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(prompt_pending("cmd_ada", "ada", 1))
        .await
        .unwrap();
    repo.insert_pending(prompt_pending("cmd_blake", "blake", 2))
        .await
        .unwrap();

    let claimed = repo
        .claim_next_ready_harness_prompt(100, 5_000, &["s_ada".to_string()])
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        claimed.command_id, "cmd_blake",
        "active turn tracker sessions must be excluded before claiming"
    );
    let skipped = repo.get("cmd_ada").await.unwrap().unwrap();
    assert_eq!(skipped.status, "pending");
}

#[tokio::test]
async fn harness_prompt_claim_waits_for_older_durable_delivery_obligation() {
    let identity_path = std::env::temp_dir().join(format!(
        "nexus-command-intents-obligation-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    ));
    let daemon = DaemonStore::open(identity_path.to_string_lossy().as_ref())
        .await
        .unwrap();
    let store = daemon.compatibility_store();
    insert_session_with_agent_id(&store, "s_ada", "ada", "a_ada").await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(prompt_pending("cmd_ada", "ada", 20))
        .await
        .unwrap();
    DeliveryObligations::new(&store)
        .insert(NewDeliveryObligation {
            message_id: "m_before_prompt".into(),
            recipient_agent_id: "a_ada".into(),
            recipient_runtime_id: Some("s_ada".into()),
            payload_json: r#"{"message":{"id":"m_before_prompt"}}"#.into(),
            dedupe_key: "m_before_prompt:a_ada".into(),
            attempt: 0,
            state: "pending".into(),
            created_at: 10,
        })
        .await
        .unwrap();

    assert!(repo
        .claim_next_ready_harness_prompt(100, 5_000, &[])
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        repo.get("cmd_ada").await.unwrap().unwrap().status,
        "pending",
        "a prompt must not overtake older durable bus delivery during restart recovery"
    );

    DeliveryObligations::new(&store)
        .remove("m_before_prompt", "a_ada")
        .await
        .unwrap();
    DeliveryObligations::new(&store)
        .insert(NewDeliveryObligation {
            message_id: "m_after_prompt".into(),
            recipient_agent_id: "a_ada".into(),
            recipient_runtime_id: Some("s_ada".into()),
            payload_json: r#"{"message":{"id":"m_after_prompt"}}"#.into(),
            dedupe_key: "m_after_prompt:a_ada".into(),
            attempt: 0,
            state: "pending".into(),
            created_at: 30,
        })
        .await
        .unwrap();
    let claimed = repo
        .claim_next_ready_harness_prompt(101, 5_000, &[])
        .await
        .unwrap()
        .expect("a newer delivery must not hold an older prompt");
    assert_eq!(claimed.command_id, "cmd_ada");

    drop(store);
    drop(daemon);
    let _ = std::fs::remove_file(identity_path);
}

#[tokio::test]
async fn harness_prompt_session_actor_claims_only_its_target_session() {
    let store = migrated().await;
    insert_session(&store, "s_ada", "ada").await;
    insert_session(&store, "s_blake", "blake").await;
    let repo = CommandIntents::new(&store);
    repo.insert_pending(prompt_pending("cmd_blake", "blake", 1))
        .await
        .unwrap();
    repo.insert_pending(prompt_pending("cmd_ada", "ada", 2))
        .await
        .unwrap();

    let claimed = repo
        .claim_next_ready_harness_prompt_for_session(100, 5_000, "s_ada")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(claimed.command_id, "cmd_ada");
    let blake = repo.get("cmd_blake").await.unwrap().unwrap();
    assert_eq!(
        blake.status, "pending",
        "session actor must not steal another session's older prompt"
    );
}

#[tokio::test]
async fn harness_prompt_session_actor_matches_agent_id_targets() {
    let store = migrated().await;
    insert_session_with_agent_id(&store, "s_ada", "ada", "a_ada").await;
    insert_session_with_agent_id(&store, "s_blake", "blake", "a_blake").await;
    let repo = CommandIntents::new(&store);
    let mut blake = prompt_pending("cmd_blake", "ignored-name", 1);
    blake.request_json =
        r#"{"agentId":"a_blake","name":"stale-blake","text":"hello blake","clientMessageId":null}"#
            .to_string();
    let mut ada = prompt_pending("cmd_ada", "ignored-name", 2);
    ada.request_json =
        r#"{"agentId":"a_ada","name":"stale-ada","text":"hello ada","clientMessageId":null}"#
            .to_string();
    repo.insert_pending(blake).await.unwrap();
    repo.insert_pending(ada).await.unwrap();

    let claimed = repo
        .claim_next_ready_harness_prompt_for_session(100, 5_000, "s_ada")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(claimed.command_id, "cmd_ada");
    let blake = repo.get("cmd_blake").await.unwrap().unwrap();
    assert_eq!(blake.status, "pending");
}

#[tokio::test]
async fn harness_prompt_agent_id_targets_the_active_runtime_not_a_newer_stale_session() {
    let store = migrated().await;
    insert_session_with_agent_id(&store, "s_ada_active", "ada", "a_ada").await;
    store
        .conn
        .execute(
            "UPDATE sessions SET project = 'runtime-metadata', created_at = 1 \
             WHERE session_id = 's_ada_active'",
            (),
        )
        .await
        .unwrap();
    insert_session_with_agent_id(&store, "s_ada_stale", "stale-ada", "a_ada").await;
    store
        .conn
        .execute(
            "UPDATE sessions SET project = 'other-metadata', created_at = 2 \
             WHERE session_id = 's_ada_stale'",
            (),
        )
        .await
        .unwrap();
    insert_durable_agent_with_active_runtime(&store, "a_ada", "current-ada", "s_ada_active").await;
    let repo = CommandIntents::new(&store);
    let mut prompt = prompt_pending("cmd_active_ada", "stale-name", 1);
    prompt.project = "caller-metadata".into();
    prompt.request_json =
        r#"{"agentId":"a_ada","name":"stale-name","text":"hello","clientMessageId":null}"#.into();
    repo.insert_pending(prompt).await.unwrap();

    assert!(repo
        .claim_next_ready_harness_prompt_for_session(100, 5_000, "s_ada_stale")
        .await
        .unwrap()
        .is_none());
    let claimed = repo
        .claim_next_ready_harness_prompt_for_session(100, 5_000, "s_ada_active")
        .await
        .unwrap()
        .expect("the active runtime owns the stable-id lane");
    assert_eq!(claimed.command_id, "cmd_active_ada");
}

#[tokio::test]
async fn harness_prompt_name_target_is_global_and_project_is_only_caller_metadata() {
    let store = migrated().await;
    insert_session(&store, "s_global_ada", "global-ada").await;
    store
        .conn
        .execute(
            "UPDATE sessions SET project = 'runtime-metadata' WHERE session_id = 's_global_ada'",
            (),
        )
        .await
        .unwrap();
    let repo = CommandIntents::new(&store);
    let mut prompt = prompt_pending("cmd_global_ada", "global-ada", 1);
    prompt.project = "caller-metadata".into();
    repo.insert_pending(prompt).await.unwrap();

    let claimed = repo
        .claim_next_ready_harness_prompt_for_session(100, 5_000, "s_global_ada")
        .await
        .unwrap()
        .expect("globally unique names route across project metadata");
    assert_eq!(claimed.command_id, "cmd_global_ada");
}

#[tokio::test]
async fn harness_prompt_name_target_rejects_global_ambiguity() {
    let store = migrated().await;
    store
        .conn
        .execute("DROP INDEX idx_sessions_name", ())
        .await
        .unwrap();
    insert_session(&store, "s_ada_one", "ambiguous-ada").await;
    insert_session(&store, "s_ada_two", "ambiguous-ada").await;
    store
        .conn
        .execute(
            "UPDATE sessions SET project = CASE session_id \
             WHEN 's_ada_one' THEN 'one' ELSE 'two' END",
            (),
        )
        .await
        .unwrap();
    let repo = CommandIntents::new(&store);
    repo.insert_pending(prompt_pending("cmd_ambiguous_ada", "ambiguous-ada", 1))
        .await
        .unwrap();

    let error = repo
        .claim_next_ready_harness_prompt(100, 5_000, &[])
        .await
        .expect_err("ambiguous global names must fail closed");
    assert!(error.to_string().contains("ambiguous"));
}
