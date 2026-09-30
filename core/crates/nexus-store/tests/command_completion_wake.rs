use std::time::Duration;

use nexus_common::now;
use nexus_store::repos::{CommandIntents, NewCommandIntent};
use nexus_store::Store;

fn pending(command_id: &str) -> NewCommandIntent {
    NewCommandIntent {
        command_id: command_id.into(),
        kind: "message.post.send".into(),
        project: "metadata-only".into(),
        caller_name: "casey".into(),
        caller_session_id: Some("s_casey".into()),
        caller_agent_id: Some("a_casey".into()),
        caller_runtime_id: Some("r_casey".into()),
        caller_client_key: Some("client-casey".into()),
        caller_kind: Some("agent".into()),
        caller_tier: Some("agent".into()),
        idempotency_key: None,
        request_json: "{}".into(),
        created_at: now(),
    }
}

#[tokio::test]
async fn terminal_command_update_wakes_held_daemon_ipc_waiter() {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    let commands = CommandIntents::new(&store);
    commands
        .insert_pending(pending("cmd-complete"))
        .await
        .unwrap();

    let epoch = store.command_completions_epoch();
    commands
        .mark_done("cmd-complete", r#"{"ok":true}"#, now())
        .await
        .unwrap();

    tokio::time::timeout(
        Duration::from_millis(100),
        store.wait_for_command_completion_after(epoch),
    )
    .await
    .expect("terminal transition must wake the held daemon IPC response");
}

#[tokio::test]
async fn nonterminal_command_update_does_not_wake_completion_waiter() {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    let commands = CommandIntents::new(&store);
    commands
        .insert_pending(pending("cmd-started"))
        .await
        .unwrap();
    let row = commands
        .claim_next(now(), 15_000)
        .await
        .unwrap()
        .expect("claim");

    let epoch = store.command_completions_epoch();
    commands
        .mark_started_for_claim(&row.command_id, row.claimed_at.expect("claimed_at"), now())
        .await
        .unwrap();

    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            store.wait_for_command_completion_after(epoch),
        )
        .await
        .is_err(),
        "claimed→started is observable queue state, not terminal response completion"
    );
}

async fn assert_completion_wake(store: &Store, epoch: u64) {
    tokio::time::timeout(
        Duration::from_millis(100),
        store.wait_for_command_completion_after(epoch),
    )
    .await
    .expect("terminal transition must signal command completion");
}

#[tokio::test]
async fn every_claimed_terminal_path_wakes_held_daemon_ipc_waiters() {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    let commands = CommandIntents::new(&store);

    commands
        .insert_pending(pending("cmd-done-claim"))
        .await
        .unwrap();
    let done = commands.claim_next(now(), 15_000).await.unwrap().unwrap();
    let epoch = store.command_completions_epoch();
    assert!(commands
        .mark_done_for_claim(
            &done.command_id,
            done.claimed_at.unwrap(),
            r#"{"ok":true}"#,
            now(),
        )
        .await
        .unwrap());
    assert_completion_wake(&store, epoch).await;

    commands.insert_pending(pending("cmd-error")).await.unwrap();
    let epoch = store.command_completions_epoch();
    commands
        .mark_error("cmd-error", r#"{"message":"failed"}"#, now())
        .await
        .unwrap();
    assert_completion_wake(&store, epoch).await;

    commands
        .insert_pending(pending("cmd-error-claim"))
        .await
        .unwrap();
    let failed = commands.claim_next(now(), 15_000).await.unwrap().unwrap();
    let epoch = store.command_completions_epoch();
    assert!(commands
        .mark_error_for_claim(
            &failed.command_id,
            failed.claimed_at.unwrap(),
            r#"{"message":"failed"}"#,
            now(),
        )
        .await
        .unwrap());
    assert_completion_wake(&store, epoch).await;

    let mut prompt = pending("cmd-cancel");
    prompt.kind = nexus_store::command_kinds::harness::PROMPT.into();
    commands.insert_pending(prompt).await.unwrap();
    let epoch = store.command_completions_epoch();
    assert!(commands
        .cancel_pending_prompt("cmd-cancel", now())
        .await
        .unwrap());
    assert_completion_wake(&store, epoch).await;

    let mut consume = pending("cmd-expire-consume");
    consume.kind = nexus_store::command_kinds::inbox::CONSUME.into();
    commands.insert_pending(consume).await.unwrap();
    commands.claim_next(now(), 15_000).await.unwrap().unwrap();
    let epoch = store.command_completions_epoch();
    assert_eq!(
        commands
            .expire_stale_inbox_consumes(now() + 30_000, 15_000)
            .await
            .unwrap(),
        1
    );
    assert_completion_wake(&store, epoch).await;

    commands
        .insert_pending(pending("cmd-shutdown"))
        .await
        .unwrap();
    commands.claim_next(now(), 15_000).await.unwrap().unwrap();
    let epoch = store.command_completions_epoch();
    assert_eq!(commands.reap_claimed_for_shutdown(now()).await.unwrap(), 1);
    assert_completion_wake(&store, epoch).await;
}
