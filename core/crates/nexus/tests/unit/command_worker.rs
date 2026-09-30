use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use nexus_common::Config;
use nexus_contracts::{
    AgentTurnExecutionPort, Kind, NexusBatch, PortResult, PromptRequest, RegisterRequest,
    RemoveRequest, RemoveResponse, SessionId, SpawnRequest, SpawnResponse, SteerRequest, Tier,
};
use nexus_store::command_kinds;
use nexus_store::repos::{CommandIntents, NewCommandIntent};
use nexus_store::Store;

use super::*;

struct FirstPromptHangs {
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl AgentTurnExecutionPort for FirstPromptHangs {
    async fn inject_turn(&self, _recipient: &SessionId, _batch: &NexusBatch) -> PortResult<()> {
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        unimplemented!("command-worker timeout tests do not launch harnesses")
    }

    async fn remove(&self, _req: RemoveRequest) -> PortResult<RemoveResponse> {
        unimplemented!("command-worker timeout tests do not remove harnesses")
    }

    async fn prompt(&self, _recipient: &SessionId, _text: String) -> PortResult<()> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
        Ok(())
    }
}

async fn test_state_with_turn_exec(turn_exec: Arc<dyn AgentTurnExecutionPort>) -> AppState {
    test_state_with_config(Config::default(), turn_exec).await
}

async fn test_state_with_config(
    config: Config,
    turn_exec: Arc<dyn AgentTurnExecutionPort>,
) -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    AppState::wire_with_turn_exec(store, &config, turn_exec)
}

async fn test_state() -> AppState {
    test_state_with_turn_exec(Arc::new(FirstPromptHangs {
        calls: AtomicUsize::new(0),
    }))
    .await
}

fn human_register(name: &str, client_key: &str) -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some(name.into()),
        harness: hid("other"),
        harness_session_id: format!("hs_{client_key}"),
        project: "default".into(),
        client_key: client_key.into(),
        runtime_credential: None,
        tier: Tier::Admin,
        kind: Some(Kind::Human),
        role: None,
        cwd: None,
    }
}

fn prompt_intent(
    id: &str,
    caller: &nexus_contracts::RegisterResponse,
    caller_name: &str,
    caller_client_key: &str,
    text: &str,
    created_at: i64,
) -> NewCommandIntent {
    NewCommandIntent {
        command_id: id.into(),
        kind: command_kinds::harness::PROMPT.into(),
        project: "default".into(),
        caller_name: caller_name.into(),
        caller_session_id: Some(caller.session_id.0.clone()),
        caller_agent_id: caller.agent_id.as_ref().map(|id| id.0.clone()),
        caller_runtime_id: Some(caller.session_id.0.clone()),
        caller_client_key: Some(caller_client_key.into()),
        caller_kind: Some("human".into()),
        caller_tier: Some("admin".into()),
        idempotency_key: None,
        request_json: serde_json::to_string(&PromptRequest {
            agent_id: None,
            name: "Target Human".into(),
            text: text.into(),
            client_message_id: None,
        })
        .unwrap(),
        created_at,
    }
}

fn steer_intent(
    id: &str,
    caller: &nexus_contracts::RegisterResponse,
    caller_name: &str,
    caller_client_key: &str,
    created_at: i64,
) -> NewCommandIntent {
    let mut intent = prompt_intent(
        id,
        caller,
        caller_name,
        caller_client_key,
        "steer now",
        created_at,
    );
    intent.kind = command_kinds::harness::STEER.into();
    intent.request_json = serde_json::to_string(&SteerRequest {
        agent_id: None,
        name: "Target Human".into(),
        text: "steer now".into(),
        client_message_id: Some("steer:1".into()),
    })
    .unwrap();
    intent
}

#[tokio::test]
async fn harness_prompt_claim_lease_outlives_execution_timeout() {
    let exec = Arc::new(FirstPromptHangs {
        calls: AtomicUsize::new(0),
    });
    let state = test_state_with_turn_exec(exec).await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Target Human", "ck_target"))
        .await
        .unwrap();

    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(prompt_intent(
        "cmd_prompt",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "claim only",
        1,
    ))
    .await
    .unwrap();

    let row = claim_next_for_lane(&state, WorkerLane::HarnessPrompt)
        .await
        .unwrap()
        .unwrap();

    let claimed_at = row.claimed_at.expect("prompt row should be claimed");
    assert_eq!(row.lease_until, Some(claimed_at + HARNESS_PROMPT_LEASE_MS));
    assert!(
        HARNESS_PROMPT_LEASE_MS
            > i64::try_from(HARNESS_PROMPT_EXECUTION_TIMEOUT.as_millis()).unwrap(),
        "prompt claim must not expire before the prompt execution timeout fires"
    );
}

#[tokio::test]
async fn explicit_steer_lane_bypasses_claimed_prompt_boundary() {
    let state = test_state().await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Target Human", "ck_target"))
        .await
        .unwrap();
    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(prompt_intent(
        "cmd_prompt_boundary",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "queue me",
        1,
    ))
    .await
    .unwrap();
    repo.insert_pending(steer_intent(
        "cmd_steer_now",
        &alex,
        "Alex Morgan",
        "ck_operator",
        2,
    ))
    .await
    .unwrap();

    let prompt = claim_next_for_lane(&state, WorkerLane::HarnessPrompt)
        .await
        .unwrap()
        .expect("prompt claims first");
    assert_eq!(prompt.command_id, "cmd_prompt_boundary");

    let steer = claim_next_for_lane(&state, WorkerLane::HarnessSteer)
        .await
        .unwrap()
        .expect("steer remains independently claimable");
    assert_eq!(steer.command_id, "cmd_steer_now");
}

#[tokio::test]
async fn command_claims_wait_for_the_store_write_gate() {
    let state = test_state().await;
    let write_guard = state.store.write_lock().lock_owned().await;
    let claimant_state = state.clone();
    let claimant =
        tokio::spawn(
            async move { claim_next_for_lane(&claimant_state, WorkerLane::Control).await },
        );

    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        !claimant.is_finished(),
        "remote Hrana claims must not overlap another durable write section"
    );

    drop(write_guard);
    let claimed = tokio::time::timeout(Duration::from_millis(100), claimant)
        .await
        .expect("claim should continue after the durable write gate opens")
        .expect("claim task should not panic")
        .expect("claim query should succeed");
    assert!(claimed.is_none());

    let write_guard = state.store.write_lock().lock_owned().await;
    let actor_state = state.clone();
    let actor_claim = tokio::spawn(async move {
        claim_next_harness_prompt_for_session(&actor_state, "missing-session").await
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        !actor_claim.is_finished(),
        "per-session prompt actors must use the same durable claim gate"
    );
    drop(write_guard);
    let claimed = tokio::time::timeout(Duration::from_millis(100), actor_claim)
        .await
        .expect("actor claim should continue after the durable write gate opens")
        .expect("actor claim task should not panic")
        .expect("actor claim query should succeed");
    assert!(claimed.is_none());
}

#[tokio::test]
async fn idle_worker_wait_wakes_on_command_intent_signal_before_poll_interval() {
    let state = test_state().await;
    let epoch = state.store.command_intents_epoch();
    let waiter_state = state.clone();
    let wait = tokio::spawn(async move {
        wait_for_command_intent_or_poll(&waiter_state, epoch).await;
    });

    tokio::time::sleep(Duration::from_millis(5)).await;
    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_signal".into(),
            kind: command_kinds::identity::REGISTER.into(),
            project: "default".into(),
            caller_name: "signal-test".into(),
            caller_session_id: None,
            caller_agent_id: None,
            caller_runtime_id: None,
            caller_client_key: None,
            caller_kind: None,
            caller_tier: None,
            idempotency_key: None,
            request_json: "{}".into(),
            created_at: now(),
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_millis(75), wait)
        .await
        .expect("command-intent signal should wake before the 250ms fallback poll")
        .expect("wait task should not panic");
}

#[tokio::test]
async fn harness_prompt_timeout_marks_error_and_unblocks_next_prompt() {
    let exec = Arc::new(FirstPromptHangs {
        calls: AtomicUsize::new(0),
    });
    let state = test_state_with_turn_exec(exec).await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Target Human", "ck_target"))
        .await
        .unwrap();

    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(prompt_intent(
        "cmd_hung_prompt",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "first hangs",
        1,
    ))
    .await
    .unwrap();
    repo.insert_pending(prompt_intent(
        "cmd_next_prompt",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "second runs",
        2,
    ))
    .await
    .unwrap();

    assert!(process_next_for_lane(&state, WorkerLane::HarnessPrompt)
        .await
        .unwrap());
    let first = repo.get("cmd_hung_prompt").await.unwrap().unwrap();
    assert_eq!(first.status, "error");
    assert!(first
        .error_json
        .as_deref()
        .unwrap()
        .contains("harness.prompt command cmd_hung_prompt timed out"));

    assert!(process_next_for_lane(&state, WorkerLane::HarnessPrompt)
        .await
        .unwrap());
    let second = repo.get("cmd_next_prompt").await.unwrap().unwrap();
    assert_eq!(second.status, "done");
}

#[tokio::test]
async fn maintenance_reaps_terminal_command_intents_by_retention() {
    let config = Config {
        // Keep a wide wall-clock margin: the full workspace runs this async test alongside many
        // other targets, so a 50 ms "recent" row can legitimately age past a 100 ms cutoff before
        // maintenance executes. The contract under test is relative ordering, not scheduler speed.
        command_intent_retention_ms: 10_000,
        ..Config::default()
    };
    let state = test_state_with_config(
        config,
        Arc::new(FirstPromptHangs {
            calls: AtomicUsize::new(0),
        }),
    )
    .await;
    let repo = CommandIntents::new(&state.store);
    let ts = now();
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();

    repo.insert_pending(prompt_intent(
        "cmd_old_done",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "old done",
        ts - 1_000,
    ))
    .await
    .unwrap();
    repo.insert_pending(prompt_intent(
        "cmd_recent_error",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "recent error",
        ts - 1_000,
    ))
    .await
    .unwrap();
    repo.insert_pending(prompt_intent(
        "cmd_pending",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "pending",
        ts - 1_000,
    ))
    .await
    .unwrap();
    repo.mark_done("cmd_old_done", r#"{"ok":true}"#, ts - 20_000)
        .await
        .unwrap();
    repo.mark_error("cmd_recent_error", r#"{"message":"recent"}"#, ts - 50)
        .await
        .unwrap();

    let mut retention_state = RetentionPolicyState::default();
    maybe_reap_operational_tables(&state, &mut retention_state)
        .await
        .unwrap();

    assert!(repo.get("cmd_old_done").await.unwrap().is_none());
    assert_eq!(
        repo.get("cmd_recent_error").await.unwrap().unwrap().status,
        "error"
    );
    assert_eq!(
        repo.get("cmd_pending").await.unwrap().unwrap().status,
        "pending"
    );
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
