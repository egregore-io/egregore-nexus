use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use nexus::daemon::{command_worker, AppState};
use nexus_common::Config;
use nexus_contracts::{
    AgentId, AgentTurnExecutionPort, CompactRequest, Kind, NexusBatch, PortResult, PromptRequest,
    RegisterRequest, RemoveRequest, RemoveResponse, SessionId, SpawnRequest, SpawnResponse,
    SteerDelivery, SteerRequest, SteerResponse, Tier, WarmRequest, WsEvent,
};
use nexus_store::command_kinds;
use nexus_store::repos::{
    CommandIntents, InitialPromptDeliveries, InitialPromptInsert, NewCommandIntent, Sessions,
};
use nexus_store::Store;

#[derive(Default)]
struct CountingExec {
    prompt: AtomicUsize,
    steer: AtomicUsize,
    compact: AtomicUsize,
}

#[async_trait::async_trait]
impl AgentTurnExecutionPort for CountingExec {
    async fn inject_turn(&self, _recipient: &SessionId, _batch: &NexusBatch) -> PortResult<()> {
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        unimplemented!("initial-prompt command-gate tests do not launch harnesses")
    }

    async fn remove(&self, _req: RemoveRequest) -> PortResult<RemoveResponse> {
        unimplemented!("initial-prompt command-gate tests do not remove harnesses")
    }

    async fn prompt(&self, _recipient: &SessionId, _text: String) -> PortResult<()> {
        self.prompt.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn steer_observed(
        &self,
        _recipient: &SessionId,
        _text: String,
        events: Arc<dyn nexus_contracts::EventSink>,
        accepted_event: WsEvent,
    ) -> PortResult<SteerResponse> {
        self.steer.fetch_add(1, Ordering::SeqCst);
        events.emit(accepted_event).await;
        Ok(SteerResponse {
            accepted: true,
            delivery: SteerDelivery::Steered,
            turn_id: Some("turn_test".into()),
        })
    }

    async fn compact(&self, _recipient: &SessionId) -> PortResult<()> {
        self.compact.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

async fn test_state(turn_exec: Arc<dyn AgentTurnExecutionPort>) -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    AppState::wire_with_turn_exec(
        store,
        &Config {
            heartbeat_ttl_ms: 86_400_000,
            ..Config::default()
        },
        turn_exec,
    )
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
        locality: Default::default(),
        access: None,
        role: None,
        cwd: None,
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
        locality: Default::default(),
        access: None,
        role: None,
        cwd: None,
    }
}

fn command_intent(
    command_id: &str,
    kind: &str,
    caller: &nexus_contracts::RegisterResponse,
    request_json: String,
    created_at: i64,
) -> NewCommandIntent {
    NewCommandIntent {
        command_id: command_id.into(),
        kind: kind.into(),
        project: "default".into(),
        caller_name: "Alex Morgan".into(),
        caller_session_id: Some(caller.session_id.0.clone()),
        caller_agent_id: caller.agent_id.as_ref().map(|id| id.0.clone()),
        caller_runtime_id: Some(caller.session_id.0.clone()),
        caller_client_key: Some("ck_operator".into()),
        caller_principal_id: None,
        caller_kind: Some("human".into()),
        caller_tier: Some("admin".into()),
        idempotency_key: None,
        request_json,
        created_at,
    }
}

async fn wait_for_status(state: &AppState, command_id: &str, status: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let row = CommandIntents::new(&state.store)
            .get(command_id)
            .await
            .unwrap()
            .expect("command row exists");
        if row.status == status {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {command_id} to become {status}; last status was {}",
            row.status
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn assert_status(state: &AppState, command_id: &str, status: &str) {
    let row = CommandIntents::new(&state.store)
        .get(command_id)
        .await
        .unwrap()
        .expect("command row exists");
    assert_eq!(row.status, status, "{command_id} status");
}

#[tokio::test]
async fn pending_initial_prompt_blocks_prompt_steer_compact_and_warm_until_accepted() {
    let exec = Arc::new(CountingExec::default());
    let state = test_state(exec.clone()).await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();
    let ada = state
        .identity
        .register(agent_register("Ada", "ck_ada"))
        .await
        .unwrap();

    InitialPromptDeliveries::new(&state.store)
        .insert_pending_once(InitialPromptInsert {
            runtime_id: ada.session_id.0.clone(),
            agent_id: ada.agent_id.as_ref().unwrap().0.clone(),
            session_id: ada.session_id.0.clone(),
            harness: "codex".into(),
            template: "You are <var.name>.".into(),
            rendered_prompt: "You are Ada.".into(),
            client_message_id: format!("initial-prompt:{}", ada.session_id.0),
            created_at_ms: 1,
        })
        .await
        .unwrap();

    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(command_intent(
        "cmd_prompt",
        command_kinds::harness::PROMPT,
        &alex,
        serde_json::to_string(&PromptRequest {
            agent_id: None,
            name: "Ada".into(),
            text: "hello".into(),
            client_message_id: None,
        })
        .unwrap(),
        10,
    ))
    .await
    .unwrap();
    repo.insert_pending(command_intent(
        "cmd_steer",
        command_kinds::harness::STEER,
        &alex,
        serde_json::to_string(&SteerRequest {
            agent_id: None,
            name: "Ada".into(),
            text: "steer now".into(),
            client_message_id: Some("steer:boot".into()),
        })
        .unwrap(),
        11,
    ))
    .await
    .unwrap();
    repo.insert_pending(command_intent(
        "cmd_compact",
        command_kinds::harness::COMPACT,
        &alex,
        serde_json::to_string(&CompactRequest {
            agent_id: None,
            name: "Ada".into(),
            client_message_id: None,
        })
        .unwrap(),
        12,
    ))
    .await
    .unwrap();
    repo.insert_pending(command_intent(
        "cmd_warm",
        command_kinds::harness::WARM,
        &alex,
        serde_json::to_string(&WarmRequest {
            agent_id: None,
            name: "Ada".into(),
        })
        .unwrap(),
        13,
    ))
    .await
    .unwrap();

    let worker = command_worker::spawn(state.clone());
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_status(&state, "cmd_prompt", "pending").await;
    assert_status(&state, "cmd_steer", "pending").await;
    assert_status(&state, "cmd_compact", "pending").await;
    assert_status(&state, "cmd_warm", "pending").await;
    assert_eq!(
        exec.steer.load(Ordering::SeqCst),
        0,
        "boot-pending steer must not reach the harness"
    );
    assert_eq!(
        exec.prompt.load(Ordering::SeqCst),
        0,
        "boot-pending prompt must not reach the harness"
    );
    assert_eq!(
        exec.compact.load(Ordering::SeqCst),
        0,
        "boot-pending compact must not reach the harness"
    );

    InitialPromptDeliveries::new(&state.store)
        .mark_accepted(&ada.session_id.0, 20)
        .await
        .unwrap();

    wait_for_status(&state, "cmd_prompt", "done", Duration::from_millis(800)).await;
    wait_for_status(&state, "cmd_steer", "done", Duration::from_millis(800)).await;
    wait_for_status(&state, "cmd_compact", "done", Duration::from_millis(800)).await;
    wait_for_status(&state, "cmd_warm", "done", Duration::from_millis(800)).await;
    assert_eq!(exec.prompt.load(Ordering::SeqCst), 1);
    assert_eq!(exec.steer.load(Ordering::SeqCst), 1);
    assert_eq!(exec.compact.load(Ordering::SeqCst), 1);

    worker.abort();
}

#[tokio::test]
async fn pending_initial_prompt_blocks_agent_id_targeted_commands_with_stale_name() {
    let exec = Arc::new(CountingExec::default());
    let state = test_state(exec.clone()).await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();
    let ada = state
        .identity
        .register(agent_register("Ada", "ck_ada"))
        .await
        .unwrap();
    let ada_agent_id = ada.agent_id.as_ref().expect("agent id").clone();
    Sessions::new(&state.store)
        .set_agent_id(&ada.session_id, &ada_agent_id.0)
        .await
        .unwrap();

    InitialPromptDeliveries::new(&state.store)
        .insert_pending_once(InitialPromptInsert {
            runtime_id: ada.session_id.0.clone(),
            agent_id: ada_agent_id.0.clone(),
            session_id: ada.session_id.0.clone(),
            harness: "codex".into(),
            template: "You are <var.name>.".into(),
            rendered_prompt: "You are Ada.".into(),
            client_message_id: format!("initial-prompt:{}", ada.session_id.0),
            created_at_ms: 1,
        })
        .await
        .unwrap();

    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(command_intent(
        "cmd_prompt_agent_id",
        command_kinds::harness::PROMPT,
        &alex,
        serde_json::to_string(&PromptRequest {
            agent_id: Some(ada_agent_id.clone()),
            name: "stale-name".into(),
            text: "hello".into(),
            client_message_id: None,
        })
        .unwrap(),
        10,
    ))
    .await
    .unwrap();
    repo.insert_pending(command_intent(
        "cmd_compact_agent_id",
        command_kinds::harness::COMPACT,
        &alex,
        serde_json::to_string(&CompactRequest {
            agent_id: Some(ada_agent_id.clone()),
            name: "stale-name".into(),
            client_message_id: None,
        })
        .unwrap(),
        11,
    ))
    .await
    .unwrap();
    repo.insert_pending(command_intent(
        "cmd_warm_agent_id",
        command_kinds::harness::WARM,
        &alex,
        serde_json::to_string(&WarmRequest {
            agent_id: Some(AgentId(ada_agent_id.0.clone())),
            name: "stale-name".into(),
        })
        .unwrap(),
        12,
    ))
    .await
    .unwrap();

    let worker = command_worker::spawn(state.clone());
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_status(&state, "cmd_prompt_agent_id", "pending").await;
    assert_status(&state, "cmd_compact_agent_id", "pending").await;
    assert_status(&state, "cmd_warm_agent_id", "pending").await;
    assert_eq!(
        exec.prompt.load(Ordering::SeqCst),
        0,
        "agentId-targeted prompt must not bypass the boot-pending gate"
    );
    assert_eq!(
        exec.compact.load(Ordering::SeqCst),
        0,
        "agentId-targeted compact must not bypass the boot-pending gate"
    );

    InitialPromptDeliveries::new(&state.store)
        .mark_accepted(&ada.session_id.0, 20)
        .await
        .unwrap();

    wait_for_status(
        &state,
        "cmd_prompt_agent_id",
        "done",
        Duration::from_millis(800),
    )
    .await;
    wait_for_status(
        &state,
        "cmd_compact_agent_id",
        "done",
        Duration::from_millis(800),
    )
    .await;
    wait_for_status(
        &state,
        "cmd_warm_agent_id",
        "done",
        Duration::from_millis(800),
    )
    .await;
    assert_eq!(exec.prompt.load(Ordering::SeqCst), 1);
    assert_eq!(exec.compact.load(Ordering::SeqCst), 1);

    worker.abort();
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
