//! Source-push delivery acceptance: topic fan-out is committed before a cold subscriber is
//! revived. A durably dead subscriber is terminal immediately; a non-dead subscriber stays pending
//! through the bounded pre-injection revive policy and is dead-lettered only after exhaustion.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use nexus::daemon::{command_worker, AppState};
use nexus_agent::{Adapter, AdapterInjectError, AdapterRegistry, MockAdapter, StreamEvent};
use nexus_common::{Config, NexusError};
use nexus_contracts::{Caller, Kind, PushRequest, RegisterRequest, SpawnRequest, Tier};
use nexus_store::command_kinds;
use nexus_store::repos::{Agents, CommandIntents, NewCommandIntent, Sessions, Sources, Topics};
use nexus_store::Store;

const PROJECT: &str = "source-wake-project";
const SOURCE: &str = "source-wake-fixture";
const TOPIC: &str = "source-wake-topic";

#[derive(Clone, Default)]
struct CountingFailOpenAdapter {
    opens: Arc<AtomicUsize>,
}

#[async_trait]
impl Adapter for CountingFailOpenAdapter {
    async fn open_session(&self) -> Result<(), NexusError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        Err(NexusError::Adapter("fixture transport cannot open".into()))
    }

    async fn resume(&self, _resume_key: &str) -> Result<(), NexusError> {
        self.open_session().await
    }

    async fn inject(&self, _prompt: String) -> Result<(), AdapterInjectError> {
        unreachable!("failed-open adapter cannot receive a prompt")
    }

    async fn stream_updates(&self) -> Result<Vec<StreamEvent>, NexusError> {
        Ok(Vec::new())
    }
}

async fn state_with_counting_fail_adapter(adapter: CountingFailOpenAdapter) -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();

    let mut registry = AdapterRegistry::new();
    registry.register(
        &hid("claude"),
        Arc::new(move |_cwd| Arc::new(adapter.clone()) as Arc<dyn Adapter>),
    );
    let state = AppState::wire_with_registry(store, &Config::default(), registry);
    state.wait_for_runtime_identity_ready().await.unwrap();
    state
}

async fn state_with_mock(mock: MockAdapter) -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();

    let mut registry = AdapterRegistry::new();
    registry.register(
        &hid("claude"),
        Arc::new(move |_cwd| Arc::new(mock.clone()) as Arc<dyn Adapter>),
    );
    let state = AppState::wire_with_registry(store, &Config::default(), registry);
    state.wait_for_runtime_identity_ready().await.unwrap();
    state
}

fn operator_request() -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some("operator".into()),
        harness: hid("other"),
        harness_session_id: "source-wake-operator-native".into(),
        project: PROJECT.into(),
        client_key: "source-wake-operator-client".into(),
        runtime_credential: None,
        tier: Tier::Admin,
        kind: Some(Kind::Human),
        locality: Default::default(),
        access: None,
        role: None,
        cwd: None,
    }
}

fn agent_request(name: &str, client_key: &str) -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some(name.into()),
        harness: hid("claude"),
        harness_session_id: format!("native-{client_key}"),
        project: PROJECT.into(),
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

fn spawn_request(name: &str) -> SpawnRequest {
    SpawnRequest {
        kind: hid("claude"),
        name: Some(name.into()),
        identity_policy: None,
        cwd: None,
        project: Some(PROJECT.into()),
        role: None,
        initial_prompt: None,
        resume: None,
        harness_args: Vec::new(),
        headless: true,
        backend: None,
    }
}

async fn setup_source(state: &AppState) {
    Sources::new(&state.store)
        .create(SOURCE, "test-token-hash", TOPIC, 1_000)
        .await
        .unwrap();
    Topics::new(&state.store)
        .ensure(TOPIC, PROJECT)
        .await
        .unwrap();
}

async fn push(state: &AppState, caller: &Caller, marker: &str) {
    state
        .push_as_source(
            caller,
            PushRequest {
                source: SOURCE.into(),
                topic: None,
                summary: Some("source wake acceptance".into()),
                body: marker.into(),
                meta: None,
            },
        )
        .await
        .unwrap();
}

async fn wait_until(mut predicate: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        if predicate() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn delivery_state(state: &AppState) -> (String, Option<String>, i64) {
    let mut rows = state
        .store
        .conn
        .query(
            "SELECT state, error_code, attempt_count FROM in_flight ORDER BY rowid DESC LIMIT 1",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("delivery row");
    (
        row.get(0).unwrap(),
        row.get(1).unwrap(),
        row.get(2).unwrap(),
    )
}

async fn wait_for_delivery_state(state: &AppState, expected: &str) -> bool {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        if delivery_state(state).await.0 == expected {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_revive_backoff(state: &AppState, session: &nexus_contracts::SessionId) -> bool {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        if state
            .pending_respawn_backoff_for(session, nexus_common::now())
            .is_some()
        {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn source_push_revives_offline_subscriber_and_injects_once() {
    let mock = MockAdapter::new();
    let state = state_with_mock(mock.clone()).await;
    state.identity.register(operator_request()).await.unwrap();
    let caller = state.identity.resolve(PROJECT, "operator").await.unwrap();
    setup_source(&state).await;

    let spawned = state
        .launch_agent(spawn_request("cold-subscriber"), PROJECT, Some(&caller))
        .await
        .unwrap();
    Topics::new(&state.store)
        .subscribe(TOPIC, &spawned.session_id.0, None)
        .await
        .unwrap();
    state
        .mark_session_offline(&spawned.session_id)
        .await
        .unwrap();

    let marker = "SOURCE-COLD-SUBSCRIBER-MARKER";
    push(&state, &caller, marker).await;

    assert!(
        wait_until(|| mock.injected_prompts().iter().any(|p| p.contains(marker))).await,
        "source push must revive and inject the committed delivery; prompts={:?}",
        mock.injected_prompts()
    );
    assert_eq!(
        mock.injected_prompts()
            .iter()
            .filter(|p| p.contains(marker))
            .count(),
        1,
        "one source push gets one automatic injection attempt"
    );
}

#[tokio::test]
async fn source_push_marks_durable_dead_subscriber_terminal_without_injection() {
    let mock = MockAdapter::new();
    let state = state_with_mock(mock.clone()).await;
    state.identity.register(operator_request()).await.unwrap();
    let caller = state.identity.resolve(PROJECT, "operator").await.unwrap();
    setup_source(&state).await;

    let spawned = state
        .launch_agent(spawn_request("dead-subscriber"), PROJECT, Some(&caller))
        .await
        .unwrap();
    Topics::new(&state.store)
        .subscribe(TOPIC, &spawned.session_id.0, None)
        .await
        .unwrap();
    let row = Sessions::new(&state.store)
        .find_by_session_id(&spawned.session_id)
        .await
        .unwrap()
        .unwrap();
    Agents::new(&state.store)
        .mark_dead(
            row.agent_id.as_deref().expect("launched agent id"),
            "no_resume_id",
        )
        .await
        .unwrap();
    state
        .mark_session_offline(&spawned.session_id)
        .await
        .unwrap();

    let marker = "SOURCE-DEAD-SUBSCRIBER-MARKER";
    push(&state, &caller, marker).await;

    assert!(
        wait_for_delivery_state(&state, "error").await,
        "known-dead source subscriber must become terminal"
    );
    let (delivery_state, error_code, attempt_count) = delivery_state(&state).await;
    assert_eq!(delivery_state, "error");
    assert_eq!(error_code.as_deref(), Some("target_dead"));
    assert_eq!(attempt_count, 0, "wake failure is not an injection attempt");
    assert!(
        !mock.injected_prompts().iter().any(|p| p.contains(marker)),
        "dead subscriber must not receive an injection"
    );
}

#[tokio::test]
async fn source_push_retries_non_dead_revive_before_terminalizing() {
    let mock = MockAdapter::new();
    mock.fail_open_session("fixture transport cannot open");
    let state = state_with_mock(mock.clone()).await;
    state.identity.register(operator_request()).await.unwrap();
    let caller = state.identity.resolve(PROJECT, "operator").await.unwrap();
    setup_source(&state).await;

    let registered = state
        .identity
        .register(agent_request(
            "unreachable-subscriber",
            "unreachable-client",
        ))
        .await
        .unwrap();
    Topics::new(&state.store)
        .subscribe(TOPIC, &registered.session_id.0, None)
        .await
        .unwrap();
    state
        .mark_session_offline(&registered.session_id)
        .await
        .unwrap();

    let marker = "SOURCE-UNREACHABLE-SUBSCRIBER-MARKER";
    push(&state, &caller, marker).await;

    assert!(wait_for_revive_backoff(&state, &registered.session_id).await);
    assert!(wait_for_delivery_state(&state, "pending").await);
    // Advance the process-local failure ledger to an expired second backoff, then let the common
    // recovery sweep perform the third real revive attempt.
    state.record_pending_respawn_failure(&registered.session_id, 0);
    state.respawn_pending_agents().await;
    assert!(
        wait_for_delivery_state(&state, "error").await,
        "third failed source-subscriber revive must become terminal"
    );
    let (delivery_state, error_code, attempt_count) = delivery_state(&state).await;
    assert_eq!(delivery_state, "error");
    assert_eq!(error_code.as_deref(), Some("target_unreachable"));
    assert_eq!(
        attempt_count, 0,
        "failed revive is not an injection attempt"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !mock.injected_prompts().iter().any(|p| p.contains(marker)),
        "revive failures must never become harness injections"
    );
}

#[tokio::test]
async fn reclaimed_source_command_does_not_repeat_an_exhausted_revive() {
    let adapter = CountingFailOpenAdapter::default();
    let opens = adapter.opens.clone();
    let state = state_with_counting_fail_adapter(adapter).await;
    let operator = state.identity.register(operator_request()).await.unwrap();
    setup_source(&state).await;

    let registered = state
        .identity
        .register(agent_request(
            "reclaim-unreachable",
            "reclaim-unreachable-client",
        ))
        .await
        .unwrap();
    Topics::new(&state.store)
        .subscribe(TOPIC, &registered.session_id.0, None)
        .await
        .unwrap();
    state
        .mark_session_offline(&registered.session_id)
        .await
        .unwrap();

    let request = PushRequest {
        source: SOURCE.into(),
        topic: None,
        summary: Some("terminal reclaim".into()),
        body: "SOURCE-TERMINAL-RECLAIM".into(),
        meta: None,
    };
    let intents = CommandIntents::new(&state.store);
    intents
        .insert_pending(NewCommandIntent {
            command_id: "cmd_source_terminal_reclaim".into(),
            kind: command_kinds::source::PUSH.into(),
            project: PROJECT.into(),
            caller_name: "operator".into(),
            caller_session_id: Some(operator.session_id.0.clone()),
            caller_agent_id: operator.agent_id.as_ref().map(|id| id.0.clone()),
            caller_runtime_id: Some(operator.session_id.0.clone()),
            caller_client_key: Some("source-wake-operator-client".into()),
            caller_principal_id: None,
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: Some("source-terminal-reclaim-key".into()),
            request_json: serde_json::to_string(&request).unwrap(),
            created_at: 1,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let first_command = intents
        .get("cmd_source_terminal_reclaim")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        first_command.status, "done",
        "source command failed before delivery: {:?}",
        first_command.error_json
    );
    assert!(wait_for_revive_backoff(&state, &registered.session_id).await);
    assert!(wait_for_delivery_state(&state, "pending").await);
    state.record_pending_respawn_failure(&registered.session_id, 0);
    state.respawn_pending_agents().await;
    assert!(wait_for_delivery_state(&state, "error").await);
    let first_wake_opens = opens.load(Ordering::SeqCst);
    assert!(first_wake_opens > 0, "first wake must attempt a revive");

    state
        .store
        .conn
        .execute(
            "UPDATE command_intents SET status = 'claimed', result_json = NULL, error_json = NULL, \
             claimed_at = 1, started_at = NULL, lease_until = 1, completed_at = NULL \
             WHERE command_id = 'cmd_source_terminal_reclaim'",
            (),
        )
        .await
        .unwrap();
    assert!(command_worker::process_next(&state).await.unwrap());
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        opens.load(Ordering::SeqCst),
        first_wake_opens,
        "terminal delivery must never trigger another automatic revive"
    );
    let (state_name, error_code, attempt_count) = delivery_state(&state).await;
    assert_eq!(state_name, "error");
    assert_eq!(error_code.as_deref(), Some("target_unreachable"));
    assert_eq!(attempt_count, 0);
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
