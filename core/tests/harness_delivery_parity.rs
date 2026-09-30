//! Deterministic public-v0.1 harness delivery parity.
//!
//! Every case uses the concrete harness adapter over a real fake-ACP subprocess. No model, network,
//! host daemon, or host Nexus state participates.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use nexus::daemon::AppState;
use nexus_agent::adapter::engine::HarnessCommand;
use nexus_agent::adapter::OpenCodeAdapter;
use nexus_agent::{Adapter, AdapterRegistry, HermesAdapter};
use nexus_common::Config;
use nexus_contracts::{
    HarnessId, Kind, RegisterRequest, SendRequest, SendTarget, SpawnRequest, Tier,
};
use nexus_harness_claude::ClaudeAdapter;
use nexus_harness_codex::CodexAdapter;
use nexus_store::Store;

const FAKE_HARNESS: &str = env!("CARGO_BIN_EXE_acceptance_fake_acp_agent");
const PROJECT: &str = "harness-parity";

fn fake_command(env: Vec<(String, String)>) -> HarnessCommand {
    HarnessCommand {
        program: FAKE_HARNESS.to_string(),
        args: vec![],
        cwd: None,
        env,
    }
}

fn register_actual_adapter(
    registry: &mut AdapterRegistry,
    harness: &HarnessId,
    command: HarnessCommand,
) {
    match harness.as_str() {
        "claude" => registry.register(
            harness,
            Arc::new(move |_ctx| {
                Arc::new(ClaudeAdapter::with_command(command.clone())) as Arc<dyn Adapter>
            }),
        ),
        "codex" => registry.register(
            harness,
            Arc::new(move |_ctx| {
                Arc::new(CodexAdapter::with_command(command.clone())) as Arc<dyn Adapter>
            }),
        ),
        "opencode" => registry.register(
            harness,
            Arc::new(move |_ctx| {
                Arc::new(OpenCodeAdapter::with_command(command.clone())) as Arc<dyn Adapter>
            }),
        ),
        "hermes" => registry.register(
            harness,
            Arc::new(move |_ctx| {
                Arc::new(HermesAdapter::with_command(command.clone())) as Arc<dyn Adapter>
            }),
        ),
        _ => panic!("not in the public v0.1 harness parity matrix"),
    }
}

async fn state_with_adapters(configs: &[(HarnessId, HarnessCommand)]) -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let mut registry = AdapterRegistry::new();
    for (harness, command) in configs {
        register_actual_adapter(&mut registry, harness, command.clone());
    }
    AppState::wire_with_registry(store, &Config::default(), registry)
}

fn operator_request() -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some("parity-operator".into()),
        harness: hid("other"),
        harness_session_id: "parity-operator-native".into(),
        project: PROJECT.into(),
        client_key: "parity-operator-client".into(),
        runtime_credential: None,
        tier: Tier::Admin,
        kind: Some(Kind::Human),
        role: None,
        cwd: None,
    }
}

fn spawn_request(harness: HarnessId, name: &str) -> SpawnRequest {
    SpawnRequest {
        kind: harness,
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

async fn delivery_row(state: &AppState, message_id: &str) -> Option<(String, Option<String>, i64)> {
    let mut rows = state
        .store
        .conn
        .query(
            "SELECT state, error_code, attempt_count FROM in_flight WHERE message_id = ?1",
            libsql::params![message_id.to_string()],
        )
        .await
        .unwrap();
    rows.next().await.unwrap().map(|row| {
        (
            row.get(0).unwrap(),
            row.get(1).unwrap(),
            row.get(2).unwrap(),
        )
    })
}

async fn wait_for_state(state: &AppState, message_id: &str, expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if delivery_row(state, message_id)
            .await
            .is_some_and(|row| row.0 == expected)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {message_id} -> {expected}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concrete_acp_adapters_deliver_unique_markers_once_without_cross_runtime_leak() {
    let harnesses = [
        (hid("claude"), "parity-claude", "PARITY-CLAUDE-71"),
        (hid("codex"), "parity-codex", "PARITY-CODEX-72"),
        (hid("opencode"), "parity-opencode", "PARITY-OPENCODE-73"),
        (hid("hermes"), "parity-hermes", "PARITY-HERMES-74"),
    ];
    let configs: Vec<_> = harnesses
        .iter()
        .map(|(harness, _, _)| (harness.clone(), fake_command(vec![])))
        .collect();
    let state = state_with_adapters(&configs).await;
    state.identity.register(operator_request()).await.unwrap();
    let caller = state
        .identity
        .resolve(PROJECT, "parity-operator")
        .await
        .unwrap();
    let mut events = state.ws.subscribe();

    let mut targets = HashMap::new();
    for (harness, name, marker) in harnesses {
        let launched = state
            .launch_agent(spawn_request(harness, name), PROJECT, Some(&caller))
            .await
            .unwrap();
        let ack = state
            .bus
            .send(
                &caller,
                SendRequest {
                    to: SendTarget::dm_name(name),
                    summary: None,
                    body: marker.into(),
                    mention: vec![],
                    metadata: None,
                    idempotency_key: Some(format!("parity-{name}")),
                },
            )
            .await
            .unwrap();
        targets.insert(marker.to_string(), (launched.session_id, ack.message_id));
    }

    for (_, message_id) in targets.values() {
        wait_for_state(&state, &message_id.0, "delivered").await;
        let row = delivery_row(&state, &message_id.0).await.unwrap();
        assert_eq!(row.1, None);
        assert_eq!(row.2, 1, "one durable injection attempt per adapter");
    }

    let mut marker_events: HashMap<String, Vec<String>> = HashMap::new();
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let Ok(Ok(notification)) =
            tokio::time::timeout(Duration::from_millis(50), events.recv()).await
        else {
            continue;
        };
        if notification.method != "agent.update" {
            continue;
        }
        let Some(params) = notification.params else {
            continue;
        };
        if params.get("kind").and_then(|v| v.as_str()) != Some("text") {
            continue;
        }
        let Some(text) = params
            .get("data")
            .and_then(|v| v.get("text"))
            .and_then(|v| v.as_str())
        else {
            continue;
        };
        let Some(session) = params.get("sessionId").and_then(|v| v.as_str()) else {
            continue;
        };
        for marker in targets
            .keys()
            .filter(|marker| text.contains(marker.as_str()))
        {
            marker_events
                .entry(marker.clone())
                .or_default()
                .push(session.to_string());
        }
    }

    assert_eq!(marker_events.keys().collect::<HashSet<_>>().len(), 4);
    for (marker, (expected_session, _)) in &targets {
        assert_eq!(
            marker_events.get(marker),
            Some(&vec![expected_session.0.clone()]),
            "{marker} must appear exactly once and only on its addressed runtime"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concrete_acp_adapter_provider_and_contract_errors_never_settle_delivered() {
    for (harness, token) in [
        (hid("claude"), "claude"),
        (hid("codex"), "codex"),
        (hid("opencode"), "opencode"),
        (hid("hermes"), "hermes"),
    ] {
        for (error_data, expected_code) in [
            (
                serde_json::json!({
                    "reason": "usage_limit_exceeded",
                    "provider": "fixture-provider",
                    "model": "fixture-model"
                }),
                "provider_limit",
            ),
            (
                serde_json::json!({ "unclassified": "fixture failure" }),
                "contract_error",
            ),
        ] {
            let command = fake_command(vec![(
                "FAKE_ACP_PROMPT_ERROR_DATA".into(),
                error_data.to_string(),
            )]);
            let state = state_with_adapters(&[(harness.clone(), command)]).await;
            state.identity.register(operator_request()).await.unwrap();
            let caller = state
                .identity
                .resolve(PROJECT, "parity-operator")
                .await
                .unwrap();
            let mut events = state.ws.subscribe();
            let name = format!("{token}-{expected_code}");
            state
                .launch_agent(
                    spawn_request(harness.clone(), &name),
                    PROJECT,
                    Some(&caller),
                )
                .await
                .unwrap();
            let ack = state
                .bus
                .send(
                    &caller,
                    SendRequest {
                        to: SendTarget::dm_name(name.clone()),
                        summary: None,
                        body: format!("ERROR-MARKER-{token}-{expected_code}"),
                        mention: vec![],
                        metadata: None,
                        idempotency_key: None,
                    },
                )
                .await
                .unwrap();

            wait_for_state(&state, &ack.message_id.0, "error").await;
            let row = delivery_row(&state, &ack.message_id.0).await.unwrap();
            assert_eq!(row.0, "error");
            assert_eq!(
                row.1.as_deref(),
                Some(expected_code),
                "{token} must preserve {expected_code} through its concrete adapter"
            );
            assert_eq!(row.2, 1);

            tokio::time::sleep(Duration::from_millis(20)).await;
            let mut emitted_delivery = false;
            while let Ok(notification) = events.try_recv() {
                if notification.method == "message.delivered" {
                    emitted_delivery = true;
                }
            }
            assert!(
                !emitted_delivery,
                "{token} {expected_code} must not emit message.delivered"
            );
        }
    }
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
