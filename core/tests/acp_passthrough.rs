//! ACP full-stream pass-through e2e.
//!
//! The daemon must forward the **whole** ACP `session/update` stream as tagged `agent.update`
//! events — text, thinking, tool calls, plans, and the harness's own available commands — never
//! just the reply text (the old `engine.rs` AgentMessageChunk-only drop is gone). This test proves
//! it end to end against the hermetic **fake ACP agent** subprocess, driven through the **production**
//! daemon wiring (`AppState::wire_with_registry` → real `CodexAdapter`/`AcpEngine` → fake harness).
//!
//! The fake harness, in `FAKE_ACP_REPLY=passthrough` mode, scripts one turn that streams, in order:
//!   `AvailableCommandsUpdate → AgentThoughtChunk → ToolCall → ToolCallUpdate(completed) →
//!    AgentMessageChunk → turn-end`.
//! The daemon relays each renderable update as a `WsEvent::AgentUpdate { kind, data }`; broadcast as
//! a JSON-RPC notification (`method == "agent.update"`, `params.kind` the snake_case kind). The
//! emitted kinds, in order, must be: `commands, thinking, tool_call, tool_call, text`.

mod common;
use common::hid;

use std::sync::Arc;

use nexus::daemon::{dispatch, AppState};
use nexus_agent::adapter::engine::HarnessCommand;
use nexus_agent::{Adapter, AdapterRegistry};
use nexus_common::Config;
use nexus_contracts::{
    Caller, Notification, Request, RequestId, SendRequest, SendTarget, SpawnRequest, Tier,
};
use nexus_harness_codex::CodexAdapter;
use nexus_store::Store;
use tokio::sync::broadcast;

/// The compiled fake harness binary (Cargo injects `CARGO_BIN_EXE_acceptance_fake_acp_agent`
/// because the
/// acceptance crate re-declares it as a `[[bin]]`).
const FAKE_HARNESS: &str = env!("CARGO_BIN_EXE_acceptance_fake_acp_agent");

const DEMO: &str = "demo";

/// A [`HarnessCommand`] that launches the fake ACP agent in **pass-through** mode (so a turn streams
/// the full renderable `session/update` sequence). The reply mode is set per-child via the command's
/// `env` so parallel tests never race on a process-global env var.
fn fake_passthrough_command() -> HarnessCommand {
    HarnessCommand {
        program: FAKE_HARNESS.to_string(),
        args: vec![],
        cwd: None,
        env: vec![("FAKE_ACP_REPLY".to_string(), "passthrough".to_string())],
    }
}

/// Production daemon wiring whose registry mints the **real** `CodexAdapter` (real `AcpEngine`)
/// pointed at the fake harness in pass-through mode, for both `claude` and `codex`.
async fn wire_real_passthrough() -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();

    let mut registry = AdapterRegistry::new();
    registry.register(
        &hid("codex"),
        Arc::new(|_cwd| {
            Arc::new(CodexAdapter::with_command(fake_passthrough_command())) as Arc<dyn Adapter>
        }),
    );
    registry.register(
        &hid("claude"),
        Arc::new(|_cwd| {
            Arc::new(CodexAdapter::with_command(fake_passthrough_command())) as Arc<dyn Adapter>
        }),
    );

    AppState::wire_with_registry(store, &Config::default(), registry)
}

/// A resolved caller for the white-box `dispatch` calls. `boss` is an admin so it may `launch`.
fn caller(name: &str, project: &str, tier: Tier) -> Caller {
    Caller {
        agent_id: None,
        session: nexus_common::new_session_id(),
        name: name.into(),
        project: project.into(),
        tier,
    }
}

/// Drive one `dispatch` call and unwrap a typed success result (panics on RPC error).
async fn call<P: serde::Serialize, T: serde::de::DeserializeOwned>(
    state: &AppState,
    caller: Option<Caller>,
    method: &str,
    params: P,
) -> T {
    let req = Request {
        jsonrpc: "2.0".into(),
        id: Some(RequestId::Num(1)),
        method: method.into(),
        params: Some(serde_json::to_value(params).unwrap()),
    };
    let resp = dispatch(state, caller, req).await;
    if let Some(e) = resp.error {
        panic!("dispatch({method}) error: [{}] {}", e.code, e.message);
    }
    serde_json::from_value(resp.result.expect("a result")).unwrap()
}

/// Collect the `kind` of the first `count` `agent.update` notifications, in order, up to a timeout.
async fn collect_agent_update_kinds(
    rx: &mut broadcast::Receiver<Notification>,
    count: usize,
    timeout_ms: u64,
) -> Vec<String> {
    let deadline = std::time::Duration::from_millis(timeout_ms);
    let watch = async {
        let mut kinds = Vec::new();
        loop {
            match rx.recv().await {
                Ok(n) if n.method == "agent.update" => {
                    if let Some(kind) = n
                        .params
                        .as_ref()
                        .and_then(|p| p.get("kind"))
                        .and_then(|k| k.as_str())
                    {
                        kinds.push(kind.to_string());
                        if kinds.len() >= count {
                            return kinds;
                        }
                    }
                }
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return kinds,
            }
        }
    };
    tokio::time::timeout(deadline, watch)
        .await
        .unwrap_or_default()
}

/// THE pass-through proof: one turn's full ACP `session/update` stream is relayed, in order, as
/// tagged `agent.update` events — `commands, thinking, tool_call, tool_call, text`.
///
/// Multi-threaded runtime to match the production daemon (`#[tokio::main]`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_acp_stream_relays_as_ordered_tagged_agent_updates() {
    let state = wire_real_passthrough().await;

    // Subscribe BEFORE the turn so we never miss a relayed event.
    let mut rx = state.ws.subscribe();

    let boss = caller("boss", DEMO, Tier::Admin);

    // (1) Launch a real-adapter agent (spawns the fake harness + ACP initialize + session/new).
    let spawn = SpawnRequest {
        kind: hid("codex"),
        name: Some("worker1".into()),
        identity_policy: None,
        cwd: None,
        project: None,
        role: None,
        initial_prompt: None,
        resume: None,
        headless: false, // wire_with_registry has no PTY → ACP path regardless; explicit for clarity
        harness_args: Vec::new(),
        backend: None,
    };
    let launched: nexus_contracts::SpawnResponse =
        call(&state, Some(boss.clone()), "launch", spawn).await;
    assert!(
        !launched.session_id.0.is_empty(),
        "launch returns a session id"
    );

    // (2) DM worker1 → bus wakes it → loop drains → inject_turn → ACP session/prompt → the fake
    // harness streams the full pass-through sequence.
    let _ack: nexus_contracts::Ack = call(
        &state,
        Some(boss.clone()),
        "send",
        SendRequest {
            to: SendTarget::dm_name("worker1"),
            summary: None,
            body: "go".into(),
            mention: vec![],
            idempotency_key: None,
        },
    )
    .await;

    // (3) The daemon must make the delivered bus input visible on the Agent Session stream before
    // relaying the ordered ACP reply stream: commands, thinking, tool_call×2, text.
    let kinds = collect_agent_update_kinds(&mut rx, 6, 5_000).await;
    assert_eq!(
        kinds,
        vec![
            "user_input",
            "commands",
            "thinking",
            "tool_call",
            "tool_call",
            "text"
        ],
        "bus input must precede the full ACP session/update stream; got {kinds:?}"
    );
}
