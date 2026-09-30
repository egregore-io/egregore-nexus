//! Real-adapter wake→inject reproduction (the gap the mock lifecycle test does NOT catch).
//!
//! `lifecycle.rs::launch_makes_agent_addressable_and_wakeable_then_remove` wires the hermetic
//! [`nexus_agent::MockAdapter`], whose `inject` synchronously pushes a string and whose
//! `stream_updates` returns instantly — so it short-circuits the real
//! `EventLoop → inject_turn → adapter.inject → ACP session/prompt` chain. A live run against a
//! REAL harness produced **zero** activity after a DM, which the mock path can never reveal.
//!
//! This test stands up the **production** daemon wiring ([`AppState::wire_with_registry`]) with a
//! registry whose factory builds the REAL `CodexAdapter` (its actual [`AcpEngine`])
//! via `CodexAdapter::with_command`, pointed at the hermetic **fake ACP agent** subprocess
//! (`CARGO_BIN_EXE_acceptance_fake_acp_agent`) — a real process speaking the genuine ACP wire
//! protocol. It
//! then drives the full chain through the daemon's `dispatch`:
//!
//! 1. `dispatch("launch", boss@demo, name=worker1)` — real adapter → fake harness (spawn +
//!    ACP `initialize` + `session/new`),
//! 2. `dispatch("send", dm→worker1)`,
//! 3. assert the fake ACP harness received a `session/prompt` (the injected turn) — observed via
//!    the relayed `agent.update` echo (the fake echoes every prompt body back as
//!    `session/update` chunks), polled with a timeout.
//!
//! With the wake→inject path broken, step 3 never happens (the live failure). With it wired, the
//! echo arrives.

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

/// The compiled fake harness binary. `CARGO_BIN_EXE_acceptance_fake_acp_agent` is injected by
/// Cargo because
/// the acceptance crate re-declares it as a `[[bin]]` (see `tests/Cargo.toml`).
const FAKE_HARNESS: &str = env!("CARGO_BIN_EXE_acceptance_fake_acp_agent");

const DEMO: &str = "demo";

/// A [`HarnessCommand`] that launches the fake ACP agent (no args, inherit cwd).
fn fake_command() -> HarnessCommand {
    HarnessCommand {
        program: FAKE_HARNESS.to_string(),
        args: vec![],
        cwd: None,
        ..Default::default()
    }
}

/// A [`HarnessCommand`] for the fake ACP agent with `FAKE_ACP_FAIL_LOAD` — its `session/load`
/// answers an error ("Resource not found") while `session/new` keeps working. This reproduces the
/// live cold-boot reality: after a daemon restart the stored ACP resume key is always stale, so the
/// not-fresh agent's `ensure_live` MUST fall back from `session/load` to a fresh `session/new`.
fn fake_command_failing_load() -> HarnessCommand {
    HarnessCommand {
        program: FAKE_HARNESS.to_string(),
        args: vec![],
        cwd: None,
        env: vec![("FAKE_ACP_FAIL_LOAD".to_string(), "1".to_string())],
    }
}

/// Production daemon wiring whose `codex`/`claude` factory mints a REAL `CodexAdapter` over the fake
/// harness with `FAKE_ACP_FAIL_LOAD` — so a not-fresh agent's `session/load` fails and the daemon's
/// `open_session_for` must fall back to `session/new` on the same connection.
async fn wire_real_against_failing_load_fake() -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let mut registry = AdapterRegistry::new();
    registry.register(
        &hid("codex"),
        Arc::new(|_cwd| {
            Arc::new(CodexAdapter::with_command(fake_command_failing_load())) as Arc<dyn Adapter>
        }),
    );
    registry.register(
        &hid("claude"),
        Arc::new(|_cwd| {
            Arc::new(CodexAdapter::with_command(fake_command_failing_load())) as Arc<dyn Adapter>
        }),
    );
    AppState::wire_with_registry(store, &Config::default(), registry)
}

/// Build production daemon wiring with a registry that mints the **real** `CodexAdapter` (real
/// `AcpEngine`) pointed at the fake harness for both `claude` and `codex`.
async fn wire_real_against_fake() -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();

    let mut registry = AdapterRegistry::new();
    // Both kinds resolve to a real ACP adapter over the fake harness, so `launch <kind>` is
    // hermetic for either while still exercising the genuine open→inject→stream protocol path.
    registry.register(
        &hid("codex"),
        Arc::new(|_cwd| Arc::new(CodexAdapter::with_command(fake_command())) as Arc<dyn Adapter>),
    );
    registry.register(
        &hid("claude"),
        Arc::new(|_cwd| Arc::new(CodexAdapter::with_command(fake_command())) as Arc<dyn Adapter>),
    );

    AppState::wire_with_registry(store, &Config::default(), registry)
}

/// A resolved caller (the daemon resolves identity server-side; here we build it directly for the
/// white-box `dispatch` calls). `boss` is an admin so it may `launch`.
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

/// Poll the broadcast stream for an `agent.update` whose chunk contains `needle`, up to a timeout.
/// The fake harness echoes every received prompt body back as `session/update` chunks, which the
/// daemon relays as `agent.update` — so observing the echo proves the harness got the
/// `session/prompt` (the injected turn).
async fn await_agent_update_echo(
    rx: &mut broadcast::Receiver<Notification>,
    needle: &str,
    timeout_ms: u64,
) -> bool {
    let deadline = std::time::Duration::from_millis(timeout_ms);
    let watch = async {
        loop {
            match rx.recv().await {
                Ok(n) => {
                    if n.method == "agent.update" {
                        // The tagged agent.update carries `{ kind, data }`; the echoed reply text
                        // lives in `data.text` of the `text`-kind events (the full-stream
                        // pass-through also relays thinking/plan, which we ignore here).
                        if let Some(params) = &n.params {
                            let text = params
                                .get("data")
                                .and_then(|d| d.get("text"))
                                .and_then(|t| t.as_str());
                            if let Some(text) = text {
                                if text.contains(needle) {
                                    return true;
                                }
                            }
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return false,
            }
        }
    };
    tokio::time::timeout(deadline, watch).await.unwrap_or(false)
}

/// THE reproduction: a DM to a LAUNCHED real-adapter agent must reach the harness as an injected
/// `session/prompt`. Mirrors the live scenario end to end, hermetically.
///
/// Runs on the **multi-threaded** runtime (`flavor = "multi_thread"`) to match the production daemon
/// (`#[tokio::main]`), so any cross-task wake/scheduling divergence the single-threaded test runtime
/// would mask is in scope.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dm_to_launched_real_adapter_agent_reaches_harness_as_session_prompt() {
    let state = wire_real_against_fake().await;

    // Subscribe BEFORE the turn so we never miss the relayed echo.
    let mut rx = state.ws.subscribe();

    let boss = caller("boss", DEMO, Tier::Admin);

    // (1) Launch a real-adapter agent named worker1 (spawns the fake harness + ACP initialize +
    // session/new under a daemon-minted session id; registers the member; spawns its EventLoop).
    let spawn = SpawnRequest {
        kind: hid("codex"),
        name: Some("worker1".into()),
        identity_policy: None,
        cwd: None,
        project: None, // mirrors the real `nexus launch <kind> --name worker1` (no --project)
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

    // (2) DM worker1 → bus rings its bell → loop drains → inject_turn → adapter.inject →
    // ACP session/prompt to the fake harness.
    const BODY: &str = "Reply with the single word pong";
    let _ack: nexus_contracts::Ack = call(
        &state,
        Some(boss.clone()),
        "send",
        SendRequest {
            to: SendTarget::dm_name("worker1"),
            summary: None,
            body: BODY.into(),
            mention: vec![],
            idempotency_key: None,
        },
    )
    .await;

    // (3) The fake harness echoes the prompt body back as session/update chunks, relayed as
    // agent.update. Observing the echo proves the harness received the session/prompt turn.
    let reached = await_agent_update_echo(&mut rx, BODY, 5_000).await;
    assert!(
        reached,
        "the DM must reach the launched real-adapter harness as a session/prompt \
         (observed via the relayed agent.update echo); none arrived within the timeout"
    );
}

/// THE boot-respawn liveness regression, reproduced end to end against a REAL adapter.
///
/// This is the exact live failure: on daemon boot, `respawn_pending_agents` brings back an agent
/// that has leftover `in_flight` mail. Its stored ACP resume key is stale (the harness process and
/// its ACP session are gone after a restart), so `ensure_live`'s `session/load` FAILS — the daemon
/// must fall back to a fresh `session/new` on the same connection, end with a LIVE adapter, and only
/// THEN drive the drain loop so the pending message injects. The bug left the loop draining into a
/// not-yet-usable adapter, so the inject failed ("no usable adapter for session") and the message
/// stayed stuck pending forever.
///
/// We model the cold boot with the `FAKE_ACP_FAIL_LOAD` harness (session/load fails, session/new
/// works) + a registered agent carrying a (now stale) resume key + a pending DM, then call the real
/// `respawn_pending_agents()` and assert the DM reaches the harness as a session/prompt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boot_respawn_with_stale_resume_key_delivers_pending_dm() {
    use nexus_contracts::{Kind, RegisterRequest, RegisterResponse};

    let state = wire_real_against_failing_load_fake().await;
    let mut rx = state.ws.subscribe();

    // A registered sender + a registered agent. `register` writes the agent's member row (carrying
    // `harness_session_id` as the resume key) and stands up its idle drain loop, but does NOT open
    // an ACP adapter — exactly the post-restart state: addressable, with a stale resume key, NOT
    // live. `harness_session_id` is a stale ACP id; on respawn `session/load` against it fails.
    let _boss: RegisterResponse = call(
        &state,
        None,
        "register",
        RegisterRequest {
            name: Some("boss".into()),
            agent_id: None,
            harness: hid("codex"),
            harness_session_id: "ck_boss".into(),
            project: DEMO.into(),
            client_key: "ck_boss".into(),
            runtime_credential: None,
            tier: Tier::Admin,
            kind: Some(Kind::Agent),
            role: None,
            cwd: None,
        },
    )
    .await;
    let _worker: RegisterResponse = call(
        &state,
        None,
        "register",
        RegisterRequest {
            name: Some("worker1".into()),
            agent_id: None,
            harness: hid("codex"),
            harness_session_id: "stale-acp-session-uuid".into(),
            project: DEMO.into(),
            client_key: "ck_w1".into(),
            runtime_credential: None,
            tier: Tier::Agent,
            kind: Some(Kind::Agent),
            role: None,
            cwd: None,
        },
    )
    .await;

    // A DM to the not-live agent: enqueued pending + bell rung, but with no live adapter the idle
    // loop's drain finds no usable adapter and HOLDS the row (the stuck state on boot).
    const BODY: &str = "queued before the restart";
    let boss = caller("boss", DEMO, Tier::Admin);
    let _ack: nexus_contracts::Ack = call(
        &state,
        Some(boss.clone()),
        "send",
        SendRequest {
            to: SendTarget::dm_name("worker1"),
            summary: None,
            body: BODY.into(),
            mention: vec![],
            idempotency_key: None,
        },
    )
    .await;

    // THE boot entry point: respawn agents with pending mail. For worker1 this runs `ensure_live`,
    // whose `session/load` fails (stale key) → falls back to `session/new` (LIVE) → drives the loop,
    // which re-drains the held DM and injects it as a session/prompt to the fake harness.
    state.respawn_pending_agents().await;

    let reached = await_agent_update_echo(&mut rx, BODY, 5_000).await;
    assert!(
        reached,
        "after boot respawn with a STALE resume key (session/load fails → session/new fallback), \
         the pending DM must reach the harness as a session/prompt; it stayed stuck instead"
    );
}
