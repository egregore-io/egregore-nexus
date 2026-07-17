//! Task 11 — two-instance LIVE e2e: TWO REAL `claude` harnesses talk over the PTY-native bus.
//!
//! The live variant of `pty_agent_to_agent` (which used two `cat` PTYs). We boot the production
//! PTY-native wiring ([`AppState::wire_pty`]) and `launch` TWO REAL `claude` agents (ada, ben) via
//! the `launch` dispatch (kind=claude → [`harness_program`] = `"claude"`). Each runs in its own
//! daemon-owned PTY with its own transcript tailer (Task 8a).
//!
//! - DM leg: the operator dispatches a `send` AS ada → a DM to ben. The bus rings ben's bell → his
//!   drain loop injects the rendered `<nexus from=ada …>` envelope into ben's real claude PTY. We
//!   assert ben's `stream_events` gains a row whose data carries the DM body / the `<nexus` envelope
//!   — the DM was delivered into ben's real PTY and echoed into his transcript → store.
//! - Thread leg: create a thread with both, ada posts; assert ben's `stream_events` gains the post.
//!
//! claude is non-deterministic, so
//! generous timeouts + substring search; a missing/unauthenticated `claude` is a SKIP; both spawned
//! harnesses are killed at the end.
//!
//! ```bash
//! NEXUS_LIVE_E2E=1 cargo test -p nexus-acceptance --test pty_agent_to_agent_live -- --nocapture --test-threads=1
//! ```

use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use nexus::daemon::{dispatch, AppState};
use nexus_common::Config;
use nexus_contracts::ids::SessionId;
use nexus_contracts::{
    Caller, CreateThreadRequest, Harness, Request, RequestId, SendRequest, SendTarget,
    SpawnRequest, SpawnResponse, ThreadMemberRequest, Tier,
};
use nexus_store::repos::StreamEvents;
use nexus_store::Store;

const PROJECT: &str = "egregore";

/// Live real-harness e2e is opt-in so the default acceptance gate stays deterministic.
fn live_e2e_enabled() -> bool {
    matches!(
        std::env::var("NEXUS_LIVE_E2E").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

/// Best-effort PATH probe: `claude --version` exits 0. SKIP (not fail) when absent.
fn claude_available() -> bool {
    std::process::Command::new("claude")
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Drive one dispatch call and unwrap a typed success result (panics on RPC error).
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
    serde_json::from_value(resp.result.unwrap_or(serde_json::Value::Null)).unwrap()
}

/// A fresh, unique working directory for a launched harness, so its derived transcript dir is BRAND
/// NEW — no stale JSONL from a prior run that the tailer could replay into the assertion.
fn unique_cwd(label: &str) -> String {
    let uniq = nexus_common::new_session_id().0;
    let dir = std::env::temp_dir().join(format!("nexus-pty-a2a-{label}-{uniq}"));
    std::fs::create_dir_all(&dir).expect("create unique agent cwd");
    dir.to_string_lossy().into_owned()
}

/// Launch a REAL claude agent named `name` via the `launch` dispatch (operator caller). Returns its
/// session id (keyed for stream_events). Spawns in a daemon-owned PTY (fresh cwd) + starts its tailer.
async fn launch_claude(state: &AppState, name: &str) -> SessionId {
    let operator = Caller {
        agent_id: None,
        session: nexus_common::new_session_id(),
        name: "operator".into(),
        project: PROJECT.into(),
        tier: Tier::Admin,
    };
    let resp: SpawnResponse = call(
        state,
        Some(operator),
        "launch",
        SpawnRequest {
            kind: Harness::Claude,
            name: Some(name.into()),
            identity_policy: None,
            role: None,
            project: Some(PROJECT.into()),
            cwd: Some(unique_cwd(name)),
            initial_prompt: None,
            resume: None,
            headless: false, // headed: this proof drives the PTY/tmux path
            harness_args: Vec::new(),
            backend: None,
        },
    )
    .await;
    resp.session_id
}

/// The caller identity for a launched agent dispatching as itself (name + project + session).
fn caller_for(name: &str, session: &SessionId) -> Caller {
    Caller {
        agent_id: None,
        session: session.clone(),
        name: name.into(),
        project: PROJECT.into(),
        tier: Tier::Agent,
    }
}

/// Poll `stream_events` for `session` until a row's data contains `needle` (any kind — the delivered
/// turn is echoed into the recipient's transcript as a `user_input`/`text`), or the deadline passes.
/// Returns whether it was seen plus the rows observed last (for a useful failure message).
async fn wait_for_event(
    store: &Store,
    session: &SessionId,
    needle: &str,
    secs: u64,
) -> (bool, Vec<(String, String)>) {
    let events = StreamEvents::new(store);
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    let mut last_seen = Vec::new();
    while std::time::Instant::now() < deadline {
        let rows = events.since(session, 0).await.unwrap();
        last_seen = rows
            .iter()
            .map(|r| (r.kind.clone(), r.data.clone()))
            .collect();
        if rows.iter().any(|r| r.data.contains(needle)) {
            return (true, last_seen);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    (false, last_seen)
}

/// Kill a launched harness via the daemon-owned supervisor (best-effort; never leak a real claude).
fn kill_harness(state: &AppState, session: &SessionId) {
    if let Some(sup) = state.pty_supervisor() {
        let _ = sup.kill(session);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn two_live_claude_agents_dm_and_thread_over_the_pty_bus() {
    if !live_e2e_enabled() {
        eprintln!("SKIP: set NEXUS_LIVE_E2E=1 to run the live two-agent Claude PTY e2e");
        return;
    }
    if !claude_available() {
        eprintln!("SKIP: `claude` not on PATH — two-instance live e2e needs a real harness binary");
        return;
    }

    // Boot the production PTY-native daemon and launch TWO real claude agents.
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let state = AppState::wire_pty(store.clone(), &Config::default());

    let ada = launch_claude(&state, "ada").await;
    let ben = launch_claude(&state, "ben").await;

    // --- DM leg: ada DMs ben. The bus delivers the rendered <nexus from=ada …> envelope into ben's
    // real claude PTY; ben's transcript tailer projects it (as a user_input/text row) to the store.
    let dm_marker = format!("PTY-DM-{}", &nexus_common::new_session_id().0[..8]);
    let dm_body = format!("hello ben, this is an e2e DM. marker {dm_marker}");
    let _ack: nexus_contracts::Ack = call(
        &state,
        Some(caller_for("ada", &ada)),
        "send",
        SendRequest {
            to: SendTarget::dm_name("ben"),
            summary: None,
            body: dm_body,
            mention: vec![],
            idempotency_key: None,
        },
    )
    .await;
    let (dm_seen, dm_rows) = wait_for_event(&store, &ben, &dm_marker, 60).await;

    // --- Thread leg: a thread with both members; ada posts → fans out to ben's PTY → his transcript.
    let post_marker = format!("PTY-POST-{}", &nexus_common::new_session_id().0[..8]);
    let _: serde_json::Value = call(
        &state,
        Some(caller_for("ada", &ada)),
        "thread.new",
        CreateThreadRequest {
            name: "standup".into(),
            members: vec![],
        },
    )
    .await;
    for m in ["ada", "ben"] {
        let _: serde_json::Value = call(
            &state,
            Some(caller_for("ada", &ada)),
            "thread.addMember",
            ThreadMemberRequest {
                name: "standup".into(),
                member: m.into(),
            },
        )
        .await;
    }
    let post_body = format!("standup: what did you do? marker {post_marker}");
    let _ack: nexus_contracts::Ack = call(
        &state,
        Some(caller_for("ada", &ada)),
        "send",
        SendRequest {
            to: SendTarget::Post {
                thread: "standup".into(),
            },
            summary: None,
            body: post_body,
            mention: vec![],
            idempotency_key: None,
        },
    )
    .await;
    let (post_seen, post_rows) = wait_for_event(&store, &ben, &post_marker, 60).await;

    // Kill both harnesses regardless of outcome.
    kill_harness(&state, &ada);
    kill_harness(&state, &ben);

    assert!(
        dm_seen,
        "ada's DM must be delivered into ben's real claude PTY and tailed to his stream_events \
         (looked for the marker in any row). saw: {dm_rows:?}"
    );
    assert!(
        post_seen,
        "ada's thread post must reach ben's real claude PTY and be tailed to his stream_events. \
         saw: {post_rows:?}"
    );
}
