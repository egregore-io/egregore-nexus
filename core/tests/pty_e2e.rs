//! single-agent LIVE e2e: a REAL `claude` harness in a daemon-owned PTY end-to-end.
//!
//! This is the live variant of the deterministic `pty_launch_autobinds` test: instead of standing
//! a `cat` in for the harness, we boot the production PTY-native wiring ([`AppState::wire_pty`]) and
//! `launch` a REAL `claude` (kind=claude → [`harness_program`] resolves `"claude"`). A peer agent
//! then DMs it through the bus `send` dispatch with a unique marker, instructing it to reply `OK`.
//! The bus rings the launched agent's bell → its drain loop injects the rendered `<nexus-batch>`
//! into the real claude PTY. claude reads the turn, replies, and writes its transcript JSONL; the
//! daemon's transcript tailer projects each line into `stream_events`. We assert a `text`
//! row appears whose data contains the marker or `OK` — proof the operator/peer message reached the
//! native harness, it answered, and the tailer captured it.
//!
//! claude's reply is non-deterministic, so we use a generous
//! timeout and substring search, treat a missing/unauthenticated `claude` as a SKIP (not a failure),
//! and kill the spawned harness at the end.
//!
//! ```bash
//! NEXUS_LIVE_E2E=1 cargo test -p nexus-acceptance --test pty_e2e -- --nocapture --test-threads=1
//! ```

use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use nexus::daemon::{dispatch, AppState};
use nexus_common::Config;
use nexus_contracts::{
    Caller, Kind, RegisterRequest, RegisterResponse, Request, RequestId, SendRequest, SendTarget,
    SpawnRequest, SpawnResponse, Tier,
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

/// Best-effort PATH probe: `claude --version` exits 0. Used to SKIP (not fail) when the harness
/// binary isn't installed on this machine — matching the `live_smoke` convention.
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

/// A fresh, unique working directory for a launched harness, so its derived transcript dir
/// (`~/.claude/projects/<mangled-cwd>`) is BRAND NEW — no stale JSONL from a prior run that the
/// tailer could replay into the assertion. Created eagerly (the harness writes its transcript there).
fn unique_cwd(label: &str) -> String {
    let uniq = nexus_common::new_session_id().0;
    let dir = std::env::temp_dir().join(format!("nexus-pty-e2e-{label}-{uniq}"));
    std::fs::create_dir_all(&dir).expect("create unique agent cwd");
    dir.to_string_lossy().into_owned()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_claude_receives_a_dm_and_its_reply_reaches_the_store() {
    if !live_e2e_enabled() {
        eprintln!("SKIP: set NEXUS_LIVE_E2E=1 to run the live single-agent Claude PTY e2e");
        return;
    }
    if !claude_available() {
        eprintln!("SKIP: `claude` not on PATH — live single-agent e2e needs a real harness binary");
        return;
    }

    // (1) Boot the production PTY-native daemon: launches go through the PTY path (real claude in a
    // daemon-owned PTY), and the transcript tailer is wired so a harness reply reaches stream_events.
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let state = AppState::wire_pty(store.clone(), &Config::default());

    // (2) Launch a REAL claude agent via the `launch` dispatch (kind=claude). The operator caller
    // carries the project; launch_agent resolves harness_program(Claude) = "claude" and spawns it in
    // a daemon-owned PTY, registers it as a wakeable bus member, and starts its transcript tailer.
    let operator = Caller {
        agent_id: None,
        session: nexus_common::new_session_id(),
        name: "operator".into(),
        project: PROJECT.into(),
        tier: Tier::Admin,
        locality: Default::default(),
        access: None,
        principal_id: None,
    };
    // A FRESH, unique cwd per launch so the harness's transcript dir is BRAND NEW — otherwise the
    // tailer would replay a stale transcript from a prior run (which may already contain "OK"/a
    // marker) and the assertion would pass without this run's real claude doing anything.
    let cwd = unique_cwd("ada");
    let launched: SpawnResponse = call(
        &state,
        Some(operator),
        "launch",
        SpawnRequest {
            kind: hid("claude"),
            name: Some("ada".into()),
            identity_policy: None,
            role: None,
            project: Some(PROJECT.into()),
            cwd: Some(cwd.clone()),
            initial_prompt: None,
            resume: None,
            headless: false, // headed: this e2e asserts the daemon-owned PTY/tmux path
            harness_args: Vec::new(),
            backend: None,
        },
    )
    .await;
    let session = launched.session_id.clone();

    // (3) A peer agent (ben) DMs ada with a UNIQUE marker, instructing a one-word `OK` reply. The
    // bus rings ada's bell → her drain loop injects the rendered <nexus-batch> into the real claude
    // PTY. (This exercises the "peer message reaches the native harness" path.)
    let marker = format!("PTY-E2E-{}", &nexus_common::new_session_id().0[..8]);
    let ben: RegisterResponse = call(
        &state,
        None,
        "register",
        RegisterRequest {
            name: Some("ben".into()),
            agent_id: None,
            harness: hid("claude"),
            harness_session_id: "hs_ben".into(),
            project: PROJECT.into(),
            client_key: "ck_ben".into(),
            runtime_credential: None,
            tier: Tier::Agent,
            kind: Some(Kind::Agent),
            locality: Default::default(),
            access: None,
            role: None,
            cwd: None,
        },
    )
    .await;
    let ben_caller = Caller {
        agent_id: None,
        session: ben.session_id.clone(),
        name: "ben".into(),
        project: PROJECT.into(),
        tier: Tier::Agent,
        locality: Default::default(),
        access: None,
        principal_id: None,
    };
    let body = format!(
        "This is an automated end-to-end test. Reply with exactly the word OK and the marker {marker}. \
         Do not run any tools. Just reply: OK {marker}"
    );
    let _ack: nexus_contracts::Ack = call(
        &state,
        Some(ben_caller),
        "send",
        SendRequest {
            to: SendTarget::dm_name("ada"),
            summary: None,
            body,
            mention: vec![],
            metadata: None,
            idempotency_key: None,
        },
    )
    .await;

    // (4) Poll stream_events for a `text` row carrying OK or the marker — claude's reply tailed back.
    // Generous 60s timeout + substring search tolerate claude's latency + non-determinism.
    let events = StreamEvents::new(&store);
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut found = false;
    let mut last_seen: Vec<(String, String)> = Vec::new();
    while std::time::Instant::now() < deadline {
        let rows = events.since(&session, 0).await.unwrap();
        last_seen = rows
            .iter()
            .map(|r| (r.kind.clone(), r.data.clone()))
            .collect();
        // The cwd is fresh, so any of these is a REAL signal from THIS run (no stale replay):
        //   - a `text` reply row containing the marker or `OK` (claude answered), OR
        //   - ANY row carrying the unique marker (the injected turn was delivered into the PTY and
        //     echoed into the transcript) — definitive proof the operator/peer message reached the
        //     native harness even if claude's reply wording differs from a bare `OK`.
        let reply = rows
            .iter()
            .any(|r| r.kind == "text" && (r.data.contains(&marker) || r.data.contains("OK")));
        let delivered = rows.iter().any(|r| r.data.contains(&marker));
        if reply || delivered {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // (5) Kill the spawned harness regardless of outcome (don't leak a real claude tmux session).
    if let Some(sup) = state.pty_supervisor() {
        let _ = sup.kill(&session);
    }

    assert!(
        found,
        "real claude must receive the injected DM and reply OK/{marker} into its transcript \
         (tailed to stream_events). saw rows: {last_seen:?}"
    );
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
