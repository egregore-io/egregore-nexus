//! PTY launch wiring and public-v0.1 context-acceptance safety.
//!
//! A passive native test process launched through `launch_agent_with_program` must auto-bind the
//! daemon-owned PTY, but durable bus delivery must fail closed because raw PTY bytes cannot prove
//! harness context acceptance. Direct operator prompts remain supported and prove the PTY reply
//! reader feeds `stream_events`.

use std::sync::Arc;

use nexus::daemon::{dispatch, AppState};
use nexus_common::Config;
use nexus_contracts::{
    Caller, Harness, Kind, PromptRequest, RegisterRequest, RegisterResponse, Request, RequestId,
    SendRequest, SendTarget, SpawnRequest, Tier,
};
use nexus_store::repos::StreamEvents;
use nexus_store::Store;
use tokio::sync::broadcast;

const PROJECT: &str = "egregore";

#[cfg(unix)]
fn passive_pty_program() -> &'static str {
    "cat"
}

#[cfg(windows)]
fn passive_pty_program() -> &'static str {
    "cmd.exe"
}

/// Boot a real in-test daemon on the PTY-native wiring (its turn-exec is the supervisor's
/// `PtyTransport`; the supervisor is owned by `AppState`).
async fn start() -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    AppState::wire_pty(store, &Config::default())
}

/// Drive one dispatch call and unwrap a typed success result.
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

/// Accumulate PTY output chunks until the decoded buffer contains `needle` or `ms` elapse.
async fn wait_for_pty_text(rx: &mut broadcast::Receiver<Vec<u8>>, needle: &str, ms: u64) -> bool {
    let watch = async {
        let mut buf = Vec::new();
        loop {
            match rx.recv().await {
                Ok(chunk) => {
                    buf.extend_from_slice(&chunk);
                    if String::from_utf8_lossy(&buf).contains(needle) {
                        return true;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return false,
            }
        }
    };
    tokio::time::timeout(std::time::Duration::from_millis(ms), watch)
        .await
        .unwrap_or(false)
}

async fn wait_for_terminal_contract_error(
    state: &AppState,
    message_id: &nexus_contracts::MessageId,
    recipient: &nexus_contracts::SessionId,
) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let mut rows = state
            .store
            .conn
            .query(
                "SELECT state, attempt_count, error_code, error_reason FROM in_flight \
                 WHERE message_id = ?1 AND recipient_session = ?2",
                libsql::params![message_id.0.clone(), recipient.0.clone()],
            )
            .await
            .unwrap();
        if let Some(row) = rows.next().await.unwrap() {
            if row.get::<String>(0).unwrap() == "error" {
                assert_eq!(row.get::<i64>(1).unwrap(), 1);
                assert_eq!(row.get::<String>(2).unwrap(), "contract_error");
                assert!(row
                    .get::<String>(3)
                    .unwrap()
                    .contains("cannot observe context acceptance"));
                return;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "delivery did not settle as terminal contract_error"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn launch_autobinds_pty_but_durable_dm_fails_before_raw_write() {
    let state = start().await;

    // Launch a passive "agent" through the PTY-native path. The program override stands in for the
    // real Claude binary; everything else (session mint, supervisor bind, register_and_wake,
    // transport wiring) is the production path. NO manual `bind` anywhere.
    let launched = state
        .launch_agent_with_program(
            SpawnRequest {
                kind: Harness::Claude,
                name: Some("ada".into()),
                identity_policy: None,
                role: None,
                project: Some(PROJECT.into()),
                cwd: None,
                initial_prompt: None,
                resume: None,
                headless: false, // ignored here (launch_agent_with_program forces PTY), set for the field
                harness_args: Vec::new(),
                backend: None,
            },
            PROJECT,
            passive_pty_program(),
            None,
        )
        .await
        .expect("pty launch");

    // Watch the launched agent's PTY output directly via the daemon-owned supervisor.
    let mut ada_out = state
        .pty_supervisor()
        .expect("wire_pty installs a supervisor")
        .pty_output(&launched.session_id)
        .expect("the launched session is tracked");

    // Register a second agent (ben) as the sender. A registered kind=agent gets its drain loop.
    let ben: RegisterResponse = call(
        &state,
        None,
        "register",
        RegisterRequest {
            name: Some("ben".into()),
            agent_id: None,
            harness: Harness::Claude,
            harness_session_id: "hs_ben".into(),
            project: PROJECT.into(),
            client_key: "ck_ben".into(),
            runtime_credential: None,
            tier: Tier::Agent,
            kind: Some(Kind::Agent),
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
    };

    // ben DMs ada through the bus → ada's bell rings → her drain loop drains the in_flight row →
    // PtyTransport::inject_turn writes the rendered <nexus-batch> into ada's auto-bound PTY.
    let ack: nexus_contracts::Ack = call(
        &state,
        Some(ben_caller),
        "send",
        SendRequest {
            to: SendTarget::dm_name("ada"),
            summary: None,
            body: "ping from ben".into(),
            mention: vec![],
            idempotency_key: None,
        },
    )
    .await;

    assert!(
        !wait_for_pty_text(&mut ada_out, "ping from ben", 250).await,
        "a raw PTY must not receive durable mail without context acceptance evidence"
    );
    wait_for_terminal_contract_error(&state, &ack.message_id, &launched.session_id).await;
}

/// After a headed passive-process launch + prompt, the PTY renders the injected text as PTY bytes.
/// The PtyReplyReader (wired into `launch_agent_with_program`) must pick those bytes up, run them
/// through `ScreenText`, and emit a `text` AgentUpdate that is persisted into `stream_events` by
/// the `WsSink`. This test asserts that marker appears in `stream_events` — proof the reader is
/// wired into the live launch path and feeds the store, WITHOUT reading any provider state file.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn passive_pty_text_reaches_stream_events() {
    // Use a unique marker so we can assert specifically on this test's text, not stale rows.
    let marker = format!("NEXUS_PTY_MARKER_{}", nexus_common::new_session_id().0);

    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let state = AppState::wire_pty(store.clone(), &Config::default());

    // (1) Launch a passive agent on the PTY-native path.
    let launched = state
        .launch_agent_with_program(
            SpawnRequest {
                kind: Harness::Claude,
                name: Some("ada".into()),
                identity_policy: None,
                role: None,
                project: Some(PROJECT.into()),
                cwd: None,
                initial_prompt: None,
                resume: None,
                headless: false,
                harness_args: Vec::new(),
                backend: None,
            },
            PROJECT,
            passive_pty_program(),
            None,
        )
        .await
        .expect("pty launch");

    // (2) Directly prompt ada with the marker text. Unlike durable bus delivery, an operator prompt
    // is allowed to write to a raw PTY. The native process renders it as PTY bytes →
    // PtyReplyReader → ScreenText strips ANSI → Text AgentUpdate → WsSink persists to
    // stream_events.
    let _: nexus_contracts::PromptResponse = call(
        &state,
        Some(Caller {
            agent_id: None,
            session: nexus_contracts::SessionId("local-operator".into()),
            name: "Local Operator".into(),
            project: PROJECT.into(),
            tier: Tier::Admin,
        }),
        "prompt",
        PromptRequest {
            agent_id: None,
            name: "ada".into(),
            text: marker.clone(),
            client_message_id: None,
        },
    )
    .await;

    // (3) Poll stream_events for up to 5 s. The marker must appear as a `text` row — proof the
    // PtyReplyReader (NOT any ~/.claude tailer) is wired into the launch path.
    let found = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let rows = StreamEvents::new(&store)
                .since(&launched.session_id, 0)
                .await
                .expect("stream_events query");
            if rows
                .iter()
                .any(|r| r.kind == "text" && r.data.contains(&marker))
            {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(false);

    assert!(
        found,
        "marker '{marker}' must appear as a 'text' stream_events row: \
         PtyReplyReader must be wired into launch_agent_with_program"
    );
}
