//! Raw PTY delivery safety at the durable bus boundary.
//!
//! A raw [`PtySession`] can prove only that bytes were written, not that a model turn completed.
//! Public v0.1 therefore refuses durable bus injection before crossing that ambiguous boundary.
//! These tests prove DM and thread fan-out settle as terminal `contract_error`, never write the
//! marker into the PTY, and remain non-retryable without an explicit DLQ requeue.

use std::sync::Arc;

use nexus::daemon::pty_transport::PtyTransport;
use nexus::daemon::{dispatch, AppState};
use nexus_common::Config;
use nexus_contracts::ids::SessionId;
use nexus_contracts::{
    Caller, CreateThreadRequest, Kind, RegisterRequest, RegisterResponse, Request, RequestId,
    SendRequest, SendTarget, ThreadMemberRequest, Tier,
};
use nexus_pty::PtySession;
use nexus_store::Store;
use portable_pty::{CommandBuilder, PtySize};
use tokio::sync::broadcast;

#[cfg(unix)]
fn passive_terminal_command() -> CommandBuilder {
    CommandBuilder::new("cat")
}

#[cfg(windows)]
fn passive_terminal_command() -> CommandBuilder {
    CommandBuilder::new("cmd.exe")
}

/// One project for every agent so DM resolution + thread fan-out (both project-scoped) reach.
const PROJECT: &str = "egregore";

/// A registered PTY-native agent: its name, captured `SessionId`, and a receiver on its `cat` PTY's
/// output stream (what the harness "sees" — the delivered turn echoed back).
struct PtyAgent {
    name: String,
    session: SessionId,
    pty: Arc<PtySession>,
}

impl PtyAgent {
    /// A fresh receiver on this agent's PTY output (each `subscribe` is an independent broadcast
    /// cursor, so re-subscribing per assertion never misses bytes written after the call).
    fn pty_out(&self) -> broadcast::Receiver<Vec<u8>> {
        self.pty.subscribe()
    }

    /// The resolved caller this agent dispatches as (name + project + its captured session).
    fn caller(&self) -> Caller {
        Caller {
            agent_id: None,
            session: self.session.clone(),
            name: self.name.clone(),
            project: PROJECT.into(),
            tier: Tier::Agent,
            locality: Default::default(),
            access: None,
            principal_id: None,
        }
    }
}

/// A real in-test daemon whose turn-executor is [`PtyTransport`]. Holds a clone of the transport so
/// `register_pty_agent` can `bind` each agent's `cat` PTY under its session id.
struct PtyTestDaemon {
    state: AppState,
    transport: PtyTransport,
}

impl PtyTestDaemon {
    /// Boot the daemon with `PtyTransport` as the `AgentTurnExecutionPort` (the ONE swap vs ACP).
    async fn start() -> PtyTestDaemon {
        let store = Arc::new(Store::open(":memory:").await.unwrap());
        store.migrate().await.unwrap();
        let transport = PtyTransport::default();
        let state =
            AppState::wire_with_turn_exec(store, &Config::default(), Arc::new(transport.clone()));
        PtyTestDaemon { state, transport }
    }

    /// Register `name` as a wakeable bus member (its idle drain loop is stood up), capture its
    /// session id, spawn a passive native `PtySession`, and `bind` it under that session so a
    /// delivered turn would write into this agent's PTY.
    async fn register_pty_agent(&self, name: &str) -> PtyAgent {
        // `register` writes the member row (kind=agent → drain loop via `ensure_agent_loop`) and
        // returns the bound session id. `client_key` keys resume; a per-name key is fine here.
        let resp: RegisterResponse = self
            .call(
                None,
                "register",
                RegisterRequest {
                    name: Some(name.into()),
                    agent_id: None,
                    harness: hid("claude"),
                    harness_session_id: format!("hs_{name}"),
                    project: PROJECT.into(),
                    client_key: format!("ck_{name}"),
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

        let pty = Arc::new(
            PtySession::spawn(
                passive_terminal_command(),
                PtySize {
                    rows: 24,
                    cols: 80,
                    pixel_width: 0,
                    pixel_height: 0,
                },
            )
            .unwrap(),
        );
        // Bind the PTY under the agent's session: a delivery to this agent would write into this
        // passive native terminal.
        self.transport.bind(resp.session_id.clone(), pty.clone());
        // Registration intentionally does not start an agent-owned drain loop until its transport
        // is live. This fixture binds after registration, so mirror the production supervisor's
        // post-bind readiness signal before exercising the durable-delivery boundary.
        self.state.ensure_agent_loop(PROJECT, name).await;

        PtyAgent {
            name: name.into(),
            session: resp.session_id,
            pty,
        }
    }

    /// Dispatch a bus `send` AS `from` (caller = that agent).
    async fn send_as(&self, from: &PtyAgent, to: SendTarget, body: &str) -> nexus_contracts::Ack {
        self.call(
            Some(from.caller()),
            "send",
            SendRequest {
                to,
                summary: None,
                body: body.into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            },
        )
        .await
    }

    /// Create a named thread with the given members (dispatch `thread.new` + `thread.addMember` for
    /// each, as the first member so the caller is project-scoped to the thread's workspace).
    async fn create_thread(&self, name: &str, creator: &PtyAgent, members: &[&str]) {
        let _: serde_json::Value = self
            .call(
                Some(creator.caller()),
                "thread.new",
                CreateThreadRequest {
                    name: name.into(),
                    members: vec![],
                },
            )
            .await;
        for m in members {
            let _: serde_json::Value = self
                .call(
                    Some(creator.caller()),
                    "thread.addMember",
                    ThreadMemberRequest {
                        name: name.into(),
                        member: (*m).into(),
                    },
                )
                .await;
        }
    }

    /// Drive one `dispatch` call and unwrap a typed success result (panics on RPC error).
    async fn call<P: serde::Serialize, T: serde::de::DeserializeOwned>(
        &self,
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
        let resp = dispatch(&self.state, caller, req).await;
        if let Some(e) = resp.error {
            panic!("dispatch({method}) error: [{}] {}", e.code, e.message);
        }
        serde_json::from_value(resp.result.unwrap_or(serde_json::Value::Null)).unwrap()
    }
}

async fn wait_for_terminal_contract_error(
    daemon: &PtyTestDaemon,
    message_id: &nexus_contracts::MessageId,
    recipient: &SessionId,
) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let mut rows = daemon
            .state
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
            let state = row.get::<String>(0).unwrap();
            if state == "error" {
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

/// Accumulate `rx.recv()` chunks until the decoded buffer contains `needle` or `ms` elapse; return
/// whether `needle` was seen. The needle is the message BODY — it appears inside the rendered
/// `<nexus-batch>` envelope `cat` echoes back, so substring search, not exact match.
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

/// A durable DM must fail closed before writing into a raw PTY that cannot prove context acceptance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_pty_dm_is_terminally_rejected_before_write() {
    let d = PtyTestDaemon::start().await;
    let ada = d.register_pty_agent("ada").await;
    let ben = d.register_pty_agent("ben").await;
    let mut ben_out = ben.pty_out();

    // ada DMs ben through the bus (the same call an agent's MCP `dm` tool makes).
    let ack = d
        .send_as(&ada, SendTarget::dm_name("ben"), "ping from ada")
        .await;
    assert!(
        !wait_for_pty_text(&mut ben_out, "ping from ada", 250).await,
        "an unsupported raw PTY must not receive a durable DM"
    );
    wait_for_terminal_contract_error(&d, &ack.message_id, &ben.session).await;
}

/// Thread fan-out applies the same fail-closed context-acceptance requirement as direct DMs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_pty_thread_delivery_is_terminally_rejected_before_write() {
    let d = PtyTestDaemon::start().await;
    let ada = d.register_pty_agent("ada").await;
    let ben = d.register_pty_agent("ben").await;
    let mut ben_out = ben.pty_out();

    d.create_thread("standup", &ada, &["ada", "ben"]).await;

    // ada posts to the thread → fans out to ben's in_flight → drain → ben's PTY.
    let ack = d
        .send_as(
            &ada,
            SendTarget::Post {
                thread: "standup".into(),
            },
            "what did you do?",
        )
        .await;
    assert!(
        !wait_for_pty_text(&mut ben_out, "what did you do?", 250).await,
        "an unsupported raw PTY must not receive a durable thread post"
    );
    wait_for_terminal_contract_error(&d, &ack.message_id, &ben.session).await;
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
