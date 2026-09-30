//! `RoutingTurnExec` — per-session turn router for codex app-server, PTY, and ACP agents.
//!
//! Dispatches each turn to the right backend in priority order:
//! 1. **codex app-server bound** → deliver via [`CodexAppServerTransport`] (`turn/start` RPC)
//! 2. **PTY bound** → deliver via [`PtyTransport`] (native TUI, bracketed-paste write)
//! 3. **neither** → headless path: delegate to the ACP [`AgentTurnExecutionPort`]
//!
//! `launch` and `remove` are NOT routed here — app.rs handles those directly per-path.

use std::sync::Arc;

use async_trait::async_trait;

use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::{
    AgentTurnExecutionPort, ContractError, EventSink, InjectResult, PortResult,
};
use nexus_contracts::{
    NexusBatch, RemoveRequest, RemoveResponse, SpawnRequest, SpawnResponse, SteerCapability,
    SteerResponse, WsEvent,
};
use nexus_harness_codex::CodexAppServerTransport;

use crate::daemon::pty_transport::PtyTransport;

/// Routes each injected turn to the correct backend by checking bindings in priority order:
/// codex app-server → PTY/tmux → headless ACP. All three backends co-exist
/// in the daemon; each session resolves at turn time.
pub struct RoutingTurnExec {
    codex: CodexAppServerTransport,
    pty: PtyTransport,
    acp: Arc<dyn AgentTurnExecutionPort>,
}

impl RoutingTurnExec {
    pub fn new(
        codex: CodexAppServerTransport,
        pty: PtyTransport,
        acp: Arc<dyn AgentTurnExecutionPort>,
    ) -> Self {
        Self { codex, pty, acp }
    }
}

#[async_trait]
impl AgentTurnExecutionPort for RoutingTurnExec {
    async fn inject_turn(&self, recipient: &SessionId, batch: &NexusBatch) -> PortResult<()> {
        if self.codex.is_bound(recipient) {
            self.codex.inject_turn(recipient, batch).await
        } else if self.pty.is_bound(recipient) {
            self.pty.inject_turn(recipient, batch).await
        } else {
            self.acp.inject_turn(recipient, batch).await
        }
    }

    async fn inject_turn_observed(
        &self,
        recipient: &SessionId,
        batch: &NexusBatch,
        events: Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> InjectResult<()> {
        if self.codex.is_bound(recipient) {
            self.codex
                .inject_turn_observed(recipient, batch, events, accepted_event)
                .await
        } else if self.pty.is_bound(recipient) {
            self.pty
                .inject_turn_observed(recipient, batch, events, accepted_event)
                .await
        } else {
            self.acp
                .inject_turn_observed(recipient, batch, events, accepted_event)
                .await
        }
    }

    async fn prompt(&self, recipient: &SessionId, text: String) -> PortResult<()> {
        if self.codex.is_bound(recipient) {
            self.codex.prompt(recipient, text).await
        } else if self.pty.is_bound(recipient) {
            self.pty.prompt(recipient, text).await
        } else {
            self.acp.prompt(recipient, text).await
        }
    }

    async fn prompt_observed(
        &self,
        recipient: &SessionId,
        text: String,
        events: Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> PortResult<()> {
        if self.codex.is_bound(recipient) {
            self.codex
                .prompt_observed(recipient, text, events, accepted_event)
                .await
        } else if self.pty.is_bound(recipient) {
            self.pty
                .prompt_observed(recipient, text, events, accepted_event)
                .await
        } else {
            self.acp
                .prompt_observed(recipient, text, events, accepted_event)
                .await
        }
    }

    async fn steer_observed(
        &self,
        recipient: &SessionId,
        text: String,
        events: Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> PortResult<SteerResponse> {
        if self.codex.is_bound(recipient) {
            self.codex
                .steer_observed(recipient, text, events, accepted_event)
                .await
        } else if self.pty.is_bound(recipient) {
            self.pty
                .steer_observed(recipient, text, events, accepted_event)
                .await
        } else {
            self.acp
                .steer_observed(recipient, text, events, accepted_event)
                .await
        }
    }

    fn steer_capability(&self, recipient: &SessionId) -> SteerCapability {
        if self.codex.is_bound(recipient) {
            self.codex.steer_capability(recipient)
        } else if self.pty.is_bound(recipient) {
            self.pty.steer_capability(recipient)
        } else {
            self.acp.steer_capability(recipient)
        }
    }

    async fn interrupt_active_turn(&self, recipient: &SessionId) -> PortResult<()> {
        if self.codex.is_bound(recipient) {
            self.codex.interrupt_active_turn(recipient).await
        } else if self.pty.is_bound(recipient) {
            self.pty.interrupt_active_turn(recipient).await
        } else {
            self.acp.interrupt_active_turn(recipient).await
        }
    }

    async fn compact(&self, recipient: &SessionId) -> PortResult<()> {
        if self.codex.is_bound(recipient) {
            self.codex.compact(recipient).await
        } else if self.pty.is_bound(recipient) {
            self.pty.compact(recipient).await
        } else {
            // ACP transports have no compact verb — the trait default reports
            // "not supported" loudly instead of faking it.
            self.acp.compact(recipient).await
        }
    }

    fn is_harness_alive(&self, recipient: &SessionId) -> Option<bool> {
        // THREE-STEP ordering — each step must check is_bound before dispatching:
        //
        // 1. codex: checked first so a session with a codex app-server binding is not
        //    accidentally resolved through the PTY path (a session can be bound in both
        //    during the startup window; codex must win). A live app-server process alone is not
        //    agent liveness; the Codex turn client must be bound to the actual session.
        //
        // 2. pty: CRITICAL — PtyTransport::is_harness_alive returns Some(false) for an
        //    UNBOUND session (no binding → no harness → not alive). Without the is_bound
        //    guard here, every headless (never-pty-bound) session would wrongly read as
        //    dead — re-creating the zombie bug fixed for tmux.
        //
        // 3. acp: fallthrough for fully headless sessions.
        if self.codex.is_bound(recipient) {
            self.codex.is_harness_alive(recipient)
        } else if self.codex.is_app_server_live(recipient) {
            // Pre-thread window: a FRESH headed codex has no Codex
            // thread until its first turn, so the turn client is not yet bound — but the
            // daemon-owned app-server is running. That is a live harness. Without this arm
            // the probe fell through to ACP (which does not know the session → Some(false))
            // and the heartbeat keeper killed every idle headed codex at its first tick.
            // "App-server alone is not agent liveness" still holds for TURN DISPATCH — the
            // is_bound arm above stays the only turn route.
            Some(true)
        } else if self.pty.is_bound(recipient) {
            self.pty.is_harness_alive(recipient)
        } else {
            self.acp.is_harness_alive(recipient)
        }
    }

    fn active_turn_sessions(&self) -> Vec<SessionId> {
        let mut sessions = self.codex.active_turn_sessions();
        sessions.extend(self.acp.active_turn_sessions());
        sessions
    }

    /// Not routed via this type — app.rs dispatches launch directly to the PTY supervisor or the
    /// ACP agent based on the harness kind in the spawn request.
    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        Err(ContractError {
            code: -32601,
            message: "routed at app.rs level, not via RoutingTurnExec".into(),
        })
    }

    /// Not routed via this type — app.rs dispatches remove directly to the correct path.
    async fn remove(&self, _req: RemoveRequest) -> PortResult<RemoveResponse> {
        Err(ContractError {
            code: -32601,
            message: "routed at app.rs level, not via RoutingTurnExec".into(),
        })
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use nexus_contracts::batch::{BatchCounts, NexusBatch};
    use nexus_contracts::ids::SessionId;
    use nexus_pty::PtySession;
    use portable_pty::{CommandBuilder, PtySize};
    use std::sync::{Arc, Mutex};

    mod test_support {
        use std::path::PathBuf;
        use std::sync::{Arc, Mutex};

        use futures::{SinkExt, StreamExt};
        use tokio::net::UnixListener;
        use tokio_tungstenite::accept_async_with_config;
        use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
        use tokio_tungstenite::tungstenite::Message;

        type RecordedCalls = Arc<Mutex<Vec<serde_json::Value>>>;

        fn sock_path(prefix: &str, tag: &str) -> PathBuf {
            std::env::temp_dir().join(format!(
                "nexus-{}-test-{}-{}.sock",
                prefix,
                std::process::id(),
                tag
            ))
        }

        pub(crate) async fn spawn_fake_server(prefix: &str, tag: &str) -> (PathBuf, RecordedCalls) {
            let path = sock_path(prefix, tag);
            let _ = std::fs::remove_file(&path);
            let calls: RecordedCalls = Arc::new(Mutex::new(Vec::new()));
            let calls_clone = calls.clone();
            let listener = UnixListener::bind(&path).expect("bind fake server socket");
            tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        break;
                    };
                    let calls_inner = calls_clone.clone();
                    let ws_config = WebSocketConfig {
                        max_frame_size: Some(128 << 20),
                        max_message_size: Some(128 << 20),
                        ..WebSocketConfig::default()
                    };
                    let ws = match accept_async_with_config(stream, Some(ws_config)).await {
                        Ok(ws) => ws,
                        Err(_) => continue,
                    };
                    tokio::spawn(handle_connection(ws, calls_inner));
                }
            });
            (path, calls)
        }

        async fn handle_connection(
            mut ws: tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>,
            calls: RecordedCalls,
        ) {
            use serde_json::json;

            while let Some(Ok(msg)) = ws.next().await {
                let text = match msg {
                    Message::Text(t) => t,
                    Message::Close(_) => break,
                    _ => continue,
                };
                let v: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let Some(id) = v.get("id").cloned() else {
                    calls.lock().unwrap().push(v);
                    continue;
                };
                let method = v.get("method").and_then(|m| m.as_str()).unwrap_or("");

                let result = if method == "initialize" {
                    json!({
                        "protocolVersion": "0.1.0",
                        "serverInfo": {"name": "fake", "version": "0.1.0"},
                        "capabilities": {}
                    })
                } else {
                    calls.lock().unwrap().push(v.clone());
                    serde_json::Value::Null
                };

                let reply = json!({"jsonrpc": "2.0", "id": id, "result": result});
                if ws
                    .send(Message::Text(serde_json::to_string(&reply).unwrap().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Fake ACP implementation that records calls
    // ---------------------------------------------------------------------------

    #[derive(Clone, Default)]
    struct FakeAcp {
        inner: Arc<Mutex<FakeAcpState>>,
    }

    #[derive(Default)]
    struct FakeAcpState {
        inject_sessions: Vec<SessionId>,
        prompt_sessions: Vec<SessionId>,
        interrupt_sessions: Vec<SessionId>,
        liveness: Option<bool>,
    }

    impl FakeAcp {
        fn with_liveness(alive: Option<bool>) -> Self {
            let fake = FakeAcp::default();
            fake.inner.lock().unwrap().liveness = alive;
            fake
        }
        fn inject_sessions(&self) -> Vec<SessionId> {
            self.inner.lock().unwrap().inject_sessions.clone()
        }
        fn prompt_sessions(&self) -> Vec<SessionId> {
            self.inner.lock().unwrap().prompt_sessions.clone()
        }
        fn interrupt_sessions(&self) -> Vec<SessionId> {
            self.inner.lock().unwrap().interrupt_sessions.clone()
        }
    }

    #[async_trait]
    impl AgentTurnExecutionPort for FakeAcp {
        async fn inject_turn(&self, recipient: &SessionId, _batch: &NexusBatch) -> PortResult<()> {
            self.inner
                .lock()
                .unwrap()
                .inject_sessions
                .push(recipient.clone());
            Ok(())
        }
        async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
            unimplemented!("not called in routing tests")
        }
        async fn remove(&self, _req: RemoveRequest) -> PortResult<RemoveResponse> {
            unimplemented!("not called in routing tests")
        }
        async fn prompt(&self, recipient: &SessionId, _text: String) -> PortResult<()> {
            self.inner
                .lock()
                .unwrap()
                .prompt_sessions
                .push(recipient.clone());
            Ok(())
        }
        fn steer_capability(&self, _recipient: &SessionId) -> SteerCapability {
            SteerCapability::InterruptAndSend
        }
        async fn interrupt_active_turn(&self, recipient: &SessionId) -> PortResult<()> {
            self.inner
                .lock()
                .unwrap()
                .interrupt_sessions
                .push(recipient.clone());
            Ok(())
        }
        fn is_harness_alive(&self, _recipient: &SessionId) -> Option<bool> {
            self.inner.lock().unwrap().liveness
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    fn empty_batch() -> NexusBatch {
        NexusBatch {
            counts: BatchCounts {
                dms: 0,
                thread: 0,
                total: 0,
            },
            dms: vec![],
            threads: vec![],
            dm_message_ids: vec![],
            thread_message_ids: vec![],
            message_ids: vec![],
        }
    }

    fn bind_cat_session(
        transport: &PtyTransport,
        session: &SessionId,
    ) -> tokio::sync::broadcast::Receiver<Vec<u8>> {
        let pty = Arc::new(
            PtySession::spawn(
                CommandBuilder::new("cat"),
                PtySize {
                    rows: 24,
                    cols: 80,
                    pixel_width: 0,
                    pixel_height: 0,
                },
            )
            .unwrap(),
        );
        let rx = pty.subscribe();
        transport.bind(session.clone(), pty as Arc<dyn nexus_pty::HarnessInput>);
        rx
    }

    #[derive(Default)]
    struct RecordingEvents(Mutex<Vec<WsEvent>>);

    #[async_trait]
    impl EventSink for RecordingEvents {
        async fn emit(&self, event: WsEvent) {
            self.0.lock().unwrap().push(event);
        }
    }

    // ---------------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn inject_turn_routes_to_pty_when_bound_to_acp_when_not() {
        let transport = PtyTransport::default();
        let session_a = SessionId("s_pty".into());
        let session_b = SessionId("s_acp".into());

        let mut rx_a = bind_cat_session(&transport, &session_a);

        let fake_acp = FakeAcp::with_liveness(Some(true));
        let router = RoutingTurnExec::new(
            CodexAppServerTransport::new(),
            transport,
            Arc::new(fake_acp.clone()),
        );

        let batch = empty_batch();

        // A → should reach the PTY (cat echoes the rendered batch)
        router.inject_turn(&session_a, &batch).await.unwrap();
        let saw_envelope = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            let mut buf = Vec::new();
            while let Ok(c) = rx_a.recv().await {
                buf.extend_from_slice(&c);
                if String::from_utf8_lossy(&buf).contains("nexus-batch") {
                    return true;
                }
            }
            false
        })
        .await
        .unwrap_or(false);
        assert!(
            saw_envelope,
            "inject_turn(A) must deliver the <nexus-batch> envelope to the PTY"
        );

        // A must NOT reach the fake ACP
        assert!(
            fake_acp.inject_sessions().is_empty(),
            "inject_turn(A) must NOT reach the fake ACP; got: {:?}",
            fake_acp.inject_sessions()
        );

        // B → should reach the fake ACP
        router.inject_turn(&session_b, &batch).await.unwrap();
        assert_eq!(
            fake_acp.inject_sessions(),
            vec![session_b.clone()],
            "inject_turn(B) must reach the fake ACP"
        );
    }

    #[tokio::test]
    async fn prompt_routes_to_pty_when_bound_to_acp_when_not() {
        let transport = PtyTransport::default();
        let session_a = SessionId("s_pty".into());
        let session_b = SessionId("s_acp".into());

        let mut rx_a = bind_cat_session(&transport, &session_a);

        let fake_acp = FakeAcp::with_liveness(Some(true));
        let router = RoutingTurnExec::new(
            CodexAppServerTransport::new(),
            transport,
            Arc::new(fake_acp.clone()),
        );

        // A → PTY (cat echoes the raw text back)
        router
            .prompt(&session_a, "HELLO-PTY-9001".to_string())
            .await
            .unwrap();
        let saw_text = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            let mut buf = Vec::new();
            while let Ok(c) = rx_a.recv().await {
                buf.extend_from_slice(&c);
                if String::from_utf8_lossy(&buf).contains("HELLO-PTY-9001") {
                    return true;
                }
            }
            false
        })
        .await
        .unwrap_or(false);
        assert!(saw_text, "prompt(A) must deliver raw text to the PTY");

        // A must NOT reach the fake ACP
        assert!(
            fake_acp.prompt_sessions().is_empty(),
            "prompt(A) must NOT reach the fake ACP"
        );

        // B → fake ACP
        router
            .prompt(&session_b, "hello-acp".to_string())
            .await
            .unwrap();
        assert_eq!(
            fake_acp.prompt_sessions(),
            vec![session_b.clone()],
            "prompt(B) must reach the fake ACP"
        );
    }

    #[tokio::test]
    async fn unbound_non_native_session_redirects_with_atomic_interrupt_and_send() {
        let session = SessionId("s_acp_redirect".into());
        let fake_acp = FakeAcp::with_liveness(Some(true));
        let router = RoutingTurnExec::new(
            CodexAppServerTransport::new(),
            PtyTransport::default(),
            Arc::new(fake_acp.clone()),
        );
        let events = Arc::new(RecordingEvents::default());
        let response = router
            .steer_observed(
                &session,
                "redirect next".into(),
                events.clone(),
                WsEvent::AgentUpdate {
                    session_id: session.clone(),
                    kind: nexus_contracts::AgentUpdateKind::UserInput,
                    data: serde_json::json!({"text": "redirect next"}),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            router.steer_capability(&session),
            SteerCapability::InterruptAndSend
        );
        assert_eq!(
            response.delivery,
            nexus_contracts::SteerDelivery::InterruptedAndStarted
        );
        assert_eq!(fake_acp.interrupt_sessions(), vec![session.clone()]);
        assert_eq!(fake_acp.prompt_sessions(), vec![session]);
        assert_eq!(events.0.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn is_harness_alive_routes_correctly_and_does_not_collapse_unbound_to_some_false() {
        let transport = PtyTransport::default();
        let session_a = SessionId("s_pty".into());
        let session_b = SessionId("s_acp_headless".into());

        // Bind session A to a live `cat` PTY — PtyTransport::is_harness_alive → Some(true)
        let _rx = bind_cat_session(&transport, &session_a);

        // The fake ACP reports Some(true) for any session it owns (B)
        let fake_acp = FakeAcp::with_liveness(Some(true));
        let router = RoutingTurnExec::new(
            CodexAppServerTransport::new(),
            transport,
            Arc::new(fake_acp),
        );

        // A — PTY path: the cat process is alive → Some(true)
        assert_eq!(
            router.is_harness_alive(&session_a),
            Some(true),
            "is_harness_alive(A) must return Some(true) from the PTY path"
        );

        // B — ACP path: must NOT collapse to Some(false) (the zombie-bug guard).
        // If we wrongly called pty.is_harness_alive(B), we'd get Some(false) because B has no
        // PTY binding. The correct answer is the fake ACP's Some(true).
        assert_eq!(
            router.is_harness_alive(&session_b),
            Some(true),
            "is_harness_alive(B) must return Some(true) from the ACP path, NOT Some(false)"
        );
    }

    #[tokio::test]
    async fn codex_app_server_only_does_not_mask_dead_agent_session() {
        let codex = CodexAppServerTransport::new();
        let session = SessionId("s_codex_appserver_only".into());
        codex.mark_live(session.clone());

        let router = RoutingTurnExec::new(
            codex,
            PtyTransport::default(),
            Arc::new(FakeAcp::with_liveness(Some(false))),
        );

        assert_eq!(
            router.is_harness_alive(&session),
            Some(false),
            "app-server keepalive must not mask a dead/unbound Codex agent session"
        );
    }

    #[tokio::test]
    async fn launch_and_remove_return_minus_32601() {
        let transport = PtyTransport::default();
        let fake_acp = FakeAcp::default();
        let router = RoutingTurnExec::new(
            CodexAppServerTransport::new(),
            transport,
            Arc::new(fake_acp),
        );

        let launch_err = router
            .launch(SpawnRequest {
                kind: nexus_contracts::Harness::Claude,
                name: None,
                identity_policy: None,
                cwd: None,
                project: None,
                role: None,
                initial_prompt: None,
                resume: None,
                harness_args: Vec::new(),
                headless: false,
                backend: None,
            })
            .await
            .unwrap_err();
        assert_eq!(
            launch_err.code, -32601,
            "launch must return -32601; got: {}",
            launch_err.code
        );
        assert!(
            launch_err.message.contains("app.rs"),
            "launch error must mention app.rs; got: {}",
            launch_err.message
        );

        let remove_err = router
            .remove(RemoveRequest {
                agent_id: None,
                name: "any".into(),
                kill: false,
            })
            .await
            .unwrap_err();
        assert_eq!(
            remove_err.code, -32601,
            "remove must return -32601; got: {}",
            remove_err.code
        );
        assert!(
            remove_err.message.contains("app.rs"),
            "remove error must mention app.rs; got: {}",
            remove_err.message
        );
    }

    // -------------------------------------------------------------------------
    // Routing priority: codex-first
    // -------------------------------------------------------------------------

    /// Prove that a session bound in BOTH codex and PTY transports routes inject to codex, not PTY.
    /// Also prove that an unbound session falls through to ACP.
    /// Also documents is_harness_alive priority: codex-bound session returns Some(true) via codex path.
    #[tokio::test]
    async fn routing_is_codex_first_before_pty_before_acp() {
        use nexus_harness_codex::app_server::CodexAppServerClient;

        let (sock, calls) = test_support::spawn_fake_server("routing", "routing-priority").await;

        let codex_transport = CodexAppServerTransport::new();
        let pty_transport = PtyTransport::default();

        // Bind session A to BOTH codex and PTY transports.
        let session_a = SessionId("s_codex_and_pty".into());
        let client = Arc::new(
            CodexAppServerClient::connect(&sock, "nexus-inject")
                .await
                .expect("client connect"),
        );
        codex_transport.bind(session_a.clone(), client, "thread-a".to_string());
        // Also bind to PTY — codex must win.
        let _rx_a = bind_cat_session(&pty_transport, &session_a);

        // Session B: unbound in both codex and PTY — should fall to ACP.
        let session_b = SessionId("s_acp_only".into());

        let fake_acp = FakeAcp::default();
        let router =
            RoutingTurnExec::new(codex_transport, pty_transport, Arc::new(fake_acp.clone()));

        // A: bound in codex → inject goes to codex transport (fake server records it).
        router
            .inject_turn(&session_a, &empty_batch())
            .await
            .unwrap();
        {
            let recorded = calls.lock().unwrap();
            let codex_got_it = recorded
                .iter()
                .any(|v| v.get("method").and_then(|m| m.as_str()) == Some("turn/start"));
            assert!(
                codex_got_it,
                "inject_turn(A) must route to codex (turn/start), not PTY"
            );
        }
        // ACP must NOT have received session A.
        assert!(
            fake_acp.inject_sessions().is_empty(),
            "inject_turn(A) must NOT reach ACP; got: {:?}",
            fake_acp.inject_sessions()
        );

        // B: not bound anywhere → falls to ACP.
        router
            .inject_turn(&session_b, &empty_batch())
            .await
            .unwrap();
        assert_eq!(
            fake_acp.inject_sessions(),
            vec![session_b.clone()],
            "inject_turn(B) must fall through to ACP"
        );

        // is_harness_alive priority: session_a is bound in both codex and PTY;
        // codex is checked first, so the answer comes from the codex transport → Some(true).
        assert_eq!(
            router.is_harness_alive(&session_a),
            Some(true),
            "is_harness_alive(A) must return Some(true) via the codex path (not PTY)"
        );
    }
}
