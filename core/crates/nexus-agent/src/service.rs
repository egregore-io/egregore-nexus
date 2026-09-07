//! The `Agent` service — the [`AgentTurnExecutionPort`] implementation (backend spec §2.3, §5, §6,
//! §11). It owns the [`AdapterRegistry`] + per-session adapter bindings, injects a drained
//! [`NexusBatch`] as a single turn (wrapping bus traffic in `<nexus-batch>`, delivering the core
//! user's plain DM unwrapped), relays the reply as ordered `agent.update` events, and surfaces an
//! adapter init failure as `agent.status=errored` while retaining the session for retry.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use nexus_common::{render_injected_turn_for, RuntimeProcessIds};
use nexus_contracts::{
    AgentTurnExecutionPort, AgentUpdateKind, ContractError, EventSink, IdentityPort, InjectError,
    InjectResult, NexusBatch, Presence, RemoveRequest, RemoveResponse, SessionId, SpawnRequest,
    SpawnResponse, SteerCapability, SteerDelivery, SteerResponse, WsEvent,
};

use crate::active_turns::ActiveTurnTracker;
use crate::adapter::engine::LaunchCtx;
use crate::adapter::{Adapter, StreamEvent};
use crate::error::AgentError;
use crate::registry::AdapterRegistry;

/// One registered agent session: its live adapter, the bound name + project (the agent's own
/// identity, used as the `from`/caller for OUTBOUND sends), and the errored flag.
#[derive(Clone)]
struct SessionEntry {
    adapter: Arc<dyn Adapter>,
    name: String,
    /// The agent's project. Retained on the binding (set at launch) though not currently read now
    /// that outbound goes through the agent's own `nexus` CLI (which carries identity via its env).
    #[allow(dead_code)]
    project: String,
    /// True once `open_session` failed — the session is retained (errored), not dropped (spec §11).
    /// An errored session refuses injection ([`Agent::adapter_for`]) so held messages stay pending.
    errored: bool,
}

/// The agent-transport service. Implements [`AgentTurnExecutionPort`].
#[derive(Clone)]
pub struct Agent {
    registry: AdapterRegistry,
    identity: Arc<dyn IdentityPort>,
    events: Arc<dyn EventSink>,
    sessions: Arc<Mutex<HashMap<SessionId, SessionEntry>>>,
    active_turns: ActiveTurnTracker,
}

impl Agent {
    /// Build the agent service over the adapter registry, the identity port (name↔session binding)
    /// and the event sink (relayed `agent.update`/`agent.status`). The outbound `BusPort` is absent
    /// until set with [`Agent::set_bus`] (the daemon wires it; unit tests leave it off).
    pub fn new(
        registry: AdapterRegistry,
        identity: Arc<dyn IdentityPort>,
        events: Arc<dyn EventSink>,
    ) -> Self {
        Self {
            registry,
            identity,
            events,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            active_turns: ActiveTurnTracker::default(),
        }
    }

    /// Bind a live adapter to a session id under `name` in `project` (used by `launch` and by tests
    /// that wire a session directly). `errored` records an init failure while keeping the session
    /// registered. `project` is the agent's own project — the scope its outbound sends resolve under.
    pub fn bind_session(
        &self,
        session: SessionId,
        name: impl Into<String>,
        project: impl Into<String>,
        adapter: Arc<dyn Adapter>,
        errored: bool,
    ) {
        self.sessions.lock().unwrap().insert(
            session,
            SessionEntry {
                adapter,
                name: name.into(),
                project: project.into(),
                errored,
            },
        );
    }

    /// Open + bind an adapter under a **caller-supplied** session id + project (used by the daemon's
    /// launch orchestration, which mints the canonical id first so the bus member row, the adapter
    /// binding, and the event loop all key on the *same* id). `project` is recorded so the agent's
    /// OUTBOUND sends resolve its own [`Caller`] under the right scope. Mirrors
    /// [`AgentTurnExecutionPort::launch`] but without minting an id: on success the adapter is bound
    /// and `agent.spawned` is emitted; on adapter init failure the session is **retained** (errored)
    /// — so it stays addressable for retry, with messages left re-driveable — and
    /// `agent.status=errored` is emitted before the error propagates (spec §11).
    pub async fn open_session_for(
        &self,
        session: SessionId,
        name: &str,
        project: &str,
        kind: nexus_contracts::HarnessId,
        cwd: Option<String>,
        env: Vec<(String, String)>,
        resume_key: Option<&str>,
    ) -> Result<Option<RuntimeProcessIds>, ContractError> {
        let bus_client_key = env_value(&env, "NEXUS_CLIENT_KEY");
        let bus_agent = env_value(&env, "NEXUS_AGENT");
        let ctx = LaunchCtx {
            cwd,
            env,
            bus_name: Some(name.to_string()),
            bus_project: Some(project.to_string()),
            bus_client_key,
            bus_agent,
            // Default `false`; the OpenCode adapter flips this on in its constructor.
            ..Default::default()
        };
        let adapter = self.registry.get(&kind, ctx).map_err(to_contract)?;
        // Resume the harness's existing ACP session (`session/load`) when a resume key is given —
        // the not-fresh path. If resume fails (bad key / harness lacks loadSession), fall back to a
        // fresh `session/new` so the caller is never blocked.
        let opened = match resume_key {
            Some(key) => match adapter.resume(key).await {
                Ok(()) => Ok(()),
                Err(e) => {
                    tracing::warn!(
                        target: "nexus_agent::resume",
                        session = %session, error = %e,
                        "session/load resume failed; falling back to session/new on the same connection"
                    );
                    // The engine is ALREADY connected (resume spawned it); do session/new directly,
                    // NOT open_session (which would re-spawn → "acp engine already connected").
                    adapter.new_session_only().await
                }
            },
            None => adapter.open_session().await,
        };
        match opened {
            Ok(()) => {
                // Capture + persist the harness's real ACP session id so this agent can be resumed
                // after a restart (the resume key for `session/load`).
                if let Some(acp) = adapter.acp_session_id().await {
                    let _ = self.identity.set_resume_key(&session, &acp).await;
                }
                let process_ledger = adapter.runtime_process_ids();
                self.bind_session(
                    session.clone(),
                    name.to_string(),
                    project.to_string(),
                    adapter,
                    false,
                );
                self.events
                    .emit(WsEvent::AgentSpawned {
                        session_id: session,
                        name: Some(name.to_string()),
                        agent_id: None,
                    })
                    .await;
                Ok(process_ledger)
            }
            Err(e) => {
                self.bind_session(
                    session.clone(),
                    name.to_string(),
                    project.to_string(),
                    adapter,
                    true,
                );
                self.events
                    .emit(WsEvent::AgentStatus {
                        session_id: session,
                        presence: Presence::Offline,
                        paused: false,
                    })
                    .await;
                Err(e.to_contract_error())
            }
        }
    }

    /// True if a session is registered (retained), regardless of errored state.
    pub fn has_session(&self, session: &SessionId) -> bool {
        self.sessions.lock().unwrap().contains_key(session)
    }

    /// True only if the session has a USABLE (non-errored) adapter — i.e. a turn can actually be
    /// injected. A bound-but-errored session (adapter never opened, retained for retry) is NOT live.
    /// `ensure_live` uses this to decide whether to (re-)spawn: `has_session` is insufficient because
    /// it returns true for errored bindings too.
    pub fn is_live(&self, session: &SessionId) -> bool {
        self.adapter_for(session).is_ok()
    }

    /// Detach a specific session binding after the caller has already resolved the project-scoped
    /// session row. This is the app-level removal seam used by `admin.remove`; it avoids routing a
    /// known daemon session back through the generic turn-exec `remove` stub.
    pub async fn detach_session(&self, session: &SessionId, kill: bool) -> bool {
        let removed = self.sessions.lock().unwrap().remove(session);
        let Some(entry) = removed else {
            return false;
        };
        if kill {
            entry.adapter.kill().await;
        }
        true
    }

    /// The adapter for a registered, **non-errored** session. A missing session is `NoSession`; a
    /// retained-but-errored session (its `open_session` failed and was not yet re-driven) refuses
    /// injection with [`AgentError::Adapter`] so the event loop leaves the rows `pending` — the
    /// message is held, never delivered into a dead session, and stays re-driveable (spec §11).
    fn adapter_for(&self, session: &SessionId) -> Result<Arc<dyn Adapter>, AgentError> {
        let sessions = self.sessions.lock().unwrap();
        let entry = sessions
            .get(session)
            .ok_or_else(|| AgentError::NoSession(session.0.clone()))?;
        if entry.errored {
            return Err(AgentError::Adapter(format!(
                "session {} is errored (adapter not open); message held",
                session.0
            )));
        }
        Ok(entry.adapter.clone())
    }

    /// Inject one already-rendered prompt into a session's ACP (`session/prompt`) and relay the
    /// FULL reply stream as ordered, tagged `agent.update` events (text, thinking, tool calls,
    /// plans, commands) — the gateway maps these to AG-UI for the UI. Shared by the bus path
    /// ([`Agent::inject_turn`], which renders a `NexusBatch`) and the direct Agent Session
    /// [`Agent::prompt`] path (which injects the operator's text straight in).
    ///
    /// `source` controls whether this transport emits its own synthetic `user_input` event before
    /// the reply: only [`InjectSource::DirectDrive`] emits it here. Bus deliveries suppress the
    /// transport-local echo because the observed bus path passes a single accepted-event projection
    /// into the adapter so it lands before assistant output without being duplicated.
    async fn inject_and_relay(
        &self,
        recipient: &SessionId,
        prompt: String,
        source: InjectSource,
        accepted_event: Option<StreamEvent>,
    ) -> InjectResult<()> {
        let adapter = self.adapter_for(recipient).map_err(|e| {
            tracing::error!(
                target: "nexus_agent::inject",
                session = %recipient, error = %e,
                "no usable adapter for session; turn held"
            );
            InjectError::Contract(to_contract(e))
        })?;
        let _active_turn = self.active_turns.begin(recipient);
        relay_turn(
            adapter,
            self.events.clone(),
            recipient.clone(),
            prompt,
            source,
            accepted_event,
        )
        .await
    }
}

fn env_value(env: &[(String, String)], key: &str) -> Option<String> {
    env.iter()
        .find_map(|(k, v)| (k == key).then(|| v.clone()))
        .filter(|v| !v.is_empty())
}

/// Discriminates the two injection paths so `relay_turn` knows whether to emit a `user_input`
/// echo event.
///
/// - [`InjectSource::Bus`]: a `<nexus-batch>` delivery from the message bus. The legacy
///   `inject_turn` path does not emit `user_input`; the observed path passes one accepted-event
///   projection into the adapter so it lands before assistant output.
/// - [`InjectSource::DirectDrive`]: direct Agent Session input (the `prompt` path). The
///   operator's typed input SHOULD be echoed as `user_input` so the web console mirrors the harness
///   input.
#[derive(Clone, Copy, PartialEq, Eq)]
enum InjectSource {
    Bus,
    DirectDrive,
}

/// Inject one prompt and relay the reply stream as tagged `agent.update` events. Pure over owned
/// handles (the adapter + event sink, both `Arc`) so it can be `await`ed inline by the bus path
/// ([`Agent::inject_turn`]) OR `tokio::spawn`ed fire-and-forget by the direct Agent Session prompt
/// ([`Agent::prompt`]) — the latter returns the instant the prompt is dispatched, with the reply
/// arriving later over the live channel (no AionUi-style send latency: the operator doesn't wait
/// for turn-end).
///
/// `source` gates the `user_input` synthetic echo: only [`InjectSource::DirectDrive`] emits it.
/// Bus deliveries suppress the echo at this layer so `<nexus-batch>` traffic is not double-rendered:
/// the observed daemon path passes the single session-visible bus-input projection into the adapter.
async fn relay_turn(
    adapter: Arc<dyn Adapter>,
    events: Arc<dyn EventSink>,
    recipient: SessionId,
    prompt: String,
    source: InjectSource,
    accepted_event: Option<StreamEvent>,
) -> InjectResult<()> {
    tracing::info!(
        target: "nexus_agent::inject",
        session = %recipient, prompt_len = prompt.len(),
        "adapter found; sending prompt (ACP session/prompt)"
    );

    // Surface the injected turn itself as a `user_input` agent.update on the SAME session stream,
    // BEFORE the reply streams in. This is "what the harness received" — the operator's typed input.
    // It makes the web `/agent/<name>:<session_id>` view and the `nexus attach` TUI mirror each
    // other (type on one → shows on both) and is persisted via `stream_events` like every other
    // agent.update. The daemon is the authoritative source: claude-agent-acp does NOT echo
    // `UserMessageChunk` for injected prompts, and all input to a daemon-owned session flows through
    // here, so there is exactly one `user_input` per direct-drive turn.
    //
    // Bus deliveries (InjectSource::Bus) do NOT emit `user_input` here: the observed path passes one
    // accepted-event projection into the adapter so it appears before assistant output.
    if source == InjectSource::DirectDrive {
        events
            .emit(WsEvent::AgentUpdate {
                session_id: recipient.clone(),
                kind: AgentUpdateKind::UserInput,
                data: serde_json::json!({ "text": prompt }),
            })
            .await;
    }

    // REALTIME relay: install the live channel BEFORE injecting and drain it concurrently with
    // the turn — emitting bounded `agent.update` deltas AS `session/update` arrives (true
    // streaming, the AionUi `responseStream` model). A coalesced provider text update may become
    // multiple exact deltas; the gateway maps each to AG-UI. Adapters with no live engine (the
    // mock) return `None` → we fall back to a single turn-end drain below.
    let drain = adapter.install_live().map(|mut rx| {
        let events = events.clone();
        let sid = recipient.clone();
        tokio::spawn(async move {
            while let Some(ev) = rx.recv().await {
                events
                    .emit(WsEvent::AgentUpdate {
                        session_id: sid.clone(),
                        kind: ev.kind,
                        data: ev.data,
                    })
                    .await;
            }
        })
    });

    let inject_res = if accepted_event.is_some() {
        adapter
            .inject_with_accepted_event(prompt, accepted_event)
            .await
    } else {
        adapter.inject(prompt).await
    };
    adapter.clear_live(); // drop the sender → the drain loop sees `None` and ends

    let result: InjectResult<()> = match drain {
        // Live path: the chunks were already streamed; just join the drain + surface inject err.
        Some(handle) => {
            let _ = handle.await;
            inject_res.map_err(|e| e.into_inject_error(&recipient))
        }
        // Fallback (mock / no live engine): drain the whole buffer at turn-end.
        None => match inject_res {
            Ok(()) => {
                let drained = adapter
                    .stream_updates()
                    .await
                    .map_err(|e| InjectError::Contract(e.to_contract_error()));
                match drained {
                    Ok(events_vec) => {
                        for ev in events_vec {
                            events
                                .emit(WsEvent::AgentUpdate {
                                    session_id: recipient.clone(),
                                    kind: ev.kind,
                                    data: ev.data,
                                })
                                .await;
                        }
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e.into_inject_error(&recipient)),
        },
    };

    // TURN-END marker: the reply is complete (StopReason / quiescence). Emit it ALWAYS — success or
    // error — so the web console closes its streaming row (the gateway maps this to AG-UI
    // `RUN_FINISHED`). Direct Agent Session input has no bus `message.created` to end the turn, so
    // without this the UI's row streams forever. Session-scoped like every `agent.update`, so only
    // this agent's pane sees it.
    events
        .emit(WsEvent::AgentUpdate {
            session_id: recipient.clone(),
            kind: AgentUpdateKind::TurnEnd,
            data: serde_json::json!({}),
        })
        .await;
    result
}

fn stream_event_from_observed_update(event: WsEvent) -> Result<StreamEvent, ContractError> {
    match event {
        WsEvent::AgentUpdate { kind, data, .. } => Ok(StreamEvent { kind, data }),
        _ => Err(ContractError {
            code: -32602,
            message: "observed inject requires an agent.update accepted event".into(),
        }),
    }
}

#[async_trait]
impl AgentTurnExecutionPort for Agent {
    async fn inject_turn(
        &self,
        recipient: &SessionId,
        batch: &NexusBatch,
    ) -> Result<(), ContractError> {
        // Bus path: render the drained batch (`<nexus-batch>` / plain-human-DM exception) and inject
        // it, relaying the reply as tagged `agent.update`s. OUTBOUND is never scraped from prose — an
        // agent sends explicitly via its `nexus` CLI/MCP. Shared inject+relay core below.
        // Source=Bus: suppress the transport-local `user_input` echo. The observed path below owns
        // the session-visible bus-input projection.
        self.inject_and_relay(
            recipient,
            render_injected_turn_for(batch, &recipient.0),
            InjectSource::Bus,
            None,
        )
        .await
        .map_err(ContractError::from)
    }

    async fn inject_turn_observed(
        &self,
        recipient: &SessionId,
        batch: &NexusBatch,
        _events: Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> InjectResult<()> {
        let accepted_event = stream_event_from_observed_update(accepted_event)?;
        self.inject_and_relay(
            recipient,
            render_injected_turn_for(batch, &recipient.0),
            InjectSource::Bus,
            Some(accepted_event),
        )
        .await
    }

    /// Overrides the trait default (`None`) so the heartbeat keeper gets a real answer instead of
    /// treating every ACP session as "unknown liveness" and re-stamping dead sessions as online
    /// (the zombie-presence bug fixed for tmux). Returns `Some(true)` when the session is
    /// registered and non-errored, `Some(false)` otherwise.
    ///
    /// **Limitation:** `is_live` checks the session registry (registered + non-errored), not the
    /// actual ACP subprocess. A *hung* child that hasn't yet errored will still report `Some(true)`.
    /// This is a partial probe — materially better than `None` for catching closed/errored sessions.
    fn is_harness_alive(&self, recipient: &SessionId) -> Option<bool> {
        Some(self.is_live(recipient))
    }

    async fn prompt(&self, recipient: &SessionId, text: String) -> Result<(), ContractError> {
        // DIRECT Agent Session prompt: inject the operator's text straight into the ACP session,
        // BYPASSING the bus (no inbox, no membership). Plain text — exactly what the operator typed,
        // AionUi-style. The reply streams back as `agent.update` (→ AG-UI via the gateway's observe).
        //
        // FIRE-AND-FORGET: resolve the adapter NOW (so a dead/errored session fails the send
        // immediately), then spawn the inject+relay and return at once. The operator's send returns
        // in milliseconds — the reply arrives later over the live channel/observe, no waiting for
        // turn-end (this is why AionUi has no perceptible send latency; we match it).
        let adapter = self.adapter_for(recipient).map_err(|e| {
            tracing::error!(
                target: "nexus_agent::inject",
                session = %recipient, error = %e,
                "no usable adapter for session; DM not dispatched"
            );
            to_contract(e)
        })?;
        let events = self.events.clone();
        let recipient = recipient.clone();
        let active_turn = self.active_turns.begin(&recipient);
        // Source=DirectDrive: the operator's typed input is echoed as `user_input` on the session
        // stream so the web console and `nexus attach` TUI mirror each other.
        tokio::spawn(async move {
            let _active_turn = active_turn;
            let _ = relay_turn(
                adapter,
                events,
                recipient,
                text,
                InjectSource::DirectDrive,
                None,
            )
            .await;
        });
        Ok(())
    }

    async fn prompt_observed(
        &self,
        recipient: &SessionId,
        text: String,
        _events: Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> Result<(), ContractError> {
        let accepted_event = stream_event_from_observed_update(accepted_event)?;
        let adapter = self.adapter_for(recipient).map_err(to_contract)?;
        let events = self.events.clone();
        let recipient = recipient.clone();
        let active_turn = self.active_turns.begin(&recipient);
        tokio::spawn(async move {
            let _active_turn = active_turn;
            let _ = relay_turn(
                adapter,
                events,
                recipient,
                text,
                InjectSource::Bus,
                Some(accepted_event),
            )
            .await;
        });
        Ok(())
    }

    fn steer_capability(&self, recipient: &SessionId) -> SteerCapability {
        self.adapter_for(recipient)
            .map(|adapter| adapter.steer_capability())
            .unwrap_or(SteerCapability::None)
    }

    fn active_turn_sessions(&self) -> Vec<SessionId> {
        self.active_turns.sessions()
    }

    async fn wait_for_turn_completion(&self, recipient: &SessionId) -> Result<(), ContractError> {
        self.active_turns.wait_for_completion(recipient).await;
        Ok(())
    }

    async fn interrupt_active_turn(&self, recipient: &SessionId) -> Result<(), ContractError> {
        self.adapter_for(recipient)
            .map_err(to_contract)?
            .interrupt_active_turn()
            .await
            .map_err(|error| error.to_contract_error())
    }

    async fn compact(&self, recipient: &SessionId) -> Result<(), ContractError> {
        self.adapter_for(recipient)
            .map_err(to_contract)?
            .compact()
            .await
            .map_err(|error| error.into_contract_error(recipient))
    }

    async fn steer_observed(
        &self,
        recipient: &SessionId,
        text: String,
        _events: Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> Result<SteerResponse, ContractError> {
        let adapter = self.adapter_for(recipient).map_err(to_contract)?;
        if adapter.steer_capability() != SteerCapability::InterruptAndSend {
            return Err(ContractError {
                code: nexus_contracts::codes::METHOD_NOT_FOUND,
                message: "this ACP adapter does not support active-turn redirect".into(),
            });
        }
        adapter
            .interrupt_active_turn()
            .await
            .map_err(|error| error.to_contract_error())?;
        let accepted_event = stream_event_from_observed_update(accepted_event)?;
        // Keep the durable redirect command in `started` until the replacement turn completes.
        // `AcpEngine` serializes this prompt behind the cancelled turn while allowing the cancel
        // notification to use the same connection concurrently.
        self.inject_and_relay(recipient, text, InjectSource::Bus, Some(accepted_event))
            .await
            .map_err(ContractError::from)?;
        Ok(SteerResponse {
            session_id: None,
            accepted: true,
            delivery: SteerDelivery::InterruptedAndStarted,
            turn_id: None,
        })
    }

    async fn launch(&self, req: SpawnRequest) -> Result<SpawnResponse, ContractError> {
        let session = nexus_common::new_session_id();
        let name = req.name.clone().unwrap_or_else(|| session.0.clone());
        // The bare port (mock-seam fallback) carries the project on the request, if any; the daemon's
        // `launch_agent` uses `open_session_for` with the resolved caller project instead.
        let project = req.project.clone().unwrap_or_default();

        // Mint the adapter (factory always exists for a known harness).
        let adapter = self
            .registry
            .get(
                &req.kind,
                LaunchCtx {
                    cwd: req.cwd.clone(),
                    ..Default::default()
                },
            )
            .map_err(to_contract)?;

        match adapter.open_session().await {
            Ok(()) => {
                self.bind_session(session.clone(), name.clone(), project, adapter, false);
                self.events
                    .emit(WsEvent::AgentSpawned {
                        session_id: session.clone(),
                        name: Some(name),
                        agent_id: None,
                    })
                    .await;
                Ok(SpawnResponse {
                    session_id: session,
                })
            }
            Err(e) => {
                // Init failure: retain the session (errored), surface agent.status=errored, and
                // propagate the error — never silently drop (spec §11).
                self.bind_session(session.clone(), name, project, adapter, true);
                self.events
                    .emit(WsEvent::AgentStatus {
                        session_id: session.clone(),
                        presence: Presence::Offline,
                        paused: false,
                    })
                    .await;
                Err(e.to_contract_error())
            }
        }
    }

    async fn remove(&self, req: RemoveRequest) -> Result<RemoveResponse, ContractError> {
        // Resolve the name → session (store retained), tear down the loop binding, emit removed.
        let caller = self.identity.resolve("", &req.name).await.ok();

        let mut removed_session: Option<SessionId> = None;
        // When kill=true we need the adapter handle BEFORE we remove the entry.  Capture it from
        // the session map then release the lock so we can call the async `kill()` outside.
        let mut kill_adapter: Option<Arc<dyn Adapter>> = None;
        {
            let mut sessions = self.sessions.lock().unwrap();
            // Prefer the resolved session id; else fall back to the name binding.
            if let Some(c) = &caller {
                if let Some(entry) = sessions.remove(&c.session) {
                    if req.kill {
                        kill_adapter = Some(entry.adapter);
                    }
                    removed_session = Some(c.session.clone());
                }
            }
            if removed_session.is_none() {
                if let Some(sid) = sessions
                    .iter()
                    .find(|(_, e)| e.name == req.name)
                    .map(|(sid, _)| sid.clone())
                {
                    if let Some(entry) = sessions.remove(&sid) {
                        if req.kill {
                            kill_adapter = Some(entry.adapter);
                        }
                    }
                    removed_session = Some(sid);
                }
            }
        }

        // Terminate the harness process if requested. This runs AFTER releasing the sessions lock
        // (the adapter's kill is async and must not hold the mutex).
        if let Some(adapter) = kill_adapter {
            adapter.kill().await;
        }

        if let Some(sid) = removed_session {
            // Mark the session offline in the store so the read-view roster reflects the
            // removal immediately (otherwise `members` reports it online until the heartbeat
            // TTL lapses). Best-effort — a store hiccup must not fail the detach.
            let _ = self.identity.set_offline(&sid).await;
            self.events
                .emit(WsEvent::AgentRemoved {
                    session_id: sid,
                    name: Some(req.name.clone()),
                })
                .await;
            Ok(RemoveResponse {
                name: Some(req.name),
                status: "removed".into(),
            })
        } else {
            Ok(RemoveResponse {
                name: Some(req.name),
                status: "not_found".into(),
            })
        }
    }
}

/// Map an [`AgentError`] into a [`ContractError`] via the workspace error.
fn to_contract(e: AgentError) -> ContractError {
    nexus_common::NexusError::from(e).to_contract_error()
}
