//! Per-session event-loop wiring and transport ownership.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use nexus_common::HookGatewayMode;
use nexus_contracts::ids::SessionId;
use nexus_contracts::{
    codes, AgentTurnExecutionPort, ContractError, EventSink, HookAction, HookBeforeSendRequest,
    HookBeforeSendResult, HookEvaluationRequest, HookEvaluationResponse, MessageHookPort,
    PortResult,
};
use nexus_dispatch::{AgentRegistry, Bell, EventLoop, LoopDeps};
use nexus_store::types::NativeThreadBindingRow;
use nexus_store::Store;

use crate::daemon::services::presence::{PresenceWriter, TransportHandle};
use crate::daemon::stream_raw_writer::spawn_stream_raw_writer;

use crate::daemon::gateway_hook_bridge::{GatewayHookBridge, GatewayHookBridgeError};

/// Concrete daemon adapter for the Gateway-owned `before_send` hook boundary.
pub struct GatewayMessageHookPort {
    bridge: Option<GatewayHookBridge>,
    mode: HookGatewayMode,
    timeout: Duration,
}

impl GatewayMessageHookPort {
    pub fn new(
        bridge: Option<GatewayHookBridge>,
        mode: HookGatewayMode,
        timeout: Duration,
    ) -> Self {
        Self {
            bridge,
            mode,
            timeout,
        }
    }

    fn unavailable_or_bypass(
        &self,
        request: HookBeforeSendRequest,
    ) -> PortResult<HookBeforeSendResult> {
        match self.mode {
            HookGatewayMode::Optional => Ok(passthrough_hook_result(request)),
            HookGatewayMode::Required => Err(ContractError {
                code: codes::HOOK_GATEWAY_UNAVAILABLE,
                message: "hook-capable Gateway is unavailable".into(),
            }),
        }
    }
}

#[async_trait::async_trait]
impl MessageHookPort for GatewayMessageHookPort {
    async fn before_send(
        &self,
        request: HookBeforeSendRequest,
    ) -> PortResult<HookBeforeSendResult> {
        let Some(bridge) = &self.bridge else {
            return self.unavailable_or_bypass(request);
        };
        let retained = request.clone();
        match bridge
            .evaluate(HookEvaluationRequest::BeforeSend(request), self.timeout)
            .await
        {
            Ok(HookEvaluationResponse::BeforeSend(result)) => Ok(result),
            Ok(HookEvaluationResponse::AfterReceipt(_)) => Err(ContractError {
                code: codes::INTERNAL_ERROR,
                message: "Gateway returned the wrong hook event result".into(),
            }),
            Err(
                GatewayHookBridgeError::Unavailable
                | GatewayHookBridgeError::UnsupportedEvent(_)
                | GatewayHookBridgeError::Backpressure
                | GatewayHookBridgeError::Disconnected
                | GatewayHookBridgeError::TimedOut,
            ) => self.unavailable_or_bypass(retained),
            Err(GatewayHookBridgeError::Remote(error)) => Err(ContractError {
                code: codes::INTERNAL_ERROR,
                message: format!("Gateway hook evaluation failed: {}", error.message),
            }),
        }
    }
}

fn passthrough_hook_result(request: HookBeforeSendRequest) -> HookBeforeSendResult {
    HookBeforeSendResult {
        evaluation_id: request.evaluation_id,
        action: HookAction::Continue,
        message: request.message,
        timing: None,
        executed_by: Vec::new(),
    }
}

/// The concrete handles needed to make a launched/registered agent **wakeable**: the shared
/// [`Bell`] + [`AgentRegistry`] (so a `dm`'s ring reaches the same per-session bell the loop parks
/// on), the shared [`EventSink`], the agent turn-exec port, and the drain caps. Built only by
/// [`AppState::wire`] from the same instances the [`DispatchPort`] uses — absent on the mock-port
/// [`AppState::new`] seam. `spawned` dedupes loop spawns so a resume/re-register never starts a
/// second loop for the same session.
#[derive(Clone)]
pub struct LoopWiring {
    pub(crate) store: Arc<Store>,
    pub(crate) bell: Bell,
    pub(crate) registry: AgentRegistry,
    pub(crate) events: Arc<dyn EventSink>,
    pub(crate) turn_exec: Arc<dyn AgentTurnExecutionPort>,
    pub(crate) gateway_stream: Option<crate::daemon::gateway_stream_socket::GatewayStreamPublisher>,
    pub(crate) drain_limit: u32,
    pub(crate) preview_chars: u32,
    /// Sessions whose [`EventLoop`] is already running (spawn-once guard + teardown handle).
    pub(crate) spawned: Arc<Mutex<HashMap<SessionId, JoinHandle<()>>>>,
    /// One-way lifecycle fence. Once graceful shutdown begins, concurrent wake tasks may finish
    /// rebinding metadata but must never create or ring a replacement delivery loop.
    pub(crate) shutting_down: Arc<AtomicBool>,
    /// Structured native forwarders, including their abort handles so teardown cannot leave a
    /// detached task polling a deleted or stopped runtime sidecar.
    pub(crate) native_forwarders: Arc<Mutex<HashMap<(String, SessionId), NativeForwarderSlot>>>,
    /// Headed PTY/tmux sessions whose raw terminal output is already being copied to stream_raw.
    pub(crate) raw_stream_writers: Arc<Mutex<HashSet<SessionId>>>,
    /// Connection-owned liveness registry + materialized presence writer.
    pub(crate) presence: PresenceWriter,
}

pub(crate) struct NativeForwarderSlot {
    owner: Option<uuid::Uuid>,
    handle: Option<JoinHandle<()>>,
}

#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct LaunchIdentity {
    pub agent_id: String,
    pub name: Option<String>,
    pub project: String,
}

impl LaunchIdentity {
    pub(crate) fn launch_label(&self) -> &str {
        self.name.as_deref().unwrap_or(&self.agent_id)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CodexResumeIdentity {
    pub(crate) identity: LaunchIdentity,
    pub(crate) binding: NativeThreadBindingRow,
}

impl LoopWiring {
    pub(crate) fn events(&self) -> Arc<dyn EventSink> {
        self.events.clone()
    }

    pub(crate) fn bell(&self) -> Bell {
        self.bell.clone()
    }

    /// Spawn the per-agent [`EventLoop`] for `session` (scoped to `project`) once. A second call for
    /// the same session is a no-op (the loop is already parked on the bell). The loop re-rings on
    /// attach, so anything already `pending` is drained immediately (crash-safety + the
    /// launch→dm→wake path). `project` must be the session's own project: the drainer filters
    /// in-flight rows by it (`pending_for(... project ...)`), so an empty/wrong project drains
    /// nothing and the agent never wakes.
    pub(crate) fn spawn_loop(&self, session: &SessionId, project: &str) {
        let mut spawned = self.spawned.lock().expect("spawned set poisoned");
        if self.shutting_down.load(Ordering::Acquire) {
            tracing::debug!(
                target: "nexus::launch",
                session = %session,
                "daemon shutdown fence refused a replacement event loop"
            );
            return;
        }
        if spawned.contains_key(session) {
            tracing::debug!(
                target: "nexus::launch",
                session = %session,
                "event loop already running; not re-spawning"
            );
            return; // already running
        }
        // The `project` here is the scope the loop will drain by. It MUST equal the project the bus
        // writes the in-flight rows under (the sender's project), or `drain_once` matches nothing and
        // the agent never wakes. An empty `project` is the classic launch→project-scope footgun.
        tracing::info!(
            target: "nexus::launch",
            session = %session,
            project = %project,
            "spawning per-agent event loop"
        );
        let handle = EventLoop::spawn(
            session.clone(),
            LoopDeps {
                store: self.store.clone(),
                bell: self.bell.clone(),
                registry: self.registry.clone(),
                turn_exec: self.turn_exec.clone(),
                events: self.events.clone(),
                project: project.to_string(),
                drain_limit: self.drain_limit,
                preview_chars: self.preview_chars,
                completion_timeout: nexus_dispatch::DEFAULT_INJECT_COMPLETION_TIMEOUT,
                provider_limit_default_cooldown: nexus_dispatch::DEFAULT_PROVIDER_LIMIT_COOLDOWN,
            },
        );
        spawned.insert(session.clone(), handle);
        self.presence
            .registry()
            .attach(session, TransportHandle::EventLoop);
    }

    /// Ring the session's bell so a running loop drains its pending queue once. Used after a
    /// successful (re)bind in [`AppState::ensure_live`]: when the loop ALREADY exists (spawn-once),
    /// `spawn_loop` returns early WITHOUT re-ringing — so a freshly-rebound (newly-live) adapter
    /// would have pending mail but no wake. An explicit ring here re-drives the drain. Harmless when
    /// the loop was just spawned (it already rang on attach; the extra ring coalesces).
    pub(crate) fn ring(&self, session: &SessionId) {
        if self.shutting_down.load(Ordering::Acquire) {
            return;
        }
        self.bell.ring(session);
    }

    /// Transfer inbox consumption to a durable pull subscription.
    ///
    /// The bell remains shared with held-receive, but the daemon-owned harness loop is aborted so
    /// exactly one consumer can claim `in_flight` rows for this session.
    pub(crate) fn claim_pull_delivery(&self, session: &SessionId) {
        self.abort_loop(session);
    }

    pub(crate) fn spawn_raw_stream_writer(
        &self,
        session: &SessionId,
        output: broadcast::Receiver<Vec<u8>>,
    ) {
        {
            let mut spawned = self
                .raw_stream_writers
                .lock()
                .expect("raw stream writer set poisoned");
            if !spawned.insert(session.clone()) {
                return;
            }
        }
        spawn_stream_raw_writer(self.store.clone(), session.clone(), output);
        self.presence
            .registry()
            .attach(session, TransportHandle::RawStream);
    }

    /// Claim a structured native forwarder slot. Forwarders read append-only native stores, so
    /// multiple tasks for one session would duplicate `/agent` stream rows.
    pub(crate) fn claim_native_forwarder(&self, harness: &str, session: &SessionId) -> bool {
        let mut spawned = self
            .native_forwarders
            .lock()
            .expect("native forwarder set poisoned");
        let key = (harness.to_string(), session.clone());
        let inserted = if spawned.contains_key(&key) {
            false
        } else {
            spawned.insert(
                key,
                NativeForwarderSlot {
                    owner: None,
                    handle: None,
                },
            );
            true
        };
        if inserted {
            self.presence.registry().attach(
                session,
                TransportHandle::NativeForwarder(harness.to_string()),
            );
        }
        inserted
    }

    /// Called under the captured binding's owner gate. A fresh owner supersedes an older
    /// reservation as well as an attached task; the map remains the sole forwarder registry.
    pub(crate) fn claim_native_forwarder_for_owner(
        &self,
        harness: &str,
        session: &SessionId,
        owner: uuid::Uuid,
    ) -> bool {
        let mut slots = self
            .native_forwarders
            .lock()
            .expect("native forwarder set poisoned");
        let key = (harness.to_string(), session.clone());
        if slots
            .get(&key)
            .is_some_and(|slot| slot.owner == Some(owner))
        {
            return false;
        }
        if let Some(old) = slots.insert(
            key,
            NativeForwarderSlot {
                owner: Some(owner),
                handle: None,
            },
        ) {
            if let Some(handle) = old.handle {
                handle.abort();
            }
        }
        self.presence.registry().attach(
            session,
            TransportHandle::NativeForwarder(harness.to_string()),
        );
        true
    }

    pub(crate) fn track_native_forwarder_for_owner(
        &self,
        harness: &str,
        session: &SessionId,
        owner: uuid::Uuid,
        handle: JoinHandle<()>,
    ) {
        let mut slots = self
            .native_forwarders
            .lock()
            .expect("native forwarder set poisoned");
        if let Some(slot) = slots
            .get_mut(&(harness.to_string(), session.clone()))
            .filter(|slot| slot.owner == Some(owner))
        {
            if let Some(old) = slot.handle.replace(handle) {
                old.abort();
            }
        } else {
            handle.abort();
        }
    }

    pub(crate) fn release_native_forwarder_for_owner(
        &self,
        harness: &str,
        session: &SessionId,
        owner: uuid::Uuid,
    ) {
        let mut slots = self
            .native_forwarders
            .lock()
            .expect("native forwarder set poisoned");
        let key = (harness.to_string(), session.clone());
        if slots
            .get(&key)
            .is_some_and(|slot| slot.owner == Some(owner))
        {
            if let Some(handle) = slots.remove(&key).and_then(|slot| slot.handle) {
                handle.abort();
            }
            self.presence.registry().detach(
                session,
                &TransportHandle::NativeForwarder(harness.to_string()),
            );
        }
    }

    /// Attach the spawned task to its reserved forwarder slot. If teardown won the race after the
    /// reservation, abort the new task immediately instead of orphaning it.
    pub(crate) fn track_native_forwarder(
        &self,
        harness: &str,
        session: &SessionId,
        handle: JoinHandle<()>,
    ) {
        let key = (harness.to_string(), session.clone());
        let mut spawned = self
            .native_forwarders
            .lock()
            .expect("native forwarder set poisoned");
        match spawned.get_mut(&key) {
            Some(slot) => {
                if let Some(previous) = slot.handle.replace(handle) {
                    previous.abort();
                }
            }
            None => handle.abort(),
        }
    }

    /// Release a failed forwarder claim so a later launch/revive/adoption can retry.
    pub(crate) fn release_native_forwarder(&self, harness: &str, session: &SessionId) {
        let mut spawned = self
            .native_forwarders
            .lock()
            .expect("native forwarder set poisoned");
        if let Some(slot) = spawned.remove(&(harness.to_string(), session.clone())) {
            if let Some(handle) = slot.handle {
                handle.abort();
            }
            self.presence.registry().detach(
                session,
                &TransportHandle::NativeForwarder(harness.to_string()),
            );
        }
    }

    pub(crate) fn abort_loop(&self, session: &SessionId) {
        if let Some(handle) = self
            .spawned
            .lock()
            .expect("spawned set poisoned")
            .remove(session)
        {
            handle.abort();
        }
        self.presence
            .registry()
            .detach(session, &TransportHandle::EventLoop);
    }

    /// Stop every delivery loop and wait until its in-progress adapter future has been dropped.
    ///
    /// Graceful daemon shutdown must cross this boundary before it closes any harness adapter. If
    /// an adapter is closed first, its pending prompt future resolves as a contract error while the
    /// old event loop is still alive, and that loop incorrectly dead-letters an intentional
    /// restart cancellation. Aborting and joining the loops first leaves the durable `injecting`
    /// attempt untouched so boot recovery can classify it as `delivery_outcome_unknown`; only an
    /// explicit operator requeue may retry that ambiguous external attempt.
    pub(crate) async fn quiesce_delivery_loops_for_shutdown(&self) -> usize {
        // Set the one-way fence before taking the spawn map. A spawn already holding the map lock
        // is inserted before we drain it; every later spawn observes the fence and refuses.
        self.shutting_down.store(true, Ordering::Release);
        let handles: Vec<(SessionId, JoinHandle<()>)> = self
            .spawned
            .lock()
            .expect("spawned set poisoned")
            .drain()
            .collect();
        let count = handles.len();

        for (_, handle) in &handles {
            handle.abort();
        }
        for (session, handle) in handles {
            let _ = handle.await;
            self.presence
                .registry()
                .detach(&session, &TransportHandle::EventLoop);
        }
        count
    }

    pub(crate) fn teardown_session_transports(&self, session: &SessionId) {
        self.registry
            .set(session, nexus_dispatch::AgentState::Offline);
        self.abort_loop(session);
        if self
            .raw_stream_writers
            .lock()
            .expect("raw stream writer set poisoned")
            .remove(session)
        {
            self.presence
                .registry()
                .detach(session, &TransportHandle::RawStream);
        }
        self.release_native_forwarder("claude", session);
        self.release_native_forwarder("opencode", session);
        self.release_native_forwarder("hermes", session);
    }

    /// Keep launched/ACP-bound agents "present": every `ttl_ms`/2 it refreshes the compatibility
    /// session row and durable runtime row for every session whose [`EventLoop`] is running. Such
    /// agents are driven over ACP and don't self-beat like a CLI peer, yet they ARE live while their
    /// loop runs — so without this they'd go stale → offline → hidden from `members` and lose the
    /// `/agents` active-runtime projection even though they stay wakeable. Spawned once at startup
    /// (no-op on the mock-port seam, which has no loop wiring).
    pub(crate) fn spawn_heartbeat_keeper(&self, ttl_ms: i64) {
        let spawned = self.spawned.clone();
        let native_forwarders = self.native_forwarders.clone();
        let raw_stream_writers = self.raw_stream_writers.clone();
        let registry = self.registry.clone();
        let presence = self.presence.clone();
        let turn_exec = self.turn_exec.clone();
        let interval_ms = (ttl_ms.max(2) / 2) as u64;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
            // `interval` yields its first tick immediately. Skip that startup tick so registration
            // paths that bind their harness just after `register` returns are not probed in the
            // tiny registered-but-not-yet-bound window.
            tick.tick().await;
            loop {
                tick.tick().await;
                let sessions: Vec<SessionId> = {
                    spawned
                        .lock()
                        .expect("spawned set poisoned")
                        .keys()
                        .cloned()
                        .collect()
                };
                for s in &sessions {
                    // TRUTHFUL presence: only keep a session "present" while its harness is actually
                    // alive. The keeper used to stamp EVERY looped session, so a harness that had died
                    // (tmux gone) kept a fresh heartbeat and read "online" forever — the operator
                    // would post to it, the inject would silently time out, and nothing replied. Now a
                    // dead harness is flipped to `offline` (and not re-stamped) so the roster tells the
                    // truth. `None` (ACP / no probe) keeps the old heartbeat behaviour.
                    if turn_exec.is_harness_alive(s) == Some(false) {
                        registry.set(s, nexus_dispatch::AgentState::Offline);
                        if let Some(handle) =
                            spawned.lock().expect("spawned set poisoned").remove(s)
                        {
                            handle.abort();
                        }
                        presence.registry().detach(s, &TransportHandle::EventLoop);
                        if raw_stream_writers
                            .lock()
                            .expect("raw stream writer set poisoned")
                            .remove(s)
                        {
                            presence.registry().detach(s, &TransportHandle::RawStream);
                        }
                        {
                            let mut native_forwarders = native_forwarders
                                .lock()
                                .expect("native forwarder set poisoned");
                            for harness in ["claude", "opencode", "hermes"] {
                                if let Some(slot) =
                                    native_forwarders.remove(&(harness.to_string(), s.clone()))
                                {
                                    if let Some(handle) = slot.handle {
                                        handle.abort();
                                    }
                                    presence.registry().detach(
                                        s,
                                        &TransportHandle::NativeForwarder(harness.to_string()),
                                    );
                                }
                            }
                        }
                        if let Err(error) = presence.mark_dead_harness_offline(s).await {
                            tracing::warn!(
                                target: "nexus::presence",
                                session = %s,
                                error = %error,
                                "failed to materialize dead harness presence"
                            );
                        }
                    } else {
                        // Alive (ACP `None` or `Some(true)`): keep the heartbeat fresh and restore
                        // liveness without erasing an explicit `busy` state. A slow-opening ACP
                        // harness (notably `opencode`, whose
                        // cold start — skill scan + model init — runs ~35s, past the keeper's ~15s
                        // tick) is registered `online` but is not yet adapter-bound during the open
                        // window, so an earlier tick saw `is_harness_alive == Some(false)` and flipped
                        // it `offline`. Without re-stamping `online` here, the column stays `offline`
                        // forever even once the session opens and streams — the agent never "comes
                        // online" in `members`. Re-stamping is idempotent for an already-live row.
                        if let Err(error) = presence.materialize_online(s).await {
                            tracing::warn!(
                                target: "nexus::presence",
                                session = %s,
                                error = %error,
                                "failed to materialize live harness presence"
                            );
                        }
                    }
                }
            }
        });
    }
}
