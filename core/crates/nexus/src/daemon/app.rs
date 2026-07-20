//! [`AppState`] — the single wiring point for the whole workspace (Task 14).
//!
//! `AppState` holds an `Arc` to every service behind its `*Port` trait, one shared
//! [`nexus_store::Store`], and the event-log [`WsSink`] (the crate's [`EventSink`] impl; name kept
//! for wire compatibility with `WsEvent`). The
//! wiring order is fixed by the dependency graph:
//!
//! `store → event-sink → identity → agent → realtime → bus → search → notify → admin`
//!
//! Per-agent [`nexus_dispatch::EventLoop`]s are spawned with [`nexus_dispatch::LoopDeps`] carrying
//! the agent service as the `turn_exec`. The same `AppState` backs the gateway/session edge and the
//! store-backed command worker, both of which execute through
//! [`crate::daemon::routing::route_request`] where appropriate.
//!
//! ## Launch / register wiring (the addressable + wakeable contract)
//!
//! A launched or registered `kind=agent` session is only useful if it is **addressable** (appears in
//! `members`, resolvable by name) **and wakeable** (a `dm` rings its bell → its [`EventLoop`] drains
//! → `inject_turn`). [`AppState::launch_agent`] orchestrates exactly that: it opens+binds the adapter
//! (`agent.launch`), writes the identity/member row under the **same** session id, then spawns the
//! per-agent [`EventLoop`]. Plain `register` of a `kind=agent` session also gets a loop via
//! [`AppState::ensure_agent_loop`]. The concrete handles needed to spawn a loop (the [`Bell`], the
//! [`AgentRegistry`], the event sink, the drain caps) live in an optional [`LoopWiring`] built only
//! by [`AppState::wire`]; the low-level [`AppState::new`] mock-port seam has none, so loop-spawning
//! is a no-op there (the dispatch unit tests use mock ports and assert the registry, not the loop).
//!
//! [`EventLoop`]: nexus_dispatch::EventLoop
//! [`Bell`]: nexus_dispatch::Bell
//! [`AgentRegistry`]: nexus_dispatch::AgentRegistry

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

mod adoption;
mod delivery_recovery;
mod launch_lifecycle;
mod launch_orchestration;
mod presence_lifecycle;
mod revive;
mod routing_recovery;
mod runtime_registration;
mod teardown;

use tokio::sync::{broadcast, Notify};

use nexus_common::{now, Config, NexusError, RuntimeProcessIds};
use nexus_contracts::ids::{AgentId, SessionId};
use nexus_contracts::{
    AdminGroupAssignRequest, AdminGroupAssignResponse, AdminPort, AgentAccessGrantRequest,
    AgentAccessGrantResponse, AgentAccessRevokeRequest, AgentAccessRevokeResponse,
    AgentCreateRequest, AgentCreateResponse, AgentCredentialCreateRequest,
    AgentCredentialCreateResponse, AgentCredentialRevokeRequest, AgentCredentialRevokeResponse,
    AgentListRequest, AgentListResponse, AgentOwnerTransferRequest, AgentOwnerTransferResponse,
    AgentRuntimeListRequest, AgentRuntimeListResponse, AgentShowRequest, AgentShowResponse,
    AgentTurnExecutionPort, AssignProjectRequest, AssignProjectResponse, AssignRoleRequest,
    AssignRoleResponse, BusPort, Caller, ContractError, DispatchPort, EventSink, GrantTierRequest,
    GrantTierResponse, HarnessId, IdentityPort, Kind, Message, MetadataResponse,
    MetadataSetRequest, NotifyPort, PushRequest, PushResponse, ReadRequest, SearchPort,
    SpawnRequest, SpawnResponse, Tier, WsEvent,
};
use nexus_dispatch::{AgentRegistry, Bell};
use nexus_harness_core::HeadedRuntimeKind;
use nexus_store::repos::inbox::TARGET_UNREACHABLE_ERROR_CODE;
use nexus_store::repos::{
    AgentAccessGrants, AgentOwner, AgentRef, AgentRuntimes, Agents, DeliveryObligations,
    DeveloperEvents, IdentitySessions, Inbox, InitialPromptDeliveries, InitialPromptInsert,
    InitialPromptInsertOutcome, NativeThreadBindings, NewAgent, NewAgentRuntime,
    NewIdentitySession, NewSession, RoutingThreads, Sessions, Threads,
};
use nexus_store::types::{AgentRuntimeRow, SessionRow};
use nexus_store::Store;

use crate::daemon::claude_native_forwarder::{
    claude_native_paths_for_runtime, spawn_claude_native_forwarder_with_tool_events,
    GatewayClaudeToolObservationSink, DEFAULT_CLAUDE_NATIVE_FORWARDER_POLL_MS,
};
use crate::daemon::claude_resume_harvest::{
    default_home, harvest_missing_claude_resume_ids, ClaudeResumeHarvestReport,
    ClaudeResumeHarvestStatus,
};
use crate::daemon::hermes_native_forwarder::{
    spawn_hermes_native_forwarder_with_tool_events, GatewayHermesToolObservationSink,
    HermesRuntimeStateRepo, HermesToolObservationSink, DEFAULT_HERMES_NATIVE_FORWARDER_POLL_MS,
};
use crate::daemon::opencode_native_forwarder::{
    spawn_opencode_native_forwarder_with_tool_events, GatewayOpenCodeToolObservationSink,
    OpenCodeRuntimeStateRepo, OpenCodeToolObservationSink,
    DEFAULT_OPENCODE_NATIVE_FORWARDER_POLL_MS,
};
use crate::daemon::pty_reply_reader::spawn_pty_reply_reader;
use crate::daemon::pty_supervisor::{headed_runtime_kind, PtySupervisor};
use crate::daemon::runtime_revive_gate::RuntimeReviveGate;
use crate::daemon::services::identity_admin::IdentityAdminService;
pub(crate) use crate::daemon::services::loop_wiring::CodexResumeIdentity;
pub use crate::daemon::services::loop_wiring::{LaunchIdentity, LoopWiring};
use crate::daemon::services::presence::{PresenceWriter, TransportHandle, TransportRegistry};
pub(crate) use crate::daemon::services::runtime_helpers::{
    claude_resume_session_id, codex_resume_agent_owner_matches, codex_resume_session_owner_matches,
    codex_thread_taken_error, default_agent_cwd_for, ensure_agent_launch_cwd, harness_token,
    kind_token, native_session_taken_error, opencode_resume_session_id,
    persist_codex_thread_binding_once, persist_codex_thread_binding_with_retry,
    protected_remove_target, resolve_agent_launch_cwd, spawn_identity_policy, tier_token,
};
pub use crate::daemon::services::runtime_helpers::{
    harness_from_token, headed_runtime_from_agent_token, launch_route, revive_route,
    teardown_route, LaunchRoute, ReviveRoute, TeardownRoute,
};
use crate::daemon::services::source::SourceService;
pub use crate::daemon::services::ws_sink::WsSink;
use crate::daemon::transcript_archive::{archive_claude_once, archive_codex_once};
use crate::harness_registry::harness_registry_by_id;

const PENDING_RESPAWN_BASE_BACKOFF_MS: i64 = 30_000;
const PENDING_RESPAWN_MAX_BACKOFF_MS: i64 = 5 * 60_000;
const PENDING_RESPAWN_TOMBSTONE_FAILURES: u32 = 3;

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingRespawnBackoff {
    pub failures: u32,
    pub next_retry_at: i64,
}

/// Compute the bounded retry delay for a pending-recipient respawn failure.
#[doc(hidden)]
pub fn pending_respawn_delay_ms(failures: u32) -> i64 {
    let exponent = failures.saturating_sub(1).min(4);
    let factor = 1_i64 << exponent;
    (PENDING_RESPAWN_BASE_BACKOFF_MS * factor).min(PENDING_RESPAWN_MAX_BACKOFF_MS)
}

fn log_claude_resume_harvest_report(report: &ClaudeResumeHarvestReport) {
    for item in &report.items {
        match item.status {
            ClaudeResumeHarvestStatus::Updated => {
                tracing::warn!(
                    target: "nexus::revive",
                    session = %item.runtime_id,
                    native_session_id = item.native_session_id.as_deref().unwrap_or(""),
                    hook_log = %item.hook_log.display(),
                    "backfilled missing Claude resume id from bridge hook"
                );
            }
            ClaudeResumeHarvestStatus::MissingHookLog
            | ClaudeResumeHarvestStatus::NoNativeSessionId
            | ClaudeResumeHarvestStatus::AmbiguousNativeSessionId
            | ClaudeResumeHarvestStatus::UpdateFailed
            | ClaudeResumeHarvestStatus::MissingRuntimeState => {
                tracing::warn!(
                    target: "nexus::revive",
                    session = %item.runtime_id,
                    status = ?item.status,
                    reason = item.reason.as_deref().unwrap_or("unknown"),
                    hook_log = %item.hook_log.display(),
                    "skipped Claude resume-id backfill"
                );
            }
            ClaudeResumeHarvestStatus::AlreadyPresent => {}
        }
    }
}

fn session_wake_state(paused: bool) -> nexus_dispatch::AgentState {
    if paused {
        nexus_dispatch::AgentState::Paused
    } else {
        nexus_dispatch::AgentState::Idle
    }
}

pub(crate) struct RegisterRuntimeRequest<'a> {
    session: &'a SessionId,
    agent_id: &'a str,
    name: Option<&'a str>,
    project: &'a str,
    kind: HarnessId,
    role: Option<String>,
    client_key: &'a str,
    cwd: Option<String>,
    transport: &'a str,
    owner: Option<&'a Caller>,
}

#[derive(Clone)]
pub(crate) struct RegisteredRuntime {
    session: SessionId,
    agent_id: String,
    name: Option<String>,
    project: String,
    kind: HarnessId,
    role: Option<String>,
    cwd: Option<String>,
}

pub(crate) struct PreparedInitialPrompt {
    rendered_prompt: String,
    client_message_id: String,
}

/// The wired application state — `Arc` handles to every port + the shared store + the event sink.
#[derive(Clone)]
pub struct AppState {
    /// The durable store (sole writer); also used directly for the `read` registry method.
    pub store: Arc<Store>,
    /// The daemon event sink (legacy type name, [`EventSink`] impl).
    pub ws: WsSink,
    pub identity: Arc<dyn IdentityPort>,
    pub agent: Arc<dyn AgentTurnExecutionPort>,
    pub realtime: Arc<dyn DispatchPort>,
    pub bus: Arc<dyn BusPort>,
    pub search: Arc<dyn SearchPort>,
    pub notify: Arc<dyn NotifyPort>,
    pub admin: Arc<dyn AdminPort>,
    pub(crate) sources: SourceService,
    pub(crate) identity_admin: IdentityAdminService,
    pub(crate) presence: PresenceWriter,
    /// The caller's project scope is per-request; `project` is the daemon default for `read`.
    pub project: String,
    /// Daemon state root containing the boot-scoped IPC manifest. Daemon-launched harnesses inherit
    /// this value so their CLI/MCP children discover the owner without receiving store credentials.
    nexus_home: String,
    /// Heartbeat freshness window used by daemon presence reconciliation.
    heartbeat_ttl_ms: i64,
    /// Retention window for terminal command-intent queue rows.
    command_intent_retention_ms: i64,
    /// Store size threshold that can trigger operational retention ahead of the normal interval.
    reap_size_threshold_mb: i64,
    /// Minimum interval between scheduled operational retention passes.
    reap_interval_ms: i64,
    /// Operator-configured default terminal backend for headed launches (`launch_backend` in
    /// nexus.toml / `NEXUS_LAUNCH_BACKEND`). `None` = built-in default ("pty"). Validated to
    /// "pty" | "tmux" at wire time so a typo fails the boot loudly instead of silently
    /// resolving to an unexpected owner.
    launch_backend: Option<String>,
    /// HMAC secret the gateway-forwarded `notify` signature is verified against (edge-only).
    pub hmac_secret: String,
    /// Loop-spawn wiring (present only on the [`AppState::wire`] production path). When `None`
    /// (mock-port tests), launch/register still register identity but spawn no loop.
    loop_wiring: Option<LoopWiring>,
    /// Boot/wake pending-inbox revive failures by recipient session.
    ///
    /// Pending delivery rows deliberately stay durable, but repeated revive attempts against the
    /// same dead fossil must not hammer harness launch paths on every boot or bell. This map is
    /// process-local so an operator restart can retry after fixing the underlying row.
    pending_respawn_backoff: Arc<Mutex<HashMap<SessionId, PendingRespawnBackoff>>>,
    /// One structured harness replacement per durable runtime. Boot adoption and the first queued
    /// delivery can request the same revive concurrently; overlapping relaunches would replace
    /// each other's bridge/terminal pair.
    runtime_revive_gate: RuntimeReviveGate,
    /// Closes the boot race between command ingress and reconstruction of the volatile runtime
    /// directory. Command workers must not reject a valid surviving bridge credential while its
    /// persistent identity capsule is still being materialized.
    runtime_identity_ready: Arc<AtomicBool>,
    runtime_identity_ready_notify: Arc<Notify>,
    /// One-way fence for graceful daemon shutdown. Once raised, command lanes finish only the
    /// dispatch boundary they already own and refuse every later claim; newly accepted rows remain
    /// pending for the next daemon instead of becoming ambiguous during transport teardown.
    command_worker_shutting_down: Arc<AtomicBool>,
    /// The concrete agent service (present only on the [`AppState::wire`] path). The daemon's launch
    /// orchestration calls [`nexus_agent::Agent::open_session_for`] on it to bind the adapter under a
    /// daemon-chosen session id. `None` on the mock-port [`AppState::new`] seam → launch falls back
    /// to the bare `AgentTurnExecutionPort::launch`.
    agent_concrete: Option<Arc<nexus_agent::Agent>>,
    /// The daemon-owned [`PtySupervisor`] (present only on the [`AppState::wire_pty`] PTY-native
    /// path). When `Some` and the launch kind has a [`harness_program`], `launch_agent` spawns the
    /// native harness in a PTY (Task 7) and tails its transcript instead of opening an ACP adapter.
    /// `None` on every other wiring (`wire`/`wire_with_registry`/`wire_with_turn_exec` for tests) →
    /// the ACP launch path is taken unchanged.
    pty: Option<Arc<PtySupervisor>>,
}

impl AppState {
    /// Assemble `AppState` from pre-built port `Arc`s and the shared store + sink. The
    /// [`AppState::wire`] helper builds the concrete services in dependency order; this constructor
    /// is the low-level seam tests use to inject mock ports.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Arc<Store>,
        ws: WsSink,
        identity: Arc<dyn IdentityPort>,
        agent: Arc<dyn AgentTurnExecutionPort>,
        realtime: Arc<dyn DispatchPort>,
        bus: Arc<dyn BusPort>,
        search: Arc<dyn SearchPort>,
        notify: Arc<dyn NotifyPort>,
        admin: Arc<dyn AdminPort>,
        project: String,
    ) -> Self {
        let sources = SourceService::new(store.clone(), bus.clone());
        let identity_admin =
            IdentityAdminService::new(store.clone(), Arc::new(ws.clone()), admin.clone());
        let transport_registry = TransportRegistry::new();
        let presence = PresenceWriter::new(
            store.clone(),
            Arc::new(ws.clone()),
            transport_registry.clone(),
        );
        AppState {
            store,
            ws,
            identity,
            agent,
            realtime,
            bus,
            search,
            notify,
            admin,
            sources,
            identity_admin,
            presence,
            project,
            nexus_home: crate::daemon::lifecycle::nexus_home()
                .to_string_lossy()
                .into_owned(),
            heartbeat_ttl_ms: Config::default().heartbeat_ttl_ms,
            command_intent_retention_ms: Config::default().command_intent_retention_ms,
            reap_size_threshold_mb: Config::default().reap_size_threshold_mb,
            reap_interval_ms: crate::daemon::retention_policy::parse_reap_interval_ms(
                &Config::default().reap_interval,
            ),
            launch_backend: None,
            hmac_secret: String::new(),
            loop_wiring: None,
            pending_respawn_backoff: Arc::new(Mutex::new(HashMap::new())),
            runtime_revive_gate: RuntimeReviveGate::default(),
            runtime_identity_ready: Arc::new(AtomicBool::new(true)),
            runtime_identity_ready_notify: Arc::new(Notify::new()),
            command_worker_shutting_down: Arc::new(AtomicBool::new(false)),
            agent_concrete: None,
            pty: None,
        }
    }

    pub(crate) async fn begin_command_worker_shutdown(&self) {
        let _claim_fence = self.store.write_lock().lock_owned().await;
        self.command_worker_shutting_down
            .store(true, Ordering::Release);
        self.store.notify_command_intent_inserted();
    }

    pub(crate) fn command_worker_is_shutting_down(&self) -> bool {
        self.command_worker_shutting_down.load(Ordering::Acquire)
    }

    /// Heartbeat freshness window used by daemon-side stale-session reapers.
    pub(crate) fn heartbeat_ttl_ms(&self) -> i64 {
        self.heartbeat_ttl_ms
    }

    /// Retention window for terminal command-intent queue rows.
    pub(crate) fn command_intent_retention_ms(&self) -> i64 {
        self.command_intent_retention_ms
    }

    /// Store size threshold that can trigger operational retention ahead of the normal interval.
    pub(crate) fn reap_size_threshold_mb(&self) -> i64 {
        self.reap_size_threshold_mb
    }

    /// Minimum interval between scheduled operational retention passes.
    pub(crate) fn reap_interval_ms(&self) -> i64 {
        self.reap_interval_ms
    }

    /// Operator-configured default terminal backend for headed launches, when set.
    pub(crate) fn launch_backend_default(&self) -> Option<&str> {
        self.launch_backend.as_deref()
    }

    /// Build the full production wiring from a migrated [`Store`] + [`Config`]: every concrete
    /// service in dependency order, each behind its port `Arc`, sharing one store and one
    /// shared [`EventSink`]. Uses the built-in adapter registry plus Claude/Codex (wired at
    /// the composition root). Returns the assembled `AppState`. Tests that need the hermetic
    /// `MockAdapter` use [`AppState::wire_with_registry`].
    pub fn wire(store: Arc<Store>, config: &Config) -> Self {
        let mut registry = nexus_agent::AdapterRegistry::with_builtins();
        nexus_harness_claude::register(&mut registry);
        nexus_harness_codex::register(&mut registry);
        crate::spawn_spec::install_spawn_specs(&mut registry);
        Self::wire_with_registry(store, config, registry)
    }

    /// Like [`AppState::wire`] but with a caller-supplied [`nexus_agent::AdapterRegistry`] — used by
    /// the acceptance suite to register a hermetic `MockAdapter` for the harness it drives, while
    /// reusing the **identical** production wiring (including the launch/register loop wiring). This
    /// is the seam that lets `core/tests/` boot a real in-test daemon with the mock adapter.
    pub fn wire_with_registry(
        store: Arc<Store>,
        config: &Config,
        registry: nexus_agent::AdapterRegistry,
    ) -> Self {
        use nexus_agent::Agent;
        use nexus_identity::Identity;

        // Broadcast buffer per local observer. Sized to absorb bursty turns (a fast harness can emit
        // dozens of `session/update` chunks in a few ms — observed codex bursts of 30+ in 3ms) plus
        // many agents streaming at once, so a momentarily-behind observer doesn't trip
        // `RecvError::Lagged` (which silently SKIPS missed events → "dropped" stream).
        let ws = WsSink::new(16_384, Some(store.clone()));
        let events: Arc<dyn EventSink> = Arc::new(ws.clone());

        // identity (built here so it can be shared with the concrete `Agent` service)
        let identity_svc = Arc::new(Identity::new(store.clone(), events.clone(), config));
        let identity: Arc<dyn IdentityPort> = identity_svc.clone();

        // agent (ACP) — the concrete service IS the turn executor on this path.
        let agent_svc = Arc::new(Agent::new(registry, identity.clone(), events.clone()));
        let agent: Arc<dyn AgentTurnExecutionPort> = agent_svc.clone();

        // Assemble the shared body around the ACP agent as the turn-exec, keeping `agent_concrete`
        // so the launch orchestration can open/bind ACP sessions.
        let state = Self::wire_shared(store, config, ws, events, identity, agent, Some(agent_svc));
        state.spawn_boot_respawn();
        state
    }

    /// Like [`AppState::wire_with_registry`] but substitutes a caller-supplied
    /// [`AgentTurnExecutionPort`] (e.g. the PTY-native [`crate::daemon::PtyTransport`]) for the ACP
    /// `Agent` service — the ONE swap of the PTY-native harness path. The SAME `LoopWiring` (shared
    /// `bell`/`registry`, the drain loop), bus, realtime, identity, and store are built exactly as in
    /// `wire_with_registry`; `turn_exec` is used everywhere the ACP `agent` port was: `self.agent`,
    /// `admin`'s agent arg, and `loop_wiring.turn_exec`. The `nexus_agent::Agent` ACP service is NOT
    /// built (the transport replaces it), so `agent_concrete` is `None` — launch falls back to the
    /// bare `AgentTurnExecutionPort::launch` (PTY launch is `PtySupervisor`'s job, Task 7), but the
    /// register/wake → bus → drain → `inject_turn` delivery path is fully wired.
    pub fn wire_with_turn_exec(
        store: Arc<Store>,
        config: &Config,
        turn_exec: Arc<dyn AgentTurnExecutionPort>,
    ) -> Self {
        use nexus_identity::Identity;

        let ws = WsSink::new(16_384, Some(store.clone()));
        let events: Arc<dyn EventSink> = Arc::new(ws.clone());

        let identity_svc = Arc::new(Identity::new(store.clone(), events.clone(), config));
        let identity: Arc<dyn IdentityPort> = identity_svc.clone();

        // No ACP `Agent` service — the supplied `turn_exec` IS the executor and `agent_concrete` is
        // None (launch falls back to the bare port; PTY launch lives in PtySupervisor).
        let state = Self::wire_shared(store, config, ws, events, identity, turn_exec, None);
        state.spawn_boot_respawn();
        state
    }

    /// The PTY-native daemon wiring: own a [`PtySupervisor`] (headed path) AND the ACP [`Agent`]
    /// service (headless path), both mounted behind a [`RoutingTurnExec`] so a single daemon serves
    /// both launch modes. The `RoutingTurnExec` dispatches each turn by `PtyTransport::is_bound`:
    /// PTY-bound sessions go to the supervisor's transport; unbound sessions go to the ACP agent.
    ///
    /// CRITICAL: the ACP `Agent` is built with the SAME `events` (`WsSink`) as the rest of the
    /// assembly — so a headless agent's `relay_turn` streams to the same sink the tailer/web read.
    /// A second `WsSink` would orphan headless agent replies; they'd never reach the web console.
    ///
    /// Used by the daemon binary and integration coverage. `wire_with_turn_exec` is preserved for
    /// callers that supply a custom turn executor
    /// with no ACP agent (e.g. the hermetic `FakeTransport` acceptance-test path).
    pub fn wire_pty(store: Arc<Store>, config: &Config) -> Self {
        Self::wire_pty_with_gateway_stream(store, config, None)
    }

    /// Production PTY-native wiring with an optional daemon-to-gateway stream publisher. Tests and
    /// non-gateway seams use [`AppState::wire_pty`]; the daemon binary passes a publisher so
    /// `WsSink` can push live frames after stream-store append ids are allocated.
    pub fn wire_pty_with_gateway_stream(
        store: Arc<Store>,
        config: &Config,
        gateway_stream: Option<crate::daemon::gateway_stream_socket::GatewayStreamPublisher>,
    ) -> Self {
        use nexus_agent::{AdapterRegistry, Agent};
        use nexus_identity::Identity;

        // (1) Build the shared event sink + identity ONCE — every service in the assembly, including
        // the ACP Agent, shares these exact instances. Do NOT build a second WsSink.
        let mut ws = WsSink::new(16_384, Some(store.clone()));
        if let Some(gateway_stream) = gateway_stream {
            ws = ws.with_gateway_stream(gateway_stream);
        }
        let gateway_stream_publisher = ws.gateway_stream_publisher();
        let events: Arc<dyn EventSink> = Arc::new(ws.clone());
        let identity_svc = Arc::new(Identity::new(store.clone(), events.clone(), config));
        let identity: Arc<dyn IdentityPort> = identity_svc.clone();

        // (2) Build the ACP Agent with the SAME events sink so headless agent replies stream to the
        // same WsSink the web console / transcript tailer reads.
        let mut registry = AdapterRegistry::with_builtins();
        nexus_harness_claude::register(&mut registry);
        nexus_harness_codex::register(&mut registry);
        crate::spawn_spec::install_spawn_specs(&mut registry);
        let agent_svc = Arc::new(Agent::new(registry, identity.clone(), events.clone()));

        // (3) Build the PTY supervisor.
        let supervisor = Arc::new(PtySupervisor::with_runtime_store_and_gateway_stream(
            store.clone(),
            gateway_stream_publisher,
        ));

        // (4) Wrap all three backends in a RoutingTurnExec (priority: codex → PTY → ACP):
        //   - codex app-server sessions (`turn/start` RPC) — Task 8
        //   - PTY/tmux sessions (bracketed-paste write) — Task 7
        //   - headless ACP sessions (default)
        // This is the daemon's single turn-executor.
        let turn_exec: Arc<dyn AgentTurnExecutionPort> =
            Arc::new(crate::daemon::routing_turn_exec::RoutingTurnExec::new(
                supervisor.codex_transport(),
                supervisor.transport(),
                agent_svc.clone(),
            ));

        // (5) Assemble the shared body: realtime, bus, search, notify, admin, loop wiring — all
        // keyed on `turn_exec` as the executor, with `agent_concrete = Some(agent_svc)` so the
        // launch orchestration can call `open_session_for` for headless ACP sessions.
        let mut state = Self::wire_shared(
            store,
            config,
            ws,
            events,
            identity,
            turn_exec,
            Some(agent_svc),
        );

        // (6) Mount the supervisor so launch_agent can spawn headed harnesses (Task 7).
        state.pty = Some(supervisor);
        state.spawn_boot_respawn();
        state
    }

    /// The shared assembly of [`AppState::wire_with_registry`] and [`AppState::wire_with_turn_exec`]:
    /// build realtime (the shared `Bell`/`AgentRegistry`), bus, search, notify, admin, and the
    /// `LoopWiring`, all keyed on the supplied `turn_exec` as the turn executor. `agent_concrete` is
    /// `Some` only on the ACP path (it backs launch's `open_session_for`). This is the single seam
    /// the PTY-native path swaps `turn_exec` at — every other wire is identical.
    fn wire_shared(
        store: Arc<Store>,
        config: &Config,
        ws: WsSink,
        events: Arc<dyn EventSink>,
        identity: Arc<dyn IdentityPort>,
        agent: Arc<dyn AgentTurnExecutionPort>,
        agent_concrete: Option<Arc<nexus_agent::Agent>>,
    ) -> Self {
        use nexus_bus::Bus;
        use nexus_dispatch::{DispatchService, ServiceDeps};
        use nexus_notify::{Notify, RoutingRules};
        use nexus_search::Search;

        // realtime — the Bell + AgentRegistry are created here and SHARED with the loop wiring
        // below, so a `dm`'s ring (via the bus → DispatchPort::enqueue) reaches the exact same
        // per-session bell that the spawned EventLoop parks on. Cloning a Bell/AgentRegistry shares
        // the inner Arc<Mutex<…>>, so both views see the same bells + states.
        let bell = Bell::new();
        let registry = AgentRegistry::new();
        let realtime_svc = Arc::new(DispatchService::new(ServiceDeps {
            store: store.clone(),
            bell: bell.clone(),
            registry: registry.clone(),
            project: String::new(),
            drain_limit: config.drain_limit,
            preview_chars: config.msg_preview_chars,
        }));
        let realtime: Arc<dyn DispatchPort> = realtime_svc.clone();

        // bus
        let message_hooks = Arc::new(
            crate::daemon::services::loop_wiring::GatewayMessageHookPort::new(
                ws.gateway_stream_publisher()
                    .map(|publisher| publisher.hook_bridge()),
                config.hook_gateway_mode,
                std::time::Duration::from_secs(31),
            ),
        );
        let bus_svc = Arc::new(Bus::new_with_message_hooks(
            store.clone(),
            realtime.clone(),
            identity.clone(),
            events.clone(),
            message_hooks,
        ));
        let bus: Arc<dyn BusPort> = bus_svc.clone();

        // search
        let search: Arc<dyn SearchPort> = Arc::new(Search::new(store.clone(), identity.clone()));

        // notify
        let notify: Arc<dyn NotifyPort> = Arc::new(Notify::new(
            store.clone(),
            bus.clone(),
            events.clone(),
            RoutingRules::new(vec![]),
        ));

        // admin
        let admin: Arc<dyn AdminPort> = Arc::new(nexus_admin::Admin::new(
            identity.clone(),
            agent.clone(),
            notify.clone(),
            events.clone(),
        ));

        // Loop wiring: the concrete handles that make a launched/registered agent wakeable. Shares
        // the same `bell`/`registry` as `realtime` above (so a ring reaches the loop's bell) and the
        // `turn_exec` as the turn executor (so a wake injects the drained batch as one turn — over
        // ACP `session/prompt` on the agent path, or written into the recipient's PTY on the
        // PtyTransport path).
        let transport_registry = TransportRegistry::new();
        let presence_writer =
            PresenceWriter::new(store.clone(), events.clone(), transport_registry.clone());
        let loop_wiring = LoopWiring {
            store: store.clone(),
            bell,
            registry,
            events: events.clone(),
            turn_exec: agent.clone(),
            gateway_stream: ws.gateway_stream_publisher(),
            drain_limit: config.drain_limit,
            preview_chars: config.msg_preview_chars,
            spawned: Arc::new(Mutex::new(HashMap::new())),
            shutting_down: Arc::new(AtomicBool::new(false)),
            native_forwarders: Arc::new(Mutex::new(HashMap::new())),
            raw_stream_writers: Arc::new(Mutex::new(HashSet::new())),
            presence: presence_writer.clone(),
        };

        // Keep launched agents present in `members` (they're driven over ACP and don't self-heartbeat).
        loop_wiring.spawn_heartbeat_keeper(config.heartbeat_ttl_ms);

        let mut state = AppState::new(
            store,
            ws,
            identity,
            agent,
            realtime,
            bus,
            search,
            notify,
            admin,
            String::new(),
        );
        state.runtime_identity_ready.store(false, Ordering::Release);
        state.hmac_secret = config.hmac_secret.clone();
        state.heartbeat_ttl_ms = config.heartbeat_ttl_ms;
        state.command_intent_retention_ms = config.command_intent_retention_ms;
        state.reap_size_threshold_mb = config.reap_size_threshold_mb;
        state.reap_interval_ms =
            crate::daemon::retention_policy::parse_reap_interval_ms(&config.reap_interval);
        state.launch_backend = config
            .launch_backend
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|backend| match backend {
                "pty" | "tmux" => backend.to_string(),
                other => panic!(
                    "invalid launch_backend {other:?} in config: expected \"pty\" or \"tmux\""
                ),
            });
        state.loop_wiring = Some(loop_wiring);
        state.presence = presence_writer;
        state.agent_concrete = agent_concrete;
        state.spawn_presence_reconciler();
        state
    }

    pub async fn read_message(
        &self,
        caller: &Caller,
        req: &ReadRequest,
    ) -> Result<Message, ContractError> {
        self.sources.read_message(caller, req).await
    }

    pub async fn push_as_source(
        &self,
        caller: &Caller,
        req: PushRequest,
    ) -> Result<PushResponse, ContractError> {
        self.push_as_source_with_idempotency(caller, req, None)
            .await
    }

    /// Execute a source push with command-envelope idempotency propagated to its canonical bus
    /// transaction. This is an internal command-worker seam: public `PushRequest` remains the
    /// producer payload, while crash-reclaim identity stays on the durable command envelope.
    pub(crate) async fn push_as_source_with_idempotency(
        &self,
        caller: &Caller,
        req: PushRequest,
        command_idempotency_key: Option<String>,
    ) -> Result<PushResponse, ContractError> {
        let outcome = self
            .sources
            .push_as_source(caller, req, command_idempotency_key)
            .await?;
        if let Some(message_id) = outcome.message_id.as_ref() {
            self.wake_source_deliveries(message_id, outcome.deliveries)
                .await;
        }
        Ok(outcome.response)
    }

    pub async fn register_source(
        &self,
        caller: &Caller,
        req: nexus_contracts::SourceRegisterRequest,
    ) -> Result<nexus_contracts::SourceRegisterResponse, ContractError> {
        self.sources.register_source(caller, req).await
    }

    pub async fn list_sources(&self) -> Result<nexus_contracts::SourceListResponse, ContractError> {
        self.sources.list_sources().await
    }

    pub async fn show_source(&self, name: &str) -> Result<nexus_contracts::Source, ContractError> {
        self.sources.show_source(name).await
    }

    pub async fn set_source_enabled(
        &self,
        name: &str,
        enabled: bool,
    ) -> Result<nexus_contracts::Source, ContractError> {
        self.sources.set_source_enabled(name, enabled).await
    }

    pub async fn rotate_source(
        &self,
        caller: &Caller,
        name: &str,
    ) -> Result<nexus_contracts::SourceTokenResponse, ContractError> {
        self.sources.rotate_source(caller, name).await
    }

    pub async fn delete_source(
        &self,
        name: &str,
    ) -> Result<nexus_contracts::SourceRef, ContractError> {
        self.sources.delete_source(name).await
    }

    pub async fn source_token(
        &self,
        caller: &Caller,
        name: &str,
    ) -> Result<nexus_contracts::SourceTokenResponse, ContractError> {
        self.sources.source_token(caller, name).await
    }

    // ── Durable agent identity/runtime lifecycle ───────────────────────────────────────────────

    fn random_suffix() -> String {
        IdentityAdminService::random_suffix()
    }

    pub(crate) fn new_client_key() -> String {
        format!("nexus_ck_{}", Self::random_suffix())
    }

    fn harness_to_store(harness: HarnessId) -> String {
        IdentityAdminService::harness_to_store(harness)
    }

    fn managed_agent_owner(caller: &Caller) -> AgentOwner {
        AgentOwner {
            name: caller.name.clone(),
            project: caller.project.clone(),
            session_id: Some(caller.session.0.clone()),
            agent_id: caller.agent_id.as_ref().map(|id| id.0.clone()),
        }
    }

    async fn caller_is_human_admin(&self, caller: &Caller) -> Result<bool, ContractError> {
        self.identity_admin.caller_is_human_admin(caller).await
    }

    pub async fn assign_agent_group(
        &self,
        caller: &Caller,
        req: AdminGroupAssignRequest,
    ) -> Result<AdminGroupAssignResponse, ContractError> {
        self.identity_admin.assign_agent_group(caller, req).await
    }

    pub async fn assign_agent_role(
        &self,
        caller: &Caller,
        req: AssignRoleRequest,
    ) -> Result<AssignRoleResponse, ContractError> {
        self.identity_admin.assign_agent_role(caller, req).await
    }

    pub async fn assign_agent_project(
        &self,
        caller: &Caller,
        req: AssignProjectRequest,
    ) -> Result<AssignProjectResponse, ContractError> {
        self.identity_admin.assign_agent_project(caller, req).await
    }

    pub async fn grant_agent_tier(
        &self,
        caller: &Caller,
        req: GrantTierRequest,
    ) -> Result<GrantTierResponse, ContractError> {
        self.identity_admin.grant_agent_tier(caller, req).await
    }

    pub async fn create_agent_identity(
        &self,
        caller: &Caller,
        req: AgentCreateRequest,
    ) -> Result<AgentCreateResponse, ContractError> {
        self.identity_admin.create_agent_identity(caller, req).await
    }

    pub async fn list_agent_identities(
        &self,
        caller: &Caller,
        req: AgentListRequest,
    ) -> Result<AgentListResponse, ContractError> {
        self.identity_admin.list_agent_identities(caller, req).await
    }

    pub async fn show_agent_identity(
        &self,
        caller: &Caller,
        req: AgentShowRequest,
    ) -> Result<AgentShowResponse, ContractError> {
        self.identity_admin.show_agent_identity(caller, req).await
    }

    pub async fn grant_agent_access(
        &self,
        caller: &Caller,
        req: AgentAccessGrantRequest,
    ) -> Result<AgentAccessGrantResponse, ContractError> {
        self.identity_admin.grant_agent_access(caller, req).await
    }

    pub async fn revoke_agent_access(
        &self,
        caller: &Caller,
        req: AgentAccessRevokeRequest,
    ) -> Result<AgentAccessRevokeResponse, ContractError> {
        self.identity_admin.revoke_agent_access(caller, req).await
    }

    pub async fn transfer_agent_owner(
        &self,
        caller: &Caller,
        req: AgentOwnerTransferRequest,
    ) -> Result<AgentOwnerTransferResponse, ContractError> {
        self.identity_admin.transfer_agent_owner(caller, req).await
    }

    pub async fn create_agent_credential(
        &self,
        caller: &Caller,
        req: AgentCredentialCreateRequest,
    ) -> Result<AgentCredentialCreateResponse, ContractError> {
        self.identity_admin
            .create_agent_credential(caller, req)
            .await
    }

    pub async fn revoke_agent_credential(
        &self,
        caller: &Caller,
        req: AgentCredentialRevokeRequest,
    ) -> Result<AgentCredentialRevokeResponse, ContractError> {
        self.identity_admin
            .revoke_agent_credential(caller, req)
            .await
    }

    pub async fn list_agent_runtimes(
        &self,
        caller: &Caller,
        req: AgentRuntimeListRequest,
    ) -> Result<AgentRuntimeListResponse, ContractError> {
        self.identity_admin.list_agent_runtimes(caller, req).await
    }

    pub async fn set_entity_metadata(
        &self,
        caller: &Caller,
        req: MetadataSetRequest,
    ) -> Result<MetadataResponse, ContractError> {
        self.identity_admin.set_entity_metadata(caller, req).await
    }
}
