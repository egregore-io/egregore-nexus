//! Headed Codex app-server bridge.
//!
//! [`CodexBridge`] owns the codex-specific data plane for a headed session: the app-server
//! subprocess, the JSON-RPC inject transport, rollout-based thread discovery, and the notification
//! forwarder. It deliberately does not spawn tmux or any PTY process; the daemon keeps that generic
//! launch primitive so `nexus-pty` can continue depending on this crate without a cycle.
//!
//! The fresh-headed flow is:
//! 1. Start `codex app-server --listen unix://<sock>` or adopt a live socket for the same session.
//! 2. Daemon launches may eagerly create and bind a thread so Nexus delivery works before the
//!    human types in the TUI. Lower-level callers may retain rollout discovery behavior.
//! 3. Launch the human TUI against that thread, or discover a TUI-created thread from the newest
//!    `rollout-*.jsonl` under `CODEX_HOME/sessions`.
//! 4. Bind injection and forward notifications to Nexus.
//!
//! Explicit resume first checks the machine Codex home. A rollout found only in a legacy
//! Nexus-owned session home is migrated into the machine home; an intentional external profile
//! remains authoritative. Credentials and configuration are never copied.

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime};

use nexus_common::RuntimeProcessIds;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_store::Store;
use serde_json::Value;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tokio_util::sync::CancellationToken;

use super::approvals::AutoApprove;
use super::client::CodexAppServerClient;
use super::forwarder::{spawn_codex_forwarder_with_tool_observations, CodexToolObservationSink};
use super::jsonrpc::CodexRpcError;
use super::supervisor::{resolve_machine_codex_home_from_env, CodexAppServer, SupervisorOpts};
use super::transport::CodexAppServerTransport;
use super::turn_completion::CodexTurnTracker;
use crate::storage::{CodexRuntimeLaunch, CodexRuntimeStateRepo};

/// Deferred persistence after successful thread discovery/binding. The bridge schedules the
/// returned future without blocking launch-time registration and orders its writes with rebind.
pub type ThreadDiscovered = Arc<
    dyn Fn(SessionId, String) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync
        + 'static,
>;

/// Optional behavior for [`CodexBridge::launch_with_options`].
#[derive(Clone, Default)]
pub struct BridgeLaunchOptions {
    /// Existing Codex thread id to resume immediately. When present, the bridge resolves the
    /// existing Codex home that already owns the matching rollout and starts the app-server with
    /// that `CODEX_HOME`, then binds that thread without waiting for a new rollout.
    pub known_thread_id: Option<String>,
    /// Additional Codex homes to search before ambient defaults when resolving
    /// [`known_thread_id`](Self::known_thread_id). Tests and future harness adapters use this as an
    /// escape hatch; production usually relies on `CODEX_HOME` or `~/.codex`.
    pub resume_codex_homes: Vec<PathBuf>,
    /// Callback used by the daemon to persist the discovered Codex thread id.
    pub on_thread_discovered: Option<ThreadDiscovered>,
    /// Shared Nexus store used to persist Codex-owned runtime sidecar state.
    pub runtime_store: Option<Arc<Store>>,
    /// Force a fresh app-server process instead of adopting an existing socket at the deterministic
    /// session path.
    ///
    /// This is a destructive recovery escape hatch for a socket known to be unhealthy. Normal
    /// daemon restart revive keeps the default `false`: it adopts a live socket, opens a fresh
    /// client connection, and installs a new completion forwarder without duplicating the agent.
    pub force_fresh_app_server: bool,
    /// Create and bind a fresh app-server thread before returning when no resume thread was given.
    /// This removes the fresh-headed bootstrap gap where Nexus could not deliver the first message
    /// until a human had already submitted a TUI turn.
    pub create_thread_if_missing: bool,
    /// Optional metadata-only tool-call observation sink for daemon-origin developer events.
    ///
    /// The bridge passes this to the app-server forwarder. It does not alter visible
    /// `agent.update` rows or turn delivery.
    pub tool_observations: Option<Arc<dyn CodexToolObservationSink>>,
}

/// Live codex app-server resources for one headed session.
struct Handle {
    identity: Arc<()>,
    lifecycle: Arc<Lifecycle>,
    server: CodexAppServer,
    forwarder: Option<tokio::task::JoinHandle<()>>,
    thread_id: Option<String>,
    #[allow(dead_code)]
    socket: PathBuf,
}

/// Serializes only setup, persisted binding writes, publication and physical retirement.
/// Native controls continue to use the short owner admission guard, not this async mutex.
#[derive(Default)]
struct Lifecycle {
    exclusion: Arc<AsyncMutex<()>>,
    retirement: Mutex<Option<CancellationToken>>,
}

impl Lifecycle {
    async fn enter(&self) -> OwnedMutexGuard<()> {
        let guard = self.exclusion.clone().lock_owned().await;
        let retirement = self.retirement.lock().unwrap().clone();
        if let Some(retirement) = retirement {
            retirement.cancelled().await;
        }
        guard
    }

    fn retire(self: &Arc<Self>, server: CodexAppServer) {
        let done = CancellationToken::new();
        *self.retirement.lock().unwrap() = Some(done.clone());
        let lifecycle = self.clone();
        tokio::spawn(async move {
            let _lifecycle = lifecycle;
            let _done = RetirementDone(done);
            server.shutdown().await;
        });
    }
}

struct RetirementDone(CancellationToken);
impl Drop for RetirementDone {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[derive(Clone)]
struct BindingAttempt(Arc<AttemptInner>);

struct AttemptInner {
    transport: CodexAppServerTransport,
    session: SessionId,
    owner: CodexTurnTracker,
    handle: Arc<()>,
    lifecycle: Arc<Lifecycle>,
}

impl Drop for AttemptInner {
    fn drop(&mut self) {
        self.transport.cancel_binding(&self.session, &self.owner);
    }
}

impl BindingAttempt {
    fn new(
        transport: CodexAppServerTransport,
        session: SessionId,
        handle: Arc<()>,
        thread: Option<String>,
        lifecycle: Arc<Lifecycle>,
    ) -> Self {
        let owner = transport.begin_binding(&session, thread);
        Self(Arc::new(AttemptInner {
            transport,
            session,
            owner,
            handle,
            lifecycle,
        }))
    }

    fn is_current(&self) -> bool {
        self.0
            .transport
            .binding_is_current(&self.0.session, &self.0.owner)
    }

    async fn enter(&self) -> Result<OwnedMutexGuard<()>, CodexRpcError> {
        let guard = tokio::select! {
            guard = self.0.lifecycle.enter() => guard,
            _ = self.0.owner.revoked() => return Err(stale_binding()),
        };
        if !self.is_current() {
            return Err(stale_binding());
        }
        Ok(guard)
    }

    async fn setup<T>(
        &self,
        future: impl std::future::Future<Output = Result<T, CodexRpcError>>,
    ) -> Result<T, CodexRpcError> {
        if self.0.owner.is_revoked() {
            return Err(stale_binding());
        }
        tokio::select! {
            result = future => result,
            _ = self.0.owner.revoked() => Err(stale_binding()),
        }
    }
}

fn stale_binding() -> CodexRpcError {
    CodexRpcError::Connect("captured Codex binding attempt was revoked or replaced".into())
}

fn defer_thread_persistence(attempt: BindingAttempt, callback: ThreadDiscovered, thread: String) {
    tokio::spawn(async move {
        let _guard = attempt.0.lifecycle.enter().await;
        if !attempt
            .0
            .transport
            .owns_binding(&attempt.0.session, &attempt.0.owner)
        {
            return;
        }
        // This task replaces the producer's detached spawn. Registration may follow launch;
        // retain the existing bounded registration retry, and never cancel a started write.
        callback(attempt.0.session.clone(), thread).await;
    });
}

/// Owns app-server lifecycle, rollout discovery, notification forwarding, and inject binding for
/// headed Codex sessions.
#[derive(Clone)]
pub struct CodexBridge {
    handles: Arc<Mutex<HashMap<SessionId, Handle>>>,
    transport: CodexAppServerTransport,
    lifecycles: Arc<Mutex<HashMap<SessionId, Weak<Lifecycle>>>>,
}

impl Default for CodexBridge {
    fn default() -> Self {
        Self::new()
    }
}

impl CodexBridge {
    /// Create an empty bridge with a shared [`CodexAppServerTransport`].
    pub fn new() -> CodexBridge {
        CodexBridge {
            handles: Arc::new(Mutex::new(HashMap::new())),
            transport: CodexAppServerTransport::new(),
            lifecycles: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Clone the inject transport so routing code can deliver turns to discovered codex threads.
    pub fn transport(&self) -> CodexAppServerTransport {
        self.transport.clone()
    }

    fn lifecycle(&self, session: &SessionId) -> Arc<Lifecycle> {
        let mut lifecycles = self.lifecycles.lock().unwrap();
        lifecycles.retain(|_, lifecycle| lifecycle.strong_count() != 0);
        if let Some(lifecycle) = lifecycles.get(session).and_then(Weak::upgrade) {
            return lifecycle;
        }
        let lifecycle = Arc::new(Lifecycle::default());
        lifecycles.insert(session.clone(), Arc::downgrade(&lifecycle));
        lifecycle
    }

    /// Whether `session` has a live app-server handle tracked by this bridge.
    pub fn has(&self, session: &SessionId) -> bool {
        self.handles.lock().unwrap().contains_key(session)
    }

    /// Return the current app-server process ledger for a live handle this bridge spawned.
    pub fn process_ledger(&self, session: &SessionId) -> Option<RuntimeProcessIds> {
        self.handles
            .lock()
            .unwrap()
            .get(session)
            .and_then(|handle| handle.server.process_ledger())
    }

    /// Remove one session, abort its forwarder if present, unbind injection, and asynchronously
    /// shut down its app-server subprocess.
    pub fn kill(&self, session: &SessionId) -> bool {
        let mut handles = self.handles.lock().unwrap();
        self.transport.unbind(session);
        self.transport.unmark_live(session);
        let Some(mut handle) = handles.remove(session) else {
            return false;
        };
        if let Some(forwarder) = handle.forwarder.take() {
            forwarder.abort();
        }
        handle.lifecycle.retire(handle.server);
        true
    }

    /// Remove every tracked session, abort all forwarders, unbind injection, and asynchronously
    /// shut down every app-server subprocess. Returns the number of handles removed.
    pub fn kill_all(&self) -> usize {
        let mut handles = self.handles.lock().unwrap();
        let count = handles.len();
        self.transport.unbind_all();
        for (session, mut handle) in handles.drain() {
            self.transport.unmark_live(&session);
            if let Some(forwarder) = handle.forwarder.take() {
                forwarder.abort();
            }
            handle.lifecycle.retire(handle.server);
        }
        count
    }

    /// Start the app-server data plane for `session` and spawn a background discovery task.
    ///
    /// Returns the app-server socket path immediately. The caller should launch the human TUI with
    /// `codex --remote unix://<sock>`; once that TUI writes its rollout, the discovery task resumes
    /// the thread and binds [`CodexAppServerTransport`].
    pub async fn launch(
        &self,
        session: SessionId,
        opts: SupervisorOpts,
        events: Arc<dyn EventSink>,
    ) -> Result<PathBuf, CodexRpcError> {
        self.launch_with_options(session, opts, events, BridgeLaunchOptions::default())
            .await
    }

    /// Start the app-server data plane for `session` with optional revive/persistence behavior.
    ///
    /// If `known_thread_id` is present, the bridge resumes and binds that thread immediately on its
    /// own JSON-RPC connection. Otherwise it waits for the human TUI to create a rollout and binds
    /// the discovered thread, matching the fresh-headed launch flow.
    pub async fn launch_with_options(
        &self,
        session: SessionId,
        mut opts: SupervisorOpts,
        events: Arc<dyn EventSink>,
        options: BridgeLaunchOptions,
    ) -> Result<PathBuf, CodexRpcError> {
        let BridgeLaunchOptions {
            known_thread_id,
            resume_codex_homes,
            on_thread_discovered,
            runtime_store,
            force_fresh_app_server,
            create_thread_if_missing,
            tool_observations,
        } = options;
        let await_binding = known_thread_id.is_some();

        let canonical_home = match opts.codex_home.clone() {
            Some(home) => home,
            None => resolve_machine_codex_home_from_env(&opts.session_dir)
                .map_err(CodexRpcError::Connect)?,
        };

        if let Some(thread_id) = known_thread_id.as_deref() {
            let Some(home) = resolve_resume_codex_home(
                &opts.session_dir,
                &canonical_home,
                &resume_codex_homes,
                thread_id,
            )
            .map_err(|error| {
                CodexRpcError::Connect(format!(
                    "migrate codex rollout for thread {thread_id}: {error}"
                ))
            })?
            else {
                return Err(CodexRpcError::Connect(format!(
                    "cannot resume codex thread {thread_id}: no rollout found under explicit resume homes, CODEX_HOME, ~/.codex, or Nexus codex session homes"
                )));
            };
            tracing::info!(
                target: "nexus::codex_bridge",
                session = %session,
                thread_id = %thread_id,
                codex_home = %home.display(),
                "codex bridge: resolved existing codex home for thread resume"
            );
            opts.codex_home = Some(home);
        } else if opts.codex_home.is_none() {
            opts.codex_home = Some(canonical_home);
        }
        let rollout_root = opts
            .codex_home
            .clone()
            .unwrap_or_else(|| opts.session_dir.join("codex-home"))
            .join("sessions");
        let resume_cwd = opts.cwd.clone();

        let existing_handle = {
            let handles = self.handles.lock().unwrap();
            handles.get(&session).map(|h| {
                (
                    h.socket.clone(),
                    h.thread_id.clone(),
                    h.server.process_ledger().map(|ids| ids.os_pid),
                    h.identity.clone(),
                )
            })
        };
        if let Some((sock_path, thread_id, known_pid, identity)) = existing_handle {
            {
                let handles = self.handles.lock().unwrap();
                if !handles
                    .get(&session)
                    .is_some_and(|handle| Arc::ptr_eq(&handle.identity, &identity))
                {
                    return Err(stale_binding());
                }
                match known_pid {
                    Some(pid) => self.transport.mark_live_with_pid(session.clone(), pid),
                    None => self.transport.mark_live(session.clone()),
                }
            }
            let desired_thread_id = known_thread_id.clone().or(thread_id);
            let bound_thread_id = self.transport.bound_thread_id(&session);
            let needs_bind = match desired_thread_id.as_deref() {
                Some(thread_id) => bound_thread_id.as_deref() != Some(thread_id),
                None => bound_thread_id.is_none(),
            };
            if needs_bind {
                let attempt = {
                    let handles = self.handles.lock().unwrap();
                    if !handles
                        .get(&session)
                        .is_some_and(|handle| Arc::ptr_eq(&handle.identity, &identity))
                    {
                        return Err(stale_binding());
                    }
                    BindingAttempt::new(
                        self.transport(),
                        session.clone(),
                        identity.clone(),
                        desired_thread_id.clone(),
                        self.lifecycle(&session),
                    )
                };
                let lifecycle_guard = attempt.enter().await?;
                if create_thread_if_missing && desired_thread_id.is_none() {
                    create_and_bind_thread(
                        self.handles.clone(),
                        self.transport(),
                        attempt.clone(),
                        session.clone(),
                        sock_path.clone(),
                        resume_cwd,
                        on_thread_discovered,
                        runtime_store,
                        events,
                        tool_observations,
                    )
                    .await?;
                } else {
                    if await_binding {
                        bind_thread(
                            self.handles.clone(),
                            self.transport(),
                            attempt.clone(),
                            session.clone(),
                            sock_path.clone(),
                            rollout_root,
                            desired_thread_id,
                            resume_cwd,
                            on_thread_discovered,
                            runtime_store,
                            events,
                            tool_observations,
                        )
                        .await?;
                    } else {
                        spawn_binding_task(
                            lifecycle_guard,
                            self.handles.clone(),
                            self.transport(),
                            attempt.clone(),
                            session.clone(),
                            sock_path.clone(),
                            rollout_root,
                            desired_thread_id,
                            resume_cwd,
                            on_thread_discovered,
                            runtime_store,
                            events,
                            tool_observations,
                        );
                    }
                }
            }
            if !self
                .handles
                .lock()
                .unwrap()
                .get(&session)
                .is_some_and(|handle| Arc::ptr_eq(&handle.identity, &identity))
            {
                return Err(stale_binding());
            }
            return Ok(sock_path);
        }

        tracing::info!(
            target: "nexus::codex_bridge",
            session = %session,
            known_thread_id = known_thread_id.as_deref().unwrap_or(""),
            "codex bridge: starting app-server"
        );
        let attempt = {
            let handles = self.handles.lock().unwrap();
            if handles.contains_key(&session) {
                return Err(stale_binding());
            }
            BindingAttempt::new(
                self.transport(),
                session.clone(),
                Arc::new(()),
                known_thread_id.clone(),
                self.lifecycle(&session),
            )
        };
        let lifecycle_guard = attempt.enter().await?;
        if force_fresh_app_server {
            let stale_sock = opts.session_dir.join("codex.sock");
            if let Err(error) = std::fs::remove_file(&stale_sock) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(
                        target: "nexus::codex_bridge",
                        session = %session,
                        socket = %stale_sock.display(),
                        error = %error,
                        "codex bridge: failed to remove stale app-server socket before forced revive"
                    );
                }
            }
        }
        let server = tokio::select! {
            server = CodexAppServer::start(opts) => server?,
            _ = attempt.0.owner.revoked() => return Err(stale_binding()),
        };
        if !attempt.is_current() {
            server.shutdown().await;
            return Err(stale_binding());
        }
        let sock_path = server.socket().to_path_buf();
        tracing::info!(
            target: "nexus::codex_bridge",
            session = %session,
            socket = %sock_path.display(),
            "codex bridge: app-server ready"
        );
        if let Some(store) = &runtime_store {
            let launch = CodexRuntimeLaunch {
                runtime_id: session.clone(),
                codex_thread_id: known_thread_id.clone(),
                codex_home: server.codex_home.clone(),
                app_server_sock: sock_path.clone(),
                app_server_pid: server.child.as_ref().and_then(|child| child.id()),
                mcp_sidecar_pids_json: None,
                app_server_adopted: server.child.is_none(),
            };
            if let Err(e) = CodexRuntimeStateRepo::new(store)
                .upsert_launch(launch)
                .await
            {
                server.shutdown().await;
                return Err(CodexRpcError::Connect(format!(
                    "persist codex runtime launch state for {session}: {e}"
                )));
            }
        }

        let app_server_pid = server.process_ledger().map(|ids| ids.os_pid);
        let stale_server = {
            let mut handles = self.handles.lock().unwrap();
            if !attempt.is_current() {
                Some(server)
            } else {
                handles.insert(
                    session.clone(),
                    Handle {
                        identity: attempt.0.handle.clone(),
                        lifecycle: attempt.0.lifecycle.clone(),
                        server,
                        forwarder: None,
                        thread_id: None,
                        socket: sock_path.clone(),
                    },
                );
                // Mark the session alive NOW (the app-server process is up) so the heartbeat keeper reads
                // it "online" and it shows in the roster / web console immediately — the turn binding only
                // lands after first-turn discovery, which would otherwise leave it "offline" until then.
                // Pid-backed when this daemon spawned the process, so liveness stays TRUTHFUL (D9/N22:
                // the keeper must not kill an idle pre-thread codex; a stale stamp must not mask a dead
                // one). Adopted sockets stay stamp-only and rely on the fast turn binding.
                match app_server_pid {
                    Some(pid) => self.transport.mark_live_with_pid(session.clone(), pid),
                    None => self.transport.mark_live(session.clone()),
                }
                None
            }
        };
        if let Some(server) = stale_server {
            server.shutdown().await;
            return Err(stale_binding());
        }

        if create_thread_if_missing && known_thread_id.is_none() {
            if let Err(error) = create_and_bind_thread(
                self.handles.clone(),
                self.transport(),
                attempt.clone(),
                session.clone(),
                sock_path.clone(),
                resume_cwd,
                on_thread_discovered,
                runtime_store,
                events,
                tool_observations,
            )
            .await
            {
                self.kill_attempt(&session, &attempt);
                return Err(error);
            }
        } else if await_binding {
            if let Err(error) = bind_thread(
                self.handles.clone(),
                self.transport(),
                attempt.clone(),
                session.clone(),
                sock_path.clone(),
                rollout_root,
                known_thread_id,
                resume_cwd,
                on_thread_discovered,
                runtime_store,
                events,
                tool_observations,
            )
            .await
            {
                self.kill_attempt(&session, &attempt);
                return Err(error);
            }
        } else {
            spawn_binding_task(
                lifecycle_guard,
                self.handles.clone(),
                self.transport(),
                attempt.clone(),
                session.clone(),
                sock_path.clone(),
                rollout_root,
                known_thread_id,
                resume_cwd,
                on_thread_discovered,
                runtime_store,
                events,
                tool_observations,
            );
        }

        Ok(sock_path)
    }

    fn kill_attempt(&self, session: &SessionId, attempt: &BindingAttempt) {
        let mut handles = self.handles.lock().unwrap();
        if !handles
            .get(session)
            .is_some_and(|handle| Arc::ptr_eq(&handle.identity, &attempt.0.handle))
            || !self.transport.owns_binding(session, &attempt.0.owner)
        {
            return;
        }
        self.transport.unbind(session);
        self.transport.unmark_live(session);
        if let Some(mut handle) = handles.remove(session) {
            if let Some(forwarder) = handle.forwarder.take() {
                forwarder.abort();
            }
            handle.lifecycle.retire(handle.server);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn create_and_bind_thread(
    handles: Arc<Mutex<HashMap<SessionId, Handle>>>,
    transport: CodexAppServerTransport,
    attempt: BindingAttempt,
    session: SessionId,
    socket: PathBuf,
    cwd: Option<PathBuf>,
    on_thread_discovered: Option<ThreadDiscovered>,
    runtime_store: Option<Arc<Store>>,
    events: Arc<dyn EventSink>,
    tool_observations: Option<Arc<dyn CodexToolObservationSink>>,
) -> Result<String, CodexRpcError> {
    if !attempt.is_current() {
        return Err(stale_binding());
    }
    let client = Arc::new(
        attempt
            .setup(CodexAppServerClient::connect_with_tracker(
                &socket,
                "nexus-bridge",
                attempt.0.owner.clone(),
            ))
            .await?,
    );
    let before_start = attempt.0.owner.native_revision();
    let thread_id = attempt
        .setup(client.thread_start_in(cwd.as_deref().and_then(Path::to_str)))
        .await?;
    attempt.0.owner.seed_idle(&thread_id, before_start);

    if !attempt.is_current() {
        return Err(stale_binding());
    }

    if let Some(store) = &runtime_store {
        CodexRuntimeStateRepo::new(store)
            .set_thread(&session, &thread_id, None)
            .await
            .map_err(|error| {
                CodexRpcError::Connect(format!(
                    "persist eagerly created codex thread for {session}: {error}"
                ))
            })?;
    }
    let forwarder = spawn_codex_forwarder_with_tool_observations(
        session.clone(),
        client.clone(),
        events,
        Arc::new(AutoApprove),
        attempt.0.owner.clone(),
        tool_observations,
    );
    let mut map = handles.lock().unwrap();
    let Some(handle) = map.get_mut(&session) else {
        forwarder.abort();
        return Err(CodexRpcError::Connect(format!(
            "codex bridge session {session} disappeared while creating its thread"
        )));
    };
    if !Arc::ptr_eq(&handle.identity, &attempt.0.handle)
        || !transport.publish_binding(&session, &attempt.0.owner, client, thread_id.clone())
    {
        forwarder.abort();
        return Err(stale_binding());
    }
    if let Some(previous) = handle.forwarder.replace(forwarder) {
        previous.abort();
    }
    handle.thread_id = Some(thread_id.clone());
    drop(map);
    if let Some(callback) = on_thread_discovered {
        defer_thread_persistence(attempt, callback, thread_id.clone());
    }
    Ok(thread_id)
}

fn spawn_binding_task(
    lifecycle_guard: OwnedMutexGuard<()>,
    handles: Arc<Mutex<HashMap<SessionId, Handle>>>,
    transport: CodexAppServerTransport,
    attempt: BindingAttempt,
    discovery_session: SessionId,
    discovery_sock: PathBuf,
    rollout_root: PathBuf,
    known_thread_id: Option<String>,
    resume_cwd: Option<PathBuf>,
    on_thread_discovered: Option<ThreadDiscovered>,
    runtime_store: Option<Arc<Store>>,
    events: Arc<dyn EventSink>,
    tool_observations: Option<Arc<dyn CodexToolObservationSink>>,
) {
    tokio::spawn(async move {
        let _lifecycle_guard = lifecycle_guard;
        if let Err(error) = bind_thread(
            handles,
            transport,
            attempt,
            discovery_session.clone(),
            discovery_sock,
            rollout_root,
            known_thread_id,
            resume_cwd,
            on_thread_discovered,
            runtime_store,
            events,
            tool_observations,
        )
        .await
        {
            tracing::warn!(
                session = %discovery_session,
                error = %error,
                "codex bridge: asynchronous thread binding failed"
            );
        }
    });
}

#[allow(clippy::too_many_arguments)]
async fn bind_thread(
    handles: Arc<Mutex<HashMap<SessionId, Handle>>>,
    transport: CodexAppServerTransport,
    attempt: BindingAttempt,
    discovery_session: SessionId,
    discovery_sock: PathBuf,
    rollout_root: PathBuf,
    known_thread_id: Option<String>,
    resume_cwd: Option<PathBuf>,
    on_thread_discovered: Option<ThreadDiscovered>,
    runtime_store: Option<Arc<Store>>,
    events: Arc<dyn EventSink>,
    tool_observations: Option<Arc<dyn CodexToolObservationSink>>,
) -> Result<String, CodexRpcError> {
    let (thread_id, rollout_path) = match known_thread_id {
        Some(id) => {
            let rollout_path = rollout_with_thread_id(&rollout_root, &id);
            (id, rollout_path)
        }
        None => {
            let Some(discovered) = (tokio::select! {
                discovered = wait_for_thread_id(&rollout_root, Duration::from_secs(900)) => discovered,
                _ = attempt.0.owner.revoked() => return Err(stale_binding()),
            }) else {
                tracing::warn!(
                    session = %discovery_session.0,
                    rollout_root = %rollout_root.display(),
                    "no thread discovered"
                );
                return Err(CodexRpcError::Connect(format!(
                    "no codex thread discovered for session {discovery_session} under {}",
                    rollout_root.display()
                )));
            };
            discovered
        }
    };

    if !attempt.is_current() || !attempt.0.owner.select_thread(&thread_id) {
        return Err(stale_binding());
    }
    let client = Arc::new(
        attempt
            .setup(CodexAppServerClient::connect_with_tracker(
                &discovery_sock,
                "nexus-bridge",
                attempt.0.owner.clone(),
            ))
            .await?,
    );

    let resume_cwd = resume_cwd
        .as_deref()
        .and_then(|path| path.to_str())
        .map(str::to_owned);
    tracing::info!(
        target: "nexus::codex_bridge",
        session = %discovery_session,
        thread_id = %thread_id,
        cwd = resume_cwd.as_deref().unwrap_or(""),
        "codex bridge: resuming thread for transport bind"
    );
    let before_resume = attempt.0.owner.native_revision();
    let resume = attempt
        .setup(client.thread_resume_in(&thread_id, resume_cwd.as_deref()))
        .await?;
    if !attempt.is_current() {
        return Err(stale_binding());
    }
    let resumed_active_turn = resumed_active_turn_id(&resume, &thread_id)?;
    // The legacy empty-object response is deliberately not idle authority.
    if resumed_active_turn.is_none()
        && resume
            .get("thread")
            .and_then(|thread| thread.get("turns"))
            .and_then(Value::as_array)
            .is_some_and(|turns| {
                turns.iter().all(|turn| {
                    turn.get("id")
                        .and_then(Value::as_str)
                        .is_some_and(|id| !id.is_empty())
                        && matches!(
                            turn.get("status").and_then(Value::as_str),
                            Some("completed" | "interrupted" | "failed")
                        )
                })
            })
    {
        attempt.0.owner.seed_idle(&thread_id, before_resume);
    }
    attempt
        .0
        .owner
        .seed_resume(&thread_id, resumed_active_turn.as_deref(), before_resume);
    if let Some(store) = &runtime_store {
        if let Err(e) = CodexRuntimeStateRepo::new(store)
            .set_thread(&discovery_session, &thread_id, rollout_path.clone())
            .await
        {
            tracing::warn!(
                session = %discovery_session.0,
                thread_id = %thread_id,
                error = %e,
                "codex bridge: failed to persist runtime thread state"
            );
        }
    }
    let forwarder = spawn_codex_forwarder_with_tool_observations(
        discovery_session.clone(),
        client.clone(),
        events,
        Arc::new(AutoApprove),
        attempt.0.owner.clone(),
        tool_observations,
    );

    let mut map = handles.lock().unwrap();
    if let Some(handle) = map.get_mut(&discovery_session) {
        if !Arc::ptr_eq(&handle.identity, &attempt.0.handle)
            || !transport.publish_binding(
                &discovery_session,
                &attempt.0.owner,
                client,
                thread_id.clone(),
            )
        {
            forwarder.abort();
            return Err(stale_binding());
        }
        if let Some(old_forwarder) = handle.forwarder.take() {
            old_forwarder.abort();
        }
        handle.forwarder = Some(forwarder);
        handle.thread_id = Some(thread_id.clone());
        // Binding is the final readiness publication. Once routing can observe this session,
        // the resume snapshot has seeded any in-progress turn and the sole forwarder is owned
        // by the live handle, so a recovered prompt cannot race the old native boundary.
        tracing::info!(
            target: "nexus::codex_bridge",
            session = %discovery_session,
            thread_id = %thread_id,
            resumed_active_turn = resumed_active_turn.as_deref().unwrap_or(""),
            "codex bridge: transport bound"
        );
    } else {
        forwarder.abort();
        return Err(CodexRpcError::Connect(format!(
            "codex bridge session {discovery_session} disappeared while binding thread {thread_id}"
        )));
    }
    drop(map);
    if let Some(callback) = on_thread_discovered {
        defer_thread_persistence(attempt, callback, thread_id.clone());
    }
    Ok(thread_id)
}

fn resumed_active_turn_id(
    resume: &Value,
    expected_thread_id: &str,
) -> Result<Option<String>, CodexRpcError> {
    let Some(thread) = resume.get("thread") else {
        // Older/fake app-servers returned an empty object. They provide no resume authority, but
        // they also cannot claim an active turn.
        return Ok(None);
    };
    let actual_thread_id = thread
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| CodexRpcError::Decode("thread/resume response missing thread.id".into()))?;
    if actual_thread_id != expected_thread_id {
        return Err(CodexRpcError::Decode(format!(
            "thread/resume returned thread {actual_thread_id}, expected {expected_thread_id}"
        )));
    }
    let turns = thread
        .get("turns")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            CodexRpcError::Decode("thread/resume response missing thread.turns".into())
        })?;
    let mut active = Vec::new();
    for turn in turns {
        if turn.get("status").and_then(Value::as_str) != Some("inProgress") {
            continue;
        }
        active.push(turn.get("id").and_then(Value::as_str).ok_or_else(|| {
            CodexRpcError::Decode("thread/resume in-progress turn missing id".into())
        })?);
    }
    match active.as_slice() {
        [] => Ok(None),
        [turn_id] => Ok(Some((*turn_id).to_string())),
        _ => Err(CodexRpcError::Decode(format!(
            "thread/resume returned {} in-progress turns for {expected_thread_id}",
            active.len()
        ))),
    }
}

#[doc(hidden)]
pub fn resolve_resume_codex_home(
    session_dir: &Path,
    canonical_home: &Path,
    explicit_homes: &[PathBuf],
    thread_id: &str,
) -> std::io::Result<Option<PathBuf>> {
    let mut seen = HashSet::new();
    let mut matching_rollout = |home: &Path| {
        if !seen.insert(home.to_path_buf()) {
            return None;
        }
        rollout_with_thread_id(&home.join("sessions"), thread_id)
    };

    if matching_rollout(canonical_home).is_some() {
        return Ok(Some(canonical_home.to_path_buf()));
    }

    for home in explicit_homes {
        if let Some(source) = matching_rollout(home) {
            if nexus_managed_codex_home(home) {
                migrate_rollout(home, canonical_home, &source)?;
                return Ok(Some(canonical_home.to_path_buf()));
            }
            return Ok(Some(home.clone()));
        }
    }
    if let Some(codex_sessions_root) = session_dir.parent() {
        if let Ok(entries) = std::fs::read_dir(codex_sessions_root) {
            let mut homes = entries
                .flatten()
                .map(|entry| entry.path().join("codex-home"))
                .collect::<Vec<_>>();
            homes.sort();
            for home in homes {
                if let Some(source) = matching_rollout(&home) {
                    migrate_rollout(&home, canonical_home, &source)?;
                    return Ok(Some(canonical_home.to_path_buf()));
                }
            }
        }
    }
    Ok(None)
}

fn nexus_managed_codex_home(home: &Path) -> bool {
    home.file_name().is_some_and(|name| name == "codex-home")
        && home
            .parent()
            .and_then(Path::parent)
            .and_then(Path::file_name)
            .is_some_and(|name| name == "codex-sessions")
}

fn migrate_rollout(source_home: &Path, target_home: &Path, source: &Path) -> std::io::Result<()> {
    if !nexus_managed_codex_home(source_home) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "refuse rollout migration from non-Nexus home {}",
                source_home.display()
            ),
        ));
    }
    let source_root = source_home.join("sessions");
    let relative = source.strip_prefix(&source_root).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "rollout {} escapes {}",
                source.display(),
                source_root.display()
            ),
        )
    })?;
    if relative
        .components()
        .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("rollout path is not contained: {}", relative.display()),
        ));
    }

    let metadata = std::fs::symlink_metadata(source)?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("rollout source is not a regular file: {}", source.display()),
        ));
    }
    let bytes = std::fs::read(source)?;
    let target_root = target_home.join("sessions");
    let destination = target_root.join(relative);
    let parent = destination.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "rollout destination has no parent",
        )
    })?;
    create_contained_directories(
        &target_root,
        relative.parent().unwrap_or_else(|| Path::new("")),
    )?;

    match std::fs::symlink_metadata(&destination) {
        Ok(destination_metadata) => {
            if !destination_metadata.file_type().is_file()
                || destination_metadata.file_type().is_symlink()
                || std::fs::read(&destination)? != bytes
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("conflicting rollout destination {}", destination.display()),
                ));
            }
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let nonce = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temporary = parent.join(format!(".nexus-rollout-{}-{nonce}.tmp", std::process::id()));
    let publish = (|| -> std::io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, &destination)?;
        #[cfg(unix)]
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if publish.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    publish
}

fn create_contained_directories(root: &Path, relative: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(root)?;
    let root_metadata = std::fs::symlink_metadata(root)?;
    if !root_metadata.file_type().is_dir() || root_metadata.file_type().is_symlink() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "rollout destination root is not a real directory: {}",
                root.display()
            ),
        ));
    }

    let mut current = root.to_path_buf();
    for component in relative.components() {
        let std::path::Component::Normal(name) = component else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "rollout destination contains a non-normal component",
            ));
        };
        current.push(name);
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {
            }
            Ok(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "rollout destination ancestry is not a directory: {}",
                        current.display()
                    ),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&current)?;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Read the newest Codex rollout under `CODEX_HOME/sessions` and return its thread id.
///
/// This is useful during revive for rows created before the daemon persisted Codex thread ids.
pub fn latest_rollout_thread_id(rollout_root: &Path) -> Option<String> {
    newest_rollout(rollout_root).and_then(|path| read_thread_id_from_rollout(&path))
}

async fn wait_for_thread_id(
    rollout_root: &Path,
    timeout: Duration,
) -> Option<(String, Option<PathBuf>)> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(path) = newest_rollout(rollout_root) {
            if let Some(thread_id) = read_thread_id_from_rollout(&path) {
                return Some((thread_id, Some(path)));
            }
        }

        if tokio::time::Instant::now() >= deadline {
            return None;
        }

        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}

fn rollout_meta(path: &Path) -> Option<serde_json::Value> {
    let file = std::fs::File::open(path).ok()?;
    let mut lines = std::io::BufReader::new(file).lines();
    let first = lines.next()?.ok()?;
    serde_json::from_str(&first).ok()
}

fn read_thread_id_from_rollout(path: &Path) -> Option<String> {
    rollout_meta(path)?
        .get("payload")?
        .get("id")?
        .as_str()
        .map(str::to_owned)
}

/// A rollout belongs to a SUB-AGENT thread when its meta line's `source` is the
/// `{"subagent": {"thread_spawn": …}}` object (codex multi-agent v2). Those threads
/// refuse direct app-server input (`-32600: direct app-server input is not allowed for
/// multi-agent v2 sub-agents`), so binding one as the session's main thread bricks every
/// bus delivery to the agent (07-11: a heavy multi-agent session had 21 sub-agent
/// rollouts and ONE main rollout — "newest" was almost never the main thread).
pub fn rollout_is_subagent(path: &Path) -> bool {
    rollout_meta(path)
        .and_then(|json| json.get("payload").cloned())
        .and_then(|p| p.get("source").cloned())
        .is_some_and(|s| s.get("subagent").is_some())
}

/// Newest MAIN-thread rollout under `root` — sub-agent rollouts are never candidates.
pub fn newest_rollout(root: &Path) -> Option<PathBuf> {
    newest_rollout_matching(root, |path| !rollout_is_subagent(path))
}

fn rollout_with_thread_id(root: &Path, thread_id: &str) -> Option<PathBuf> {
    newest_rollout_matching(root, |path| {
        read_thread_id_from_rollout(path).as_deref() == Some(thread_id)
    })
}

fn newest_rollout_matching(root: &Path, matches: impl Fn(&Path) -> bool) -> Option<PathBuf> {
    fn walk(dir: &Path, best: &mut Option<(SystemTime, PathBuf)>, matches: &dyn Fn(&Path) -> bool) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };

        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                walk(&path, best, matches);
                continue;
            }
            if !file_type.is_file() {
                continue;
            }

            let is_rollout = path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|name| name.starts_with("rollout-") && name.ends_with(".jsonl"));
            if !is_rollout {
                continue;
            }
            if !matches(&path) {
                continue;
            }

            let Ok(modified) = entry.metadata().and_then(|m| m.modified()) else {
                continue;
            };

            match best {
                Some((current, _)) if modified <= *current => {}
                _ => *best = Some((modified, path)),
            }
        }
    }

    let mut best = None;
    walk(root, &mut best, &matches);
    best.map(|(_, path)| path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tempdir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "nexus-codex-bridge-unit-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .subsec_nanos(),
            tag,
        ));
        std::fs::create_dir_all(&dir).expect("create tempdir");
        dir
    }

    #[test]
    fn read_thread_id_from_rollout_reads_first_line_payload_id() {
        let dir = tempdir("read");
        let rollout = dir.join("rollout-test.jsonl");
        std::fs::write(
            &rollout,
            "{\"payload\":{\"id\":\"thread-123\"}}\n{\"payload\":{\"id\":\"other\"}}\n",
        )
        .expect("write rollout");

        assert_eq!(
            read_thread_id_from_rollout(&rollout).as_deref(),
            Some("thread-123")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn newest_rollout_walks_recursively_and_ignores_non_rollouts() {
        let dir = tempdir("newest");
        let old_dir = dir.join("a");
        let new_dir = dir.join("b").join("c");
        std::fs::create_dir_all(&old_dir).expect("create old dir");
        std::fs::create_dir_all(&new_dir).expect("create new dir");
        let old = old_dir.join("rollout-old.jsonl");
        let new = new_dir.join("rollout-new.jsonl");
        std::fs::write(&old, "{}\n").expect("write old");
        std::thread::sleep(Duration::from_millis(5));
        std::fs::write(dir.join("not-rollout.jsonl"), "{}\n").expect("write ignored");
        std::thread::sleep(Duration::from_millis(5));
        std::fs::write(&new, "{}\n").expect("write new");

        assert_eq!(newest_rollout(&dir).as_deref(), Some(new.as_path()));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
