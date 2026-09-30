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
//! Explicit resume is different: Nexus finds the existing Codex home that already contains the
//! requested thread rollout, starts `codex app-server` with that `CODEX_HOME`, and opens the TUI
//! with `codex resume --remote ... <thread>`. It never copies rollout history into a fresh home.

use std::collections::HashMap;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use nexus_common::RuntimeProcessIds;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_store::Store;

use super::approvals::AutoApprove;
use super::client::CodexAppServerClient;
use super::forwarder::{spawn_codex_forwarder_with_tool_observations, CodexToolObservationSink};
use super::jsonrpc::CodexRpcError;
use super::supervisor::{CodexAppServer, SupervisorOpts};
use super::transport::CodexAppServerTransport;
use crate::storage::{CodexRuntimeLaunch, CodexRuntimeStateRepo};

/// Synchronous notification fired after a Codex thread id has been discovered and successfully
/// resumed/bound. Callers that need async persistence can spawn from this callback.
pub type ThreadDiscovered = Arc<dyn Fn(SessionId, String) + Send + Sync + 'static>;

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
    /// Daemon restart revive uses this for headed Codex sessions: the old app-server socket may
    /// still accept `turn/start`, but its completion stream can belong to the dead daemon's adopted
    /// transport. Fresh launch/adoption keeps the default `false` so idempotent launch calls can
    /// still reuse a process that this daemon already owns.
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
    server: CodexAppServer,
    forwarder: Option<tokio::task::JoinHandle<()>>,
    thread_id: Option<String>,
    #[allow(dead_code)]
    socket: PathBuf,
}

/// Owns app-server lifecycle, rollout discovery, notification forwarding, and inject binding for
/// headed Codex sessions.
#[derive(Clone)]
pub struct CodexBridge {
    handles: Arc<Mutex<HashMap<SessionId, Handle>>>,
    transport: CodexAppServerTransport,
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
        }
    }

    /// Clone the inject transport so routing code can deliver turns to discovered codex threads.
    pub fn transport(&self) -> CodexAppServerTransport {
        self.transport.clone()
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
        let Some(mut handle) = self.handles.lock().unwrap().remove(session) else {
            return false;
        };
        if let Some(forwarder) = handle.forwarder.take() {
            forwarder.abort();
        }
        self.transport.unbind(session);
        self.transport.unmark_live(session);
        tokio::spawn(async move { handle.server.shutdown().await });
        true
    }

    /// Remove every tracked session, abort all forwarders, unbind injection, and asynchronously
    /// shut down every app-server subprocess. Returns the number of handles removed.
    pub fn kill_all(&self) -> usize {
        let handles: Vec<_> = self.handles.lock().unwrap().drain().collect();
        let count = handles.len();
        for (session, mut handle) in handles {
            if let Some(forwarder) = handle.forwarder.take() {
                forwarder.abort();
            }
            self.transport.unbind(&session);
            self.transport.unmark_live(&session);
            tokio::spawn(async move { handle.server.shutdown().await });
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

        if let Some(thread_id) = known_thread_id.as_deref() {
            let Some(home) =
                codex_home_with_thread(&opts.session_dir, &resume_codex_homes, thread_id)
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
                )
            })
        };
        if let Some((sock_path, thread_id, known_pid)) = existing_handle {
            match known_pid {
                Some(pid) => self.transport.mark_live_with_pid(session.clone(), pid),
                None => self.transport.mark_live(session.clone()),
            }
            let desired_thread_id = known_thread_id.clone().or(thread_id);
            let bound_thread_id = self.transport.bound_thread_id(&session);
            let needs_bind = match desired_thread_id.as_deref() {
                Some(thread_id) => bound_thread_id.as_deref() != Some(thread_id),
                None => bound_thread_id.is_none(),
            };
            if needs_bind {
                if create_thread_if_missing && desired_thread_id.is_none() {
                    create_and_bind_thread(
                        self.handles.clone(),
                        self.transport(),
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
                    spawn_binding_task(
                        self.handles.clone(),
                        self.transport(),
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
            return Ok(sock_path);
        }

        tracing::info!(
            target: "nexus::codex_bridge",
            session = %session,
            known_thread_id = known_thread_id.as_deref().unwrap_or(""),
            "codex bridge: starting app-server"
        );
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
        let server = CodexAppServer::start(opts).await?;
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
        self.handles.lock().unwrap().insert(
            session.clone(),
            Handle {
                server,
                forwarder: None,
                thread_id: None,
                socket: sock_path.clone(),
            },
        );
        // Mark the session alive NOW (the app-server process is up) so the heartbeat keeper reads
        // it "online" and it shows in the roster / web console immediately — the turn binding only
        // lands after first-turn discovery, which would otherwise leave it "offline" until then.
        // Pid-backed when this daemon spawned the process, so liveness stays process-backed (
        // the keeper must not kill an idle pre-thread codex; a stale stamp must not mask a dead
        // one). Adopted sockets stay stamp-only and rely on the fast turn binding.
        match app_server_pid {
            Some(pid) => self.transport.mark_live_with_pid(session.clone(), pid),
            None => self.transport.mark_live(session.clone()),
        }

        if create_thread_if_missing && known_thread_id.is_none() {
            if let Err(error) = create_and_bind_thread(
                self.handles.clone(),
                self.transport(),
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
                self.kill(&session);
                return Err(error);
            }
        } else {
            spawn_binding_task(
                self.handles.clone(),
                self.transport(),
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
}

#[allow(clippy::too_many_arguments)]
async fn create_and_bind_thread(
    handles: Arc<Mutex<HashMap<SessionId, Handle>>>,
    transport: CodexAppServerTransport,
    session: SessionId,
    socket: PathBuf,
    cwd: Option<PathBuf>,
    on_thread_discovered: Option<ThreadDiscovered>,
    runtime_store: Option<Arc<Store>>,
    events: Arc<dyn EventSink>,
    tool_observations: Option<Arc<dyn CodexToolObservationSink>>,
) -> Result<String, CodexRpcError> {
    let client = Arc::new(CodexAppServerClient::connect(&socket, "nexus-bridge").await?);
    let thread_id = client
        .thread_start_in(cwd.as_deref().and_then(Path::to_str))
        .await?;

    transport.bind(session.clone(), client.clone(), thread_id.clone());
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
    if let Some(callback) = &on_thread_discovered {
        callback(session.clone(), thread_id.clone());
    }
    let forwarder = spawn_codex_forwarder_with_tool_observations(
        session.clone(),
        client,
        events,
        Arc::new(AutoApprove),
        transport.turn_tracker(),
        tool_observations,
    );
    let mut map = handles.lock().unwrap();
    let Some(handle) = map.get_mut(&session) else {
        transport.unbind(&session);
        forwarder.abort();
        return Err(CodexRpcError::Connect(format!(
            "codex bridge session {session} disappeared while creating its thread"
        )));
    };
    if let Some(previous) = handle.forwarder.replace(forwarder) {
        previous.abort();
    }
    handle.thread_id = Some(thread_id.clone());
    Ok(thread_id)
}

fn spawn_binding_task(
    handles: Arc<Mutex<HashMap<SessionId, Handle>>>,
    transport: CodexAppServerTransport,
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
        let (thread_id, rollout_path) = match known_thread_id {
            Some(id) => {
                let rollout_path = rollout_with_thread_id(&rollout_root, &id);
                (id, rollout_path)
            }
            None => {
                let Some(discovered) =
                    wait_for_thread_id(&rollout_root, Duration::from_secs(900)).await
                else {
                    tracing::warn!(
                        session = %discovery_session.0,
                        rollout_root = %rollout_root.display(),
                        "no thread discovered"
                    );
                    return;
                };
                discovered
            }
        };

        let client = match CodexAppServerClient::connect(&discovery_sock, "nexus-bridge").await {
            Ok(client) => Arc::new(client),
            Err(e) => {
                tracing::warn!(
                    session = %discovery_session.0,
                    socket = %discovery_sock.display(),
                    "codex bridge: connect after thread discovery failed: {e}"
                );
                return;
            }
        };

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
        if let Err(e) = client
            .thread_resume_in(&thread_id, resume_cwd.as_deref())
            .await
        {
            tracing::warn!(
                session = %discovery_session.0,
                thread_id = %thread_id,
                "codex bridge: thread_resume after discovery failed: {e}"
            );
            return;
        }

        transport.bind(discovery_session.clone(), client.clone(), thread_id.clone());
        tracing::info!(
            target: "nexus::codex_bridge",
            session = %discovery_session,
            thread_id = %thread_id,
            "codex bridge: transport bound"
        );
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
        if let Some(callback) = &on_thread_discovered {
            callback(discovery_session.clone(), thread_id.clone());
        }
        let forwarder = spawn_codex_forwarder_with_tool_observations(
            discovery_session.clone(),
            client,
            events,
            Arc::new(AutoApprove),
            transport.turn_tracker(),
            tool_observations,
        );

        let mut map = handles.lock().unwrap();
        if let Some(handle) = map.get_mut(&discovery_session) {
            if let Some(old_forwarder) = handle.forwarder.take() {
                old_forwarder.abort();
            }
            handle.forwarder = Some(forwarder);
            handle.thread_id = Some(thread_id);
        } else {
            transport.unbind(&discovery_session);
            forwarder.abort();
        }
    });
}

fn codex_home_with_thread(
    session_dir: &Path,
    explicit_homes: &[PathBuf],
    thread_id: &str,
) -> Option<PathBuf> {
    codex_resume_home_candidates(session_dir, explicit_homes)
        .into_iter()
        .find(|home| rollout_with_thread_id(&home.join("sessions"), thread_id).is_some())
}

fn codex_resume_home_candidates(session_dir: &Path, explicit_homes: &[PathBuf]) -> Vec<PathBuf> {
    let mut homes = Vec::new();
    for home in explicit_homes {
        push_unique_path(&mut homes, home.clone());
    }
    if let Some(home) = std::env::var_os("CODEX_HOME") {
        push_unique_path(&mut homes, PathBuf::from(home));
    }
    if let Some(home) = std::env::var_os("HOME") {
        push_unique_path(&mut homes, PathBuf::from(home).join(".codex"));
    }
    if let Some(codex_sessions_root) = session_dir.parent() {
        if let Ok(entries) = std::fs::read_dir(codex_sessions_root) {
            for entry in entries.flatten() {
                push_unique_path(&mut homes, entry.path().join("codex-home"));
            }
        }
    }
    homes
}

fn push_unique_path(paths: &mut Vec<PathBuf>, path: PathBuf) {
    if !paths.iter().any(|existing| existing == &path) {
        paths.push(path);
    }
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
/// bus delivery to the agent. The newest rollout need not be the main thread.
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
            if path.is_dir() {
                walk(&path, best, matches);
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
