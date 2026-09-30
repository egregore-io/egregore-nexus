//! Codex app-server supervisor: spawn/adopt, use machine auth, wait-until-ready, shutdown.
//!
//! # Spawn invocation (pinned from source)
//!
//! omnigent/omnigent/codex_native_app_server.py line 561-565:
//!
//!   ```python
//!   argv = [self.codex_path, "app-server", "--listen", resolved_listen]
//!   proc_env = {**self.env, "CODEX_HOME": str(self.codex_home)}
//!   ```
//!
//! Exact command: `<codex_exe> app-server -c <override> ... --listen <endpoint>`
//! with the machine-owned `CODEX_HOME` in the environment for normal Nexus sessions.
//! Unix uses a private Unix-domain socket. Windows uses the native Windows Codex binary with a
//! loopback-only WebSocket endpoint because Codex does not expose Unix sockets there.
//! Nexus never copies credentials or provider configuration. It layers Nexus runtime state through
//! `-c` overrides. External resume launches may pass an explicit `CODEX_HOME`; that home remains
//! authoritative and untouched.
//!
//! The supervisor also writes `<session_dir>/app-server.pid` as one JSON line after spawn and
//! before readiness polling:
//!
//! ```json
//! {"pid":123,"pgid":123}
//! ```
//!
//! If startup wedges after spawn, devs can kill the stray app-server tree with `kill -- -<pgid>`.
//!
//! # Readiness polling
//!
//! The supervisor attempts `JsonRpc::connect` + an `initialize` round-trip
//! every 100 ms for up to 30 s. This matches `omnigent/_wait_until_ready` which
//! polls the socket path existence + connects a JSON-RPC client.

use std::path::{Path, PathBuf};
use std::time::Duration;

use nexus_common::process_ids::runtime_process_ids_for_pid;
use nexus_common::RuntimeProcessIds;
use tokio::process::Child;

use super::jsonrpc::{CodexRpcError, JsonRpc};
use super::protocol::{initialize_params, method};

/// Grace period for normal app-server shutdown. The app-server may have spawned shell/MCP/tool
/// descendants; shutdown targets the process group first, then escalates if the tree stays alive.
const APP_SERVER_TERM_GRACE: Duration = Duration::from_secs(5);
/// Short drop-time grace used as the final test/abnormal-path leak guard. Explicit shutdown still
/// gets the full graceful timeout above.
const APP_SERVER_DROP_TERM_GRACE: Duration = Duration::from_millis(500);

#[cfg(windows)]
const WINDOWS_APP_SERVER_LISTEN: &str = "ws://127.0.0.1:0";

#[cfg(windows)]
fn strip_ansi_control_sequences(input: &str) -> String {
    let mut stripped = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' && matches!(chars.peek(), Some(&'[')) {
            chars.next();
            for next in chars.by_ref() {
                if ('@'..='~').contains(&next) {
                    break;
                }
            }
            continue;
        }
        stripped.push(ch);
    }
    stripped
}

#[cfg(windows)]
fn loopback_websocket_endpoint_from_log(log: &str) -> Option<PathBuf> {
    let stripped = strip_ansi_control_sequences(log);
    stripped.split_whitespace().find_map(|token| {
        let token = token
            .trim_matches(|ch: char| matches!(ch, '"' | '\'' | ',' | ';' | '(' | ')' | '[' | ']'));
        let address = token.strip_prefix("ws://")?;
        let socket = address.parse::<std::net::SocketAddr>().ok()?;
        socket
            .ip()
            .is_loopback()
            .then(|| PathBuf::from(format!("ws://{socket}")))
    })
}

#[cfg(windows)]
async fn wait_for_windows_websocket_endpoint(
    child: &mut Child,
    stderr_path: &Path,
    deadline: tokio::time::Instant,
) -> Result<PathBuf, CodexRpcError> {
    loop {
        let log = std::fs::read_to_string(stderr_path).unwrap_or_default();
        if let Some(endpoint) = loopback_websocket_endpoint_from_log(&log) {
            return Ok(endpoint);
        }
        if let Some(status) = child.try_wait().map_err(|error| {
            CodexRpcError::Connect(format!(
                "inspect native Windows Codex app-server process: {error}"
            ))
        })? {
            return Err(CodexRpcError::Connect(format!(
                "native Windows Codex app-server exited before reporting its loopback endpoint ({status}); see {stderr_path:?}"
            )));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(CodexRpcError::Connect(format!(
                "native Windows Codex app-server did not report its loopback endpoint; see {stderr_path:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[cfg(unix)]
const SIGTERM: i32 = 15;
#[cfg(unix)]
const SIGKILL: i32 = 9;
#[cfg(unix)]
const ESRCH: i32 = 3;

#[cfg(unix)]
extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
    fn getpgrp() -> i32;
}

#[cfg(unix)]
fn current_process_group() -> i32 {
    // SAFETY: getpgrp has no preconditions and returns the caller's process group id.
    unsafe { getpgrp() }
}

#[cfg(unix)]
fn signal_process_group(pgid: u32, sig: i32) -> std::io::Result<()> {
    let pgid = i32::try_from(pgid).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("process group id {pgid} does not fit in i32"),
        )
    })?;
    if pgid <= 0 || pgid == current_process_group() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("refusing to signal unsafe process group {pgid}"),
        ));
    }
    // SAFETY: kill(2) with a negative pid signals the process group whose id is abs(pid). The
    // caller rejects pgid 0/current group to avoid self-termination.
    let rc = unsafe { kill(-pgid, sig) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(unix)]
fn missing_process_group(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound || error.raw_os_error() == Some(ESRCH)
}

fn process_group_for_child(child: &Child, ledger: Option<RuntimeProcessIds>) -> Option<u32> {
    ledger.map(|ids| ids.os_pgid).or_else(|| child.id())
}

async fn terminate_child_process_group_async(
    child: &mut Child,
    ledger: Option<RuntimeProcessIds>,
    grace: Duration,
    context: &'static str,
) {
    #[cfg(unix)]
    {
        if let Some(pgid) = process_group_for_child(child, ledger) {
            match signal_process_group(pgid, SIGTERM) {
                Ok(()) => {}
                Err(e) if missing_process_group(&e) => {}
                Err(e) => {
                    tracing::warn!(
                        target: "nexus_harness_codex::app_server",
                        pgid,
                        context,
                        error = %e,
                        "failed to send SIGTERM to Codex app-server process group"
                    );
                }
            }

            match tokio::time::timeout(grace, child.wait()).await {
                Ok(Ok(_)) => return,
                Ok(Err(e)) => {
                    tracing::warn!(
                        target: "nexus_harness_codex::app_server",
                        pgid,
                        context,
                        error = %e,
                        "failed waiting for Codex app-server after SIGTERM"
                    );
                }
                Err(_) => {}
            }

            match signal_process_group(pgid, SIGKILL) {
                Ok(()) => {}
                Err(e) if missing_process_group(&e) => {}
                Err(e) => {
                    tracing::warn!(
                        target: "nexus_harness_codex::app_server",
                        pgid,
                        context,
                        error = %e,
                        "failed to send SIGKILL to Codex app-server process group"
                    );
                    let _ = child.start_kill();
                }
            }
            let _ = child.wait().await;
            return;
        }
    }

    #[cfg(not(unix))]
    {
        let _ = ledger;
        let _ = grace;
        let _ = context;
    }

    let _ = child.kill().await;
    let _ = child.wait().await;
}

fn terminate_child_process_group_sync(
    child: &mut Child,
    ledger: Option<RuntimeProcessIds>,
    grace: Duration,
    context: &'static str,
) {
    #[cfg(unix)]
    {
        if let Some(pgid) = process_group_for_child(child, ledger) {
            match signal_process_group(pgid, SIGTERM) {
                Ok(()) => {}
                Err(e) if missing_process_group(&e) => {}
                Err(e) => {
                    tracing::warn!(
                        target: "nexus_harness_codex::app_server",
                        pgid,
                        context,
                        error = %e,
                        "failed to send SIGTERM to Codex app-server process group"
                    );
                }
            }

            let deadline = std::time::Instant::now() + grace;
            loop {
                match child.try_wait() {
                    Ok(Some(_)) => return,
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(
                            target: "nexus_harness_codex::app_server",
                            pgid,
                            context,
                            error = %e,
                            "failed waiting for Codex app-server after SIGTERM"
                        );
                        break;
                    }
                }
                if std::time::Instant::now() >= deadline {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }

            match signal_process_group(pgid, SIGKILL) {
                Ok(()) => {}
                Err(e) if missing_process_group(&e) => {}
                Err(e) => {
                    tracing::warn!(
                        target: "nexus_harness_codex::app_server",
                        pgid,
                        context,
                        error = %e,
                        "failed to send SIGKILL to Codex app-server process group"
                    );
                    let _ = child.start_kill();
                }
            }

            let deadline = std::time::Instant::now() + Duration::from_millis(500);
            loop {
                match child.try_wait() {
                    Ok(Some(_)) => return,
                    Ok(None) => {}
                    Err(_) => return,
                }
                if std::time::Instant::now() >= deadline {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

    #[cfg(not(unix))]
    {
        let _ = ledger;
        let _ = grace;
        let _ = context;
    }

    let _ = child.start_kill();
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Options for spawning a Codex app-server session.
#[derive(Debug, Clone)]
pub struct SupervisorOpts {
    /// Path to the `codex` executable (or a hermetic fake for tests).
    pub codex_exe: String,
    /// Per-session runtime directory. The supervisor creates `codex.sock` here on Unix and writes
    /// `app-server.pid` for spawned processes on every platform. Provider auth/config never lives
    /// here.
    pub session_dir: PathBuf,
    /// Existing Codex home to use for resume launches. When set, the supervisor starts the
    /// app-server with this `CODEX_HOME` and leaves the home completely untouched.
    pub codex_home: Option<PathBuf>,
    /// Optional model override passed as `-c model="<model>"`.
    pub model: Option<String>,
    /// Optional MCP bus server passed as `-c mcp_servers.nexus-bus.*=...` overrides.
    pub bus_mcp: Option<BusMcp>,
    /// Working directory for the app-server subprocess. This is the operator's launch cwd, so
    /// Codex-created threads and tool calls see the same project as the attached TUI.
    pub cwd: Option<PathBuf>,
    /// Extra environment variables for the app-server subprocess (inherited by codex's shell
    /// tool calls). Used to set `NEXUS_NAME`/`NEXUS_PROJECT`/`NEXUS_CLIENT_KEY` so any `nexus`
    /// CLI the agent runs is attributed to the AGENT, not the operator who started the daemon.
    /// When `NEXUS_NAME` and `NEXUS_SESSION_ID` are both present, the supervisor also turns them
    /// into Codex `developer_instructions` so the model starts with its assigned Nexus identity.
    pub env: Vec<(String, String)>,
}

/// MCP server entry written into `config.toml` as `[mcp_servers.nexus-bus]`.
#[derive(Debug, Clone)]
pub struct BusMcp {
    pub command: String,
    pub args: Vec<String>,
}

/// A live Codex app-server subprocess supervised by this crate.
///
/// The server process is running and its local endpoint is ready for connections.
/// Call [`CodexAppServer::shutdown`] to SIGTERM the process cleanly.
pub struct CodexAppServer {
    /// Local endpoint descriptor. This is a Unix socket path on Unix and a loopback `ws://`
    /// descriptor on Windows. It remains path-shaped for storage compatibility.
    pub(crate) sock: PathBuf,
    /// The `CODEX_HOME` directory passed to the subprocess (kept for
    /// observability and future cleanup hooks).
    #[allow(dead_code)]
    pub(crate) codex_home: PathBuf,
    /// The live child process when this supervisor spawned it. `None` means an already-running
    /// app-server socket was adopted; in that case shutdown only drops Nexus-side bindings.
    pub(crate) child: Option<Child>,
    /// Exact OS process tuple for the spawned app-server root process.
    pub(crate) process_ledger: Option<RuntimeProcessIds>,
    /// Best-effort process-id sidecar written for spawned app-servers.
    pub(crate) pid_file: Option<PathBuf>,
}

impl CodexAppServer {
    /// Spawn or adopt a Codex app-server session and wait until it is ready to accept
    /// JSON-RPC requests (≤30 s).
    ///
    /// Steps:
    /// 1. Resolve `CODEX_HOME`: either explicit `opts.codex_home`, a non-Nexus machine
    ///    `CODEX_HOME`, or `HOME/.codex`.
    /// 2. Reuse an existing live `<session_dir>/codex.sock` when present on Unix.
    /// 3. Pass Nexus runtime state (features, model, identity instructions, project trust,
    ///    `mcp_servers.nexus-bus`) as `-c` overrides for every spawn.
    /// 4. Spawn the native platform Codex app-server on a Unix socket (Unix) or loopback
    ///    WebSocket (Windows), with the resolved machine-owned `CODEX_HOME`.
    /// 5. Write `<session_dir>/app-server.pid` with `{pid,pgid}` before readiness polling.
    /// 6. Poll `JsonRpc::connect` + `initialize` until it round-trips or
    ///    the 30 s deadline expires.
    pub async fn start(opts: SupervisorOpts) -> Result<CodexAppServer, CodexRpcError> {
        // 1. Prepare the machine-owned CODEX_HOME directory. An inherited Nexus session home is
        // legacy runtime state, not machine authority, so the resolver falls back to HOME/.codex.
        let codex_home = match opts.codex_home.clone() {
            Some(home) => home,
            None => resolve_machine_codex_home_from_env(&opts.session_dir)
                .map_err(CodexRpcError::Connect)?,
        };
        std::fs::create_dir_all(&codex_home).map_err(|e| {
            CodexRpcError::Connect(format!("create CODEX_HOME {codex_home:?}: {e}"))
        })?;

        #[cfg(unix)]
        let sock = opts.session_dir.join("codex.sock");
        #[cfg(unix)]
        if let Some(existing) = Self::try_adopt_existing(&sock, codex_home.clone()).await? {
            return Ok(existing);
        }

        // 2. Spawn the subprocess.
        #[cfg(unix)]
        let listen_url = {
            // Remove any stale socket from a previous run.
            let _ = std::fs::remove_file(&sock);
            format!("unix://{}", sock.display())
        };
        #[cfg(windows)]
        let listen_url = WINDOWS_APP_SERVER_LISTEN.to_string();

        let stderr_path = opts.session_dir.join("app-server.stderr.log");
        let stderr_file = std::fs::File::create(&stderr_path).map_err(|error| {
            CodexRpcError::Connect(format!(
                "create Codex app-server stderr log {stderr_path:?}: {error}"
            ))
        })?;
        let mut command = tokio::process::Command::new(&opts.codex_exe);
        command.arg("app-server");
        let developer_instructions = nexus_identity_developer_instructions(&opts.env);
        for override_arg in config_overrides(
            opts.model.as_deref(),
            developer_instructions.as_deref(),
            opts.bus_mcp.as_ref(),
            opts.cwd.as_deref(),
        ) {
            command.arg("-c").arg(override_arg);
        }
        if opts.bus_mcp.is_some() {
            if let Some(override_arg) = mcp_env_override(&opts.env) {
                command.arg("-c").arg(override_arg);
            }
        }
        command
            .arg("--listen")
            .arg(&listen_url)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            // Capture the app-server's own stderr (codex MCP boot, errors) to a file under the
            // session dir so launch problems are diagnosable instead of vanishing into /dev/null.
            .stderr(std::process::Stdio::from(stderr_file));
        // Start a new session so SIGTERM reaches the whole process group
        // (mirrors omnigent's `start_new_session=True` on POSIX).
        #[cfg(unix)]
        command.process_group(0);
        // Startup can still fail after spawn (for example readiness timeout). If that happens
        // before a CodexAppServer is returned, Tokio's drop guard must not leave the test fake or
        // real app-server process behind.
        command.kill_on_drop(true);
        scrub_inherited_identity_env(&mut command);
        command
            .env("CODEX_HOME", &codex_home)
            .envs(opts.env.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        if let Some(cwd) = &opts.cwd {
            command.current_dir(cwd);
        }
        let child = command
            .spawn()
            .map_err(|e| CodexRpcError::Connect(format!("spawn {:?}: {e}", opts.codex_exe)))?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        #[cfg(unix)]
        let endpoint = sock;
        #[cfg(windows)]
        let (child, endpoint) = {
            let mut child = child;
            let endpoint =
                wait_for_windows_websocket_endpoint(&mut child, &stderr_path, deadline).await?;
            (child, endpoint)
        };
        let process_ledger = child.id().and_then(runtime_process_ids_for_pid);
        let pid_file = process_ledger.map(|ids| {
            let pid_file = opts.session_dir.join("app-server.pid");
            let contents = format!("{{\"pid\":{},\"pgid\":{}}}\n", ids.os_pid, ids.os_pgid);
            std::fs::write(&pid_file, contents)
                .map_err(|e| {
                    CodexRpcError::Connect(format!("write app-server pid file {pid_file:?}: {e}"))
                })
                .map(|_| pid_file)
        });
        let pid_file = match pid_file {
            Some(Ok(path)) => Some(path),
            Some(Err(e)) => return Err(e),
            None => None,
        };

        // 4. Poll until ready (≤30 s).
        let poll_interval = Duration::from_millis(100);

        let rpc = loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(CodexRpcError::Timeout);
            }

            // Try to connect and send initialize.
            match JsonRpc::connect(&endpoint).await {
                Ok(rpc) => {
                    // Attempt the initialize handshake.
                    match rpc
                        .request(method::INITIALIZE, initialize_params("nexus-harness"))
                        .await
                    {
                        Ok(_) => break rpc,
                        Err(CodexRpcError::Connect(_)) | Err(CodexRpcError::Closed) => {
                            // Server not yet ready — keep polling.
                        }
                        Err(e) => return Err(e),
                    }
                }
                Err(CodexRpcError::Connect(_)) => {
                    // Endpoint not yet ready.
                }
                Err(e) => return Err(e),
            }

            tokio::time::sleep(poll_interval).await;
        };

        // Drop the RPC handle (the readiness probe is done; callers construct
        // their own handle via `CodexAppServer::socket()`).
        drop(rpc);

        Ok(CodexAppServer {
            sock: endpoint,
            codex_home,
            child: Some(child),
            process_ledger,
            pid_file,
        })
    }

    /// Adopt an already-running app-server bound to `sock`.
    ///
    /// This is used when a daemon loses its in-memory bridge but the Codex app-server process is
    /// still alive. We verify the socket by performing the same initialize round-trip as startup.
    /// The returned handle has no child process, so [`shutdown`](Self::shutdown) will not terminate
    /// the app-server it did not spawn.
    #[cfg(unix)]
    async fn try_adopt_existing(
        sock: &Path,
        codex_home: PathBuf,
    ) -> Result<Option<CodexAppServer>, CodexRpcError> {
        if !sock.exists() {
            return Ok(None);
        }

        let rpc = match JsonRpc::connect(sock).await {
            Ok(rpc) => rpc,
            Err(CodexRpcError::Connect(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        match rpc
            .request(method::INITIALIZE, initialize_params("nexus-harness"))
            .await
        {
            Ok(_) => {
                drop(rpc);
                Ok(Some(CodexAppServer {
                    sock: sock.to_path_buf(),
                    codex_home,
                    child: None,
                    process_ledger: None,
                    pid_file: None,
                }))
            }
            Err(CodexRpcError::Connect(_)) | Err(CodexRpcError::Closed) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Local endpoint descriptor for the app-server.
    pub fn socket(&self) -> &Path {
        &self.sock
    }

    /// Exact process tuple for this app-server when Nexus spawned it in this process.
    pub fn process_ledger(&self) -> Option<RuntimeProcessIds> {
        self.process_ledger
    }

    /// Shut down the app-server by sending SIGTERM to its process group
    /// (mirrors omnigent `_terminate_process_tree`), then awaiting exit.
    ///
    /// On Unix the child's process group is terminated (SIGTERM) so any
    /// grandchildren (shell wrappers, etc.) are also cleaned up.  On
    /// non-Unix platforms the direct child is killed instead.
    pub async fn shutdown(mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };

        terminate_child_process_group_async(
            &mut child,
            self.process_ledger,
            APP_SERVER_TERM_GRACE,
            "shutdown",
        )
        .await;

        // Remove the socket file on clean shutdown.
        let _ = std::fs::remove_file(&self.sock);
        if let Some(pid_file) = &self.pid_file {
            let _ = std::fs::remove_file(pid_file);
        }
    }
}

impl Drop for CodexAppServer {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        terminate_child_process_group_sync(
            &mut child,
            self.process_ledger,
            APP_SERVER_DROP_TERM_GRACE,
            "drop",
        );
        let _ = std::fs::remove_file(&self.sock);
    }
}

fn scrub_inherited_identity_env(command: &mut tokio::process::Command) {
    for (key, _) in std::env::vars_os() {
        if should_scrub_inherited_env(&key) {
            command.env_remove(key);
        }
    }
}

fn should_scrub_inherited_env(key: &std::ffi::OsStr) -> bool {
    let Some(key) = key.to_str() else {
        return false;
    };
    key.starts_with("NEXUS_")
        || key.starts_with("CODEX_")
        || matches!(
            key,
            "CLAUDE_CONFIG_DIR" | "HERMES_HOME" | "OPENCODE_HOME" | "OPENCODE_CONFIG_DIR"
        )
}

// ---------------------------------------------------------------------------
// Machine provider-home resolution
// ---------------------------------------------------------------------------

/// Resolve the machine-owned Codex home without consulting or mutating credential contents.
///
/// `CODEX_HOME` values that point at `<...>/codex-sessions/<session>/codex-home` are legacy
/// Nexus-owned runtime homes. They are deliberately ignored so a daemon launched from one Nexus
/// agent cannot make that agent's stale credential copy authoritative for every later session.
#[doc(hidden)]
pub fn resolve_machine_codex_home(
    session_dir: &Path,
    codex_home: Option<&Path>,
    home: Option<&Path>,
) -> Result<PathBuf, String> {
    if let Some(selected) = codex_home.filter(|path| !path.as_os_str().is_empty()) {
        if !selected.is_absolute() {
            return Err(format!(
                "machine CODEX_HOME must be absolute, got {}",
                selected.display()
            ));
        }
        if !is_nexus_managed_codex_home(selected, session_dir) {
            return Ok(selected.to_path_buf());
        }
    }

    let home = home
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or_else(|| {
            "cannot resolve machine CODEX_HOME: neither CODEX_HOME nor HOME is set".to_string()
        })?;
    if !home.is_absolute() {
        return Err(format!(
            "machine HOME must be absolute to resolve CODEX_HOME, got {}",
            home.display()
        ));
    }
    Ok(home.join(".codex"))
}

#[doc(hidden)]
pub fn resolve_machine_codex_home_from_env(session_dir: &Path) -> Result<PathBuf, String> {
    let codex_home = std::env::var_os("CODEX_HOME").map(PathBuf::from);
    let home = std::env::var_os("HOME").map(PathBuf::from);
    resolve_machine_codex_home(session_dir, codex_home.as_deref(), home.as_deref())
}

fn is_nexus_managed_codex_home(path: &Path, session_dir: &Path) -> bool {
    let expected_shape = path.file_name().is_some_and(|name| name == "codex-home")
        && path
            .parent()
            .and_then(Path::parent)
            .and_then(Path::file_name)
            .is_some_and(|name| name == "codex-sessions");
    if expected_shape {
        return true;
    }

    session_dir.parent().is_some_and(|sessions_root| {
        path.starts_with(sessions_root) && path.file_name().is_some_and(|name| name == "codex-home")
    })
}

/// Build `codex app-server -c key=value` overrides for Nexus runtime state.
///
/// Machine `config.toml` remains Codex-owned state; Nexus identity, trust, MCP, and feature flags
/// are layered on argv for every spawn instead of being persisted.
#[doc(hidden)]
pub fn config_overrides(
    model: Option<&str>,
    developer_instructions: Option<&str>,
    bus_mcp: Option<&BusMcp>,
    trusted_project: Option<&Path>,
) -> Vec<String> {
    let mut args = vec![
        "features.apps=false".to_string(),
        "features.enable_mcp_apps=false".to_string(),
    ];
    if let Some(model) = model {
        args.push(format!("model={}", toml_string(model)));
    }
    if let Some(instructions) = developer_instructions {
        args.push(format!(
            "developer_instructions={}",
            toml_string(instructions)
        ));
    }
    if let Some(bus) = bus_mcp {
        args.push(format!(
            "mcp_servers.nexus-bus.command={}",
            toml_string(&bus.command)
        ));
        args.push(format!(
            "mcp_servers.nexus-bus.args={}",
            toml_string_array(&bus.args)
        ));
    }
    if let Some(project) = trusted_project {
        args.push(format!(
            "projects.{}.trust_level=\"trusted\"",
            toml_string(&project.to_string_lossy())
        ));
    }
    args
}

/// Build the Codex MCP-specific environment override for daemon IPC discovery.
///
/// Codex intentionally sanitizes the environment of MCP subprocesses instead of inheriting the
/// app-server's full environment. Pass only the daemon home, autostart policy, and inherited
/// Tokio worker cap; the MCP command line carries identity/client key and no child receives
/// direct-store coordinates.
#[doc(hidden)]
pub fn mcp_env_override(env: &[(String, String)]) -> Option<String> {
    const KEYS: [&str; 3] = ["NEXUS_HOME", "NEXUS_NO_AUTOSTART", "TOKIO_WORKER_THREADS"];
    let entries = KEYS
        .into_iter()
        .filter_map(|key| {
            env_value(env, key)
                .filter(|value| !value.is_empty())
                .map(|value| format!("{key} = {}", toml_string(value)))
        })
        .collect::<Vec<_>>();
    if entries.is_empty() {
        None
    } else {
        Some(format!(
            "mcp_servers.nexus-bus.env={{ {} }}",
            entries.join(", ")
        ))
    }
}

/// Convert Nexus identity environment into Codex developer instructions.
#[doc(hidden)]
pub fn nexus_identity_developer_instructions(env: &[(String, String)]) -> Option<String> {
    let name = env_value(env, "NEXUS_NAME")?.trim();
    let session = env_value(env, "NEXUS_SESSION_ID")?.trim();
    if name.is_empty() || session.is_empty() {
        return None;
    }

    let project = env_value(env, "NEXUS_PROJECT")
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let project_clause = project
        .map(|project| format!(" in project {project}"))
        .unwrap_or_default();
    Some(format!(
        "Nexus identity: You are {name} (session {session}){project_clause}. Never sign messages as anyone else. If unsure, run `nexus whoami`."
    ))
}

fn env_value<'a>(env: &'a [(String, String)], key: &str) -> Option<&'a str> {
    env.iter()
        .find_map(|(k, v)| (k == key).then_some(v.as_str()))
}

fn toml_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

fn toml_string_array(items: &[String]) -> String {
    let values = items
        .iter()
        .map(|item| toml_string(item))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{values}]")
}
