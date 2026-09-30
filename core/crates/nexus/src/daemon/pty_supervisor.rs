//! `PtySupervisor` — launches a native harness (claude/codex/opencode) in a daemon-owned PTY, binds the
//! [`PtyTransport`] so drained `<nexus-batch>` turns reach the harness's PTY input, and tracks the
//! live [`PtySession`]s by [`SessionId`] (for attach/resize/kill).
//!
//! Supervision primitives. The application launch path supplies the store, sink and bell
//! needed to spawn the transcript tailer.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use portable_pty::{CommandBuilder, PtySize};
use tokio::sync::broadcast;

use nexus_common::process_ids::runtime_process_ids_for_pid;
use nexus_common::RuntimeProcessIds;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_contracts::HarnessId;
use nexus_dispatch::Bell;
use nexus_harness_claude::native::bridge::{
    write_launch_settings_with_identity, ClaudeNativeBridgePaths,
};
use nexus_harness_claude::storage::{ClaudeRuntimeLaunch, ClaudeRuntimeStateRepo};
use nexus_harness_codex::{
    remote_command_with_executable, remote_resume_command_with_executable,
    resolve_codex_executable, BridgeLaunchOptions, BusMcp, CodexAppServerTransport, CodexBridge,
    CodexToolObservationSink, SupervisorOpts, ThreadDiscovered,
};
use nexus_harness_core::native_executable::resolve_opencode_executable;
use nexus_harness_core::{
    Harness as HarnessContract, HarnessIdentity as ContractHarnessIdentity, HeadedRuntimeKind,
};
use nexus_pty::command::{
    apply_headed_terminal_environment, harness_command_with_identity, HarnessIdentity,
};
#[cfg(windows)]
use nexus_pty::ConPtyBackend;
use nexus_pty::{
    bracketed_paste, HarnessInput, PtyError, PtySession, ScreenModelBackend, TerminalBackend,
    TmuxHarness, TurnCompletionEvidence,
};
use nexus_store::Store;
use nexus_transcript::ToolCallObservation;

use crate::daemon::claude_native_forwarder::ClaudeTurnCompletion;
use crate::daemon::gateway_stream_socket::GatewayStreamPublisher;
use crate::daemon::hermes_gateway::{
    write_hermes_gateway_profile, HermesGatewayBridge, HermesGatewayProfile,
};
use crate::daemon::hermes_native_forwarder::{HermesRuntimeLaunch, HermesRuntimeStateRepo};
use crate::daemon::opencode_native_forwarder::{OpenCodeRuntimeLaunch, OpenCodeRuntimeStateRepo};
use crate::daemon::opencode_plugin_bridge::{
    write_opencode_plugin_files, OpenCodePluginBridge, OpenCodePluginBridgeOptions,
};
use crate::daemon::pty_transport::PtyTransport;
use crate::daemon::terminal_socket::{TerminalSocketEndpoint, TerminalSocketRegistry};
use crate::harness_registry::harness_registry_by_id;

const OPENCODE_PLUGIN_READY_TIMEOUT: Duration = Duration::from_secs(75);
const OPENCODE_PLUGIN_READY_POLL: Duration = Duration::from_millis(200);

fn opencode_viewer_backend_kind(viewer_backend: &str) -> Result<&'static str, PtyError> {
    match viewer_backend {
        "pty" => Ok("raw"),
        "tmux" => Ok("tmux"),
        other => Err(PtyError::Spawn(format!(
            "unsupported OpenCode viewer backend {other:?}; expected pty or tmux"
        ))),
    }
}

fn raw_pty_query_responder(runtime: HeadedRuntimeKind) -> bool {
    // OpenCode's native plugin owns prompt/completion and its viewer has its own terminal-probe
    // timeout. Injecting synthetic terminal replies into that TUI makes it exit after the setup
    // turn. Other raw headed runtimes still require the daemon responder when unattended.
    runtime != HeadedRuntimeKind::OpenCodePlugin
}

fn claude_native_resume_id_from_tail(args: &[String]) -> Option<&str> {
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == "--resume" {
            return iter.next().map(String::as_str).filter(|id| !id.is_empty());
        }
        if let Some(id) = arg.strip_prefix("--resume=") {
            if !id.is_empty() {
                return Some(id);
            }
        }
    }
    None
}

/// Spawns native-harness TUIs and binds them to a shared [`PtyTransport`] so the existing
/// drain/`render_batch`/`inject_turn` machinery delivers turns to a native harness.
///
/// Two terminal backends exist: the default daemon-owned raw [`PtySession`] and the legacy
/// [`TmuxHarness`] opt-in. The deterministic, offline tests also use a raw [`PtySession`] running
/// `cat` (it echoes its input, which the cat-based proofs assert on). `pty_backends` tracks the
/// local terminal handles for raw and tmux-backed headed runtimes.
///
/// `codex_bridge` tracks headed codex app-server sessions. `opencode_plugins` tracks headed
/// OpenCode native-plugin bridges. Both use a foreground TUI for the human while delivery goes
/// through a structured server/plugin channel instead of terminal keystrokes.
pub struct PtySupervisor {
    transport: PtyTransport,
    /// Lifecycle driver for the tmux terminal backend. The daemon stores bound runtime handles
    /// behind [`PtyBackend`], while this driver owns tmux launch/adopt/kill probes.
    tmux_backend: TmuxBackendDriver,
    /// Codex-specific bridge for headed app-server sessions.
    codex_bridge: CodexBridge,
    /// OpenCode native-plugin bridges keyed by Nexus session id. Kept alive for the lifetime of the
    /// tmux viewer; dropping a bridge shuts down its loopback endpoint.
    opencode_plugins: Arc<Mutex<HashMap<SessionId, Arc<OpenCodePluginBridge>>>>,
    runtime_store: Option<Arc<Store>>,
    /// Optional daemon-to-gateway publisher used for Codex app-server tool-call observations.
    gateway_stream: Option<GatewayStreamPublisher>,
    pty_backends: Arc<Mutex<HashMap<SessionId, Arc<dyn PtyBackend>>>>,
    claude_completions: Arc<Mutex<HashMap<SessionId, Arc<ClaudeTurnCompletion>>>>,
    claude_startup_markers: Arc<Mutex<HashMap<SessionId, (PathBuf, u64)>>>,
    terminal: TerminalSocketRegistry,
}

/// Backend-neutral handle for local headed terminal runtimes.
///
/// Tmux is the first production implementation. The supervisor stores raw and tmux runtime handles
/// behind this contract so launch/adopt/respawn/read paths can move off tmux-specific maps without
/// changing harness behavior in the same slice.
pub trait PtyBackend: Send + Sync {
    fn kind(&self) -> &'static str;
    fn terminal_backend(&self) -> Arc<dyn TerminalBackend>;
    fn output(&self) -> broadcast::Receiver<Vec<u8>>;
    fn kill(&self) -> bool;
    fn runtime_process_ids(&self) -> Option<RuntimeProcessIds>;
}

#[derive(Clone, Copy, Default)]
struct TmuxBackendDriver;

impl TmuxBackendDriver {
    fn launch(
        &self,
        session: &SessionId,
        program: &str,
        args: &[String],
        cwd: &str,
        size: PtySize,
        env: &[(String, String)],
    ) -> Result<Arc<TmuxHarness>, PtyError> {
        let session_name = self.session_name(session);
        TmuxHarness::launch_with_env(&session_name, program, args, cwd, size.cols, size.rows, env)
            .map(Arc::new)
            .map_err(PtyError::Spawn)
    }

    fn adopt(&self, session: &SessionId, cwd: &str) -> Result<Arc<TmuxHarness>, PtyError> {
        let session_name = self.session_name(session);
        TmuxHarness::adopt(&session_name, cwd)
            .map(Arc::new)
            .map_err(PtyError::Spawn)
    }

    fn session_name(&self, session: &SessionId) -> String {
        tmux_session_name(session)
    }

    fn socket_path_for_session(&self, session: &SessionId) -> PathBuf {
        tmux_socket_path_for_session_name(&self.session_name(session))
    }

    fn session_exists(&self, session: &SessionId) -> bool {
        let session_name = self.session_name(session);
        tmux_session_exists(&self.socket_path_for_session(session), &session_name)
    }

    fn kill_if_exists(&self, session: &SessionId) {
        let session_name = self.session_name(session);
        kill_tmux_session_if_exists(&session_name);
    }
}

struct RawPtyBackend {
    pty: Arc<PtySession>,
    terminal: Arc<dyn TerminalBackend>,
}

impl PtyBackend for RawPtyBackend {
    fn kind(&self) -> &'static str {
        "raw"
    }

    fn terminal_backend(&self) -> Arc<dyn TerminalBackend> {
        self.terminal.clone()
    }

    fn output(&self) -> broadcast::Receiver<Vec<u8>> {
        self.pty.subscribe()
    }

    fn kill(&self) -> bool {
        self.pty.kill().is_ok()
    }

    fn runtime_process_ids(&self) -> Option<RuntimeProcessIds> {
        self.pty.child_pid().and_then(runtime_process_ids_for_pid)
    }
}

struct TmuxPtyBackend {
    harness: Arc<TmuxHarness>,
}

impl PtyBackend for TmuxPtyBackend {
    fn kind(&self) -> &'static str {
        "tmux"
    }

    fn terminal_backend(&self) -> Arc<dyn TerminalBackend> {
        self.harness.clone() as Arc<dyn TerminalBackend>
    }

    fn output(&self) -> broadcast::Receiver<Vec<u8>> {
        self.harness.pipe_output()
    }

    fn kill(&self) -> bool {
        self.harness.kill().is_ok()
    }

    fn runtime_process_ids(&self) -> Option<RuntimeProcessIds> {
        self.harness
            .pane_pid()
            .and_then(runtime_process_ids_for_pid)
    }
}

struct HermesGatewayHarness {
    tmux: Arc<TmuxHarness>,
    bridge: Arc<HermesGatewayBridge>,
}

/// Claude native input coupled to its structured Stop/StopFailure hook boundary.
struct ClaudeNativeHarness {
    input: Arc<dyn HarnessInput>,
    completion: Arc<ClaudeTurnCompletion>,
}

const CLAUDE_RAW_PROMPT_READY_TIMEOUT: Duration = Duration::from_secs(30);
const CLAUDE_RAW_PASTE_COMMIT_TIMEOUT: Duration = Duration::from_secs(5);
const CLAUDE_RAW_SUBMIT_VERIFY_TIMEOUT: Duration = Duration::from_secs(10);
const CLAUDE_RAW_POLL_INTERVAL: Duration = Duration::from_millis(150);
const CLAUDE_RAW_PASTE_SETTLE: Duration = Duration::from_millis(100);
const CLAUDE_RAW_SUBMIT_RETRY: Duration = Duration::from_secs(1);

/// Raw-PTY Claude input with the same readiness and verified-submit contract as the tmux driver.
/// The daemon-owned screen model is the terminal emulator, so this remains reliable without a
/// human attachment.
struct ClaudeRawPtyInput {
    input: Arc<dyn HarnessInput>,
    terminal: Arc<ScreenModelBackend>,
    completion: Arc<ClaudeTurnCompletion>,
}

#[async_trait]
impl HarnessInput for ClaudeRawPtyInput {
    async fn send_turn(&self, text: &str) -> Result<(), String> {
        let ready_deadline = Instant::now() + CLAUDE_RAW_PROMPT_READY_TIMEOUT;
        while Instant::now() < ready_deadline {
            if claude_raw_prompt_rendered(&self.terminal.contents()) {
                break;
            }
            if !self.input.is_alive() {
                return Err("Claude raw PTY exited before its input prompt rendered".into());
            }
            tokio::time::sleep(CLAUDE_RAW_POLL_INTERVAL).await;
        }
        if !claude_raw_prompt_rendered(&self.terminal.contents()) {
            return Err(format!(
                "Claude raw PTY input prompt never rendered within {}s",
                CLAUDE_RAW_PROMPT_READY_TIMEOUT.as_secs()
            ));
        }

        let observed_submission = self.completion.submission_snapshot();
        let writer = self.terminal.attach().writer;
        // Match the proven tmux sequence: clear any stale draft, commit one bracketed paste, then
        // submit only after the daemon-side screen model can see the draft.
        writer.write_bytes(&[0x01, 0x0b])?;
        writer.write_bytes(&bracketed_paste(&format!("{text}\n")))?;

        let (first_needle, last_needle) = claude_raw_submit_needles(text);
        let commit_deadline = Instant::now() + CLAUDE_RAW_PASTE_COMMIT_TIMEOUT;
        let mut draft_seen = false;
        while Instant::now() < commit_deadline {
            if claude_raw_draft_in_input_box(&self.terminal.contents(), &first_needle, &last_needle)
            {
                draft_seen = true;
                break;
            }
            tokio::time::sleep(CLAUDE_RAW_POLL_INTERVAL).await;
        }

        tokio::time::sleep(CLAUDE_RAW_PASTE_SETTLE).await;
        writer.write_bytes(b"\r")?;
        if !draft_seen {
            return Ok(());
        }

        let verify_deadline = Instant::now() + CLAUDE_RAW_SUBMIT_VERIFY_TIMEOUT;
        let mut last_enter = Instant::now();
        while Instant::now() < verify_deadline {
            tokio::time::sleep(CLAUDE_RAW_POLL_INTERVAL).await;
            if self.completion.submission_snapshot() > observed_submission {
                return Ok(());
            }
            if !claude_raw_draft_in_input_box(
                &self.terminal.contents(),
                &first_needle,
                &last_needle,
            ) {
                return Ok(());
            }
            if last_enter.elapsed() >= CLAUDE_RAW_SUBMIT_RETRY {
                writer.write_bytes(b"\r")?;
                last_enter = Instant::now();
            }
        }
        Err(format!(
            "Claude raw PTY did not accept the submitted turn within {}s; draft remains visible",
            CLAUDE_RAW_SUBMIT_VERIFY_TIMEOUT.as_secs()
        ))
    }

    async fn interrupt_active_turn(&self) -> Result<(), String> {
        self.input.interrupt_active_turn().await
    }

    async fn compact(&self) -> Result<(), String> {
        self.send_turn("/compact").await
    }

    fn is_alive(&self) -> bool {
        self.input.is_alive()
    }
}

fn claude_raw_prompt_rendered(screen: &str) -> bool {
    let non_empty: Vec<&str> = screen
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    let start = non_empty.len().saturating_sub(12);
    non_empty[start..].iter().any(|line| line.contains('❯'))
}

fn claude_raw_submit_needles(text: &str) -> (String, String) {
    let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
    let visible: String = normalized
        .chars()
        .filter(|ch| !ch.is_control() || *ch == '\n')
        .collect();
    let first = visible.trim().chars().take(32).collect();
    let chars: Vec<char> = visible.trim().chars().collect();
    let last = chars[chars.len().saturating_sub(32)..].iter().collect();
    (first, last)
}

fn claude_raw_draft_in_input_box(screen: &str, first: &str, last: &str) -> bool {
    let lines: Vec<&str> = screen.lines().collect();
    let Some(prompt_line) = lines.iter().rposition(|line| line.contains('❯')) else {
        return false;
    };
    let mut region = String::new();
    for (offset, line) in lines[prompt_line..].iter().enumerate() {
        if offset > 0 && line.chars().filter(|ch| *ch == '─').count() >= 8 {
            break;
        }
        if offset == 0 {
            region.push_str(line.rsplit_once('❯').map(|(_, tail)| tail).unwrap_or(""));
        } else {
            region.push('\n');
            region.push_str(line);
        }
    }
    if region.contains("[Pasted text") {
        return true;
    }
    (!first.is_empty() && region.contains(first))
        || (!last.is_empty() && region.contains(last))
        || region.chars().any(char::is_alphanumeric)
}

#[async_trait]
impl HarnessInput for ClaudeNativeHarness {
    async fn send_turn(&self, text: &str) -> Result<(), String> {
        let observed = self.completion.snapshot();
        self.input.send_turn(text).await?;
        self.completion
            .wait_after(observed, Duration::from_secs(600))
            .await
    }

    fn turn_completion_evidence(&self) -> TurnCompletionEvidence {
        TurnCompletionEvidence::Terminal
    }

    async fn interrupt_active_turn(&self) -> Result<(), String> {
        self.input.interrupt_active_turn().await
    }

    async fn compact(&self) -> Result<(), String> {
        self.input.compact().await
    }

    fn is_alive(&self) -> bool {
        self.input.is_alive()
    }
}

/// OpenCode's loopback bridge can outlive the foreground `opencode attach` process. Delivery uses
/// the bridge, but runtime liveness must require both halves; otherwise a dead TUI leaves a live
/// HTTP listener, receives fresh heartbeats forever, and strands the claimed turn in `injecting`.
struct OpenCodeHeadedHarness {
    bridge: Arc<dyn HarnessInput>,
    runtime: Arc<dyn HarnessInput>,
}

impl OpenCodeHeadedHarness {
    fn new(bridge: Arc<dyn HarnessInput>, runtime: Arc<dyn HarnessInput>) -> Self {
        Self { bridge, runtime }
    }
}

#[async_trait]
impl HarnessInput for OpenCodeHeadedHarness {
    async fn send_turn(&self, text: &str) -> Result<(), String> {
        self.bridge.send_turn(text).await
    }

    fn turn_completion_evidence(&self) -> TurnCompletionEvidence {
        self.bridge.turn_completion_evidence()
    }

    async fn interrupt_active_turn(&self) -> Result<(), String> {
        self.bridge.interrupt_active_turn().await
    }

    async fn compact(&self) -> Result<(), String> {
        self.bridge.compact().await
    }

    fn is_alive(&self) -> bool {
        self.bridge.is_alive() && self.runtime.is_alive()
    }
}

/// Gateway-backed, observation-only sink for Codex app-server tool-call phases.
struct GatewayCodexToolObservationSink {
    publisher: GatewayStreamPublisher,
    agent_name: String,
}

impl GatewayCodexToolObservationSink {
    fn new(publisher: GatewayStreamPublisher, agent_name: String) -> Self {
        Self {
            publisher,
            agent_name,
        }
    }
}

impl CodexToolObservationSink for GatewayCodexToolObservationSink {
    fn publish_tool_call(&self, session: &SessionId, observation: ToolCallObservation) {
        self.publisher
            .publish_tool_call_observation(session, &self.agent_name, observation);
    }
}

#[async_trait]
impl HarnessInput for HermesGatewayHarness {
    async fn send_turn(&self, text: &str) -> Result<(), String> {
        self.bridge.send_rendered_turn(text).await
    }

    async fn compact(&self) -> Result<(), String> {
        self.bridge.send_rendered_turn("/compress").await
    }

    fn turn_completion_evidence(&self) -> TurnCompletionEvidence {
        self.bridge.turn_completion_evidence()
    }

    fn is_alive(&self) -> bool {
        self.tmux.has_session()
    }
}

/// Hermes on the raw daemon-owned PTY, using the default backend like every other harness.
/// Turn input goes through the gateway bridge — never
/// the PTY — exactly as in the tmux shape; only liveness comes from the raw PTY child.
struct HermesRawPtyHarness {
    pty: Arc<PtySession>,
    bridge: Arc<HermesGatewayBridge>,
}

#[async_trait]
impl HarnessInput for HermesRawPtyHarness {
    async fn send_turn(&self, text: &str) -> Result<(), String> {
        self.bridge.send_rendered_turn(text).await
    }

    async fn compact(&self) -> Result<(), String> {
        self.bridge.send_rendered_turn("/compress").await
    }

    fn turn_completion_evidence(&self) -> TurnCompletionEvidence {
        self.bridge.turn_completion_evidence()
    }

    fn is_alive(&self) -> bool {
        self.pty.child_alive()
    }
}

impl Default for PtySupervisor {
    fn default() -> Self {
        Self {
            transport: PtyTransport::default(),
            tmux_backend: TmuxBackendDriver,
            codex_bridge: CodexBridge::new(),
            opencode_plugins: Arc::new(Mutex::new(HashMap::new())),
            runtime_store: None,
            gateway_stream: None,
            pty_backends: Arc::new(Mutex::new(HashMap::new())),
            claude_completions: Arc::new(Mutex::new(HashMap::new())),
            claude_startup_markers: Arc::new(Mutex::new(HashMap::new())),
            terminal: TerminalSocketRegistry::default(),
        }
    }
}

/// The interactive binary name for a harness kind. `claude`/`codex` run native TUIs in a PTY;
/// other kinds have no PTY-native binary (Pi/Other → ACP path), so this returns `None`.
pub fn harness_program(kind: &HarnessId) -> Option<&'static str> {
    let program = harness_registry_by_id(kind).program();
    (!program.is_empty()).then_some(program)
}

pub fn harness_agent_token(kind: &HarnessId) -> &'static str {
    harness_registry_by_id(kind).agent_token()
}

pub fn headed_runtime_kind(kind: &HarnessId) -> HeadedRuntimeKind {
    harness_registry_by_id(kind).headed_runtime_kind()
}

fn require_headed_harness(kind: &HarnessId) -> Result<&'static dyn HarnessContract, PtyError> {
    let harness = harness_registry_by_id(kind);
    if harness.program().is_empty() {
        return Err(PtyError::Spawn(format!(
            "harness {kind} has no headed TUI program"
        )));
    }
    Ok(harness)
}

/// The tmux session name for a launched harness: a `nexus-` prefix + the session id, sanitized to
/// tmux-safe chars (tmux forbids `.` and `:` in session names — they're target separators). Pure.
pub fn tmux_session_name(session: &SessionId) -> String {
    let safe: String = session
        .0
        .chars()
        .map(|c| if c == '.' || c == ':' { '-' } else { c })
        .collect();
    format!("nexus-{safe}")
}

impl PtySupervisor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_runtime_store(runtime_store: Arc<Store>) -> Self {
        Self::with_runtime_store_and_gateway_stream(runtime_store, None)
    }

    /// Create a supervisor with durable runtime state and optional daemon-to-gateway publisher.
    ///
    /// The publisher is used only for metadata-only Codex app-server developer events; it does not
    /// alter PTY turn delivery, store writes, or realtime wake behavior.
    pub fn with_runtime_store_and_gateway_stream(
        runtime_store: Arc<Store>,
        gateway_stream: Option<GatewayStreamPublisher>,
    ) -> Self {
        Self {
            runtime_store: Some(runtime_store),
            gateway_stream,
            ..Self::default()
        }
    }

    fn codex_bridge_options(
        &self,
        agent_name: &str,
        known_thread_id: Option<String>,
        resume_codex_homes: Vec<PathBuf>,
        on_thread_discovered: Option<ThreadDiscovered>,
        force_fresh_app_server: bool,
    ) -> BridgeLaunchOptions {
        let create_thread_if_missing = known_thread_id.is_none();
        let tool_observations = self.gateway_stream.clone().map(|publisher| {
            Arc::new(GatewayCodexToolObservationSink::new(
                publisher,
                agent_name.to_string(),
            )) as Arc<dyn CodexToolObservationSink>
        });
        BridgeLaunchOptions {
            known_thread_id,
            resume_codex_homes,
            on_thread_discovered,
            runtime_store: self.runtime_store.clone(),
            force_fresh_app_server,
            create_thread_if_missing,
            tool_observations,
        }
    }

    /// Spawn `program` in a raw daemon-owned **PTY** with the nexus-bus MCP wired, bind it to the
    /// transport under the caller-supplied [`SessionId`], track the session, and return the PTY.
    ///
    /// This is the deterministic, offline raw path. Production Claude and Codex launches go through
    /// [`launch_headed_pty`] instead — a raw PTY makes Claude's TUI never complete an injected turn.
    /// The transcript tailer
    /// is NOT spawned here (the caller supplies the store/sink/bell it needs). On Windows, the
    /// terminal/transport traits are exposed through `ConPtyBackend` so this same raw path uses the
    /// platform ConPTY host instead of a Unix PTY.
    ///
    /// [`launch_headed_pty`]: PtySupervisor::launch_headed_pty
    #[allow(clippy::too_many_arguments)]
    pub async fn launch(
        &self,
        session: &SessionId,
        program: &str,
        agent_id: &str,
        name: Option<&str>,
        project: &str,
        client_key: &str,
        nexus_exe: &str,
        cwd: &str,
        size: PtySize,
    ) -> Result<Arc<PtySession>, PtyError> {
        let launch_label = name.unwrap_or(agent_id);
        let cmd = harness_command_with_identity(
            program,
            HarnessIdentity {
                name: launch_label,
                project,
                client_key,
                agent: "other",
            },
            nexus_exe,
            Some(cwd),
        );
        let pty = Arc::new(PtySession::spawn(cmd, size)?);
        #[cfg(windows)]
        let (input_backend, terminal_backend) = {
            let conpty = Arc::new(ConPtyBackend::from_session(pty.clone()));
            (
                conpty.clone() as Arc<dyn HarnessInput>,
                conpty as Arc<dyn TerminalBackend>,
            )
        };
        #[cfg(not(windows))]
        let (input_backend, terminal_backend) = (
            pty.clone() as Arc<dyn HarnessInput>,
            pty.clone() as Arc<dyn TerminalBackend>,
        );
        self.bind_pty_backend(
            session,
            Arc::new(RawPtyBackend {
                pty: pty.clone(),
                terminal: terminal_backend,
            }) as Arc<dyn PtyBackend>,
        )?;
        self.transport.bind(session.clone(), input_backend);
        Ok(pty)
    }

    /// Spawn the REAL headed harness inside the supervisor's current PTY backend, bind it to the
    /// transport under the caller-supplied [`SessionId`], and track it.
    ///
    /// tmux is the fix for the raw-PTY hang: claude inside a real terminal emulator COMPLETES the
    /// unattended turn (writes its transcript, replies) where the raw PTY left it stuck. The harness
    /// command (MCP + the permission-bypass flags) is built exactly as the raw path; the tmux
    /// session is created with `cd <cwd> && exec <program> <args…>`. The transcript tailer is NOT
    /// spawned here (the caller supplies the store/sink/bell it needs).
    #[allow(clippy::too_many_arguments)]
    pub async fn launch_headed_pty(
        &self,
        session: &SessionId,
        kind: &HarnessId,
        agent_id: &str,
        name: Option<&str>,
        project: &str,
        client_key: &str,
        nexus_exe: &str,
        cwd: &str,
        size: PtySize,
        harness_args: &[String],
        events: Option<Arc<dyn EventSink>>,
        bell: Option<Bell>,
    ) -> Result<(), PtyError> {
        let harness = require_headed_harness(kind)?;
        let launch_label = name.unwrap_or(agent_id);
        let program = harness.program();
        let agent = harness.agent_token();
        let runtime = harness.headed_runtime_kind();
        // Pre-seed claude's workspace-trust for this fresh cwd. A native claude launched in an
        // untrusted dir sits at the "trust this folder?" dialog (which `--dangerously-skip-permissions`
        // does NOT dismiss) and never processes the daemon's injected turn — proven by the live e2e.
        // Seeding the trust entry up front lets the harness start processing immediately. Best-effort.
        if runtime == HeadedRuntimeKind::ClaudeNative {
            seed_claude_trust(cwd);
        }
        let command = harness
            .headed_pty_command(
                &ContractHarnessIdentity {
                    name: launch_label,
                    project,
                    client_key,
                    agent,
                },
                nexus_exe,
                harness_args,
            )
            .map_err(|e| PtyError::Spawn(format!("resolve {program} headed command: {e}")))?;
        let prog = command.program;
        let mut args = command.args;
        if prog != program {
            return Err(PtyError::Spawn(format!(
                "harness registry program mismatch: requested {program}, got {prog}"
            )));
        }
        if runtime == HeadedRuntimeKind::ClaudeNative {
            let state_dir = default_nexus_state_dir();
            let known_claude_session_id = claude_native_resume_id_from_tail(harness_args);
            let paths = self
                .prepare_claude_native_runtime(
                    session,
                    agent_id,
                    name,
                    project,
                    client_key,
                    nexus_exe,
                    cwd,
                    &state_dir,
                    known_claude_session_id,
                )
                .await?;
            append_claude_settings_arg(&mut args, &paths);
        }
        let mut env = nexus_runtime_env(session, agent_id, name, project, client_key, agent);
        append_claude_config_env(&mut env, runtime);
        let hermes_bridge = if runtime == HeadedRuntimeKind::HermesGateway {
            let events = events.ok_or_else(|| {
                PtyError::Spawn("hermes gateway launch requires an event sink".into())
            })?;
            let bell = bell
                .ok_or_else(|| PtyError::Spawn("hermes gateway launch requires a bell".into()))?;
            let profile = hermes_gateway_profile(session, launch_label);
            write_hermes_gateway_profile(&profile)
                .map_err(|e| PtyError::Spawn(format!("write Hermes gateway profile: {e}")))?;
            env.push((
                "HERMES_HOME".to_string(),
                profile.home.to_string_lossy().into_owned(),
            ));
            let bridge = HermesGatewayBridge::start(
                session.clone(),
                profile.bridge_socket.clone(),
                profile.bridge_token.clone(),
                events,
                bell,
            )
            .map_err(|e| PtyError::Spawn(format!("start Hermes gateway bridge: {e}")))?;
            env.push((
                "NEXUS_HERMES_BRIDGE_SOCKET".to_string(),
                bridge.endpoint().to_string(),
            ));
            env.push((
                "NEXUS_HERMES_BRIDGE_TOKEN".to_string(),
                profile.bridge_token.clone(),
            ));
            Some((bridge, profile.home.clone()))
        } else {
            None
        };
        let harness = self
            .tmux_backend
            .launch(session, &prog, &args, cwd, size, &env)?;
        if runtime == HeadedRuntimeKind::ClaudeNative {
            let tmux_socket = harness.socket_path().to_string_lossy().into_owned();
            self.persist_claude_tmux_state(&session, Some(&tmux_socket), harness.session_name())
                .await?;
            self.persist_claude_process_detail(session, harness.pane_pid(), None)
                .await?;
        }
        if let Some((_, hermes_home)) = hermes_bridge.as_ref() {
            self.persist_hermes_gateway_state(
                session,
                &hermes_home.join("state.db"),
                cwd,
                harness.pane_pid(),
                "tmux",
            )
            .await?;
        }
        self.bind_tmux_backend(session, harness.clone())?;
        if let Some((bridge, _)) = hermes_bridge {
            self.transport.bind(
                session.clone(),
                Arc::new(HermesGatewayHarness {
                    tmux: harness.clone(),
                    bridge,
                }) as Arc<dyn HarnessInput>,
            );
        } else if runtime == HeadedRuntimeKind::ClaudeNative {
            self.transport.bind(
                session.clone(),
                Arc::new(ClaudeNativeHarness {
                    input: harness.clone() as Arc<dyn HarnessInput>,
                    completion: self.claude_turn_completion(session),
                }),
            );
        } else {
            self.transport
                .bind(session.clone(), harness.clone() as Arc<dyn HarnessInput>);
        }
        Ok(())
    }

    /// Spawn the REAL headed harness on a raw daemon-owned PTY (the non-tmux backend).
    ///
    /// De-tmux (#22) production path: same registry command build, claude trust seeding, native
    /// runtime prep, and identity env as [`launch_headed_pty`] — but the PTY is owned by the
    /// daemon directly and viewed through the per-session terminal socket (`nexus attach` /
    /// `nexus pty-attach`, hostable by any terminal emulator, e.g. Alacritty). Caveat carried on
    /// the CLI: an UNATTENDED full-screen TUI on a raw PTY may hold an injected turn incomplete
    /// until a viewer attaches; tmux remains the opt-in for fully detached headed runs. Hermes
    /// launches here like every other harness because its gateway bridge is
    /// backend-agnostic; turn input flows through the bridge socket, not the PTY.
    ///
    /// [`launch_headed_pty`]: PtySupervisor::launch_headed_pty
    #[allow(clippy::too_many_arguments)]
    pub async fn launch_headed_raw_pty(
        &self,
        session: &SessionId,
        kind: &HarnessId,
        agent_id: &str,
        name: Option<&str>,
        project: &str,
        client_key: &str,
        nexus_exe: &str,
        cwd: &str,
        size: PtySize,
        harness_args: &[String],
        events: Option<Arc<dyn EventSink>>,
        bell: Option<Bell>,
    ) -> Result<(), PtyError> {
        let harness = require_headed_harness(kind)?;
        let launch_label = name.unwrap_or(agent_id);
        let program = harness.program();
        let agent = harness.agent_token();
        let runtime = harness.headed_runtime_kind();
        if runtime == HeadedRuntimeKind::ClaudeNative {
            seed_claude_trust(cwd);
        }
        let command = harness
            .headed_pty_command(
                &ContractHarnessIdentity {
                    name: launch_label,
                    project,
                    client_key,
                    agent,
                },
                nexus_exe,
                harness_args,
            )
            .map_err(|e| PtyError::Spawn(format!("resolve {program} headed command: {e}")))?;
        let prog = command.program;
        let mut args = command.args;
        if prog != program {
            return Err(PtyError::Spawn(format!(
                "harness registry program mismatch: requested {program}, got {prog}"
            )));
        }
        if runtime == HeadedRuntimeKind::ClaudeNative {
            let state_dir = default_nexus_state_dir();
            let known_claude_session_id = claude_native_resume_id_from_tail(harness_args);
            let paths = self
                .prepare_claude_native_runtime(
                    session,
                    agent_id,
                    name,
                    project,
                    client_key,
                    nexus_exe,
                    cwd,
                    &state_dir,
                    known_claude_session_id,
                )
                .await?;
            append_claude_settings_arg(&mut args, &paths);
        }
        let mut env = nexus_runtime_env(session, agent_id, name, project, client_key, agent);
        append_claude_config_env(&mut env, runtime);
        // Hermes gateway bridge — identical to the tmux arm's shape: profile + HERMES_HOME +
        // bridge socket/token env, and the bridge listener started BEFORE the process spawns
        // so Hermes can connect back immediately. It is backend-agnostic by design and launches
        // on the default raw PTY like every other harness.
        let hermes_bridge = if runtime == HeadedRuntimeKind::HermesGateway {
            let events = events.ok_or_else(|| {
                PtyError::Spawn("hermes gateway launch requires an event sink".into())
            })?;
            let bell = bell
                .ok_or_else(|| PtyError::Spawn("hermes gateway launch requires a bell".into()))?;
            let profile = hermes_gateway_profile(session, launch_label);
            write_hermes_gateway_profile(&profile)
                .map_err(|e| PtyError::Spawn(format!("write Hermes gateway profile: {e}")))?;
            env.push((
                "HERMES_HOME".to_string(),
                profile.home.to_string_lossy().into_owned(),
            ));
            let bridge = HermesGatewayBridge::start(
                session.clone(),
                profile.bridge_socket.clone(),
                profile.bridge_token.clone(),
                events,
                bell,
            )
            .map_err(|e| PtyError::Spawn(format!("start Hermes gateway bridge: {e}")))?;
            env.push((
                "NEXUS_HERMES_BRIDGE_SOCKET".to_string(),
                bridge.endpoint().to_string(),
            ));
            env.push((
                "NEXUS_HERMES_BRIDGE_TOKEN".to_string(),
                profile.bridge_token.clone(),
            ));
            Some((bridge, profile.home.clone()))
        } else {
            None
        };
        let mut cmd = CommandBuilder::new(&prog);
        scrub_inherited_nexus_identity_env(&mut cmd);
        for a in &args {
            cmd.arg(a);
        }
        cmd.cwd(cwd);
        for (k, v) in &env {
            cmd.env(k, v);
        }
        apply_headed_terminal_environment(&mut cmd);
        let pty = Arc::new(PtySession::spawn(cmd, size)?);
        #[cfg(windows)]
        let (input_backend, terminal_backend) = {
            let conpty = Arc::new(ConPtyBackend::from_session(pty.clone()));
            (
                conpty.clone() as Arc<dyn HarnessInput>,
                conpty as Arc<dyn TerminalBackend>,
            )
        };
        #[cfg(not(windows))]
        let (input_backend, terminal_backend) = (
            pty.clone() as Arc<dyn HarnessInput>,
            pty.clone() as Arc<dyn TerminalBackend>,
        );
        if runtime == HeadedRuntimeKind::ClaudeNative {
            self.persist_claude_process_detail(session, pty.child_pid(), None)
                .await?;
        }
        if let Some((_, hermes_home)) = hermes_bridge.as_ref() {
            self.persist_hermes_gateway_state(
                session,
                &hermes_home.join("state.db"),
                cwd,
                pty.child_pid(),
                "pty",
            )
            .await?;
        }
        let terminal_model = self.bind_pty_backend(
            session,
            Arc::new(RawPtyBackend {
                pty: pty.clone(),
                terminal: terminal_backend,
            }) as Arc<dyn PtyBackend>,
        )?;
        // Turn input for hermes goes through the gateway bridge, never the PTY; the raw PTY
        // still serves attach/viewer output like any other harness.
        if let Some((bridge, _)) = hermes_bridge {
            self.transport.bind(
                session.clone(),
                Arc::new(HermesRawPtyHarness {
                    pty: pty.clone(),
                    bridge,
                }) as Arc<dyn HarnessInput>,
            );
        } else if runtime == HeadedRuntimeKind::ClaudeNative {
            let completion = self.claude_turn_completion(session);
            self.transport.bind(
                session.clone(),
                Arc::new(ClaudeNativeHarness {
                    input: Arc::new(ClaudeRawPtyInput {
                        input: input_backend,
                        terminal: terminal_model,
                        completion: completion.clone(),
                    }),
                    completion,
                }),
            );
        } else {
            self.transport.bind(session.clone(), input_backend);
        }
        Ok(())
    }

    /// REVIVE an existing session whose harness died: re-launch the harness under the SAME
    /// [`SessionId`] (so the tmux session name, transcript dir, and the web `/agent/<name>:<sid>`
    /// view + its finalized `agent_session_*` history all carry over) and re-bind it to the
    /// transport. The caller passes the harness contract's revive tail, so native resume flags stay
    /// centralized in the adapter instead of being hard-coded in the supervisor. Best-effort kills
    /// any stale binding for the id first. The caller restarts the transcript tailer + the drain
    /// loop.
    #[allow(clippy::too_many_arguments)]
    pub async fn respawn_headed_pty(
        &self,
        session: &SessionId,
        kind: &HarnessId,
        name: &str,
        project: &str,
        client_key: &str,
        nexus_exe: &str,
        cwd: &str,
        size: PtySize,
        harness_args: &[String],
    ) -> Result<(), PtyError> {
        // Clear any lingering (dead) binding + tmux server for this id so the relaunch is clean.
        self.kill(session);
        let harness = require_headed_harness(kind)?;
        let program = harness.program();
        let agent = harness.agent_token();
        let runtime = harness.headed_runtime_kind();
        if runtime == HeadedRuntimeKind::ClaudeNative {
            seed_claude_trust(cwd);
        }
        let command = harness
            .headed_pty_command(
                &ContractHarnessIdentity {
                    name,
                    project,
                    client_key,
                    agent,
                },
                nexus_exe,
                harness_args,
            )
            .map_err(|e| PtyError::Spawn(format!("resolve {program} revive command: {e}")))?;
        let prog = command.program;
        let mut args = command.args;
        if prog != program {
            return Err(PtyError::Spawn(format!(
                "harness registry program mismatch: requested {program}, got {prog}"
            )));
        }
        if runtime == HeadedRuntimeKind::ClaudeNative {
            let state_dir = default_nexus_state_dir();
            let known_claude_session_id = claude_native_resume_id_from_tail(harness_args);
            let paths = self
                .prepare_claude_native_runtime(
                    session,
                    name,
                    Some(name),
                    project,
                    client_key,
                    nexus_exe,
                    cwd,
                    &state_dir,
                    known_claude_session_id,
                )
                .await?;
            append_claude_settings_arg(&mut args, &paths);
        }
        let mut env = nexus_runtime_env(session, name, Some(name), project, client_key, agent);
        append_claude_config_env(&mut env, runtime);
        let harness = self
            .tmux_backend
            .launch(session, &prog, &args, cwd, size, &env)?;
        if runtime == HeadedRuntimeKind::ClaudeNative {
            let tmux_socket = harness.socket_path().to_string_lossy().into_owned();
            self.persist_claude_tmux_state(session, Some(&tmux_socket), harness.session_name())
                .await?;
        }
        self.bind_tmux_backend(session, harness.clone())?;
        self.transport
            .bind(session.clone(), harness.clone() as Arc<dyn HarnessInput>);
        Ok(())
    }

    /// Prepare launch-local Claude native bridge files and persist sidecar runtime state.
    ///
    /// This is separated from tmux launch so tests can verify Claude's native bridge setup without
    /// spawning a real Claude process. The bridge remains harness-local: it writes settings/hooks
    /// under `state_dir` and never adds daemon RPC, WebSocket, Unix-socket, or gateway control APIs.
    pub async fn prepare_claude_native_runtime(
        &self,
        session: &SessionId,
        agent_id: &str,
        name: Option<&str>,
        project: &str,
        client_key: &str,
        nexus_exe: &str,
        cwd: &str,
        state_dir: &Path,
        known_claude_session_id: Option<&str>,
    ) -> Result<ClaudeNativeBridgePaths, PtyError> {
        let launch_label = name.unwrap_or(agent_id);
        nexus_harness_claude::skill::install_bus_skill(cwd);
        let paths = ClaudeNativeBridgePaths::new(state_dir, session);
        write_launch_settings_with_identity(
            &paths,
            launch_label,
            Some(session),
            project,
            nexus_exe,
            client_key,
            "claude",
        )
        .map_err(|e| PtyError::Io(format!("write claude native bridge settings: {e}")))?;
        if let Some(store) = &self.runtime_store {
            ClaudeRuntimeStateRepo::new(store)
                .upsert_launch(ClaudeRuntimeLaunch {
                    runtime_id: session.clone(),
                    bridge_dir: paths.bridge_dir.clone(),
                    claude_session_id: known_claude_session_id.map(str::to_string),
                    launch_cwd: PathBuf::from(cwd),
                    transcript_path: Some(paths.bridge_dir.join("transcript.jsonl")),
                    bridge_pid: None,
                    hook_pids_json: None,
                })
                .await
                .map_err(|e| PtyError::Io(format!("persist claude runtime state: {e}")))?;
        }
        Ok(paths)
    }

    /// Backend kind currently bound to a local headed runtime.
    pub fn pty_backend_kind(&self, id: &SessionId) -> Option<&'static str> {
        self.pty_backends
            .lock()
            .unwrap()
            .get(id)
            .map(|backend| backend.kind())
    }

    pub(crate) fn claude_turn_completion(&self, session: &SessionId) -> Arc<ClaudeTurnCompletion> {
        self.claude_completions
            .lock()
            .unwrap()
            .entry(session.clone())
            .or_insert_with(|| Arc::new(ClaudeTurnCompletion::default()))
            .clone()
    }

    /// Snapshot Claude's hook log before a cold respawn. Delivery waits for a newly appended
    /// SessionStart record, so terminal text cannot be injected into a resumed TUI before Claude
    /// has installed its prompt handler.
    pub(crate) fn arm_claude_startup_wait(&self, session: &SessionId) {
        let paths = ClaudeNativeBridgePaths::new(&default_nexus_state_dir(), session);
        let offset = std::fs::metadata(&paths.hook_log_path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        self.claude_startup_markers
            .lock()
            .unwrap()
            .insert(session.clone(), (paths.hook_log_path, offset));
    }

    pub(crate) fn wait_for_claude_startup(&self, session: &SessionId) -> Result<(), String> {
        use std::io::{Read, Seek, SeekFrom};

        let Some((hook_log, offset)) = self.claude_startup_markers.lock().unwrap().remove(session)
        else {
            return Ok(());
        };
        let deadline = Instant::now() + Duration::from_secs(75);
        loop {
            let started = std::fs::File::open(&hook_log)
                .ok()
                .and_then(|mut file| {
                    file.seek(SeekFrom::Start(offset)).ok()?;
                    let mut appended = String::new();
                    file.read_to_string(&mut appended).ok()?;
                    Some(appended.contains("\"event\":\"SessionStart\""))
                })
                .unwrap_or(false);
            if started {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(
                    "Claude native resume did not emit SessionStart before input-ready timeout"
                        .to_string(),
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    pub(crate) fn clear_claude_startup_wait(&self, session: &SessionId) {
        self.claude_startup_markers.lock().unwrap().remove(session);
    }

    /// Output stream for the bound local PTY backend.
    pub fn pty_output(&self, id: &SessionId) -> Option<broadcast::Receiver<Vec<u8>>> {
        self.pty_backends
            .lock()
            .unwrap()
            .get(id)
            .map(|backend| backend.output())
    }

    /// Local terminal socket endpoint for a PTY-backed runtime, if that runtime exposes one.
    ///
    /// Raw PTY and tmux-backed headed runtimes bind this side-channel. ACP/headless runtimes do not.
    pub fn terminal_endpoint(&self, id: &SessionId) -> Option<TerminalSocketEndpoint> {
        self.terminal.endpoint(id)
    }

    /// Whether `id` is backed by the headed OpenCode native-plugin bridge.
    pub fn has_opencode_plugin(&self, id: &SessionId) -> bool {
        self.opencode_plugins.lock().unwrap().contains_key(id)
    }

    /// Adopt an already-running daemon-owned tmux harness after a daemon restart.
    ///
    /// Launches use deterministic tmux socket/session names derived from the Nexus session id. When
    /// the daemon process restarts, the tmux process can still be alive but absent from this
    /// supervisor's in-memory maps; adoption reconstructs the [`TmuxHarness`] handle and re-binds it
    /// to [`PtyTransport`] without relaunching the agent.
    ///
    /// This is correct only for runtimes whose input is the tmux pane. Structured headed runtimes
    /// such as OpenCode's native-plugin bridge must relaunch their bridge instead of adopting tmux
    /// as the turn input, otherwise a daemon restart leaves delivery pointed at a stale pane while
    /// the completion callback belongs to the dead daemon.
    pub fn adopt_pty_backend(&self, session: &SessionId, cwd: &str) -> Result<(), PtyError> {
        if self.pty_backends.lock().unwrap().contains_key(session) {
            return Ok(());
        }
        let harness = self.tmux_backend.adopt(session, cwd)?;
        self.bind_tmux_backend(session, harness.clone())?;
        self.transport
            .bind(session.clone(), harness.clone() as Arc<dyn HarnessInput>);
        Ok(())
    }

    /// Kill whichever backend is bound to `id` (raw PTY, tmux, codex app-server, or OpenCode plugin)
    /// AND drop it
    /// from tracking. Best-effort; returns whether a backend was found and killed without error.
    /// This is the harness-side of `admin.delete` / `admin.remove --kill`: it runs `tmux
    /// kill-session` so the claude/codex process actually terminates (a removed agent must not
    /// keep running headless). For codex app-server sessions, the forwarder is aborted and the
    /// subprocess is sent SIGTERM via `CodexAppServer::shutdown`.
    pub fn kill(&self, id: &SessionId) -> bool {
        let mut killed = false;
        self.terminal.unbind(id);
        if let Some(backend) = self.pty_backends.lock().unwrap().remove(id) {
            killed |= backend.kill();
        }
        if let Some(bridge) = self.opencode_plugins.lock().unwrap().remove(id) {
            bridge.shutdown();
            killed = true;
        }
        killed | self.codex_bridge.kill(id)
    }

    /// Kill EVERY tracked harness backend (raw PTY + tmux + codex app-server + OpenCode plugin) and clear tracking.
    /// Called on graceful daemon shutdown so harness processes never outlive the daemon.
    /// Returns the count killed.
    pub fn kill_all(&self) -> usize {
        self.terminal.clear();
        let backends: Vec<_> = self
            .pty_backends
            .lock()
            .unwrap()
            .drain()
            .map(|(_, v)| v)
            .collect();
        let opencode_bridges: Vec<_> = self
            .opencode_plugins
            .lock()
            .unwrap()
            .drain()
            .map(|(_, v)| v)
            .collect();
        let mut n = 0;
        for backend in backends {
            if backend.kill() {
                n += 1;
            }
        }
        for bridge in opencode_bridges {
            bridge.shutdown();
            n += 1;
        }
        n += self.codex_bridge.kill_all();
        n
    }

    /// A clone of the bound [`PtyTransport`] so `AppState::wire_pty` can install it in the
    /// `RoutingTurnExec`.
    pub fn transport(&self) -> PtyTransport {
        self.transport.clone()
    }

    /// A clone of the bound [`CodexAppServerTransport`] so `AppState::wire_pty` can install it
    /// as the codex dispatch layer in `RoutingTurnExec`.
    pub fn codex_transport(&self) -> CodexAppServerTransport {
        self.codex_bridge.transport()
    }

    /// Launch a headed codex session: start the codex app-server bridge, then attach the human TUI
    /// via `codex --remote unix://<sock>`. The TUI viewer defaults to a raw daemon-owned PTY;
    /// tmux is an explicit legacy opt-in.
    ///
    /// Binds the caller-supplied [`SessionId`]. Does NOT call `spawn_pty_reply_reader`.
    /// Codex-specific thread discovery and forwarding are delegated to [`CodexBridge`].
    ///
    /// `codex_exe` defaults to `"codex"` in production; tests supply the fake binary path.
    #[allow(clippy::too_many_arguments)]
    pub async fn launch_codex_appserver(
        &self,
        session: &SessionId,
        agent_id: &str,
        name: Option<&str>,
        project: &str,
        client_key: &str,
        nexus_exe: &str,
        cwd: &str,
        size: PtySize,
        events: Arc<dyn EventSink>,
        codex_exe: &str,
        model: Option<String>,
        state_dir: &str,
        known_thread_id: Option<String>,
        resume_codex_homes: Vec<PathBuf>,
        harness_args: &[String],
        on_thread_discovered: Option<ThreadDiscovered>,
        viewer_backend: &str,
    ) -> Result<(), PtyError> {
        self.start_codex_appserver_session(
            session,
            agent_id,
            name,
            project,
            client_key,
            nexus_exe,
            cwd,
            size,
            events,
            codex_exe,
            model,
            state_dir,
            known_thread_id,
            resume_codex_homes,
            harness_args,
            on_thread_discovered,
            false,
            viewer_backend,
        )
        .await
    }

    /// REVIVE a headed codex app-server session under the SAME Nexus [`SessionId`]. A live
    /// deterministic app-server socket is adopted; only a dead or missing socket is replaced. The
    /// stored Codex thread id, when present, binds a new completion forwarder and launches the human
    /// TUI with `codex resume --remote ... <thread>`.
    #[allow(clippy::too_many_arguments)]
    pub async fn respawn_codex_appserver(
        &self,
        session: &SessionId,
        agent_id: &str,
        name: &str,
        project: &str,
        client_key: &str,
        nexus_exe: &str,
        cwd: &str,
        size: PtySize,
        events: Arc<dyn EventSink>,
        codex_exe: &str,
        model: Option<String>,
        state_dir: &str,
        known_thread_id: Option<String>,
        resume_codex_homes: Vec<PathBuf>,
        on_thread_discovered: Option<ThreadDiscovered>,
        viewer_backend: &str,
    ) -> Result<(), PtyError> {
        if !self.codex_bridge.has(session) {
            self.kill(session);
        }
        self.start_codex_appserver_session(
            session,
            agent_id,
            Some(name),
            project,
            client_key,
            nexus_exe,
            cwd,
            size,
            events,
            codex_exe,
            model,
            state_dir,
            known_thread_id,
            resume_codex_homes,
            &[],
            on_thread_discovered,
            false,
            viewer_backend,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_codex_appserver_session(
        &self,
        session: &SessionId,
        agent_id: &str,
        name: Option<&str>,
        project: &str,
        client_key: &str,
        nexus_exe: &str,
        cwd: &str,
        size: PtySize,
        events: Arc<dyn EventSink>,
        codex_exe: &str,
        model: Option<String>,
        state_dir: &str,
        known_thread_id: Option<String>,
        resume_codex_homes: Vec<PathBuf>,
        harness_args: &[String],
        on_thread_discovered: Option<ThreadDiscovered>,
        force_fresh_app_server: bool,
        viewer_backend: &str,
    ) -> Result<(), PtyError> {
        let launch_label = name.unwrap_or(agent_id);
        // The daemon has already reserved and registered this runtime. Install the model-facing
        // bus skill, but do not add a second SessionStart registration hook to the attached TUI:
        // Codex presents newly written project hooks for interactive review before rendering the
        // session, which can strand an otherwise-live headed agent behind a modal.
        nexus_harness_codex::skill::install_skill_only(cwd);
        let codex_exe = resolve_codex_executable(codex_exe).map_err(|error| {
            PtyError::Spawn(format!("resolve native Codex executable: {error}"))
        })?;

        // Per-session directory under the daemon's state root: holds codex-home/ + codex.sock.
        let session_dir = std::path::PathBuf::from(state_dir)
            .join("codex-sessions")
            .join(session.0.as_str());
        std::fs::create_dir_all(&session_dir)
            .map_err(|e| PtyError::Io(format!("create session_dir {session_dir:?}: {e}")))?;

        let opts = SupervisorOpts {
            codex_exe: codex_exe.clone(),
            session_dir,
            codex_home: None,
            model,
            bus_mcp: Some(codex_appserver_bus_mcp(
                nexus_exe,
                launch_label,
                project,
                client_key,
            )),
            cwd: Some(std::path::PathBuf::from(cwd)),
            // Attribute any `nexus` CLI the agent runs from its shell to the AGENT, not the
            // operator who launched the daemon. The client key is the stored daemon-owned runtime
            // secret, not ambient shell state.
            env: codex_appserver_env(session, agent_id, name, project, client_key, nexus_exe),
        };

        let sock_path = self
            .codex_bridge
            .launch_with_options(
                session.clone(),
                opts,
                events,
                self.codex_bridge_options(
                    launch_label,
                    known_thread_id.clone(),
                    resume_codex_homes,
                    on_thread_discovered,
                    force_fresh_app_server,
                ),
            )
            .await
            .map_err(|e| PtyError::Spawn(format!("codex app-server launch failed: {e}")))?;
        let tui_thread_id = self
            .codex_bridge
            .transport()
            .bound_thread_id(session)
            .or(known_thread_id.clone());

        let session_name = self.tmux_backend.session_name(session);
        if viewer_backend == "tmux" && force_fresh_app_server {
            self.tmux_backend.kill_if_exists(session);
        }
        if viewer_backend == "tmux" && self.tmux_backend.session_exists(session) {
            let harness = self.tmux_backend.adopt(session, cwd)?;
            let tmux_socket = self.tmux_backend.socket_path_for_session(session);
            self.bind_tmux_backend(session, harness)?;
            self.persist_codex_tmux_state(
                session,
                Some(&tmux_socket.to_string_lossy()),
                &session_name,
            )
            .await?;
        } else {
            // Launch the human TUI against the same local app-server endpoint.
            let sock_str = sock_path.to_string_lossy();
            let cmd = match tui_thread_id.as_deref() {
                Some(thread_id) => remote_resume_command_with_executable(
                    &codex_exe,
                    &sock_str,
                    thread_id,
                    Some(cwd),
                    harness_args,
                ),
                None => {
                    remote_command_with_executable(&codex_exe, &sock_str, Some(cwd), harness_args)
                }
            };
            let argv: Vec<String> = cmd
                .get_argv()
                .iter()
                .map(|s| s.to_string_lossy().into_owned())
                .collect();
            let prog = argv.first().cloned().unwrap_or_else(|| "codex".to_string());
            let args: Vec<String> = argv.into_iter().skip(1).collect();
            let env = codex_appserver_env(session, agent_id, name, project, client_key, nexus_exe);
            if viewer_backend == "tmux" {
                let harness = self
                    .tmux_backend
                    .launch(session, &prog, &args, cwd, size, &env)?;
                self.persist_codex_tmux_state(
                    session,
                    Some(&harness.socket_path().to_string_lossy()),
                    harness.session_name(),
                )
                .await?;
                self.bind_tmux_backend(session, harness)?;
            } else {
                self.clear_codex_tmux_state(session).await?;
                self.launch_codex_remote_raw_pty(session, &prog, &args, cwd, size, &env)?;
            }
        }

        Ok(())
    }

    #[doc(hidden)]
    pub fn launch_codex_remote_raw_pty(
        &self,
        session: &SessionId,
        prog: &str,
        args: &[String],
        cwd: &str,
        size: PtySize,
        env: &[(String, String)],
    ) -> Result<(), PtyError> {
        let mut cmd = CommandBuilder::new(prog);
        scrub_inherited_nexus_identity_env(&mut cmd);
        for arg in args {
            cmd.arg(arg);
        }
        cmd.cwd(cwd);
        for (k, v) in env {
            cmd.env(k, v);
        }
        apply_headed_terminal_environment(&mut cmd);
        let pty = Arc::new(PtySession::spawn(cmd, size)?);
        #[cfg(windows)]
        let terminal_backend =
            Arc::new(ConPtyBackend::from_session(pty.clone())) as Arc<dyn TerminalBackend>;
        #[cfg(not(windows))]
        let terminal_backend = pty.clone() as Arc<dyn TerminalBackend>;
        self.bind_pty_backend(
            session,
            Arc::new(RawPtyBackend {
                pty: pty.clone(),
                terminal: terminal_backend,
            }) as Arc<dyn PtyBackend>,
        )?;
        Ok(())
    }

    async fn persist_codex_tmux_state(
        &self,
        session: &SessionId,
        tmux_socket: Option<&str>,
        tmux_session: &str,
    ) -> Result<(), PtyError> {
        let Some(store) = &self.runtime_store else {
            return Ok(());
        };
        nexus_harness_codex::storage::CodexRuntimeStateRepo::new(store)
            .set_tmux(session, tmux_socket, tmux_session)
            .await
            .map_err(|e| PtyError::Io(format!("persist codex tmux state: {e}")))
    }

    async fn clear_codex_tmux_state(&self, session: &SessionId) -> Result<(), PtyError> {
        let Some(store) = &self.runtime_store else {
            return Ok(());
        };
        nexus_harness_codex::storage::CodexRuntimeStateRepo::new(store)
            .clear_tmux(session)
            .await
            .map_err(|e| PtyError::Io(format!("clear codex tmux state: {e}")))
    }

    async fn persist_claude_tmux_state(
        &self,
        session: &SessionId,
        tmux_socket: Option<&str>,
        tmux_session: &str,
    ) -> Result<(), PtyError> {
        let Some(store) = &self.runtime_store else {
            return Ok(());
        };
        ClaudeRuntimeStateRepo::new(store)
            .set_tmux(session, tmux_socket, tmux_session)
            .await
            .map_err(|e| PtyError::Io(format!("persist claude tmux state: {e}")))
    }

    async fn persist_claude_process_detail(
        &self,
        session: &SessionId,
        bridge_pid: Option<u32>,
        hook_pids_json: Option<&str>,
    ) -> Result<(), PtyError> {
        let Some(store) = &self.runtime_store else {
            return Ok(());
        };
        ClaudeRuntimeStateRepo::new(store)
            .set_process_detail(session, bridge_pid, hook_pids_json)
            .await
            .map_err(|e| PtyError::Io(format!("persist claude process detail: {e}")))
    }

    async fn persist_hermes_gateway_state(
        &self,
        session: &SessionId,
        hermes_db_path: &Path,
        cwd: &str,
        acp_child_pid: Option<u32>,
        viewer_backend: &str,
    ) -> Result<(), PtyError> {
        let Some(store) = &self.runtime_store else {
            return Ok(());
        };
        HermesRuntimeStateRepo::new(store)
            .upsert_launch(HermesRuntimeLaunch {
                runtime_id: session.clone(),
                hermes_db_path: hermes_db_path.to_path_buf(),
                hermes_session_id: None,
                launch_cwd: PathBuf::from(cwd),
                acp_child_pid,
                viewer_backend: viewer_backend.to_string(),
            })
            .await
            .map_err(|e| PtyError::Io(format!("persist hermes gateway state: {e}")))
    }

    /// Whether `id` has a live codex app-server handle (a headed codex session). Used by tests
    /// and health checks; the transport binding is the routing signal (`codex_transport`).
    pub fn has_codex_appserver(&self, id: &SessionId) -> bool {
        self.codex_bridge.has(id)
    }

    /// Persist the process ledger for the currently bound local PTY/tmux backend.
    pub async fn stamp_bound_process_ledger(&self, session: &SessionId) -> Result<(), PtyError> {
        let Some(entry) = self
            .pty_backends
            .lock()
            .unwrap()
            .get(session)
            .and_then(|backend| backend.runtime_process_ids())
        else {
            return Ok(());
        };
        self.set_runtime_process_ledger(session, entry).await
    }

    /// Persist the process ledger for a headed Codex app-server subprocess.
    pub async fn stamp_codex_appserver_process_ledger(
        &self,
        session: &SessionId,
    ) -> Result<(), PtyError> {
        let Some(entry) = self.codex_bridge.process_ledger(session) else {
            return Ok(());
        };
        self.set_runtime_process_ledger(session, entry).await
    }

    async fn set_runtime_process_ledger(
        &self,
        session: &SessionId,
        entry: RuntimeProcessIds,
    ) -> Result<(), PtyError> {
        let Some(store) = &self.runtime_store else {
            return Ok(());
        };
        nexus_store::repos::AgentRuntimes::new(store)
            .set_process_ids(&session.0, entry)
            .await
            .map_err(|e| PtyError::Io(format!("persist runtime process ledger: {e}")))
    }

    /// Launch a headed OpenCode session with a native in-process plugin:
    ///
    /// 1. start a Nexus loopback bridge and bind its [`HarnessInput`] under `session`;
    /// 2. generate a launch-local OpenCode plugin + serve shim under `state_dir`;
    /// 3. run `node <serve-shim> [opencode-native-args...]` in the requested raw PTY or tmux
    ///    viewer backend.
    ///
    /// The shim starts `opencode serve` with the plugin loaded, waits for the plugin-owned session
    /// id, then opens a foreground `opencode attach` TUI to that exact session. Delivery never types
    /// into tmux; the plugin drives `prompt_async` and reports `session.idle` back to the bridge.
    /// Fresh sessions get a launch-local OpenCode DB. Operator-supplied explicit `-s` / `--session`
    /// resumes use the native OpenCode store so the requested existing session can be resolved;
    /// daemon revives set `resume_isolated_store` so the stored session id is resolved in the same
    /// per-Nexus-session DB the first launch created.
    #[allow(clippy::too_many_arguments)]
    pub async fn launch_opencode_plugin(
        &self,
        session: &SessionId,
        agent_id: &str,
        name: Option<&str>,
        project: &str,
        client_key: &str,
        cwd: &str,
        size: PtySize,
        events: Arc<dyn EventSink>,
        state_dir: &str,
        harness_args: &[String],
        resume_isolated_store: bool,
        viewer_backend: &str,
    ) -> Result<(), PtyError> {
        let backend_kind = opencode_viewer_backend_kind(viewer_backend)?;
        let opencode_bin = resolve_opencode_executable(
            std::env::var("NEXUS_OPENCODE_BIN").ok().as_deref(),
            std::env::var("PATH").ok().as_deref(),
        )
        .map_err(PtyError::Spawn)?;
        let bridge = Arc::new(
            OpenCodePluginBridge::start(
                session.clone(),
                events,
                OpenCodePluginBridgeOptions::default(),
            )
            .await
            .map_err(|e| PtyError::Spawn(format!("opencode plugin bridge failed: {e}")))?,
        );
        let files = write_opencode_plugin_files(&PathBuf::from(state_dir), session)
            .map_err(|e| PtyError::Io(format!("write opencode plugin files: {e}")))?;
        let _ = std::fs::remove_file(&files.ready_path);

        self.tmux_backend.kill_if_exists(session);
        let mut args = vec![files.serve_path.to_string_lossy().into_owned()];
        args.extend(harness_args.iter().cloned());
        let mut env = nexus_runtime_env(session, agent_id, name, project, client_key, "opencode");
        env.push((
            "NEXUS_OPENCODE_BRIDGE_URL".to_string(),
            bridge.endpoint().base_url().to_string(),
        ));
        env.push((
            "NEXUS_OPENCODE_BRIDGE_TOKEN".to_string(),
            bridge.endpoint().token().to_string(),
        ));
        env.push((
            "NEXUS_OPENCODE_PLUGIN_PATH".to_string(),
            files.plugin_path.to_string_lossy().into_owned(),
        ));
        env.push((
            "NEXUS_OPENCODE_HOME".to_string(),
            files.data_dir.to_string_lossy().into_owned(),
        ));
        env.push((
            "NEXUS_OPENCODE_READY_PATH".to_string(),
            files.ready_path.to_string_lossy().into_owned(),
        ));
        env.push(("NEXUS_OPENCODE_BIN".to_string(), opencode_bin));
        if resume_isolated_store {
            env.push((
                "NEXUS_OPENCODE_RESUME_ISOLATED".to_string(),
                "1".to_string(),
            ));
        }

        let node = std::env::var("NEXUS_NODE_BIN").unwrap_or_else(|_| "node".to_string());
        enum ViewerRuntime {
            Raw(Arc<PtySession>),
            Tmux(Arc<TmuxHarness>),
        }

        let runtime = if backend_kind == "raw" {
            let mut cmd = CommandBuilder::new(&node);
            scrub_inherited_nexus_identity_env(&mut cmd);
            for arg in &args {
                cmd.arg(arg);
            }
            cmd.cwd(cwd);
            for (key, value) in &env {
                cmd.env(key, value);
            }
            apply_headed_terminal_environment(&mut cmd);
            let pty = Arc::new(PtySession::spawn(cmd, size)?);
            if let Err(err) = wait_for_opencode_plugin_ready_raw(
                pty.as_ref(),
                &files.ready_path,
                OPENCODE_PLUGIN_READY_TIMEOUT,
            )
            .await
            {
                bridge.shutdown();
                let _ = pty.kill();
                return Err(PtyError::Spawn(err));
            }
            ViewerRuntime::Raw(pty)
        } else {
            let harness = self
                .tmux_backend
                .launch(session, &node, &args, cwd, size, &env)?;
            if let Err(err) = wait_for_opencode_plugin_ready(
                harness.as_ref(),
                &files.ready_path,
                OPENCODE_PLUGIN_READY_TIMEOUT,
            )
            .await
            {
                bridge.shutdown();
                let _ = harness.kill();
                return Err(PtyError::Spawn(err));
            }
            ViewerRuntime::Tmux(harness)
        };
        let opencode_ready = read_opencode_plugin_ready(&files.ready_path)?;
        self.persist_opencode_plugin_state(
            session,
            &files.data_dir.join("opencode.db"),
            opencode_ready.session_id.as_deref(),
            cwd,
            opencode_ready.pid,
            viewer_backend,
        )
        .await?;

        let runtime_input: Arc<dyn HarnessInput> = match runtime {
            ViewerRuntime::Raw(pty) => {
                #[cfg(windows)]
                let (input_backend, terminal_backend) = {
                    let conpty = Arc::new(ConPtyBackend::from_session(pty.clone()));
                    (
                        conpty.clone() as Arc<dyn HarnessInput>,
                        conpty as Arc<dyn TerminalBackend>,
                    )
                };
                #[cfg(not(windows))]
                let (input_backend, terminal_backend) = (
                    pty.clone() as Arc<dyn HarnessInput>,
                    pty.clone() as Arc<dyn TerminalBackend>,
                );
                self.bind_pty_backend_with_query_responder(
                    session,
                    Arc::new(RawPtyBackend {
                        pty,
                        terminal: terminal_backend,
                    }) as Arc<dyn PtyBackend>,
                    raw_pty_query_responder(HeadedRuntimeKind::OpenCodePlugin),
                )?;
                input_backend
            }
            ViewerRuntime::Tmux(harness) => {
                self.bind_tmux_backend(session, harness.clone())?;
                harness as Arc<dyn HarnessInput>
            }
        };
        self.transport.bind(
            session.clone(),
            Arc::new(OpenCodeHeadedHarness::new(
                bridge.input() as Arc<dyn HarnessInput>,
                runtime_input,
            )),
        );
        self.opencode_plugins
            .lock()
            .unwrap()
            .insert(session.clone(), bridge);
        Ok(())
    }

    async fn persist_opencode_plugin_state(
        &self,
        session: &SessionId,
        opencode_db_path: &Path,
        opencode_session_id: Option<&str>,
        cwd: &str,
        plugin_bridge_pid: Option<u32>,
        viewer_backend: &str,
    ) -> Result<(), PtyError> {
        let Some(store) = &self.runtime_store else {
            return Ok(());
        };
        OpenCodeRuntimeStateRepo::new(store)
            .upsert_launch(OpenCodeRuntimeLaunch {
                runtime_id: session.clone(),
                opencode_db_path: opencode_db_path.to_path_buf(),
                opencode_session_id: opencode_session_id.map(str::to_string),
                launch_cwd: PathBuf::from(cwd),
                plugin_bridge_pid,
                viewer_backend: viewer_backend.to_string(),
            })
            .await
            .map_err(|e| PtyError::Io(format!("persist opencode plugin state: {e}")))
    }

    fn bind_terminal_backend(
        &self,
        session: &SessionId,
        backend: Arc<dyn TerminalBackend>,
        respond_to_queries: bool,
    ) -> Result<Arc<ScreenModelBackend>, PtyError> {
        // Every bound terminal gets a daemon-side screen model, so a fresh attach starts
        // from a coherent snapshot (authoritative size + full repaint) instead of a raw
        // mid-stream byte tap (the historical mid-life-attach garble).
        let model = ScreenModelBackend::wrap(backend);
        // Raw ptys have no terminal emulator behind them — the daemon answers the TUI's
        // terminal queries itself (the un-tmux'd replacement for "tmux answers them";
        // enabling this under tmux would double-reply).
        if respond_to_queries {
            model.spawn_query_responder();
        }
        self.terminal
            .bind(session.clone(), model.clone())
            .map_err(PtyError::Io)?;
        Ok(model)
    }

    fn bind_pty_backend(
        &self,
        session: &SessionId,
        backend: Arc<dyn PtyBackend>,
    ) -> Result<Arc<ScreenModelBackend>, PtyError> {
        let respond_to_queries = backend.kind() == "raw";
        self.bind_pty_backend_with_query_responder(session, backend, respond_to_queries)
    }

    fn bind_pty_backend_with_query_responder(
        &self,
        session: &SessionId,
        backend: Arc<dyn PtyBackend>,
        respond_to_queries: bool,
    ) -> Result<Arc<ScreenModelBackend>, PtyError> {
        let model =
            self.bind_terminal_backend(session, backend.terminal_backend(), respond_to_queries)?;
        self.pty_backends
            .lock()
            .unwrap()
            .insert(session.clone(), backend);
        Ok(model)
    }

    fn bind_tmux_backend(
        &self,
        session: &SessionId,
        harness: Arc<TmuxHarness>,
    ) -> Result<(), PtyError> {
        self.bind_pty_backend(
            session,
            Arc::new(TmuxPtyBackend {
                harness: harness.clone(),
            }) as Arc<dyn PtyBackend>,
        )?;
        Ok(())
    }
}

async fn wait_for_opencode_plugin_ready(
    harness: &TmuxHarness,
    ready_path: &Path,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        if opencode_plugin_ready_is_complete(ready_path) {
            return Ok(());
        }
        if !harness.has_session() {
            let pane = harness.capture_pane();
            let detail = if pane.trim().is_empty() {
                "tmux pane exited before OpenCode plugin reported ready".to_string()
            } else {
                format!(
                    "tmux pane exited before OpenCode plugin reported ready; last pane output: {}",
                    pane.trim()
                )
            };
            return Err(detail);
        }
        if Instant::now() >= deadline {
            let pane = harness.capture_pane();
            let detail = if pane.trim().is_empty() {
                format!(
                    "OpenCode plugin did not report ready within {}s",
                    timeout.as_secs()
                )
            } else {
                format!(
                    "OpenCode plugin did not report ready within {}s; last pane output: {}",
                    timeout.as_secs(),
                    pane.trim()
                )
            };
            return Err(detail);
        }
        tokio::time::sleep(OPENCODE_PLUGIN_READY_POLL).await;
    }
}

async fn wait_for_opencode_plugin_ready_raw(
    pty: &PtySession,
    ready_path: &Path,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        if opencode_plugin_ready_is_complete(ready_path) {
            return Ok(());
        }
        if !pty.child_alive() {
            return Err("raw PTY exited before OpenCode plugin reported ready".to_string());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "OpenCode plugin did not report ready within {}s",
                timeout.as_secs()
            ));
        }
        tokio::time::sleep(OPENCODE_PLUGIN_READY_POLL).await;
    }
}

/// Build the `nexus-bus` MCP config for headed Codex app-server launches. The MCP helper is part of
/// the launched agent, so it must resume the same daemon-owned session key as the agent shell.
fn codex_appserver_bus_mcp(nexus_exe: &str, name: &str, project: &str, client_key: &str) -> BusMcp {
    BusMcp {
        command: nexus_exe.to_string(),
        args: vec![
            "mcp".to_string(),
            "--as".to_string(),
            name.to_string(),
            "--project".to_string(),
            project.to_string(),
            "--client-key".to_string(),
            client_key.to_string(),
            "--agent".to_string(),
            "codex".to_string(),
        ],
    }
}

/// Environment inherited by the Codex app-server process and its shell tool calls. Keep this in
/// sync with `codex_appserver_bus_mcp`: both represent the same agent identity.
fn codex_appserver_env(
    session: &SessionId,
    agent_id: &str,
    name: Option<&str>,
    project: &str,
    client_key: &str,
    nexus_exe: &str,
) -> Vec<(String, String)> {
    let env = nexus_runtime_env(session, agent_id, name, project, client_key, "codex");
    with_nexus_cli_path(env, nexus_exe)
}

fn nexus_runtime_env(
    session: &SessionId,
    agent_id: &str,
    name: Option<&str>,
    project: &str,
    client_key: &str,
    agent: &str,
) -> Vec<(String, String)> {
    let mut env = vec![
        ("NEXUS_SESSION_ID".to_string(), session.0.clone()),
        ("NEXUS_AGENT_ID".to_string(), agent_id.to_string()),
        ("NEXUS_CLIENT_KEY".to_string(), client_key.to_string()),
        ("NEXUS_PROJECT".to_string(), project.to_string()),
        ("NEXUS_AGENT".to_string(), agent.to_string()),
        // Agents are agents, never the operator: blank any inherited tier override so a
        // daemon environment can never elevate a spawned agent (B21 — admin is assigned via
        // `admin grant-tier`, never inherited or defaulted).
        ("NEXUS_TIER".to_string(), String::new()),
    ];
    if let Some(name) = name {
        env.push(("NEXUS_NAME".to_string(), name.to_string()));
    }
    // The tmux launcher first scrubs every inherited `NEXUS_*` variable so stale operator
    // identity cannot leak into a harness. Restore only daemon IPC discovery and the operator's
    // explicit Tokio runtime bound; child processes never receive direct durable-store
    // coordinates.
    for key in ["NEXUS_HOME", "NEXUS_NO_AUTOSTART", "TOKIO_WORKER_THREADS"] {
        if let Ok(value) = std::env::var(key) {
            if !value.is_empty() {
                env.push((key.to_string(), value));
            }
        }
    }
    env
}

fn scrub_inherited_nexus_identity_env(cmd: &mut CommandBuilder) {
    for key in [
        "NEXUS_NAME",
        "NEXUS_CLIENT_KEY",
        "NEXUS_PROJECT",
        "NEXUS_AGENT",
        "NEXUS_AGENT_ID",
        "NEXUS_SESSION_ID",
        "NEXUS_RUNTIME_ID",
        "NEXUS_TIER",
        "NEXUS_KIND",
        "CLAUDE_CODE_SESSION_ID",
    ] {
        cmd.env_remove(key);
    }
}

fn with_nexus_cli_path(mut env: Vec<(String, String)>, nexus_exe: &str) -> Vec<(String, String)> {
    // Codex shell snapshots can intentionally replace PATH with a conservative login-shell value.
    // Keep the executable as an explicit launch capability so the bus skill never depends on PATH.
    env.push(("NEXUS_CLI".to_string(), nexus_exe.to_string()));
    let Some(exe_dir) = Path::new(nexus_exe)
        .parent()
        .and_then(|path| path.to_str())
        .filter(|path| !path.is_empty())
    else {
        return env;
    };
    let path = match std::env::var("PATH") {
        Ok(existing) if !existing.is_empty() => format!("{exe_dir}:{existing}"),
        _ => exe_dir.to_string(),
    };
    env.push(("PATH".to_string(), path));
    env
}

fn hermes_gateway_profile(session: &SessionId, name: &str) -> HermesGatewayProfile {
    let safe_name: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let base = std::env::temp_dir().join(format!("nexus-hermes-gateway-{}-{safe_name}", session.0));
    HermesGatewayProfile {
        home: base.join("home"),
        source_home: default_hermes_source_home(),
        bridge_socket: base.join("bridge.sock"),
        bridge_token: format!("nexus-hermes-{}", session.0),
        nexus_name: name.to_string(),
        session_id: session.clone(),
    }
}

fn default_hermes_source_home() -> PathBuf {
    if let Ok(home) = std::env::var("NEXUS_HERMES_SOURCE_HOME") {
        if !home.is_empty() {
            return PathBuf::from(home);
        }
    }
    if let Ok(home) = std::env::var("HERMES_HOME") {
        if !home.is_empty() {
            return PathBuf::from(home);
        }
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
    PathBuf::from(home).join(".hermes")
}

fn append_claude_settings_arg(args: &mut Vec<String>, paths: &ClaudeNativeBridgePaths) {
    args.push("--settings".to_string());
    args.push(paths.settings_path.to_string_lossy().into_owned());
}

fn append_claude_config_env(env: &mut Vec<(String, String)>, runtime: HeadedRuntimeKind) {
    let config_dir = std::env::var("CLAUDE_CONFIG_DIR").ok();
    append_claude_config_env_from(env, runtime, config_dir.as_deref());
}

fn append_claude_config_env_from(
    env: &mut Vec<(String, String)>,
    runtime: HeadedRuntimeKind,
    config_dir: Option<&str>,
) {
    if runtime != HeadedRuntimeKind::ClaudeNative {
        return;
    }
    if let Some(config_dir) = config_dir.filter(|value| !value.trim().is_empty()) {
        env.push(("CLAUDE_CONFIG_DIR".to_string(), config_dir.to_string()));
    }
}

fn default_nexus_state_dir() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
    PathBuf::from(home).join(".nexus")
}

fn tmux_socket_path_for_session_name(session_name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("nexus-tmux-{session_name}.sock"))
}

fn tmux_session_exists(socket: &Path, session_name: &str) -> bool {
    std::process::Command::new("tmux")
        .args([
            "-S",
            &socket.to_string_lossy(),
            "has-session",
            "-t",
            session_name,
        ])
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn kill_tmux_session_if_exists(session_name: &str) {
    let socket = tmux_socket_path_for_session_name(session_name);
    if !tmux_session_exists(&socket, session_name) {
        return;
    }
    let _ = std::process::Command::new("tmux")
        .args([
            "-S",
            &socket.to_string_lossy(),
            "kill-session",
            "-t",
            session_name,
        ])
        .status();
}

struct OpenCodePluginReady {
    session_id: Option<String>,
    pid: Option<u32>,
}

fn read_opencode_plugin_ready(ready_path: &Path) -> Result<OpenCodePluginReady, PtyError> {
    let raw = std::fs::read_to_string(ready_path)
        .map_err(|e| PtyError::Io(format!("read opencode ready file {ready_path:?}: {e}")))?;
    let ready: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|e| PtyError::Io(format!("parse opencode ready file {ready_path:?}: {e}")))?;
    let session_id = ready
        .get("sessionId")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let pid = ready
        .get("pid")
        .and_then(|v| v.as_u64())
        .and_then(|pid| u32::try_from(pid).ok());
    Ok(OpenCodePluginReady { session_id, pid })
}

fn opencode_plugin_ready_is_complete(ready_path: &Path) -> bool {
    read_opencode_plugin_ready(ready_path).is_ok()
}

/// Merge a workspace-trust entry for `cwd` into a parsed `~/.claude.json` value, preserving every
/// other key (the user's other projects + top-level state). Pure, so it's unit-tested.
fn merge_claude_trust(mut root: serde_json::Value, cwd: &str) -> serde_json::Value {
    if !root.is_object() {
        root = serde_json::json!({});
    }
    let obj = root.as_object_mut().unwrap();
    let projects = obj
        .entry("projects")
        .or_insert_with(|| serde_json::json!({}));
    if !projects.is_object() {
        *projects = serde_json::json!({});
    }
    let entry = projects
        .as_object_mut()
        .unwrap()
        .entry(cwd.to_string())
        .or_insert_with(|| serde_json::json!({}));
    if let Some(e) = entry.as_object_mut() {
        e.insert("hasTrustDialogAccepted".into(), serde_json::json!(true));
        e.insert(
            "hasCompletedProjectOnboarding".into(),
            serde_json::json!(true),
        );
    }
    root
}

fn claude_user_config_path(config_dir: Option<&str>, home: Option<&str>) -> PathBuf {
    if let Some(config_dir) = config_dir.filter(|value| !value.trim().is_empty()) {
        return PathBuf::from(config_dir).join(".claude.json");
    }
    PathBuf::from(
        home.filter(|value| !value.trim().is_empty())
            .unwrap_or("/tmp"),
    )
    .join(".claude.json")
}

/// Best-effort: ensure claude trusts `cwd` (skip the "trust this folder?" dialog) by merging a
/// trust entry into Claude's active user config. `CLAUDE_CONFIG_DIR` owns that config when set;
/// otherwise it is `~/.claude.json`. Reads → [`merge_claude_trust`] → atomic write (tmp+rename).
/// A process-wide lock serializes the daemon's own concurrent launches; racing with claude itself
/// is avoided by seeding BEFORE spawn. Failures are logged, never fatal.
fn seed_claude_trust(cwd: &str) {
    use std::sync::OnceLock;
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let _guard = LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();

    let config_dir = std::env::var("CLAUDE_CONFIG_DIR").ok();
    let home = std::env::var("HOME").ok();
    let path = claude_user_config_path(config_dir.as_deref(), home.as_deref());
    let root: serde_json::Value = std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    let merged = merge_claude_trust(root, cwd);
    let serialized = match serde_json::to_string(&merged) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(target: "nexus::pty", error = %e, "seed_claude_trust: serialize failed");
            return;
        }
    };
    let tmp = path.with_extension("json.nexus-tmp");
    if std::fs::write(&tmp, &serialized).is_err() {
        tracing::warn!(target: "nexus::pty", "seed_claude_trust: write failed");
        return;
    }
    if std::fs::rename(&tmp, &path).is_err() {
        tracing::warn!(target: "nexus::pty", "seed_claude_trust: rename failed");
        let _ = std::fs::remove_file(&tmp);
    }
}

#[cfg(test)]
#[path = "../../tests/unit/pty_supervisor.rs"]
mod tests;
