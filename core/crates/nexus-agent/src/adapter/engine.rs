//! The real ACP engine shared by harness adapters such as Claude and Codex
//! (backend spec §5). It owns the `agent-client-protocol` SDK client connection to a spawned
//! harness subprocess and turns the [`super::Adapter`] contract into ACP calls:
//!
//! - [`AcpEngine::spawn_and_initialize`] launches the harness ([`HarnessCommand`]) with its
//!   stdin/stdout piped, drives the SDK background actors on a dedicated task, completes the
//!   ACP `initialize` handshake, and publishes the live connection handle.
//! - [`AcpEngine::new_session`] / [`AcpEngine::load_session`] map to ACP `session/new` /
//!   `session/load` and record the assigned `SessionId`.
//! - [`AcpEngine::inject`] maps to ACP `session/prompt`; the agent's streamed `session/update`
//!   `AgentMessageChunk`s are captured into a per-turn buffer that [`AcpEngine::take_updates`]
//!   drains as ordered [`StreamEvent`]s for relay.
//!
//! ## Turn-end detection (the bit a real codex-acp turn exercises)
//!
//! ACP signals **end-of-turn distinctly from the per-chunk stream**: the agent streams
//! `session/update` `AgentMessageChunk` *notifications* during the turn, and then completes the
//! turn by **responding to the `session/prompt` request** with a [`PromptResponse`] carrying a
//! `StopReason` (`EndTurn` / `MaxTokens` / `MaxTurnRequests` / `Refusal` / `Cancelled`). That
//! `StopReason` *is* the turn-end signal — it is what real `codex-acp` emits at its
//! `TurnComplete`/`response.completed`, and what AionCore keys off too. So [`AcpEngine::inject`]
//! drives the prompt to completion by awaiting that response and **keys turn-end off the
//! returned `StopReason`**; only once it is observed does the turn's buffered chunks become the
//! reply. Crucially the wait is **bounded** ([`TURN_TIMEOUT`]): a bridge that streams chunks but
//! never delivers the turn-end response (or whose response is lost) must surface an error, never
//! hang the per-agent event loop forever (which would also wedge every future turn).
//!
//! Concurrency mirrors the SDK's documented pattern (see its `connect_with` doc example):
//! `connect_with` runs the actors on a `tokio::spawn`ed task; its `main_fn` performs
//! `initialize`, hands the `ConnectionTo<Agent>` out, then parks on a shutdown oneshot until
//! the engine is dropped. Every request is just `connection.send_request(..).block_task()`
//! on the cloneable handle, so calls are naturally concurrent. The streamed `AgentMessageChunk`
//! notifications are dispatched **serially, in wire order, ahead of the prompt response** by the
//! SDK's single incoming-dispatch loop, so by the time `block_task()` resolves on the prompt
//! response the per-turn buffer already holds every chunk — [`AcpEngine::take_updates`] then
//! drains the complete reply.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::schema::v1::{
    CancelNotification, ContentBlock, EnvVariable, InitializeRequest, LoadSessionRequest,
    McpServer as SchemaMcpServer, McpServerStdio, NewSessionRequest, PromptRequest, PromptResponse,
    RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    SelectedPermissionOutcome, SessionId, SessionNotification, SessionUpdate, TextContent,
};
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::{Agent, Client, ConnectionTo};
use nexus_acp_stream::{translate, StreamEvent};
use tokio::process::Command;
use tokio::sync::{oneshot, Mutex as AsyncMutex};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::{debug, info, warn};

use nexus_common::process_ids::runtime_process_ids_for_pid;
use nexus_common::{NexusError, RuntimeProcessIds};
use nexus_contracts::HarnessId;

use super::provider_limit::classify_acp_prompt_error;
use super::{AdapterInjectError, AdapterOperatorAction};

/// Timeout for the ACP `initialize` handshake. A real harness completes this in well under a
/// second; the generous bound only guards against a wedged child.
const INIT_TIMEOUT: Duration = Duration::from_secs(30);

/// Upper bound for ACP `session/new` and `session/load` after initialization. These lifecycle
/// requests normally finish in seconds, but a bridge can initialize successfully and then leave
/// one unanswered forever. Keep this below the CLI's 120-second harness-launch deadline so the
/// daemon can record a terminal launch result instead of abandoning a claimed command.
const SESSION_OPEN_TIMEOUT: Duration = Duration::from_secs(60);

/// Upper bound on a single `session/prompt` turn — i.e. how long [`AcpEngine::inject`] waits for
/// the harness to deliver the turn-end [`PromptResponse`]/`StopReason` after the prompt is sent.
///
/// A model turn can legitimately run for minutes (long generations, tool loops), so this is
/// deliberately generous. Its job is **liveness, not latency**: if a bridge streams chunks but
/// never signals turn-end (a lost/never-sent prompt response — the live `codex-acp` failure mode
/// this guard exists for), `inject` must return an error so the per-agent event loop re-parks and
/// stays able to run the *next* turn, instead of blocking forever inside one wedged drain.
/// Overridable via `NEXUS_ACP_TURN_TIMEOUT_SECS` for operators with unusually long turns.
const TURN_TIMEOUT: Duration = Duration::from_secs(600);

/// Resolve the per-turn timeout, honouring `NEXUS_ACP_TURN_TIMEOUT_SECS` (seconds) if set and
/// parseable, else [`TURN_TIMEOUT`].
fn turn_timeout() -> Duration {
    std::env::var("NEXUS_ACP_TURN_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&s| s > 0)
        .map(Duration::from_secs)
        .unwrap_or(TURN_TIMEOUT)
}

/// Stream-quiescence window: after at least one reply chunk has arrived, how long the stream must
/// stay silent (no further `session/update`) before [`AcpEngine::inject`] infers turn-end.
///
/// This is the fallback for real `codex-acp` builds that stream the reply and then go idle
/// **without** sending the `session/prompt` response (the live hang). It must be comfortably longer
/// than the inter-chunk gap of a streaming model so a mid-turn pause is never mistaken for the end,
/// yet short enough that a finished-but-unanswered turn surfaces promptly. The window only starts
/// counting **after content has streamed**, so a slow first token (the model still thinking) waits
/// on the full [`TURN_TIMEOUT`], not this.
const QUIESCENCE_WINDOW: Duration = Duration::from_secs(8);

/// Largest text delta Nexus relays from one ACP assistant update.
///
/// Some real ACP bridges coalesce a complete response into one `AgentMessageChunk`. Keeping that
/// provider framing would turn a multi-paragraph answer into one large WebSocket frame and prevent
/// clients from painting progressive content. Nexus therefore normalizes only oversized text
/// updates into bounded UTF-8 slices. Concatenating the slices is byte-for-byte identical to the
/// provider text; all non-text updates retain their original one-notification/one-event framing.
const MAX_LIVE_TEXT_DELTA_BYTES: usize = 512;

/// Grace period for harness process-tree shutdown. ACP stdio adapters commonly launch a shell /
/// node wrapper that spawns provider-specific children; stopping only the wrapper leaks the
/// descendants. The engine therefore starts each harness in its own process group and terminates
/// that group as a unit: SIGTERM first for orderly cleanup, then SIGKILL if the group does not
/// exit promptly.
const PROCESS_TREE_TERM_GRACE: Duration = Duration::from_secs(5);

/// Upper bound for reaping descendants after process-group escalation. Linux makes the Nexus
/// process a child subreaper before ACP spawn, so grandchildren orphaned by an exiting wrapper
/// are adopted here and can be collected instead of lingering as container/host zombies.
const PROCESS_TREE_REAP_GRACE: Duration = Duration::from_secs(2);

/// Grace period used by the daemon boot orphan sweep. These are already detached ACP process
/// trees whose supervising daemon died, so the sweep is intentionally shorter than a normal
/// engine-owned shutdown.
const BOOT_SWEEP_TERM_GRACE: Duration = Duration::from_millis(500);

/// Resolve the quiescence window, honouring `NEXUS_ACP_QUIESCENCE_MS` (milliseconds) if set and
/// parseable, else [`QUIESCENCE_WINDOW`].
fn quiescence_window() -> Duration {
    std::env::var("NEXUS_ACP_QUIESCENCE_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .map(Duration::from_millis)
        .unwrap_or(QUIESCENCE_WINDOW)
}

/// Quiet window applied after a resumed Hermes session reports `session/load` success.
///
/// Hermes can replay historical `session/update` rows after the load response. Until that replay
/// has gone quiet, those rows are not evidence for any subsequent prompt. Keep the production
/// default aligned with the normal turn quiescence boundary; the override exists for hermetic
/// regressions only.
fn load_replay_settle_window() -> Duration {
    std::env::var("NEXUS_ACP_LOAD_REPLAY_SETTLE_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .map(Duration::from_millis)
        .unwrap_or(QUIESCENCE_WINDOW)
}

#[cfg(unix)]
const SIGTERM: i32 = 15;
#[cfg(unix)]
const SIGKILL: i32 = 9;
#[cfg(unix)]
const ESRCH: i32 = 3;

#[cfg(target_os = "linux")]
fn enable_child_subreaper() -> std::io::Result<()> {
    // SAFETY: PR_SET_CHILD_SUBREAPER changes only the calling process's child-reaping policy.
    // The remaining arguments are unused for this prctl operation.
    let rc = unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn enable_child_subreaper() -> std::io::Result<()> {
    Ok(())
}

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
    // SAFETY: kill(2) with a negative pid signals the process group whose id is abs(pid).
    // The caller already rejects pgid 0/current group to avoid self-termination.
    let rc = unsafe { kill(-pgid, sig) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(unix)]
fn is_missing_process_group_error(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound || error.raw_os_error() == Some(ESRCH)
}

#[cfg(unix)]
fn process_group_exists(pgid: u32) -> bool {
    match signal_process_group(pgid, 0) {
        Ok(()) => true,
        Err(error) if is_missing_process_group_error(&error) => false,
        // EPERM and other errors still prove that the process group exists.
        Err(_) => true,
    }
}

#[cfg(target_os = "linux")]
fn reap_exited_process_group_children(pgid: u32) -> usize {
    let Ok(pgid) = i32::try_from(pgid) else {
        return 0;
    };
    let mut reaped = 0;
    loop {
        let mut status = 0;
        // SAFETY: negative waitpid selects only children in this isolated process group;
        // WNOHANG prevents teardown from blocking on a still-running descendant.
        let pid = unsafe { libc::waitpid(-pgid, &mut status, libc::WNOHANG) };
        if pid <= 0 {
            return reaped;
        }
        reaped += 1;
    }
}

#[cfg(not(target_os = "linux"))]
fn reap_exited_process_group_children(_pgid: u32) -> usize {
    0
}

#[cfg(unix)]
async fn wait_for_process_group_exit(pgid: u32, grace: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + grace;
    loop {
        reap_exited_process_group_children(pgid);
        if !process_group_exists(pgid) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn terminate_child_process_tree(
    child: &mut tokio::process::Child,
    grace: Duration,
    context: &'static str,
) {
    #[cfg(unix)]
    {
        if let Some(pid) = child.id() {
            let shutdown_started = tokio::time::Instant::now();
            let mut direct_child_reaped = false;
            match signal_process_group(pid, SIGTERM) {
                Ok(()) => {
                    info!(
                        target: "nexus_agent::acp",
                        pid,
                        context,
                        grace_ms = grace.as_millis() as u64,
                        "sent SIGTERM to ACP harness process group"
                    );
                }
                Err(e) => {
                    warn!(
                        target: "nexus_agent::acp",
                        pid,
                        context,
                        error = %e,
                        "failed to send SIGTERM to ACP harness process group"
                    );
                }
            }

            match tokio::time::timeout(grace, child.wait()).await {
                Ok(Ok(status)) => {
                    direct_child_reaped = true;
                    debug!(
                        target: "nexus_agent::acp",
                        pid,
                        context,
                        ?status,
                        "ACP harness process exited after SIGTERM"
                    );
                }
                Ok(Err(e)) => {
                    warn!(
                        target: "nexus_agent::acp",
                        pid,
                        context,
                        error = %e,
                        "failed waiting for ACP harness after SIGTERM"
                    );
                }
                Err(_) => {
                    warn!(
                        target: "nexus_agent::acp",
                        pid,
                        context,
                        grace_ms = grace.as_millis() as u64,
                        "ACP harness process group did not exit after SIGTERM; escalating"
                    );
                }
            }

            let remaining = grace.saturating_sub(shutdown_started.elapsed());
            if wait_for_process_group_exit(pid, remaining).await {
                return;
            }

            warn!(
                target: "nexus_agent::acp",
                pid,
                context,
                grace_ms = grace.as_millis() as u64,
                "ACP harness descendants survived SIGTERM after wrapper exit; escalating process group"
            );

            match signal_process_group(pid, SIGKILL) {
                Ok(()) => {
                    info!(
                        target: "nexus_agent::acp",
                        pid,
                        context,
                        "sent SIGKILL to ACP harness process group"
                    );
                }
                Err(e) => {
                    warn!(
                        target: "nexus_agent::acp",
                        pid,
                        context,
                        error = %e,
                        "failed to send SIGKILL to ACP harness process group"
                    );
                    let _ = child.start_kill();
                }
            }

            if !direct_child_reaped {
                let _ = child.wait().await;
            }
            if !wait_for_process_group_exit(pid, PROCESS_TREE_REAP_GRACE).await {
                warn!(
                    target: "nexus_agent::acp",
                    pid,
                    context,
                    grace_ms = PROCESS_TREE_REAP_GRACE.as_millis() as u64,
                    "ACP harness process group remained after SIGKILL and bounded reap"
                );
            }
            return;
        }
    }

    #[cfg(not(unix))]
    {
        let _ = context;
        let _ = grace;
    }

    let _ = child.start_kill();
    let _ = child.wait().await;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AcpOrphanSweepReport {
    /// Processes inspected under `/proc`.
    pub scanned: usize,
    /// Candidate orphan ACP processes whose process groups were selected for reaping.
    pub candidates: usize,
    /// Distinct process groups signalled.
    pub groups_signalled: usize,
    /// Signal attempts that failed.
    pub failures: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProcSnapshot {
    pid: u32,
    ppid: u32,
    pgrp: u32,
    cmdline: String,
    environ: Vec<u8>,
    parent_cmdline: Option<String>,
}

fn env_contains_key(environ: &[u8], key: &str) -> bool {
    let prefix = format!("{key}=");
    environ
        .split(|b| *b == 0)
        .any(|entry| entry.starts_with(prefix.as_bytes()))
}

fn looks_like_user_init(cmdline: Option<&str>) -> bool {
    let Some(cmdline) = cmdline else {
        return false;
    };
    let lower = cmdline.to_ascii_lowercase();
    lower.contains("systemd --user") || lower == "systemd" || lower.ends_with("/systemd")
}

fn is_acp_harness_process(cmdline: &str) -> bool {
    let lower = cmdline.to_ascii_lowercase();
    [
        "codex-acp",
        "claude-agent-acp",
        "opencode acp",
        "hermes acp",
        "fake_acp_agent",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn is_app_server_process(cmdline: &str) -> bool {
    let lower = cmdline.to_ascii_lowercase();
    lower.contains("app-server")
        || lower.contains("fake_codex_app_server")
        || lower.contains("codex.sock")
        || lower.contains("--listen unix://")
}

fn is_orphaned_nexus_acp_process(snapshot: &ProcSnapshot) -> bool {
    if snapshot.pid == std::process::id() {
        return false;
    }
    if !env_contains_key(&snapshot.environ, "NEXUS_CLIENT_KEY") {
        return false;
    }
    if !matches!(snapshot.ppid, 1) && !looks_like_user_init(snapshot.parent_cmdline.as_deref()) {
        return false;
    }
    if is_app_server_process(&snapshot.cmdline) {
        return false;
    }
    is_acp_harness_process(&snapshot.cmdline)
}

#[cfg(target_os = "linux")]
fn parse_proc_stat_ids(stat: &str) -> Option<(u32, u32)> {
    let close = stat.rfind(')')?;
    let fields = stat[close + 1..].split_whitespace().collect::<Vec<_>>();
    // After the `(comm)` field, procfs exposes: state, ppid, pgrp, session, ...
    let ppid = fields.get(1)?.parse::<u32>().ok()?;
    let pgrp = fields.get(2)?.parse::<u32>().ok()?;
    Some((ppid, pgrp))
}

#[cfg(target_os = "linux")]
fn read_proc_cmdline(pid: u32) -> Option<String> {
    let path = format!("/proc/{pid}/cmdline");
    let raw = std::fs::read(path).ok()?;
    if raw.is_empty() {
        return None;
    }
    Some(
        raw.split(|b| *b == 0)
            .filter(|part| !part.is_empty())
            .map(|part| String::from_utf8_lossy(part))
            .collect::<Vec<_>>()
            .join(" "),
    )
}

#[cfg(target_os = "linux")]
fn read_proc_snapshot(pid: u32) -> Option<ProcSnapshot> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let (ppid, pgrp) = parse_proc_stat_ids(&stat)?;
    let cmdline = read_proc_cmdline(pid).unwrap_or_else(|| {
        stat.split_once('(')
            .and_then(|(_, rest)| rest.rsplit_once(')'))
            .map(|(comm, _)| comm.to_string())
            .unwrap_or_default()
    });
    let environ = std::fs::read(format!("/proc/{pid}/environ")).ok()?;
    let parent_cmdline = read_proc_cmdline(ppid);
    Some(ProcSnapshot {
        pid,
        ppid,
        pgrp,
        cmdline,
        environ,
        parent_cmdline,
    })
}

/// Reap orphaned ACP stdio harness process groups left behind by a crashed daemon.
///
/// The sweep is deliberately conservative: it only targets init/user-systemd-parented processes
/// that still carry a Nexus client credential and whose command line identifies an ACP stdio
/// harness. Codex app-server processes are explicitly excluded because they are durable runtime
/// attachments that the daemon can re-adopt after restart.
pub fn reap_orphaned_acp_harness_processes() -> AcpOrphanSweepReport {
    #[cfg(not(target_os = "linux"))]
    {
        AcpOrphanSweepReport::default()
    }

    #[cfg(target_os = "linux")]
    {
        let mut report = AcpOrphanSweepReport::default();
        let mut groups = HashSet::new();
        let current_group = current_process_group();

        let Ok(entries) = std::fs::read_dir("/proc") else {
            return report;
        };

        for entry in entries.flatten() {
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<u32>().ok())
            else {
                continue;
            };
            let Some(snapshot) = read_proc_snapshot(pid) else {
                continue;
            };
            report.scanned += 1;
            if !is_orphaned_nexus_acp_process(&snapshot) {
                continue;
            }
            if snapshot.pgrp == 0 || snapshot.pgrp as i32 == current_group {
                warn!(
                    target: "nexus_agent::acp",
                    pid = snapshot.pid,
                    pgrp = snapshot.pgrp,
                    "skipping orphan ACP candidate with unsafe process group"
                );
                continue;
            }
            report.candidates += 1;
            groups.insert(snapshot.pgrp);
        }

        for &pgrp in &groups {
            if let Err(e) = signal_process_group(pgrp, SIGTERM) {
                report.failures += 1;
                warn!(
                    target: "nexus_agent::acp",
                    pgrp,
                    error = %e,
                    "failed to SIGTERM orphan ACP process group during boot sweep"
                );
            } else {
                report.groups_signalled += 1;
            }
        }

        if report.groups_signalled > 0 {
            std::thread::sleep(BOOT_SWEEP_TERM_GRACE);
        }

        for &pgrp in &groups {
            if let Err(e) = signal_process_group(pgrp, SIGKILL) {
                if !is_missing_process_group_error(&e) {
                    report.failures += 1;
                    warn!(
                        target: "nexus_agent::acp",
                        pgrp,
                        error = %e,
                        "failed to SIGKILL orphan ACP process group during boot sweep"
                    );
                }
            }
        }

        report
    }
}

/// How [`AcpEngine::inject`]'s turn ended — used to branch the post-turn logging/result.
enum TurnEnd {
    /// The canonical ACP turn-end: the `session/prompt` request was answered (carrying a
    /// `StopReason`), or the request errored.
    Response(Result<PromptResponse, agent_client_protocol::Error>),
    /// Turn-end inferred from stream quiescence: content streamed, then no further
    /// `session/update` arrived for the quiescence window. Direct/legacy turns accept any buffered
    /// content. Observed Hermes turns accept this only after a real renderable harness event; the
    /// synthetic accepted-input event is deliberately excluded.
    Quiescent,
    /// The bridge appended an interrupt-and-send replacement prompt, answered the cancelled
    /// request with its exact handoff diagnostic, and then emitted real replacement model output.
    /// The model-output boundary prevents retrying a prompt already present in its context.
    InterruptedPromptHandoff,
}

const INTERRUPTED_HANDOFF_DIAGNOSTIC: &str =
    "[ede_diagnostic] result_type=user last_content_type=n/a stop_reason=null";
const INTERRUPTED_HANDOFF_MESSAGE: &str =
    "Internal error: [ede_diagnostic] result_type=user last_content_type=n/a stop_reason=null";

fn is_interrupted_prompt_handoff(error: &agent_client_protocol::Error) -> bool {
    (error.message == INTERRUPTED_HANDOFF_MESSAGE && error.data.is_none())
        || (error.message == "Internal error"
            && error.data.as_ref().and_then(serde_json::Value::as_str)
                == Some(INTERRUPTED_HANDOFF_DIAGNOSTIC))
}

/// Resolve once the reply stream has **streamed content and then gone quiet** for `window`.
///
/// Waits for the first chunk (any reply content), then watches the activity counter: every new
/// `session/update` resets the window; when a full `window` elapses with no new activity, returns.
/// Never returns before content arrives, so a slow first token is handled by the caller's overall
/// ceiling instead of being mistaken for an (empty) quiescent turn.
async fn wait_for_quiescence(activity: &TurnActivity, window: Duration) {
    // Phase 1: wait until the turn has produced at least one reply chunk. Re-check under the
    // notify-then-check pattern so a chunk that lands between checks is never missed.
    loop {
        if activity.buffered_len() > 0 {
            break;
        }
        let notified = activity.notify.notified();
        if activity.buffered_len() > 0 {
            break;
        }
        notified.await;
    }

    // Phase 2: idle-watch. Sleep the window; if the activity counter advanced while we slept, the
    // stream is still live — reset and wait again. Otherwise the stream is quiescent → done.
    loop {
        let before = activity.activity();
        tokio::time::sleep(window).await;
        if activity.activity() == before {
            return;
        }
    }
}

/// Resolve after this turn has produced at least one real renderable harness update and then gone
/// quiet. Unlike [`wait_for_quiescence`], this ignores synthetic events inserted by Nexus itself.
/// That distinction is required for observed delivery: accepted-input proves only that Nexus sent
/// the prompt, while a new model event proves that the recipient harness actually processed it.
async fn wait_for_model_quiescence(
    activity: &TurnActivity,
    model_events_before: u64,
    window: Duration,
) {
    loop {
        if activity.model_events() > model_events_before {
            break;
        }
        let notified = activity.notify.notified();
        if activity.model_events() > model_events_before {
            break;
        }
        notified.await;
    }

    loop {
        let before = activity.activity();
        tokio::time::sleep(window).await;
        if activity.activity() == before {
            return;
        }
    }
}

/// Wait until Hermes promotes the exact prompt from its private busy-session queue into a real
/// follow-up turn. Hermes 0.17 emits that promotion as a `UserMessageChunk` immediately before it
/// recursively invokes the model. The returned counter is the model-event boundary captured when
/// that echo arrived, so events from the older turn can never settle the promoted prompt.
async fn wait_for_prompt_promotion(activity: &TurnActivity, expected_prompt: &str) -> u64 {
    loop {
        if let Some(model_events_before) = activity.prompt_promotion_boundary(expected_prompt) {
            return model_events_before;
        }
        let notified = activity.notify.notified();
        if let Some(model_events_before) = activity.prompt_promotion_boundary(expected_prompt) {
            return model_events_before;
        }
        notified.await;
    }
}

/// Wait until the ACP update stream remains unchanged for a complete window.
///
/// Unlike turn quiescence this intentionally does not require a first event: a resumed session
/// with no replay still crosses one bounded quiet interval, while any late replay resets it.
async fn wait_for_activity_quiescence(activity: &TurnActivity, window: Duration) {
    loop {
        let before = activity.activity();
        tokio::time::sleep(window).await;
        if activity.activity() == before {
            return;
        }
    }
}

/// The exact subprocess to spawn for a harness: program + args + working directory.
///
/// Built by the per-harness adapters (see [`super::claude`] and the codex adapter in `nexus-harness-codex`) and consumed
/// by [`AcpEngine::spawn_and_initialize`]. Captured as owned data so it is cheap to clone and
/// log, and so the spawn site stays harness-agnostic.
#[derive(Debug, Clone, Default)]
pub struct HarnessCommand {
    /// Program to exec (e.g. `npx`, `node`, or `claude` for the stream-json fallback).
    pub program: String,
    /// Arguments, in order (e.g. the ACP adapter entrypoint/package).
    pub args: Vec<String>,
    /// Working directory for the child (`None` = inherit the daemon's cwd).
    pub cwd: Option<String>,
    /// Extra environment variables to set on the child, on top of the inherited parent env. Empty
    /// for real harnesses; the hermetic tests use it to script the fake harness's reply per-child
    /// (e.g. `FAKE_ACP_REPLY=envelope`) without touching the process-global env — so parallel tests
    /// never race on a shared env var.
    pub env: Vec<(String, String)>,
}

impl HarnessCommand {
    /// A readable one-line preview (program + args) for logs — never the env.
    fn preview(&self) -> String {
        format!("{} {}", self.program, self.args.join(" "))
    }
}

/// Per-launch context handed to an adapter factory: the working directory + the environment to set
/// on the spawned harness. The env carries the agent's OWN identity
/// (`NEXUS_NAME`/`NEXUS_CLIENT_KEY`/`NEXUS_PROJECT`/`NEXUS_AGENT`) plus a `PATH`
/// that includes the `nexus` binary — so when the agent runs `nexus dm/reply/post` from its shell,
/// the CLI authenticates as that agent. That shell invocation IS the agent's outbound (inbound is
/// the `<nexus from=…>` turn it reads; outbound is the command it runs).
///
/// The optional `bus_name`/`bus_project` fields carry the bus MCP identity. `bus_client_key` and
/// `bus_agent` carry the verified runtime credential metadata. When name/project are present,
/// [`build_new_session_request`] and [`build_load_session_request`] inject a stdio MCP server into
/// every `session/new` and `session/load` so both fresh and resumed agents get a live Nexus bus;
/// daemon-owned launches include all four values so MCP calls cannot fall back to an ambient shell
/// identity.
#[derive(Debug, Clone, Default)]
pub struct LaunchCtx {
    /// Working directory for the harness.
    pub cwd: Option<String>,
    /// Extra env applied to the child on top of the inherited parent env (the agent's identity:
    /// `NEXUS_NAME`/`NEXUS_CLIENT_KEY`/`NEXUS_PROJECT`/`NEXUS_AGENT` + a `PATH` with the `nexus`
    /// binary). The bus how-to is delivered separately as the installed `nexus-bus` **skill** (see
    /// [`crate::adapter::skill`]) — each adapter installs it into its own harness's skills dir.
    pub env: Vec<(String, String)>,
    /// Agent name for the `nexus mcp --as <name>` stdio server injected into ACP session setup.
    /// When `None`, no MCP server is injected (e.g. test-harness or admin-spawn paths).
    pub bus_name: Option<String>,
    /// Project scope for the `nexus mcp --project <project>` arg.
    pub bus_project: Option<String>,
    /// Verified runtime client key for the `nexus mcp --client-key <key>` arg.
    pub bus_client_key: Option<String>,
    /// Harness token for the `nexus mcp --agent <agent>` arg.
    pub bus_agent: Option<String>,
    /// When `true`, ACP session setup does NOT inject the stdio `nexus-bus` MCP server, even when
    /// the bus identity is present. Set by the OpenCode and Hermes adapters:
    /// opencode REJECTS a stdio MCP server in `session/new` (`-32602 Invalid params` → the session
    /// never opens, so the agent never comes online) and instead reads its MCP from OpenCode config
    /// supplied outside ACP (`OPENCODE_CONFIG_CONTENT` for headless ACP, project config for headed
    /// compatibility). claude/codex leave this `false` and keep getting the bus over ACP. Default
    /// `false` (inject) for backward-compatible behavior.
    pub suppress_acp_mcp: bool,
}

/// Per-turn streaming state, shared between the SDK notification handler and [`AcpEngine::inject`].
///
/// It holds the captured reply chunks **and** an activity signal so direct/legacy `inject` can
/// detect turn-end two ways:
///  1. the canonical ACP signal — the `session/prompt` response's `StopReason` (handled in
///     `inject` directly via `block_task`), and
///  2. **stream quiescence** — some real `codex-acp` builds stream the reply (and trailing
///     `usage_update`s) but then go idle **without ever sending the prompt response**
///     (confirmed live: the turn finishes, the bridge sits at 0% CPU, but `session/prompt` is left
///     unanswered forever). For those, "no further `session/update` for a quiet window after
///     content arrived" is the de-facto direct-session turn-end. Durable bus injection requires
///     the canonical prompt response instead. `last_activity` + `notify` let the compatibility
///     path watch for quiescence without polling a lock in a hot loop.
struct TurnActivity {
    /// The ordered, translated stream events captured so far this turn — the **full** ACP stream
    /// (text, thinking, tool calls, plans, available commands), not just the reply text. Each is a
    /// [`StreamEvent`] the daemon relays as a tagged `agent.update`.
    buffer: Mutex<Vec<StreamEvent>>,
    /// Bumped on **every** incoming `session/update` (renderable or not — a trailing `usage_update`
    /// still means the bridge is alive and the turn is not yet quiescent). Read by `inject` to know
    /// whether the stream advanced during a quiescence window.
    activity: std::sync::atomic::AtomicU64,
    /// Number of renderable events that arrived from the harness. Synthetic accepted-input
    /// events use [`Self::push_event`] directly and do not increment this counter.
    model_events: std::sync::atomic::AtomicU64,
    /// Non-rendered ACP user echoes observed during this turn. Hermes emits the exact queued prompt
    /// as a `UserMessageChunk` when it leaves the process-private busy queue and enters the next
    /// model turn. Keep the model-event counter from that instant so later output is causally tied
    /// to the promoted prompt even if the waiter wakes after several notifications were dispatched.
    prompt_promotions: Mutex<Vec<(String, u64)>>,
    /// Pulsed alongside `activity` so a waiting `inject` wakes promptly on new stream traffic
    /// instead of sleeping out the whole window every time.
    notify: tokio::sync::Notify,
    /// Optional LIVE sink: when a turn is being relayed in realtime, each normalized renderable
    /// [`StreamEvent`] is sent here AS IT ARRIVES (in addition to buffering), so the caller can
    /// emit bounded `agent.update` deltas instead of waiting for turn-end. A provider text update
    /// may normalize into multiple deltas; other renderable updates remain one event. This is the
    /// realtime streaming path (AionUi `responseStream` model); the buffer remains for quiescence
    /// detection and a non-live fallback. Set per turn via `install_live`, dropped via
    /// `clear_live`.
    live: Mutex<Option<tokio::sync::mpsc::UnboundedSender<StreamEvent>>>,
}

impl TurnActivity {
    fn new() -> Self {
        Self {
            buffer: Mutex::new(Vec::new()),
            activity: std::sync::atomic::AtomicU64::new(0),
            model_events: std::sync::atomic::AtomicU64::new(0),
            prompt_promotions: Mutex::new(Vec::new()),
            notify: tokio::sync::Notify::new(),
            live: Mutex::new(None),
        }
    }

    /// Begin live relay for a turn: install a fresh channel and return its receiver. Each subsequent
    /// `ingest` sends normalized renderable events here AS THEY ARRIVE (realtime). Call
    /// `clear_live` at turn-end to close the receiver.
    fn install_live(&self) -> tokio::sync::mpsc::UnboundedReceiver<StreamEvent> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        *self.live.lock().unwrap() = Some(tx);
        rx
    }

    /// End live relay: drop the sender so the receiver's `recv()` returns `None` and the drain ends.
    fn clear_live(&self) {
        *self.live.lock().unwrap() = None;
    }

    /// Ingest one incoming `session/update`: translate it (the full-stream pass-through), buffer
    /// every renderable [`StreamEvent`], and mark stream activity for **every** notification —
    /// renderable or not — so a trailing book-keeping update (e.g. `usage_update`) still resets the
    /// quiescence window (the bridge is alive). A non-renderable update is not buffered but is still
    /// counted as activity. This replaces the old AgentMessageChunk-only capture: no variant is
    /// dropped from the relayed stream now.
    fn ingest(&self, update: &SessionUpdate) {
        if let SessionUpdate::UserMessageChunk(chunk) = update {
            if let ContentBlock::Text(text) = &chunk.content {
                self.prompt_promotions
                    .lock()
                    .unwrap()
                    .push((text.text.clone(), self.model_events()));
            }
            self.touch();
            debug!(
                target: "nexus_agent::acp",
                "captured non-rendered user-message echo for prompt correlation"
            );
            return;
        }

        let events = translate_for_relay(update);
        if let Some(first) = events.first() {
            // Hermes 0.17 emits this adapter-authored status line when it has only placed the
            // prompt in a process-private queue. It is not model output and must not satisfy
            // observed-delivery quiescence. The event remains buffered/relayed so clients can
            // display the truthful harness status.
            if events.len() != 1 || !is_hermes_busy_queue_ack(first) {
                self.model_events
                    .fetch_add(1, std::sync::atomic::Ordering::Release);
            }
            for event in events {
                let idx = self.push_event(event);
                debug!(
                    target: "nexus_agent::acp",
                    event_index = idx,
                    "captured renderable session/update (relayed as agent.update)"
                );
            }
        } else {
            self.touch();
            debug!(
                target: "nexus_agent::acp",
                update = ?std::mem::discriminant(update),
                "non-renderable session/update (kept stream alive, not buffered)"
            );
        }
    }

    fn push_event(&self, event: StreamEvent) -> usize {
        // LIVE relay: forward this chunk immediately (realtime streaming) if a live sink is
        // installed. Buffer it too — for quiescence detection + the non-live fallback.
        if let Some(tx) = self.live.lock().unwrap().as_ref() {
            let _ = tx.send(event.clone());
        }
        let idx = {
            let mut buf = self.buffer.lock().unwrap();
            buf.push(event);
            buf.len() - 1
        };
        self.touch();
        idx
    }

    /// Mark stream activity (any `session/update` arrived) and wake a waiting `inject`.
    fn touch(&self) {
        self.activity
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        self.notify.notify_waiters();
    }

    fn activity(&self) -> u64 {
        self.activity.load(std::sync::atomic::Ordering::Acquire)
    }

    fn model_events(&self) -> u64 {
        self.model_events.load(std::sync::atomic::Ordering::Acquire)
    }

    fn buffered_len(&self) -> usize {
        self.buffer.lock().unwrap().len()
    }

    fn has_hermes_busy_queue_ack(&self) -> bool {
        self.buffer
            .lock()
            .unwrap()
            .iter()
            .any(is_hermes_busy_queue_ack)
    }

    fn prompt_promotion_boundary(&self, expected_prompt: &str) -> Option<u64> {
        self.prompt_promotions
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find_map(|(prompt, boundary)| (prompt == expected_prompt).then_some(*boundary))
    }

    /// Reset for a fresh turn: clear the buffer (activity is a monotonic counter, so callers snapshot
    /// it instead of resetting — avoids a race with late notifications from a prior turn).
    fn reset_buffer(&self) {
        self.buffer.lock().unwrap().clear();
        self.prompt_promotions.lock().unwrap().clear();
    }

    fn take(&self) -> Vec<StreamEvent> {
        std::mem::take(&mut *self.buffer.lock().unwrap())
    }
}

/// Translate one ACP update into the events Nexus relays. The pure ACP translator deliberately
/// preserves provider framing; this engine boundary adds one transport normalization: oversized
/// assistant text becomes bounded UTF-8 deltas so a coalescing bridge cannot collapse progressive
/// browser output into one WebSocket frame.
fn translate_for_relay(update: &SessionUpdate) -> Vec<StreamEvent> {
    let SessionUpdate::AgentMessageChunk(chunk) = update else {
        return translate(update).into_iter().collect();
    };
    let ContentBlock::Text(text) = &chunk.content else {
        return translate(update).into_iter().collect();
    };
    if text.text.len() <= MAX_LIVE_TEXT_DELTA_BYTES {
        return vec![StreamEvent::text(text.text.clone())];
    }

    let mut events = Vec::new();
    let mut start = 0;
    while start < text.text.len() {
        let mut end = (start + MAX_LIVE_TEXT_DELTA_BYTES).min(text.text.len());
        while !text.text.is_char_boundary(end) {
            end -= 1;
        }
        events.push(StreamEvent::text(&text.text[start..end]));
        start = end;
    }
    events
}

/// Hermes' exact ACP busy-session acknowledgement. This is emitted by adapter code, not the model,
/// and accompanies an immediate `EndTurn` while the submitted prompt exists only in Hermes memory.
fn is_hermes_busy_queue_ack(event: &StreamEvent) -> bool {
    event.kind == nexus_contracts::AgentUpdateKind::Text
        && event
            .data
            .get("text")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|text| {
                let text = text.trim();
                text.starts_with("Queued for the next turn. (") && text.ends_with(" queued)")
            })
}

/// A live, initialized ACP connection plus the harness's assigned session id.
struct LiveConn {
    /// Cloneable SDK connection handle. Outgoing requests/notifications route through the SDK's
    /// own actors, so this is used directly by every method.
    connection: ConnectionTo<Agent>,
    /// The current ACP session id (set by `session/new` or `session/load`).
    session_id: Option<SessionId>,
    /// Dropped on engine teardown to release the background task's `main_fn`.
    _shutdown_tx: oneshot::Sender<()>,
}

/// The real ACP engine. One per launched harness; cheaply shareable (`Arc<dyn Adapter>` wraps it).
pub struct AcpEngine {
    /// The live connection, populated by [`AcpEngine::spawn_and_initialize`]. Behind an async
    /// mutex so the `&self` adapter methods can mutate the recorded session id.
    conn: AsyncMutex<Option<LiveConn>>,
    /// Serialize prompt turns while allowing `session/cancel` to use the live connection
    /// concurrently. Holding `conn` across the prompt response used to make cancellation
    /// impossible: the cancel notification waited behind the very turn it needed to stop.
    turn_lock: AsyncMutex<()>,
    /// Per-turn reply buffer + stream-activity signal shared with the notification handler.
    updates: Arc<TurnActivity>,
    /// Handle to the spawned child process retained for [`AcpEngine::kill`]. Populated by
    /// [`AcpEngine::spawn_and_initialize`]; cleared (taken) by the background connection task
    /// when it performs the final `child.wait()` at shutdown. Shared with the background task via
    /// `Arc<Mutex<…>>` so both sides can reach it without a channel.
    child: Arc<Mutex<Option<tokio::process::Child>>>,
    /// Kill signal sender. Set by [`AcpEngine::spawn_and_initialize`] when the harness is
    /// spawned; the matching receiver is held by `run_connection`, which owns the `Child` after
    /// `take()`. The nested sender acknowledges bounded process-group termination so
    /// [`AcpEngine::kill`] does not return while descendants are still running.
    kill_tx: Mutex<Option<tokio::sync::oneshot::Sender<oneshot::Sender<()>>>>,
    /// The OS PID of the spawned child, recorded at spawn time so tests (and diagnostics) can
    /// verify process liveness after a kill without requiring access to the `Child` handle itself.
    /// Zero if no child has been spawned yet.
    child_pid: std::sync::atomic::AtomicU32,
    /// Child process group captured from `/proc/<pid>/stat` at spawn time.
    child_pgid: std::sync::atomic::AtomicU32,
    /// Optional harness identity used only to classify structured ACP prompt failures before they
    /// flatten into `session/prompt failed: ...` strings. Engines without a harness keep legacy
    /// generic error behavior.
    harness: Option<HarnessId>,
    /// Bounded lifecycle request window. Production uses [`SESSION_OPEN_TIMEOUT`]; the explicit
    /// builder lets hermetic tests exercise the failure path without a minute-long wait.
    session_open_timeout: Duration,
}

impl Default for AcpEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl AcpEngine {
    /// A fresh, unconnected engine.
    pub fn new() -> Self {
        Self {
            conn: AsyncMutex::new(None),
            turn_lock: AsyncMutex::new(()),
            updates: Arc::new(TurnActivity::new()),
            child: Arc::new(Mutex::new(None)),
            kill_tx: Mutex::new(None),
            child_pid: std::sync::atomic::AtomicU32::new(0),
            child_pgid: std::sync::atomic::AtomicU32::new(0),
            harness: None,
            session_open_timeout: SESSION_OPEN_TIMEOUT,
        }
    }

    /// A fresh engine that can classify structured ACP prompt failures for `harness`.
    pub fn for_harness(harness: HarnessId) -> Self {
        Self {
            harness: Some(harness),
            ..Self::new()
        }
    }

    /// Override the ACP session-open deadline for an embedded engine.
    ///
    /// Normal adapters retain the conservative 60-second production default. This seam is also
    /// used by the hermetic liveness regressions so a deliberately silent fake bridge fails fast.
    pub fn with_session_open_timeout(mut self, timeout: Duration) -> Self {
        self.session_open_timeout = timeout;
        self
    }

    async fn terminate_timed_out_session_open(&self) {
        self.kill().await;
        // Drop the stale SDK connection as well as its shutdown sender. A resume timeout must not
        // let the service's same-connection fresh-session fallback issue another request against
        // a process tree we just reaped.
        self.conn.lock().await.take();
    }

    /// The harness's ACP session id assigned by `session/new` / `session/load` (the resume key).
    /// Persisted by the daemon so a not-fresh agent can be re-spawned and resumed via
    /// `session/load`. `None` until a session is opened.
    pub async fn acp_session_id(&self) -> Option<String> {
        self.conn
            .lock()
            .await
            .as_ref()
            .and_then(|c| c.session_id.as_ref())
            .map(|s| s.0.to_string())
    }

    /// Cancel the active ACP turn for this engine's current session.
    ///
    /// `session/cancel` is a notification and therefore returns once it is accepted by the SDK
    /// connection. The next prompt remains serialized by `turn_lock` until the cancelled prompt
    /// observes its terminal response, so redirect-now cannot overlap two model turns.
    pub async fn cancel_active_turn(&self) -> Result<(), NexusError> {
        let (connection, session_id) = {
            let guard = self.conn.lock().await;
            let live = guard
                .as_ref()
                .ok_or_else(|| NexusError::Adapter("acp engine not connected".into()))?;
            let session_id = live.session_id.clone().ok_or_else(|| {
                NexusError::Adapter("no active session (open or resume first)".into())
            })?;
            (live.connection.clone(), session_id)
        };
        connection
            .send_notification(CancelNotification::new(session_id))
            .map_err(|e| NexusError::Adapter(format!("session/cancel failed: {e}")))?;
        Ok(())
    }

    /// Install the realtime relay channel for the turn about to be injected: each renderable
    /// `session/update` is forwarded here AS IT ARRIVES, so the caller emits bounded
    /// `agent.update` deltas (true streaming) instead of waiting for turn-end. Pair with
    /// [`AcpEngine::clear_live`].
    pub fn install_live(&self) -> tokio::sync::mpsc::UnboundedReceiver<StreamEvent> {
        self.updates.install_live()
    }

    /// Close the realtime relay channel at turn-end (drops the sender → the drain loop ends).
    pub fn clear_live(&self) {
        self.updates.clear_live();
    }

    /// The OS PID of the spawned harness child, or `None` if no child has been spawned.
    /// Useful for tests that need to verify process liveness after a kill without
    /// holding a reference to the `Child` handle.
    pub fn spawned_pid(&self) -> Option<u32> {
        let pid = self.child_pid.load(std::sync::atomic::Ordering::Acquire);
        if pid == 0 {
            None
        } else {
            Some(pid)
        }
    }

    /// Exact OS process tuple captured for the spawned ACP harness process group.
    pub fn runtime_process_ids(&self) -> Option<RuntimeProcessIds> {
        let os_pid = self.child_pid.load(std::sync::atomic::Ordering::Acquire);
        let os_pgid = self.child_pgid.load(std::sync::atomic::Ordering::Acquire);
        if os_pid == 0 || os_pgid == 0 {
            return None;
        }
        Some(RuntimeProcessIds { os_pid, os_pgid })
    }

    /// True once a live connection has been established.
    pub async fn is_connected(&self) -> bool {
        self.conn.lock().await.is_some()
    }

    /// Terminate the spawned harness process group. No-op if the process has already exited or was
    /// never spawned. Called by the `kill=true` path in `admin.remove`.
    ///
    /// After initialization completes, `run_connection` owns the `Child` (it called
    /// `child_arc.take()` at startup). So this method fires a oneshot kill signal instead of
    /// directly signalling a shared Arc: the connection task receives the signal on its select
    /// branch and terminates the child's process group. This avoids any
    /// ownership race — the task that holds the child is always the one that kills it.
    ///
    /// As a backstop for the narrow window before `run_connection` takes the child (i.e. kill
    /// fires during the initialize handshake), the Arc path is still attempted first. On Linux,
    /// Nexus is a child subreaper and explicitly collects descendants orphaned by a wrapper exit;
    /// the returned future resolves only after bounded group shutdown completes or is reported.
    pub async fn kill(&self) {
        // Backstop: if kill fires during the initialize handshake before run_connection has taken
        // the child out of the Arc, directly kill it here.
        let early_child = {
            let mut guard = self.child.lock().unwrap();
            guard.take()
        };
        let killed_early = if let Some(mut child) = early_child {
            terminate_child_process_tree(&mut child, PROCESS_TREE_TERM_GRACE, "early kill").await;
            info!(target: "nexus_agent::acp", "kill: terminated process group via Arc (early-kill path, before run_connection took ownership)");
            true
        } else {
            false
        };
        // Primary path: signal run_connection to kill its owned child. This is what fires in the
        // normal case (connection is live, run_connection has ownership of the child).
        let tx = self.kill_tx.lock().unwrap().take();
        if let Some(tx) = tx.filter(|_| !killed_early) {
            // Ignore send errors: if the receiver is gone the connection task has already ended,
            // which means the process is already dead (or being reaped). This is a no-op.
            let (complete_tx, complete_rx) = oneshot::channel();
            if tx.send(complete_tx).is_ok() {
                let max_wait = PROCESS_TREE_TERM_GRACE + PROCESS_TREE_REAP_GRACE;
                if tokio::time::timeout(max_wait, complete_rx).await.is_err() {
                    warn!(
                        target: "nexus_agent::acp",
                        wait_ms = max_wait.as_millis() as u64,
                        "kill: process-group shutdown did not acknowledge before the bounded deadline"
                    );
                }
            }
            info!(target: "nexus_agent::acp", "kill: completed process-group shutdown via owned child");
        } else {
            debug!(target: "nexus_agent::acp", "kill: no kill signal available (already fired, or never spawned)");
        }
    }

    /// Spawn `cmd`, wire the SDK client over its stdio, and complete the ACP `initialize`
    /// handshake. On success the live connection handle is stored for subsequent
    /// `session/new` / `session/prompt`. Idempotent guard: a second call while already
    /// connected is an error (callers open exactly once per launch).
    pub async fn spawn_and_initialize(&self, cmd: &HarnessCommand) -> Result<(), NexusError> {
        if self.conn.lock().await.is_some() {
            return Err(NexusError::Adapter("acp engine already connected".into()));
        }

        let preview = cmd.preview();
        enable_child_subreaper().map_err(|error| {
            NexusError::Adapter(format!(
                "failed to enable ACP child subreaper before spawning `{preview}`: {error}"
            ))
        })?;
        let mut command = Command::new(&cmd.program);
        command
            .args(&cmd.args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        #[cfg(unix)]
        command.process_group(0);
        if let Some(cwd) = &cmd.cwd {
            command.current_dir(cwd);
        }
        // Drop any identity/harness-home state inherited from the daemon or operator shell before
        // applying this runtime's explicit env. A launched agent must authenticate as itself, never
        // as whichever agent last registered in the parent process environment.
        scrub_inherited_identity_env(&mut command);
        // Per-child env overrides. Used by the hermetic tests to script the fake harness's reply
        // without a process-global env race, and by real launches to inject the runtime identity.
        for (k, v) in &cmd.env {
            command.env(k, v);
        }
        // Don't leave a zombie if the parent task is dropped mid-handshake.
        command.kill_on_drop(true);

        let mut child = command.spawn().map_err(|e| {
            NexusError::Adapter(format!("failed to spawn harness `{preview}`: {e}"))
        })?;

        // Record the PID immediately so kill() diagnostics and tests can verify process liveness.
        if let Some(pid) = child.id() {
            self.child_pid
                .store(pid, std::sync::atomic::Ordering::Release);
            if let Some(entry) = runtime_process_ids_for_pid(pid) {
                self.child_pgid
                    .store(entry.os_pgid, std::sync::atomic::Ordering::Release);
            }
        }

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| NexusError::Adapter("harness stdin unavailable".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| NexusError::Adapter("harness stdout unavailable".into()))?;

        // Drain stderr to the tracing log so a harness that dies during init leaves a trail.
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(drain_stderr(stderr));
        }

        let (init_tx, init_rx) = oneshot::channel::<Result<(), NexusError>>();
        let (ready_tx, ready_rx) = oneshot::channel::<ConnectionTo<Agent>>();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        // Fired by the background task the moment the SDK connection ends, so a harness that
        // dies during startup fails the launch immediately instead of waiting out INIT_TIMEOUT.
        let (closed_tx, closed_rx) = oneshot::channel::<()>();
        // Kill signal: engine fires the sender; run_connection awaits the receiver on its select
        // branch, terminates the owned process group, then acknowledges completion. Created here
        // so it is set before run_connection runs, guaranteeing kill() can always reach a running
        // child and await the actual teardown boundary.
        let (kill_signal_tx, kill_signal_rx) = oneshot::channel::<oneshot::Sender<()>>();
        *self.kill_tx.lock().unwrap() = Some(kill_signal_tx);

        // Store the child in the shared Arc so `kill()` can reach it during the init window
        // (before run_connection takes it out), then pass a clone to the background task which
        // takes it out when the connection starts (for child.wait() + the kill-signal branch).
        *self.child.lock().unwrap() = Some(child);
        let child_arc = Arc::clone(&self.child);

        tokio::spawn(run_connection(
            stdin,
            stdout,
            child_arc,
            Arc::clone(&self.updates),
            init_tx,
            ready_tx,
            shutdown_rx,
            closed_tx,
            kill_signal_rx,
        ));

        // Resolve as soon as any of: initialize completes, the connection closes, or the
        // handshake times out. Racing `closed_rx` is what makes a crash-on-startup harness
        // (the pipe EOFs before a response) fail fast rather than hang for INIT_TIMEOUT.
        tokio::select! {
            biased;
            init = init_rx => match init {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(NexusError::Adapter(
                        "harness closed before ACP initialize completed".into(),
                    ))
                }
            },
            _ = closed_rx => {
                return Err(NexusError::Adapter(format!(
                    "harness `{preview}` exited before ACP initialize completed",
                )))
            }
            _ = tokio::time::sleep(INIT_TIMEOUT) => {
                return Err(NexusError::Adapter(format!(
                    "ACP initialize timed out after {}s for `{preview}`",
                    INIT_TIMEOUT.as_secs()
                )))
            }
        }

        let connection = ready_rx
            .await
            .map_err(|_| NexusError::Adapter("ACP connection handle was not published".into()))?;

        *self.conn.lock().await = Some(LiveConn {
            connection,
            session_id: None,
            _shutdown_tx: shutdown_tx,
        });
        Ok(())
    }

    /// Open a fresh ACP session (`session/new`) rooted at `cwd`, recording the assigned id.
    ///
    /// When `ctx` carries bus identity (`bus_name`/`bus_project`), the nexus internal
    /// MCP server is injected into the request via [`build_new_session_request`] so the launched
    /// agent receives the bus tools over ACP.
    pub async fn new_session(&self, cwd: Option<&str>, ctx: &LaunchCtx) -> Result<(), NexusError> {
        let req = build_new_session_request(cwd, ctx);
        {
            let mut guard = self.conn.lock().await;
            let live = guard
                .as_mut()
                .ok_or_else(|| NexusError::Adapter("acp engine not connected".into()))?;
            match tokio::time::timeout(
                self.session_open_timeout,
                live.connection.send_request(req).block_task(),
            )
            .await
            {
                Ok(Ok(resp)) => {
                    live.session_id = Some(resp.session_id);
                    return Ok(());
                }
                Ok(Err(error)) => {
                    return Err(NexusError::Adapter(format!("session/new failed: {error}")))
                }
                Err(_) => {}
            }
        }

        self.terminate_timed_out_session_open().await;
        Err(NexusError::Adapter(format!(
            "session/new timed out after {}s",
            self.session_open_timeout.as_secs_f64()
        )))
    }

    /// Re-attach to an existing ACP session (`session/load`) by `resume_key`, rooted at `cwd`.
    pub async fn load_session(
        &self,
        resume_key: &str,
        cwd: Option<&str>,
        ctx: &LaunchCtx,
    ) -> Result<(), NexusError> {
        let session_id = SessionId::new(resume_key);
        let request = build_load_session_request(resume_key, cwd, ctx);
        {
            let mut guard = self.conn.lock().await;
            let live = guard
                .as_mut()
                .ok_or_else(|| NexusError::Adapter("acp engine not connected".into()))?;
            match tokio::time::timeout(
                self.session_open_timeout,
                live.connection.send_request(request).block_task(),
            )
            .await
            {
                Ok(Ok(_)) => live.session_id = Some(session_id),
                Ok(Err(error)) => {
                    return Err(NexusError::Adapter(format!("session/load failed: {error}")))
                }
                Err(_) => {
                    drop(guard);
                    self.terminate_timed_out_session_open().await;
                    return Err(NexusError::Adapter(format!(
                        "session/load timed out after {}s",
                        self.session_open_timeout.as_secs_f64()
                    )));
                }
            }
        }

        // Hermes may emit historical updates after acknowledging session/load. If the next prompt
        // starts immediately, its observed-delivery snapshot mistakes that replay for a fresh model
        // response and can settle mail that never reached the new context. Drain to a bounded quiet
        // boundary before exposing the resumed session to prompt injection, then discard the replay.
        if self
            .harness
            .as_ref()
            .is_some_and(|h| h.as_str() == "hermes")
        {
            wait_for_activity_quiescence(&self.updates, load_replay_settle_window()).await;
            self.updates.reset_buffer();
        }
        Ok(())
    }

    /// Inject one rendered prompt as a single ACP `session/prompt` turn and **drive it to
    /// turn-end**. Clears the per-turn buffer first; the agent's streamed `AgentMessageChunk`s
    /// accumulate there while the turn runs. After this returns, [`AcpEngine::take_updates`] drains
    /// the complete reply.
    ///
    /// ## Direct/legacy turn-end compatibility
    ///
    /// The **canonical** ACP turn-end is the harness **responding to the `session/prompt`
    /// request** with a [`PromptResponse`] carrying a `StopReason` (distinct from the per-chunk
    /// notifications). Direct/legacy calls await that response but do **not** rely on it alone:
    /// some real `codex-acp` builds stream the whole reply (chunks + a trailing `usage_update`) and
    /// then go **idle without ever sending that response** (confirmed live — the bridge sits at 0%
    /// CPU, `session/prompt` unanswered). So the wait also keys off **stream quiescence**: once at
    /// least one reply chunk has arrived, if no further `session/update` lands for
    /// [`quiescence_window`], the direct call is treated as complete. Observed durable bus delivery
    /// through [`Self::inject_with_accepted_event`] is stricter: it normally requires
    /// `PromptResponse`. Hermes 0.17 is the one compatibility exception: when it renders a real
    /// model update but leaves `session/prompt` unanswered, model-output quiescence is accepted as
    /// recipient-side delivery evidence. A hard [`turn_timeout`] ceiling prevents either path from
    /// wedging the caller.
    pub async fn inject(&self, prompt: String) -> Result<(), AdapterInjectError> {
        self.inject_with_accepted_event(prompt, None).await
    }

    /// Like [`AcpEngine::inject`], but prepends a synthetic stream event immediately after the ACP
    /// prompt request is submitted and before assistant updates can be relayed. Presence of this
    /// event identifies durable observed delivery, which normally requires the canonical
    /// `PromptResponse`. Hermes may also settle from quiescence after a real model event. The
    /// exact interrupt-and-send handoff diagnostic may settle only after subsequent replacement
    /// model output becomes quiescent. The synthetic accepted event alone is never sufficient.
    pub async fn inject_with_accepted_event(
        &self,
        prompt: String,
        accepted_event: Option<StreamEvent>,
    ) -> Result<(), AdapterInjectError> {
        // Prompt turns are serialized independently of the connection handle. Cancellation must
        // be able to acquire/clone `conn` while this turn is awaiting its response.
        let _turn = self.turn_lock.lock().await;

        // Fresh buffer for this turn. Snapshot the (monotonic) activity counter so we measure only
        // THIS turn's stream traffic, never a stale notification from a prior turn.
        self.updates.reset_buffer();
        let activity = Arc::clone(&self.updates);
        let model_events_before = activity.model_events();

        let (connection, session_id) = {
            let guard = self.conn.lock().await;
            let live = guard
                .as_ref()
                .ok_or_else(|| NexusError::Adapter("acp engine not connected".into()))?;
            let session_id = live.session_id.clone().ok_or_else(|| {
                NexusError::Adapter("no active session (open or resume first)".into())
            })?;
            (live.connection.clone(), session_id)
        };
        let acp_session = session_id.0.clone();
        let require_terminal_response = accepted_event.is_some();

        // The wire hop: the ACP `session/prompt` request actually leaving for the harness. A live
        // run that reaches "no usable adapter" / an empty drain never gets here — so seeing this
        // line confirms the prompt was transmitted to the subprocess.
        info!(
            target: "nexus_agent::acp",
            acp_session = %acp_session,
            prompt_len = prompt.len(),
            require_terminal_response,
            "sending ACP session/prompt; awaiting permitted turn-completion evidence"
        );
        let expected_prompt = prompt.clone();
        let prompt_turn = connection.send_request(PromptRequest::new(
            session_id,
            vec![ContentBlock::Text(TextContent::new(prompt))],
        ));
        if let Some(event) = accepted_event {
            self.updates.push_event(event);
        }

        // Resolve the permitted outcomes, hard-bounded by the turn budget:
        //   (1) the canonical PromptResponse/StopReason,
        //   (2) stream quiescence after content (the idle-bridge turn-end),
        //   (3) the overall ceiling (neither happened → error, never hang).
        let timeout = turn_timeout();
        let quiescence = quiescence_window();
        let mut prompt_response = Box::pin(prompt_turn.block_task());
        let outcome = tokio::time::timeout(timeout, async {
            if require_terminal_response
                && self
                    .harness
                    .as_ref()
                    .is_some_and(|h| h.as_str() == "hermes")
            {
                tokio::select! {
                    // Canonical Hermes completion remains preferred when available.
                    response = &mut prompt_response => {
                        let deferred = response.as_ref().is_ok_and(|response| {
                            response.usage.is_none() && activity.has_hermes_busy_queue_ack()
                        });
                        if deferred {
                            // Hermes acknowledged only its volatile busy-session queue. Keep the
                            // Nexus obligation unsettled until Hermes emits the exact user prompt
                            // at queue promotion, then require new model output after that causal
                            // boundary. A daemon restart before promotion cancels this wait and the
                            // durable obligation is replayed into the resurrected harness.
                            let promoted_at = wait_for_prompt_promotion(
                                &activity,
                                &expected_prompt,
                            ).await;
                            wait_for_model_quiescence(
                                &activity,
                                promoted_at,
                                quiescence,
                            ).await;
                            TurnEnd::Quiescent
                        } else {
                            TurnEnd::Response(response)
                        }
                    },
                    // Hermes 0.17 can render the completed model turn yet never answer
                    // `session/prompt`. A real model update followed by silence proves that the
                    // recipient processed the delivery. Synthetic accepted-input does not advance
                    // `model_events`, so an empty/provider-rejected turn cannot take this path.
                    () = wait_for_model_quiescence(
                        &activity,
                        model_events_before,
                        quiescence,
                    ) => TurnEnd::Quiescent,
                }
            } else if require_terminal_response {
                // Other durable bus deliveries require the authoritative ACP terminal response.
                // The ACP bridge has one bounded handoff exception: after cancel + replace it
                // can append the replacement prompt, answer the cancelled request with a machine
                // diagnostic, then stream the replacement model turn. Never retry that already-
                // appended prompt; require a new real model event and quiescence instead.
                let response = prompt_response.await;
                if response
                    .as_ref()
                    .err()
                    .is_some_and(is_interrupted_prompt_handoff)
                {
                    wait_for_model_quiescence(&activity, model_events_before, quiescence).await;
                    TurnEnd::InterruptedPromptHandoff
                } else {
                    TurnEnd::Response(response)
                }
            } else {
                tokio::select! {
                    // (1) Canonical: the prompt request was answered. This is the authoritative end.
                    response = &mut prompt_response => TurnEnd::Response(response),
                    // (2) Legacy/direct-drive fallback: content streamed, then the bridge fell
                    // silent. Never used by the observed durable bus-delivery seam.
                    () = wait_for_quiescence(&activity, quiescence) => TurnEnd::Quiescent,
                }
            }
        })
        .await;

        let buffered = activity.buffered_len();
        let model_events = activity.model_events().saturating_sub(model_events_before);
        match outcome {
            // Hard ceiling tripped. Fail the turn so the caller re-parks instead of wedging here
            // forever; durable delivery must never infer success from buffered content.
            Err(_) => {
                warn!(
                    target: "nexus_agent::acp",
                    acp_session = %acp_session,
                    timeout_secs = timeout.as_secs(),
                    buffered_chunks = buffered,
                    require_terminal_response,
                    "ACP session/prompt timed out without permitted turn-completion evidence"
                );
                Err(AdapterInjectError::CompletionTimeout {
                    origin: acp_completion_source(self.harness.as_ref()),
                })
            }
            // (1) Canonical turn-end.
            Ok(TurnEnd::Response(Ok(response))) => {
                let provider_input_observed = response
                    .usage
                    .as_ref()
                    .is_some_and(|usage| usage.input_tokens > 0);
                // Hermes 0.17 returns EndTurn even when its provider rejected the request before
                // processing input. For durable bus delivery that is false success: ACP accepted
                // the prompt, but the recipient model never processed it. A model update proves a
                // visible response; input-token usage proves a legitimate silent turn. With
                // neither signal, fail closed so the in-flight row becomes a terminal
                // operator-action error and is never retried automatically. Direct/legacy drive
                // keeps its existing empty-turn behavior.
                if require_terminal_response
                    && self
                        .harness
                        .as_ref()
                        .is_some_and(|h| h.as_str() == "hermes")
                    && model_events == 0
                    && !provider_input_observed
                {
                    warn!(
                        target: "nexus_agent::acp",
                        acp_session = %acp_session,
                        stop_reason = ?response.stop_reason,
                        "Hermes ACP returned EndTurn without a model update; refusing false delivery"
                    );
                    return Err(AdapterInjectError::OperatorAction(AdapterOperatorAction {
                        harness: HarnessId::new("hermes").expect("builtin harness id is valid"),
                        reason: "Hermes ACP completed without model output; provider completion was not observed"
                            .to_string(),
                        provider: None,
                        model: None,
                        source: "hermes.acp.empty_completion".to_string(),
                    }));
                }
                info!(
                    target: "nexus_agent::acp",
                    acp_session = %acp_session,
                    stop_reason = ?response.stop_reason,
                    buffered_chunks = buffered,
                    model_events,
                    provider_input_observed,
                    "ACP turn-end received (StopReason); reply stream complete"
                );
                Ok(())
            }
            Ok(TurnEnd::Response(Err(e))) => {
                warn!(target: "nexus_agent::acp", acp_session = %acp_session, error = %e, "ACP session/prompt failed");
                if let Some(harness) = self.harness.clone() {
                    let source = format!("{harness}.acp.prompt_error");
                    if let Some(classified) = classify_acp_prompt_error(harness, &e, &source) {
                        return Err(classified);
                    }
                }
                Err(NexusError::Adapter(format!("session/prompt failed: {e}")).into())
            }
            // (2) Quiescence turn-end: the bridge streamed the reply and went idle without ever
            // answering session/prompt. For observed Hermes delivery, the waiter already proved
            // that at least one real model event arrived; direct calls retain legacy behavior.
            Ok(TurnEnd::Quiescent) => {
                info!(
                    target: "nexus_agent::acp",
                    acp_session = %acp_session,
                    buffered_chunks = buffered,
                    quiescence_ms = quiescence.as_millis() as u64,
                    "ACP turn-end inferred from stream quiescence (no PromptResponse; bridge went \
                     idle after streaming the reply)"
                );
                Ok(())
            }
            Ok(TurnEnd::InterruptedPromptHandoff) => {
                info!(
                    target: "nexus_agent::acp",
                    acp_session = %acp_session,
                    buffered_chunks = buffered,
                    model_events,
                    quiescence_ms = quiescence.as_millis() as u64,
                    "ACP replacement handoff completed after causal model output"
                );
                Ok(())
            }
        }
    }

    /// Drain the [`StreamEvent`]s captured during the most recent [`AcpEngine::inject`], in order.
    /// Called by [`super::Adapter::stream_updates`] right after `inject` returns (turn-end already
    /// observed), so the buffer holds the complete, ordered stream for the turn.
    pub fn take_updates(&self) -> Vec<StreamEvent> {
        let events = self.updates.take();
        debug!(
            target: "nexus_agent::acp",
            events = events.len(),
            "draining captured turn updates for relay"
        );
        events
    }
}

fn acp_completion_source(harness: Option<&HarnessId>) -> String {
    let harness = harness.map(|h| h.as_str()).unwrap_or("other");
    format!("{harness}.acp.turn_completion")
}

/// Resolve an optional cwd string to an absolute path for ACP `cwd` fields (which must be a
/// path). Falls back to the process cwd, then `/`.
fn resolve_cwd(cwd: Option<&str>) -> std::path::PathBuf {
    cwd.map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| "/".into()))
}

/// Build a [`NewSessionRequest`] for the given working directory, optionally injecting the nexus
/// internal MCP server when bus identity is present in `ctx`.
///
/// When `ctx.bus_name`/`bus_project` are both `Some`, a single stdio MCP server is
/// appended whose command is the current `nexus` executable (falling back to the literal string
/// `"nexus"`) and whose args are:
///
/// ```text
/// mcp --as <name> --project <project> [--client-key <key>] [--agent <agent>]
/// ```
///
/// This wires the bus tools into every ACP `session/new` request so launched agents always have
/// the nexus bus available over the standard MCP channel.
pub fn build_new_session_request(cwd: Option<&str>, ctx: &LaunchCtx) -> NewSessionRequest {
    let root = resolve_cwd(cwd);
    NewSessionRequest::new(root).mcp_servers(bus_mcp_servers(ctx))
}

/// Build `session/load` with the same launch-scoped Nexus MCP identity as `session/new`.
/// ACP treats MCP servers as session-setup data rather than durable provider state, so omitting
/// them on resume leaves the loaded model context pointing at a dead pre-restart MCP process.
pub fn build_load_session_request(
    resume_key: &str,
    cwd: Option<&str>,
    ctx: &LaunchCtx,
) -> LoadSessionRequest {
    LoadSessionRequest::new(SessionId::new(resume_key), resolve_cwd(cwd))
        .mcp_servers(bus_mcp_servers(ctx))
}

fn bus_mcp_servers(ctx: &LaunchCtx) -> Vec<SchemaMcpServer> {
    let mut servers = Vec::new();
    if !ctx.suppress_acp_mcp {
        if let (Some(name), Some(project)) = (&ctx.bus_name, &ctx.bus_project) {
            let nexus_exe =
                std::env::current_exe().unwrap_or_else(|_| std::path::PathBuf::from("nexus"));

            let mut args = vec![
                "mcp".to_string(),
                "--as".to_string(),
                name.clone(),
                "--project".to_string(),
                project.clone(),
            ];
            if let Some(key) = &ctx.bus_client_key {
                args.push("--client-key".to_string());
                args.push(key.clone());
            }
            if let Some(agent) = &ctx.bus_agent {
                args.push("--agent".to_string());
                args.push(agent.clone());
            }

            let stdio = McpServerStdio::new("nexus-bus", nexus_exe)
                .args(args)
                .env(mcp_store_env(ctx));

            servers.push(SchemaMcpServer::Stdio(stdio));
        }
    }
    servers
}

/// Daemon discovery must be explicit on the ACP MCP descriptor. Some ACP bridges launch MCP
/// children from a clean environment instead of inheriting the harness process environment.
/// Durable-store coordinates are intentionally absent: MCP reaches the sole store owner over IPC.
/// The optional Tokio worker cap is resource policy, not identity or store authority, and must
/// survive bridges that intentionally sanitize their MCP child environment.
fn mcp_store_env(ctx: &LaunchCtx) -> Vec<EnvVariable> {
    ctx.env
        .iter()
        .filter(|(key, _)| {
            matches!(
                key.as_str(),
                "NEXUS_HOME" | "NEXUS_NO_AUTOSTART" | "TOKIO_WORKER_THREADS"
            )
        })
        .map(|(key, value)| EnvVariable::new(key.clone(), value.clone()))
        .collect()
}

fn scrub_inherited_identity_env(command: &mut Command) {
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
            "CLAUDE_CONFIG_DIR"
                | "HERMES_HOME"
                | "OPENCODE_HOME"
                | "OPENCODE_CONFIG_DIR"
                | "OPENCODE_CONFIG_CONTENT"
                | "OPENCODE_DB"
        )
}

/// Forward harness stderr lines to the tracing log (best-effort).
async fn drain_stderr(stderr: tokio::process::ChildStderr) {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            warn!(target: "nexus_agent::harness", stderr = trimmed, "harness stderr");
        }
    }
}

/// The SDK background task: register the `session/update` + permission handlers, run the
/// `initialize` handshake, publish the connection handle, then keep the connection alive until
/// the engine drops the shutdown sender. The child is shared with the engine via an
/// `Arc<Mutex<Option<Child>>>` so `AcpEngine::kill()` can reach it; this task takes the child
/// out at teardown to perform the final `wait()` / cleanup.
///
/// The `kill_signal_rx` oneshot is fired by [`AcpEngine::kill`] to request process-tree
/// termination and carries the completion acknowledgement. This task is the exclusive owner of
/// the `Child` after it calls `child_arc.take()`, so it is the only place that can safely wait on
/// the child while also signalling and verifying the whole process group.
#[allow(clippy::too_many_arguments)]
async fn run_connection(
    stdin: tokio::process::ChildStdin,
    stdout: tokio::process::ChildStdout,
    child_arc: Arc<Mutex<Option<tokio::process::Child>>>,
    updates: Arc<TurnActivity>,
    init_tx: oneshot::Sender<Result<(), NexusError>>,
    ready_tx: oneshot::Sender<ConnectionTo<Agent>>,
    shutdown_rx: oneshot::Receiver<()>,
    closed_tx: oneshot::Sender<()>,
    kill_signal_rx: oneshot::Receiver<oneshot::Sender<()>>,
) {
    let transport = agent_client_protocol::ByteStreams::new(stdin.compat_write(), stdout.compat());

    let mut init_tx = Some(init_tx);
    let mut ready_tx = Some(ready_tx);
    let mut shutdown_rx = Some(shutdown_rx);

    let connect = Client
        .builder()
        .name("nexus-agent")
        .on_receive_notification(
            {
                let updates = Arc::clone(&updates);
                async move |notification: SessionNotification, _cx: ConnectionTo<Agent>| {
                    // Full-stream pass-through (backend plan 50): translate EVERY `session/update`
                    // and buffer every renderable one — text, thinking, tool calls, plans, the
                    // harness's available commands — no AgentMessageChunk-only drop. A
                    // non-renderable book-keeping update (mode/config/session-info/usage, or a
                    // user-echo) is not buffered, but `ingest` still bumps stream activity for it:
                    // a trailing `usage_update` after the reply means the bridge is alive, so it
                    // resets the quiescence window — otherwise we could declare turn-end mid-stream.
                    // Turn-end proper is the `session/prompt` response's StopReason (or, for bridges
                    // that never send it, quiescence once content has streamed).
                    updates.ingest(&notification.update);
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .on_receive_request(
            {
                // Auto-approve permission requests so an unattended turn never wedges. The bus is
                // the trust boundary in Nexus; the harness runs in its own sandbox.
                async move |request: RequestPermissionRequest, responder, _cx| {
                    let option_id = request.options.first().map(|opt| opt.option_id.clone());
                    let outcome = match option_id {
                        Some(id) => {
                            RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(id))
                        }
                        None => RequestPermissionOutcome::Cancelled,
                    };
                    responder.respond(RequestPermissionResponse::new(outcome))
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_with(transport, async move |connection: ConnectionTo<Agent>| {
            // Step 1 — initialize handshake (the canonical place to call block_task).
            let init = connection
                .send_request(InitializeRequest::new(ProtocolVersion::LATEST))
                .block_task()
                .await;

            let Some(tx) = init_tx.take() else {
                return Ok(());
            };
            match init {
                Ok(_) => {
                    let _ = tx.send(Ok(()));
                }
                Err(e) => {
                    let _ = tx.send(Err(NexusError::Adapter(format!(
                        "ACP initialize failed: {e}"
                    ))));
                    return Ok(());
                }
            }

            // Step 2 — publish the connection handle so the engine can issue requests.
            if let Some(tx) = ready_tx.take() {
                if tx.send(connection).is_err() {
                    return Ok(());
                }
            }

            // Step 3 — park until the engine is dropped.
            if let Some(rx) = shutdown_rx.take() {
                let _ = rx.await;
            }
            Ok(())
        });

    // Take the child out of the Arc for this task's exclusive use (wait + final kill). After this
    // point the engine's Arc-based kill() path will see `None`. The primary kill path is the
    // kill_signal_rx branch below: engine.kill() fires the oneshot; we receive it here and
    // terminate the owned child's process group. The Arc backstop in kill() covers the narrow
    // window before this take() runs (init handshake phase).
    let mut child = child_arc.lock().unwrap().take();

    // Race four outcomes:
    //   1. SDK connection ends normally (shutdown signal from engine drop).
    //   2. The harness process exits on its own (pipe EOFs don't always surface in the SDK loop).
    //   3. engine.kill() fires the kill signal → terminate the owned child's process group.
    //   4. (None arm) Child already killed before this task took it → just drain the SDK conn.
    match child {
        Some(ref mut c) => {
            tokio::select! {
                result = connect => {
                    if let Err(e) = result {
                        debug!(error = %e, "ACP connection closed with error");
                    }
                    terminate_child_process_tree(c, PROCESS_TREE_TERM_GRACE, "connection closed").await;
                }
                status = c.wait() => {
                    debug!(?status, "harness process exited; ending ACP connection");
                }
                completion = kill_signal_rx => {
                    // engine.kill() was called. Terminate the whole process group so wrapper
                    // descendants cannot re-parent to init and survive the direct child.
                    terminate_child_process_tree(c, PROCESS_TREE_TERM_GRACE, "kill signal").await;
                    if let Ok(completion) = completion {
                        let _ = completion.send(());
                    }
                }
            }
        }
        None => {
            // Child was already killed (engine.kill() fired before this task ran, and the Arc
            // backstop already reaped the process group). Just drive the connection to completion so the
            // SDK actors shut down cleanly. The kill_signal_rx will fire again if kill() is called
            // a second time — ignore it via fuse (drop the receiver so it's a no-op).
            drop(kill_signal_rx);
            if let Err(e) = connect.await {
                debug!(error = %e, "ACP connection closed with error (no child)");
            }
        }
    }

    // Signal that the connection has ended. During the launch race this fails the handshake
    // fast (harness crashed on startup); after a healthy launch the receiver is long gone and
    // this is a harmless no-op.
    let _ = closed_tx.send(());
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::{ContentChunk, TextContent, ToolCall, UsageUpdate};
    use nexus_contracts::AgentUpdateKind;

    fn text_chunk(s: &str) -> SessionUpdate {
        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(TextContent::new(s))))
    }
    fn thought_chunk(s: &str) -> SessionUpdate {
        SessionUpdate::AgentThoughtChunk(ContentChunk::new(ContentBlock::Text(TextContent::new(s))))
    }

    /// A scripted turn (thought → tool_call → text) ingested by the notification handler buffers
    /// **three** ordered `StreamEvent`s with kinds Thinking, ToolCall, Text — the full stream, no
    /// AgentMessageChunk-only drop.
    #[test]
    fn ingest_buffers_full_stream_in_order() {
        let activity = TurnActivity::new();
        activity.ingest(&thought_chunk("hmm"));
        activity.ingest(&SessionUpdate::ToolCall(ToolCall::new("tc_1", "Read")));
        activity.ingest(&text_chunk("done"));

        let drained = activity.take();
        let kinds: Vec<AgentUpdateKind> = drained.iter().map(|e| e.kind).collect();
        assert_eq!(
            kinds,
            vec![
                AgentUpdateKind::Thinking,
                AgentUpdateKind::ToolCall,
                AgentUpdateKind::Text
            ]
        );
        assert_eq!(drained[0].data["text"], "hmm");
        assert_eq!(drained[1].data["id"], "tc_1");
        assert_eq!(drained[2].data["text"], "done");
    }

    /// A non-renderable update (a trailing `usage_update`) is NOT buffered, but it still bumps
    /// stream activity (quiescence book-keeping) — the engine must keep the stream alive for every
    /// notification, renderable or not.
    #[test]
    fn ingest_non_renderable_bumps_activity_without_buffering() {
        let activity = TurnActivity::new();
        let before = activity.activity();
        activity.ingest(&SessionUpdate::UsageUpdate(UsageUpdate::new(0, 0)));
        assert!(
            activity.activity() > before,
            "every notification bumps activity"
        );
        assert_eq!(
            activity.buffered_len(),
            0,
            "a non-renderable update is not buffered"
        );
    }

    #[test]
    fn inherited_opencode_state_env_is_scrubbed() {
        for key in [
            "OPENCODE_HOME",
            "OPENCODE_CONFIG_DIR",
            "OPENCODE_CONFIG_CONTENT",
            "OPENCODE_DB",
        ] {
            assert!(
                should_scrub_inherited_env(std::ffi::OsStr::new(key)),
                "{key} must not leak into daemon-owned OpenCode launches"
            );
        }
    }

    #[test]
    fn orphan_sweep_candidate_requires_nexus_key_orphan_parent_and_acp_command() {
        let snapshot = ProcSnapshot {
            pid: 42,
            ppid: 1,
            pgrp: 42,
            cmdline: "node /tmp/codex-acp/server.js".to_string(),
            environ: b"NEXUS_CLIENT_KEY=ck_test\0NEXUS_NAME=agent\0".to_vec(),
            parent_cmdline: None,
        };

        assert!(is_orphaned_nexus_acp_process(&snapshot));

        let mut without_key = snapshot.clone();
        without_key.environ = b"NEXUS_NAME=agent\0".to_vec();
        assert!(!is_orphaned_nexus_acp_process(&without_key));

        let mut supervised = snapshot.clone();
        supervised.ppid = 1234;
        supervised.parent_cmdline = Some("nexus daemon run".to_string());
        assert!(!is_orphaned_nexus_acp_process(&supervised));
    }

    #[test]
    fn orphan_sweep_excludes_durable_app_server_processes() {
        let snapshot = ProcSnapshot {
            pid: 99,
            ppid: 1,
            pgrp: 99,
            cmdline: "codex app-server --listen unix:///tmp/codex.sock".to_string(),
            environ: b"NEXUS_CLIENT_KEY=ck_test\0NEXUS_NAME=agent\0".to_vec(),
            parent_cmdline: None,
        };

        assert!(
            !is_orphaned_nexus_acp_process(&snapshot),
            "durable app-server processes are adopted after restart, not swept"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn proc_stat_parser_handles_comm_with_spaces() {
        let stat = "1234 (fake acp agent) S 1 1234 1234 0 -1 4194560";
        assert_eq!(parse_proc_stat_ids(stat), Some((1, 1234)));
    }
}
