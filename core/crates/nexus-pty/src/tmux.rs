//! [`TmuxHarness`] — run a native harness (claude/codex) inside a detached **tmux** session and
//! deliver turns into it. A raw daemon-owned PTY makes claude's interactive TUI submit an injected
//! turn but never COMPLETE it; running claude inside tmux (a real terminal emulator) makes the
//! unattended turn complete (proven by a spike). This is the production delivery backend.
//!
//! [`send_turn`](TmuxHarness::send_turn) ports omnigent's `inject_user_message` sequence
//! (`~/Projects/Research/omnigent/omnigent/claude_native_bridge.py`) faithfully, because those
//! nuances are load-bearing:
//!   1. Wait for the tmux session to exist, then for claude's input box to render (poll
//!      `capture-pane` for the prompt glyph) — keystrokes sent into the boot gap are dropped.
//!   2. Clear any stale draft: `send-keys C-a` (Home) then `C-k` (kill-to-end).
//!   3. Deliver the payload via `load-buffer` (a temp file) + `paste-buffer -p` (bracketed paste),
//!      NOT `send-keys` argv — tmux caps a single client→server command at ~16KB. Interior `\n`
//!      become CR so the TUI keeps the paste as data, and a trailing newline absorbs a trailing `\`.
//!   4. Verified submit: poll `capture-pane` until the draft is visible (paste committed), send
//!      `Enter`, then poll that the draft left the box — re-sending `Enter` while it hasn't — and
//!      fail loud if it never submits.

use std::io::Read as _;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use tokio::sync::broadcast;

// ── Tunables (ported from omnigent's claude_native_bridge constants) ──────────────────────────────

/// Per-`tmux` subprocess timeout. omnigent: `_TMUX_SEND_TIMEOUT_S`.
const TMUX_CMD_TIMEOUT: Duration = Duration::from_secs(5);
/// How long to wait for the input box to render before injecting. omnigent: `_TMUX_READY_TIMEOUT_S`.
const PROMPT_READY_TIMEOUT: Duration = Duration::from_secs(30);
/// Poll cadence while waiting on readiness / draft visibility. omnigent: `_CLAUDE_READY_POLL_INTERVAL_S`.
const READY_POLL_INTERVAL: Duration = Duration::from_millis(150);
/// Let the TUI commit a paste before the separate submit Enter. omnigent: `_PASTE_SETTLE_S`.
const PASTE_SETTLE: Duration = Duration::from_millis(100);
/// How long to wait for the pasted draft to land in the input box. omnigent: `_PASTE_COMMIT_TIMEOUT_S`.
const PASTE_COMMIT_TIMEOUT: Duration = Duration::from_secs(5);
/// After the submit Enter, how long to keep verifying the draft left the box. omnigent: `_SUBMIT_VERIFY_TIMEOUT_S`.
const SUBMIT_VERIFY_TIMEOUT: Duration = Duration::from_secs(10);
/// Minimum spacing between repeated submit Enters during verification. omnigent: `_SUBMIT_RETRY_INTERVAL_S`.
const SUBMIT_RETRY_INTERVAL: Duration = Duration::from_secs(1);
/// claude renders this glyph in its input box once interactive. omnigent: `_CLAUDE_PROMPT_GLYPH`.
const CLAUDE_PROMPT_GLYPH: &str = "❯";
/// Trailing non-empty lines to scan for the prompt glyph. Claude can leave several footer/status
/// lines after compaction, so the tail must be wide enough to catch the live input box without
/// treating old scrollback as ready.
/// omnigent: `_PROMPT_SCAN_TAIL_LINES` (widened for Nexus compaction recovery).
const PROMPT_SCAN_TAIL_LINES: usize = 12;
/// claude collapses large pastes into this placeholder in the input box. omnigent: `_PASTED_PLACEHOLDER_PREFIX`.
const PASTED_PLACEHOLDER_PREFIX: &str = "[Pasted text";
/// Chars of the draft's first line to match against the captured pane. omnigent: `_DRAFT_NEEDLE_MAX_CHARS`.
const DRAFT_NEEDLE_MAX_CHARS: usize = 24;
/// The tmux buffer name our paste rides in.
const PASTE_BUFFER: &str = "nexus-paste";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TuiSubmitStrategy {
    Claude,
}

impl TuiSubmitStrategy {
    fn prompt_rendered(self, pane: &str) -> bool {
        match self {
            Self::Claude => claude_prompt_rendered(pane),
        }
    }

    fn submit_needle(self, content: &str) -> String {
        match self {
            Self::Claude => claude_submit_needle(content),
        }
    }

    fn draft_in_input_box(self, pane: &str, needle: &str) -> bool {
        match self {
            Self::Claude => claude_draft_in_input_box(pane, needle),
        }
    }
}

struct PipeRelay {
    tx: broadcast::Sender<Vec<u8>>,
    stop: Arc<AtomicBool>,
    path: PathBuf,
}

impl PipeRelay {
    fn subscribe(&self) -> broadcast::Receiver<Vec<u8>> {
        self.tx.subscribe()
    }
}

impl Drop for PipeRelay {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Clone)]
struct TmuxTerminal {
    /// `tmux` binary (resolved from PATH at launch, e.g. `/home/<user>/.local/bin/tmux`).
    tmux_bin: String,
    /// Per-session private socket path (`-S`), so sessions never collide on the default server and
    /// teardown can't take out an unrelated tmux. Mirrors omnigent's per-terminal socket.
    socket: PathBuf,
    /// tmux session name (`-s`), also the `has-session`/`kill-session` target.
    session_name: String,
    /// Pane target for send/capture, e.g. `nexus-<id>:0.0`.
    target: String,
    /// Where temp paste files are written (the harness cwd; private + always present).
    scratch_dir: PathBuf,
    /// Shared pane-output stream for terminal/web attachments. `tmux pipe-pane -o` only attaches
    /// the first pipe; cache the broadcast sender so repeated terminal attaches subscribe to the
    /// same relay instead of creating dead side files.
    pipe_relay: Arc<Mutex<Option<PipeRelay>>>,
}

/// A native harness running inside a detached tmux session on a per-session private socket.
///
/// Composes a tmux terminal backend with a harness-specific TUI submit/readiness strategy. The
/// operator attaches by running [`attach_argv`](TmuxHarness::attach_argv) (a real
/// `tmux … attach -t <session>`), not through the daemon PTY WS.
#[derive(Clone)]
pub struct TmuxHarness {
    terminal: TmuxTerminal,
    submit_strategy: TuiSubmitStrategy,
}

impl TmuxHarness {
    /// Launch `program args…` inside a fresh detached tmux session sized `w`×`h`, in `cwd`.
    ///
    /// The pane runs `cd <cwd> && exec <program> <args…>` so the harness replaces the shell (a clean
    /// process, and the pane dies with it). On a per-session private socket (`-S`). Returns once
    /// `tmux new-session` succeeds; readiness (input box rendered) is awaited lazily on the first
    /// [`send_turn`].
    pub fn launch(
        session_name: &str,
        program: &str,
        args: &[String],
        cwd: &str,
        w: u16,
        h: u16,
    ) -> Result<TmuxHarness, String> {
        Self::launch_with_env(session_name, program, args, cwd, w, h, &[])
    }

    /// Launch `program args…` in tmux with explicit per-runtime environment overrides.
    ///
    /// The pane first unsets inherited Nexus identity and harness-home variables, then exports the
    /// supplied overrides. This prevents a headed harness from inheriting another agent's
    /// `NEXUS_*`/`CODEX_*` state from the daemon or operator shell.
    pub fn launch_with_env(
        session_name: &str,
        program: &str,
        args: &[String],
        cwd: &str,
        w: u16,
        h: u16,
        overrides: &[(String, String)],
    ) -> Result<TmuxHarness, String> {
        let tmux_bin = resolve_tmux();
        // Private socket beside the harness cwd-derived scratch: unique per session name.
        let socket = std::env::temp_dir().join(format!("nexus-tmux-{session_name}.sock"));
        let scratch_dir = PathBuf::from(cwd);

        // Build the in-pane command. `exec` so the harness is the pane's process (no lingering
        // shell), and each argv element is single-quote-escaped so flag values with JSON/spaces
        // survive the shell.
        let mut env = tmux_headed_env_defaults();
        env.extend_from_slice(overrides);
        let shell_cmd = tmux_launch_shell_command(program, args, cwd, &env);

        let out = Command::new(&tmux_bin)
            .args(tmux_new_session_args(
                &socket,
                session_name,
                w,
                h,
                &shell_cmd,
            ))
            .output()
            .map_err(|e| format!("tmux new-session spawn failed: {e}"))?;
        if !out.status.success() {
            let detail = String::from_utf8_lossy(&out.stderr);
            return Err(format!("tmux new-session failed: {}", detail.trim()));
        }

        // Resolve the REAL pane target. The window/pane indices are NOT necessarily `0.0`: a user's
        // tmux.conf may set `base-index`/`pane-base-index` to 1 (observed on this host → `<sess>:1.1`),
        // and a hardcoded `:0.0` makes every `capture-pane`/`send-keys` fail with "can't find window",
        // so readiness never resolves and nothing is injected. Ask tmux for the canonical target.
        let target = resolve_pane_target(&tmux_bin, &socket, session_name)?;

        Ok(TmuxHarness {
            terminal: TmuxTerminal {
                tmux_bin,
                socket,
                session_name: session_name.to_string(),
                target,
                scratch_dir,
                pipe_relay: Arc::new(Mutex::new(None)),
            },
            submit_strategy: TuiSubmitStrategy::Claude,
        })
    }

    /// Adopt an already-running Nexus tmux harness after a daemon restart.
    ///
    /// Launches use a deterministic private socket path and session name, so the new daemon can
    /// reconstruct the small [`TmuxHarness`] handle without relaunching or touching the pane
    /// process. This restores turn injection for live headed sessions that outlived the daemon.
    pub fn adopt(session_name: &str, cwd: &str) -> Result<TmuxHarness, String> {
        let tmux_bin = resolve_tmux();
        let socket = std::env::temp_dir().join(format!("nexus-tmux-{session_name}.sock"));
        let socket_s = socket.to_string_lossy().into_owned();
        let out = Command::new(&tmux_bin)
            .args(["-S", &socket_s, "has-session", "-t", session_name])
            .output()
            .map_err(|e| format!("tmux has-session spawn failed: {e}"))?;
        if !out.status.success() {
            let detail = String::from_utf8_lossy(&out.stderr);
            return Err(format!(
                "tmux session {session_name} is not running on {socket_s}: {}",
                detail.trim()
            ));
        }
        let target = resolve_pane_target(&tmux_bin, &socket, session_name)?;
        Ok(TmuxHarness {
            terminal: TmuxTerminal {
                tmux_bin,
                socket,
                session_name: session_name.to_string(),
                target,
                scratch_dir: PathBuf::from(cwd),
                pipe_relay: Arc::new(Mutex::new(None)),
            },
            submit_strategy: TuiSubmitStrategy::Claude,
        })
    }

    /// The `tmux … attach -t <session>` argv an operator (or `nexus attach`) runs to take over the
    /// harness's real terminal. Carries the private socket so it finds the right server.
    pub fn attach_argv(&self) -> Vec<String> {
        self.terminal.attach_argv()
    }

    /// Private tmux socket path for this harness.
    pub fn socket_path(&self) -> &std::path::Path {
        self.terminal.socket_path()
    }

    /// Private tmux session name for this harness.
    pub fn session_name(&self) -> &str {
        self.terminal.session_name()
    }

    /// Whether the tmux session still exists (`has-session` exits 0).
    pub fn has_session(&self) -> bool {
        self.terminal.has_session()
    }

    /// Return the PID of the process currently attached to the harness pane.
    ///
    /// This is the root process Nexus launched with `exec <program> ...`; callers combine it with
    /// `/proc/<pid>/stat` to persist an exact process ledger for crash-time orphan cleanup.
    pub fn pane_pid(&self) -> Option<u32> {
        self.terminal.pane_pid()
    }

    /// The current visible contents of the harness pane (`capture-pane -p`). Never errors — a
    /// transient failure during boot means "not ready yet", returned as `""`.
    pub fn capture_pane(&self) -> String {
        self.terminal.capture_pane()
    }

    /// Stream the pane's raw PTY output as a `broadcast::Receiver<Vec<u8>>`.
    ///
    /// Uses `tmux pipe-pane -o` to hook the pane's raw output (ANSI sequences included — exactly
    /// what [`nexus_pty::screen_text::ScreenText`] expects) into a temp output file. A dedicated
    /// thread reads that file incrementally (blocking `read`, re-reading on zero-byte returns to
    /// simulate tail-follow) and sends each chunk into a `broadcast::channel`. Callers subscribe
    /// with `rx = pipe_output()` and pass the receiver to `spawn_pty_reply_reader`.
    ///
    /// The temp file is named `nexus-pipe-<session>-<nanos>.bin` and placed in `scratch_dir`
    /// (the harness cwd). It accumulates pane output for the session's lifetime; the relay task
    /// stays alive for later web-terminal reconnects.
    ///
    /// `pipe-pane -o` toggles piping on, so this method installs it once and returns fresh
    /// subscribers from the cached broadcast sender on later calls.
    pub fn pipe_output(&self) -> broadcast::Receiver<Vec<u8>> {
        self.terminal.pipe_output()
    }

    /// Write raw terminal-client bytes into the tmux pane.
    ///
    /// This is for interactive terminal bytes, not Nexus turn injection. Printable runs are sent as
    /// literals; common control sequences are mapped to tmux key names so xterm.js basics work
    /// without the Claude-specific prompt-readiness machinery.
    pub fn write_terminal_bytes(&self, bytes: &[u8]) -> Result<(), String> {
        self.terminal.write_terminal_bytes(bytes)
    }

    /// Resize the tmux window that owns this harness pane.
    pub fn resize_terminal(&self, cols: u16, rows: u16) -> Result<(), String> {
        self.terminal.resize_terminal(cols, rows)
    }

    /// Hard-stop: kill the tmux session (terminates the harness and the pane).
    pub fn kill(&self) -> Result<(), String> {
        self.terminal.kill()
    }

    // ── delivery (omnigent inject_user_message port) ─────────────────────────────────────────────

    /// Deliver `content` as one verified, submitted turn. See the module docs for the sequence.
    pub(crate) fn inject(&self, content: &str) -> Result<(), String> {
        // (1) Wait for claude's input box to render. Keystrokes typed into the boot gap are dropped.
        self.wait_for_prompt_ready()?;

        // (2) Clear any leftover draft: C-a (Home) then C-k (kill-to-end). C-u only clears backwards.
        self.terminal
            .run_tmux(&["send-keys", "-t", &self.terminal.target, "C-a"])?;
        self.terminal
            .run_tmux(&["send-keys", "-t", &self.terminal.target, "C-k"])?;

        // (3) Deliver via a file-backed tmux buffer + bracketed paste (NOT send-keys argv — ~16KB
        //     cap). Trailing "\n" absorbs a trailing "\" so it can't escape the submit Enter.
        let payload = paste_payload_bytes(&format!("{content}\n"));
        let paste_path = self.write_paste_file(&payload)?;
        let paste_path_str = paste_path.to_string_lossy().into_owned();
        let load = self
            .terminal
            .run_tmux(&["load-buffer", "-b", PASTE_BUFFER, &paste_path_str]);
        let paste = load.and_then(|()| {
            self.terminal.run_tmux(&[
                "paste-buffer",
                "-p", // bracketed-paste markers — the TUI keeps newlines as data
                "-d", // drop the buffer after pasting (no stale server-side copies)
                "-b",
                PASTE_BUFFER,
                "-t",
                &self.terminal.target,
            ])
        });
        let _ = std::fs::remove_file(&paste_path);
        paste?;

        // (4) Verified submit. Wait until the draft is visibly committed before sending Enter, then
        //     verify it left the box (re-sending Enter while it hasn't), else fail loud.
        let needle = self.submit_strategy.submit_needle(content);
        let mut draft_seen = false;
        let deadline = Instant::now() + PASTE_COMMIT_TIMEOUT;
        while Instant::now() < deadline {
            if self
                .submit_strategy
                .draft_in_input_box(&self.capture_pane(), &needle)
            {
                draft_seen = true;
                break;
            }
            std::thread::sleep(READY_POLL_INTERVAL);
        }
        std::thread::sleep(PASTE_SETTLE);
        self.terminal
            .run_tmux(&["send-keys", "-t", &self.terminal.target, "Enter"])?;
        if !draft_seen {
            // The draft was never observed, so its absence proves nothing — submit blind as before.
            return Ok(());
        }
        let deadline = Instant::now() + SUBMIT_VERIFY_TIMEOUT;
        let mut last_enter = Instant::now();
        while Instant::now() < deadline {
            std::thread::sleep(READY_POLL_INTERVAL);
            if !self
                .submit_strategy
                .draft_in_input_box(&self.capture_pane(), &needle)
            {
                return Ok(());
            }
            if last_enter.elapsed() >= SUBMIT_RETRY_INTERVAL {
                self.terminal
                    .run_tmux(&["send-keys", "-t", &self.terminal.target, "Enter"])?;
                last_enter = Instant::now();
            }
        }
        Err(format!(
            "harness did not accept the submitted turn within {}s (draft still in the input box); \
             the message was not delivered",
            SUBMIT_VERIFY_TIMEOUT.as_secs()
        ))
    }

    /// Block until the harness's input box is ready for keystrokes (prompt glyph in the pane tail).
    fn wait_for_prompt_ready(&self) -> Result<(), String> {
        let deadline = Instant::now() + PROMPT_READY_TIMEOUT;
        while Instant::now() < deadline {
            if self.submit_strategy.prompt_rendered(&self.capture_pane()) {
                return Ok(());
            }
            std::thread::sleep(READY_POLL_INTERVAL);
        }
        Err(format!(
            "harness terminal did not become ready within {}s (input prompt never rendered)",
            PROMPT_READY_TIMEOUT.as_secs()
        ))
    }

    fn write_paste_file(&self, payload: &[u8]) -> Result<PathBuf, String> {
        let name = format!("nexus-paste-{}-{}.bin", std::process::id(), nanos());
        let path = self.terminal.scratch_dir.join(name);
        std::fs::write(&path, payload).map_err(|e| format!("write paste file failed: {e}"))?;
        Ok(path)
    }
}

impl TmuxTerminal {
    fn attach_argv(&self) -> Vec<String> {
        vec![
            self.tmux_bin.clone(),
            "-2".into(),
            "-S".into(),
            self.socket.to_string_lossy().into_owned(),
            "attach".into(),
            "-t".into(),
            self.session_name.clone(),
        ]
    }

    fn socket_path(&self) -> &std::path::Path {
        &self.socket
    }

    fn session_name(&self) -> &str {
        &self.session_name
    }

    fn has_session(&self) -> bool {
        self.run_tmux_raw(&["has-session", "-t", &self.session_name])
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    fn pane_pid(&self) -> Option<u32> {
        let out = self
            .run_tmux_raw(&["display-message", "-p", "-t", &self.target, "#{pane_pid}"])
            .ok()?;
        if !out.status.success() {
            return None;
        }
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse::<u32>()
            .ok()
    }

    fn capture_pane(&self) -> String {
        match self.run_tmux_raw(&["capture-pane", "-t", &self.target, "-p"]) {
            Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).into_owned(),
            _ => String::new(),
        }
    }

    /// Like [`capture_pane`](Self::capture_pane) but with `-e`: inline escape sequences, so
    /// colors/attributes survive. Used to seed screen state, never for prompt-glyph scraping.
    fn capture_pane_escapes(&self) -> String {
        match self.run_tmux_raw(&["capture-pane", "-e", "-t", &self.target, "-p"]) {
            Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).into_owned(),
            _ => String::new(),
        }
    }

    /// The pane's current winsize (columns, rows) — the terminal's authoritative size.
    fn pane_size(&self) -> Option<(u16, u16)> {
        let out = self
            .run_tmux_raw(&[
                "display-message",
                "-p",
                "-t",
                &self.target,
                "#{pane_width} #{pane_height}",
            ])
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let text = String::from_utf8_lossy(&out.stdout);
        let mut parts = text.split_whitespace();
        let cols = parts.next()?.parse::<u16>().ok()?;
        let rows = parts.next()?.parse::<u16>().ok()?;
        Some((cols, rows))
    }

    /// The pane's cursor position (column, row), 0-indexed.
    fn cursor_position(&self) -> Option<(u16, u16)> {
        let out = self
            .run_tmux_raw(&[
                "display-message",
                "-p",
                "-t",
                &self.target,
                "#{cursor_x} #{cursor_y}",
            ])
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let text = String::from_utf8_lossy(&out.stdout);
        let mut parts = text.split_whitespace();
        let col = parts.next()?.parse::<u16>().ok()?;
        let row = parts.next()?.parse::<u16>().ok()?;
        Some((col, row))
    }

    /// A coherent repaint of the current pane a fresh terminal parser can start from: home +
    /// clear, the visible screen with escapes and CRLF line endings, then the real cursor
    /// position. The old plain `capture_pane` seed fed `\n`-only colorless text straight into
    /// engines — a staircase paint with a wrong cursor (part of the historical attach garble).
    fn seed_screen_bytes(&self) -> Vec<u8> {
        let captured = self.capture_pane_escapes();
        if captured.is_empty() {
            return Vec::new();
        }
        let mut bytes = b"\x1b[H\x1b[2J".to_vec();
        let lines: Vec<&str> = captured.trim_end_matches('\n').split('\n').collect();
        bytes.extend(lines.join("\r\n").into_bytes());
        if let Some((col, row)) = self.cursor_position() {
            bytes.extend(format!("\x1b[{};{}H", row + 1, col + 1).into_bytes());
        }
        bytes
    }

    fn pipe_output(&self) -> broadcast::Receiver<Vec<u8>> {
        let mut relay = self.pipe_relay.lock().unwrap();
        if let Some(relay) = relay.as_ref() {
            return relay.subscribe();
        }

        let (tx, rx) = broadcast::channel::<Vec<u8>>(1024);

        // Create the temp output file. tmux pipe-pane appends raw pane output to it.
        let name = format!("nexus-pipe-{}-{}.bin", self.session_name, nanos());
        let pipe_path = self.scratch_dir.join(name);
        // Touch the file so tmux can open it immediately.
        let _ = std::fs::write(&pipe_path, b"");

        // Replayed adoption snapshot: after a daemon restart, the tmux pane can already contain
        // the live harness prompt before the new daemon reattaches `pipe-pane`. `pipe-pane` only
        // emits future bytes, so seed the stream with the current visible pane once; otherwise web
        // terminal attach renders blank until the harness prints again. Coherent form (clear +
        // escapes + CRLF + cursor) so parsers/engines start from a correct screen.
        let seed = self.seed_screen_bytes();
        if !seed.is_empty() {
            let _ = tx.send(seed);
        }

        // Run `tmux pipe-pane -t <target> -o 'cat >> <path>'`.
        // The shell command appends the pane's raw output (ANSI bytes) to the file.
        let pipe_cmd = format!("cat >> {}", sh_quote(&pipe_path.to_string_lossy()));
        let _ = self.run_tmux(&["pipe-pane", "-t", &self.target, "-o", &pipe_cmd]);

        let stop = Arc::new(AtomicBool::new(false));
        *relay = Some(PipeRelay {
            tx: tx.clone(),
            stop: stop.clone(),
            path: pipe_path.clone(),
        });
        spawn_pipe_output_relay(pipe_path, tx, stop, Arc::downgrade(&self.pipe_relay));

        rx
    }

    fn write_terminal_bytes(&self, bytes: &[u8]) -> Result<(), String> {
        for action in terminal_input_actions(bytes) {
            match action {
                TerminalInputAction::Literal(text) => {
                    self.run_tmux(&["send-keys", "-t", &self.target, "-l", &text])?;
                }
                TerminalInputAction::Key(key) => {
                    self.run_tmux(&["send-keys", "-t", &self.target, key])?;
                }
            }
        }
        Ok(())
    }

    fn resize_terminal(&self, cols: u16, rows: u16) -> Result<(), String> {
        self.run_tmux(&[
            "resize-window",
            "-t",
            &self.session_name,
            "-x",
            &cols.to_string(),
            "-y",
            &rows.to_string(),
        ])
    }

    fn kill(&self) -> Result<(), String> {
        self.run_tmux(&["kill-session", "-t", &self.session_name])
    }

    /// Run `tmux -S <socket> <args…>`, mapping non-zero/timeout to `Err`.
    fn run_tmux(&self, args: &[&str]) -> Result<(), String> {
        let out = self.run_tmux_raw(args)?;
        if out.status.success() {
            Ok(())
        } else {
            let detail = String::from_utf8_lossy(&out.stderr);
            let detail = if detail.trim().is_empty() {
                String::from_utf8_lossy(&out.stdout).trim().to_string()
            } else {
                detail.trim().to_string()
            };
            Err(format!(
                "tmux {} failed: {}",
                args.first().copied().unwrap_or(""),
                detail
            ))
        }
    }

    /// Run a tmux command and return the raw `Output` (caller inspects status). Enforces the per-cmd
    /// timeout by spawning + waiting; tmux client commands are near-instant, so a hang means a wedged
    /// server — we kill it rather than block the daemon.
    fn run_tmux_raw(&self, args: &[&str]) -> Result<std::process::Output, String> {
        let socket = self.socket.to_string_lossy().into_owned();
        let mut child = Command::new(&self.tmux_bin)
            .arg("-S")
            .arg(&socket)
            .args(args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| format!("tmux spawn failed: {e}"))?;
        let deadline = Instant::now() + TMUX_CMD_TIMEOUT;
        loop {
            match child.try_wait() {
                Ok(Some(_status)) => return child.wait_with_output().map_err(|e| e.to_string()),
                Ok(None) => {
                    if Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        return Err(format!(
                            "tmux command timed out after {}s",
                            TMUX_CMD_TIMEOUT.as_secs()
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(e) => return Err(format!("tmux wait failed: {e}")),
            }
        }
    }
}

fn spawn_pipe_output_relay(
    relay_path: PathBuf,
    relay_tx: broadcast::Sender<Vec<u8>>,
    stop: Arc<AtomicBool>,
    relay_state: Weak<Mutex<Option<PipeRelay>>>,
) {
    std::thread::spawn(move || {
        use std::io::Seek;
        let mut f = match std::fs::OpenOptions::new().read(true).open(&relay_path) {
            Ok(f) => f,
            Err(_) => {
                clear_cached_pipe_relay(&relay_state, &relay_path);
                return;
            }
        };
        let mut buf = [0u8; 4096];
        loop {
            if stop.load(Ordering::Acquire) {
                clear_cached_pipe_relay(&relay_state, &relay_path);
                break;
            }
            if relay_tx.receiver_count() == 0
                && clear_unobserved_cached_pipe_relay(&relay_state, &relay_path)
            {
                break;
            }
            match f.read(&mut buf) {
                Ok(0) => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Ok(n) => {
                    let _ = relay_tx.send(buf[..n].to_vec());
                }
                Err(_) => {
                    // Seek to end on transient error to stay in sync with the writer.
                    let _ = f.seek(std::io::SeekFrom::End(0));
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
        let _ = std::fs::remove_file(&relay_path);
    });
}

fn clear_cached_pipe_relay(
    relay_state: &Weak<Mutex<Option<PipeRelay>>>,
    relay_path: &std::path::Path,
) -> bool {
    let Some(relay_state) = relay_state.upgrade() else {
        return true;
    };
    let Ok(mut relay) = relay_state.lock() else {
        return true;
    };
    if relay
        .as_ref()
        .map(|relay| relay.path.as_path() == relay_path)
        .unwrap_or(false)
    {
        relay.take();
        return true;
    }
    false
}

fn clear_unobserved_cached_pipe_relay(
    relay_state: &Weak<Mutex<Option<PipeRelay>>>,
    relay_path: &std::path::Path,
) -> bool {
    let Some(relay_state) = relay_state.upgrade() else {
        return true;
    };
    let Ok(mut relay) = relay_state.lock() else {
        return true;
    };
    let Some(current) = relay.as_ref() else {
        return true;
    };
    if current.path.as_path() != relay_path {
        return false;
    }
    if current.tx.receiver_count() != 0 {
        return false;
    }
    relay.take();
    true
}

fn resolve_pane_target(
    tmux_bin: &str,
    socket: &std::path::Path,
    session_name: &str,
) -> Result<String, String> {
    let out = Command::new(tmux_bin)
        .args([
            "-S",
            &socket.to_string_lossy(),
            "list-panes",
            "-t",
            session_name,
            "-F",
            "#{session_name}:#{window_index}.#{pane_index}",
        ])
        .output()
        .map_err(|e| format!("tmux list-panes spawn failed: {e}"))?;
    if !out.status.success() {
        let detail = String::from_utf8_lossy(&out.stderr);
        let detail = if detail.trim().is_empty() {
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        } else {
            detail.trim().to_string()
        };
        return Err(format!("tmux list-panes failed: {detail}"));
    }
    let first = String::from_utf8_lossy(&out.stdout)
        .lines()
        .next()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    Ok(first.unwrap_or_else(|| session_name.to_string()))
}

#[async_trait::async_trait]
impl crate::HarnessInput for TmuxHarness {
    /// Deliver `text` as one submitted turn. The verified-submit sequence is blocking + poll-based
    /// (it shells to `tmux` and sleeps between captures), so it runs on a blocking thread to keep the
    /// async runtime free.
    async fn send_turn(&self, text: &str) -> Result<(), String> {
        // SAFETY of the borrow: `inject` only reads `&self` fields and shells out; we hold a 'static
        // future by cloning the small identity into a worker. Simpler: run synchronously on a blocking
        // thread via `spawn_blocking` over a cheap clone.
        let this = self.cheap_clone();
        let text = text.to_string();
        tokio::task::spawn_blocking(move || this.inject(&text))
            .await
            .map_err(|e| format!("send_turn worker panicked: {e}"))?
    }

    async fn interrupt_active_turn(&self) -> Result<(), String> {
        let this = self.cheap_clone();
        tokio::task::spawn_blocking(move || {
            this.terminal
                .run_tmux(&["send-keys", "-t", &this.terminal.target, "C-c"])
        })
        .await
        .map_err(|e| format!("interrupt worker panicked: {e}"))?
    }

    fn is_alive(&self) -> bool {
        self.has_session()
    }
}

impl TmuxHarness {
    /// A field-by-field clone for moving into a blocking worker (all fields are cheap owned data).
    fn cheap_clone(&self) -> TmuxHarness {
        TmuxHarness {
            terminal: self.terminal.clone(),
            submit_strategy: self.submit_strategy,
        }
    }
}

impl crate::TerminalWriter for TmuxHarness {
    fn write_bytes(&self, bytes: &[u8]) -> Result<(), String> {
        self.write_terminal_bytes(bytes)
    }
}

impl crate::TerminalBackend for TmuxHarness {
    fn attach(&self) -> crate::TerminalAttachment {
        crate::TerminalAttachment {
            reader: self.pipe_output(),
            writer: Arc::new(self.clone()),
        }
    }

    fn resize(&self, cols: u16, rows: u16) -> Result<(), String> {
        self.resize_terminal(cols, rows)
    }

    fn current_size(&self) -> Option<(u16, u16)> {
        self.terminal.pane_size()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TerminalInputAction {
    Literal(String),
    Key(&'static str),
}

fn terminal_input_actions(bytes: &[u8]) -> Vec<TerminalInputAction> {
    let mut out = Vec::new();
    let mut literal = Vec::new();
    let flush_literal = |literal: &mut Vec<u8>, out: &mut Vec<TerminalInputAction>| {
        if !literal.is_empty() {
            out.push(TerminalInputAction::Literal(
                String::from_utf8_lossy(literal).into_owned(),
            ));
            literal.clear();
        }
    };

    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\r' | b'\n' => {
                flush_literal(&mut literal, &mut out);
                out.push(TerminalInputAction::Key("Enter"));
                i += 1;
            }
            0x7f => {
                flush_literal(&mut literal, &mut out);
                out.push(TerminalInputAction::Key("BSpace"));
                i += 1;
            }
            0x03 => {
                flush_literal(&mut literal, &mut out);
                out.push(TerminalInputAction::Key("C-c"));
                i += 1;
            }
            0x1b if bytes.get(i + 1) == Some(&b'[') && bytes.get(i + 2).is_some() => {
                let key = match bytes[i + 2] {
                    b'A' => Some("Up"),
                    b'B' => Some("Down"),
                    b'C' => Some("Right"),
                    b'D' => Some("Left"),
                    _ => None,
                };
                if let Some(key) = key {
                    flush_literal(&mut literal, &mut out);
                    out.push(TerminalInputAction::Key(key));
                    i += 3;
                } else {
                    literal.push(bytes[i]);
                    i += 1;
                }
            }
            b => {
                literal.push(b);
                i += 1;
            }
        }
    }
    flush_literal(&mut literal, &mut out);
    out
}

/// Reap orphan PTY backend servers left by a previous (unclean) daemon shutdown.
///
/// The current production backend is tmux: each harness runs on a private socket
/// `<tmpdir>/nexus-tmux-*.sock`; we `kill-server` on each and remove the stale socket file.
/// Best-effort, called on daemon boot so orphaned native harness processes never pile up across
/// restarts. Returns the number of sockets reaped.
pub fn reap_orphan_pty_backends() -> usize {
    let dir = std::env::temp_dir();
    let tmux = resolve_tmux();
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return 0;
    };
    let mut reaped = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        if !(name.starts_with("nexus-tmux-") && name.ends_with(".sock")) {
            continue;
        }
        // Best-effort: kill the server on this private socket, then drop the (now-dead) socket file.
        let _ = std::process::Command::new(&tmux)
            .arg("-S")
            .arg(&path)
            .arg("kill-server")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        let _ = std::fs::remove_file(&path);
        reaped += 1;
    }
    reaped
}

// ── pure helpers (ported from omnigent; unit-tested below) ────────────────────────────────────────

/// Resolve the `tmux` binary: prefer `~/.local/bin/tmux` (the 3.5a install on this host), else PATH.
fn resolve_tmux() -> String {
    if let Ok(home) = std::env::var("HOME") {
        let local = PathBuf::from(&home).join(".local/bin/tmux");
        if local.is_file() {
            return local.to_string_lossy().into_owned();
        }
    }
    "tmux".to_string()
}

/// Defaults for headed tmux panes so TUIs render with color even if the daemon shell is bland.
fn tmux_headed_env_defaults() -> Vec<(String, String)> {
    vec![
        ("TERM".to_string(), "screen-256color".to_string()),
        ("COLORTERM".to_string(), "truecolor".to_string()),
    ]
}

fn tmux_new_session_args(
    socket: &std::path::Path,
    session_name: &str,
    w: u16,
    h: u16,
    shell_cmd: &str,
) -> Vec<String> {
    vec![
        "-2".into(),
        "-S".into(),
        socket.to_string_lossy().into_owned(),
        "new-session".into(),
        "-d".into(),
        "-s".into(),
        session_name.into(),
        "-x".into(),
        w.to_string(),
        "-y".into(),
        h.to_string(),
        "sh".into(),
        "-c".into(),
        shell_cmd.into(),
    ]
}

/// Single-quote-escape for `sh -c`: wrap in `'…'`, and turn any interior `'` into `'\''`.
fn sh_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        if ch == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

/// Build the shell command passed to `tmux new-session`.
///
/// This is public for integration tests; production callers should use [`TmuxHarness::launch`] or
/// [`TmuxHarness::launch_with_env`] so socket/session bookkeeping stays correct.
#[doc(hidden)]
pub fn tmux_launch_shell_command(
    program: &str,
    args: &[String],
    cwd: &str,
    env: &[(String, String)],
) -> String {
    let mut shell_cmd = String::from(
        "for v in $(env | sed -n 's/=.*//p' | grep -E '^(NEXUS_|CODEX_|CLAUDE_CONFIG_DIR$|HERMES_HOME$|OPENCODE_HOME$|OPENCODE_CONFIG_DIR$)' || true); do unset \"$v\"; done; ",
    );
    shell_cmd.push_str("unset NO_COLOR; ");
    for (key, value) in env {
        if safe_env_key(key) {
            shell_cmd.push_str("export ");
            shell_cmd.push_str(key);
            shell_cmd.push('=');
            shell_cmd.push_str(&sh_quote(value));
            shell_cmd.push_str("; ");
        }
    }
    shell_cmd.push_str("cd ");
    shell_cmd.push_str(&sh_quote(cwd));
    shell_cmd.push_str(" && exec ");
    shell_cmd.push_str(&sh_quote(program));
    for a in args {
        shell_cmd.push(' ');
        shell_cmd.push_str(&sh_quote(a));
    }
    shell_cmd
}

fn safe_env_key(key: &str) -> bool {
    !key.is_empty()
        && key
            .chars()
            .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
        && !key.as_bytes()[0].is_ascii_digit()
}

/// Monotonic-ish nanos for unique temp file names.
fn nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// Whether claude's input prompt is rendered: scan the last [`PROMPT_SCAN_TAIL_LINES`] non-empty
/// lines for the glyph (restricting to the tail avoids matching the glyph in scrollback).
fn claude_prompt_rendered(pane: &str) -> bool {
    let non_empty: Vec<&str> = pane.lines().filter(|l| !l.trim().is_empty()).collect();
    let start = non_empty.len().saturating_sub(PROMPT_SCAN_TAIL_LINES);
    non_empty[start..]
        .iter()
        .any(|l| l.contains(CLAUDE_PROMPT_GLYPH))
}

/// A short marker for spotting the draft in the input box: the first non-empty line of `content`
/// (after CRLF normalization), truncated at the first control char and to [`DRAFT_NEEDLE_MAX_CHARS`].
fn claude_submit_needle(content: &str) -> String {
    let normalized = content.replace("\r\n", "\n").replace('\r', "\n");
    for line in normalized.split('\n') {
        // Truncate at the first control char (the TUI renders those differently).
        let mut cut = line.len();
        for (idx, ch) in line.char_indices() {
            if (ch as u32) < 0x20 {
                cut = idx;
                break;
            }
        }
        let trimmed = line[..cut].trim();
        if !trimmed.is_empty() {
            return trimmed.chars().take(DRAFT_NEEDLE_MAX_CHARS).collect();
        }
    }
    String::new()
}

/// Whether the pasted draft is still visible in the input box: look only at the LAST glyph line (the
/// live input box sits at the bottom, below the transcript echo) and check for the needle or the
/// large-paste placeholder after the glyph.
fn claude_draft_in_input_box(pane: &str, needle: &str) -> bool {
    let last_glyph_line = pane.lines().rfind(|l| l.contains(CLAUDE_PROMPT_GLYPH));
    let Some(line) = last_glyph_line else {
        return false;
    };
    let tail = line
        .rsplit_once(CLAUDE_PROMPT_GLYPH)
        .map(|(_, t)| t)
        .unwrap_or("");
    if tail.contains(PASTED_PLACEHOLDER_PREFIX) {
        return true;
    }
    !needle.is_empty() && tail.contains(needle)
}

/// Encode text as the `load-buffer` payload: normalize line endings, map `\n`/`\r`→CR (`0x0d`),
/// `\t`→`0x09`, DROP other control bytes (a stray ESC would prematurely close the bracketed paste),
/// pass everything else through as UTF-8. `paste-buffer -p` adds the bracketed-paste markers itself.
fn paste_payload_bytes(text: &str) -> Vec<u8> {
    let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
    let mut body = Vec::with_capacity(normalized.len());
    for ch in normalized.chars() {
        match ch {
            '\n' => body.push(0x0d),
            '\t' => body.push(0x09),
            c if (c as u32) < 0x20 => {} // drop other control bytes
            c => {
                let mut buf = [0u8; 4];
                body.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            }
        }
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paste_payload_maps_newlines_to_cr_and_drops_control_bytes() {
        // CRLF + lone CR coalesce to a single CR each; tab survives; a stray ESC is dropped.
        let out = paste_payload_bytes("a\r\nb\rc\td\x1be");
        assert_eq!(out, vec![b'a', 0x0d, b'b', 0x0d, b'c', 0x09, b'd', b'e']);
    }

    #[test]
    fn prompt_rendered_only_matches_glyph_in_the_tail() {
        // Glyph buried in scrollback (followed by >PROMPT_SCAN_TAIL_LINES non-empty lines) is NOT
        // "ready".
        let scrollback = format!(
            "{CLAUDE_PROMPT_GLYPH} old\n{}",
            (0..=PROMPT_SCAN_TAIL_LINES)
                .map(|idx| format!("l{idx}"))
                .collect::<Vec<_>>()
                .join("\n")
        );
        assert!(!claude_prompt_rendered(&scrollback));
        // Glyph in the live input box at the bottom IS ready.
        let live = format!("transcript line\n\n{CLAUDE_PROMPT_GLYPH} ");
        assert!(claude_prompt_rendered(&live));
    }

    #[test]
    fn prompt_rendered_tolerates_post_compaction_footer_noise() {
        let mut pane = format!("compact summary\n{CLAUDE_PROMPT_GLYPH} ");
        for line in [
            "esc to interrupt",
            "ctrl-r to expand",
            "tokens compacted",
            "session resumed",
            "nexus hook ready",
            "status: idle",
        ] {
            pane.push('\n');
            pane.push_str(line);
        }
        assert!(
            claude_prompt_rendered(&pane),
            "post-compaction status/footer lines should not hide the live prompt"
        );
    }

    #[test]
    fn claude_submit_needle_takes_first_nonempty_line_truncated() {
        assert_eq!(
            TuiSubmitStrategy::Claude.submit_needle("  \n  fix the bug \nmore"),
            "fix the bug"
        );
        // Truncated at the first control char.
        assert_eq!(
            TuiSubmitStrategy::Claude.submit_needle("hello\tworld"),
            "hello"
        );
        // Capped at DRAFT_NEEDLE_MAX_CHARS.
        let long = "x".repeat(50);
        assert_eq!(
            TuiSubmitStrategy::Claude.submit_needle(&long).len(),
            DRAFT_NEEDLE_MAX_CHARS
        );
        assert_eq!(TuiSubmitStrategy::Claude.submit_needle("   \n   "), "");
    }

    #[test]
    fn claude_draft_in_input_box_uses_last_glyph_line_and_placeholder() {
        let needle = "fix the bug";
        // Transcript echo of the glyph above, live draft below → matched on the LAST glyph line only.
        let pane = format!(
            "{CLAUDE_PROMPT_GLYPH} fix the bug (submitted, in transcript)\n\
             some response\n\
             {CLAUDE_PROMPT_GLYPH} fix the bug"
        );
        assert!(TuiSubmitStrategy::Claude.draft_in_input_box(&pane, needle));
        // After submit the box is empty → not present.
        let empty = format!("response\n{CLAUDE_PROMPT_GLYPH} ");
        assert!(!TuiSubmitStrategy::Claude.draft_in_input_box(&empty, needle));
        // Large paste collapses to the placeholder.
        let placeholder =
            format!("{CLAUDE_PROMPT_GLYPH} {PASTED_PLACEHOLDER_PREFIX} #1 +400 lines]");
        assert!(TuiSubmitStrategy::Claude.draft_in_input_box(&placeholder, needle));
    }

    #[test]
    fn sh_quote_escapes_interior_single_quotes() {
        assert_eq!(sh_quote("a b"), "'a b'");
        assert_eq!(sh_quote("it's"), "'it'\\''s'");
    }

    #[test]
    fn tmux_headed_env_defaults_force_color_term() {
        let env = tmux_headed_env_defaults();
        assert_eq!(
            env,
            vec![
                ("TERM".to_string(), "screen-256color".to_string()),
                ("COLORTERM".to_string(), "truecolor".to_string()),
            ]
        );
    }

    #[test]
    fn attach_argv_carries_socket_and_session() {
        // Build a harness without launching tmux (just exercise the pure argv builder).
        let h = TmuxHarness {
            terminal: TmuxTerminal {
                tmux_bin: "tmux".into(),
                socket: PathBuf::from("/tmp/x.sock"),
                session_name: "nexus-s_1".into(),
                target: "nexus-s_1:0.0".into(),
                scratch_dir: PathBuf::from("/tmp"),
                pipe_relay: Arc::new(Mutex::new(None)),
            },
            submit_strategy: TuiSubmitStrategy::Claude,
        };
        assert_eq!(
            h.attach_argv(),
            vec![
                "tmux",
                "-2",
                "-S",
                "/tmp/x.sock",
                "attach",
                "-t",
                "nexus-s_1"
            ]
        );
        assert_eq!(h.socket_path(), PathBuf::from("/tmp/x.sock").as_path());
        assert_eq!(h.session_name(), "nexus-s_1");
    }

    #[test]
    fn terminal_input_actions_map_common_xterm_bytes() {
        assert_eq!(
            terminal_input_actions(b"abc\r\x7f\x1b[A"),
            vec![
                TerminalInputAction::Literal("abc".into()),
                TerminalInputAction::Key("Enter"),
                TerminalInputAction::Key("BSpace"),
                TerminalInputAction::Key("Up"),
            ]
        );
    }

    #[test]
    fn tmux_new_session_args_force_256color_client() {
        let args = tmux_new_session_args(
            &PathBuf::from("/tmp/x.sock"),
            "nexus-s_1",
            80,
            24,
            "sh -c 'printf hi'",
        );
        assert_eq!(
            args,
            vec![
                "-2",
                "-S",
                "/tmp/x.sock",
                "new-session",
                "-d",
                "-s",
                "nexus-s_1",
                "-x",
                "80",
                "-y",
                "24",
                "sh",
                "-c",
                "sh -c 'printf hi'",
            ]
        );
    }

    /// Test the file-read→broadcast relay mechanism of `pipe_output` in isolation.
    ///
    /// We bypass the tmux subprocess entirely: write known bytes directly to the temp file that the
    /// relay task reads, and assert those bytes arrive on the broadcast receiver. This validates the
    /// incrementally-reading relay loop without requiring a real tmux session.
    #[tokio::test]
    async fn pipe_output_relay_broadcasts_bytes_written_to_the_pipe_file() {
        use std::io::Write as _;
        use tokio::sync::broadcast;

        // Create the relay plumbing directly through the same helper used by pipe_output.
        let pipe_path = std::env::temp_dir().join(format!("nexus-pipe-relay-test-{}.bin", nanos()));
        std::fs::write(&pipe_path, b"").expect("touch pipe file");

        let (tx, mut rx) = broadcast::channel::<Vec<u8>>(64);
        let stop = Arc::new(AtomicBool::new(false));
        let relay_state = Arc::new(Mutex::new(Some(PipeRelay {
            tx: tx.clone(),
            stop: stop.clone(),
            path: pipe_path.clone(),
        })));
        spawn_pipe_output_relay(
            pipe_path.clone(),
            tx.clone(),
            stop,
            Arc::downgrade(&relay_state),
        );

        // Write known bytes to the file (simulating tmux pipe-pane appending pane output).
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&pipe_path)
                .expect("append to pipe file");
            f.write_all(b"hello from pipe\r\n").expect("write");
            f.flush().expect("flush");
        }

        // The relay task must deliver the bytes to the broadcast receiver within 2 s.
        let payload = tokio::time::timeout(Duration::from_secs(2), async {
            let mut buf = Vec::new();
            loop {
                match rx.recv().await {
                    Ok(chunk) => {
                        buf.extend_from_slice(&chunk);
                        if String::from_utf8_lossy(&buf).contains("hello from pipe") {
                            return Some(buf);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        })
        .await
        .expect("relay must deliver bytes within 2 s");

        assert!(
            payload.is_some(),
            "relay must deliver 'hello from pipe' to the broadcast"
        );
        drop(rx);
        let cleaned = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if relay_state.lock().unwrap().is_none() && !pipe_path.exists() {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or(false);
        drop(tx);
        assert!(
            cleaned,
            "relay must clear cached state and remove the pipe file when receivers drop"
        );
    }

    #[tokio::test]
    async fn adopted_pipe_output_replays_existing_pane_contents() {
        let session_name = format!("nexus-adopt-pipe-{}", nanos());
        let cwd = std::env::temp_dir().join(format!("nexus-adopt-pipe-cwd-{}", nanos()));
        std::fs::create_dir_all(&cwd).expect("create cwd");
        let cwd_s = cwd.to_string_lossy().into_owned();
        let harness =
            TmuxHarness::launch(&session_name, "cat", &[], &cwd_s, 80, 24).expect("launch cat");
        harness
            .write_terminal_bytes(b"POST_RESTART_VISIBLE\r")
            .expect("write marker");

        let pane_has_marker = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if harness.capture_pane().contains("POST_RESTART_VISIBLE") {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or(false);
        assert!(
            pane_has_marker,
            "cat pane must show the pre-adoption marker"
        );

        let adopted = TmuxHarness::adopt(&session_name, &cwd_s).expect("adopt existing tmux");
        let mut rx = adopted.pipe_output();
        let replayed = tokio::time::timeout(Duration::from_secs(2), async {
            let mut buf = Vec::new();
            while let Ok(chunk) = rx.recv().await {
                buf.extend_from_slice(&chunk);
                if String::from_utf8_lossy(&buf).contains("POST_RESTART_VISIBLE") {
                    return true;
                }
            }
            false
        })
        .await
        .unwrap_or(false);

        let _ = harness.kill();
        assert!(
            replayed,
            "adopted terminal stream must replay the current pane so web attach renders after restart"
        );
    }
}
