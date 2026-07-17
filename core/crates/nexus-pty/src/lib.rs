//! Daemon-owned native-harness supervision for native-TUI harnesses (claude/codex). No ACP — the
//! daemon launches the interactive binary and injects a drained turn into its input.
//!
//! Two delivery backends sit behind the [`HarnessInput`] trait:
//!
//! - [`PtySession`] — a raw daemon-owned PTY. It echoes its input on its output stream, which makes
//!   it a deterministic, offline stand-in (`cat`) for the integration tests. BUT a real claude TUI
//!   driven over a raw PTY submits an injected turn yet never COMPLETES it (proven), so it is not
//!   used for production harness delivery.
//! - [`TmuxHarness`] — runs the harness inside a real terminal emulator (`tmux`). A real claude in a
//!   detached tmux session, fed via `tmux load-buffer`/`paste-buffer` + a verified submit `Enter`,
//!   COMPLETES the unattended turn and writes its transcript. This is the production backend.
//! - `ConPtyBackend` — on Windows, runs raw PTY sessions through the platform ConPTY host while
//!   exposing the same [`TerminalBackend`] and [`HarnessInput`] contracts as Unix raw PTYs.
//!
//! See `docs/adding-a-harness.md` for headed runtime integration guidance.
pub mod command;
#[cfg(any(windows, test))]
mod conpty;
pub mod query_responder;
pub mod screen_model;
pub mod screen_text;
mod session;
mod tmux;
#[cfg(any(windows, test))]
pub use conpty::ConPtyBackend;
pub use screen_model::ScreenModelBackend;
pub use session::{PtyError, PtySession};
pub use tmux::reap_orphan_pty_backends;
#[doc(hidden)]
pub use tmux::tmux_launch_shell_command;
pub use tmux::TmuxHarness;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;

/// Claude commits a bracketed paste asynchronously. Submitting in the same PTY write can leave a
/// large payload sitting in the composer as `[Pasted text ...]`; match the proven tmux sequence by
/// giving the paste one UI tick before sending Enter as a separate terminal input event.
const RAW_PASTE_SETTLE: Duration = Duration::from_millis(100);

/// Evidence a native input backend can provide after [`HarnessInput::send_turn`] returns.
///
/// Durable Nexus delivery may settle once the harness reports
/// [`ContextAccepted`](Self::ContextAccepted) or stronger evidence. Raw PTY/tmux input defaults to
/// [`InputAcceptedOnly`](Self::InputAcceptedOnly): writing bytes or seeing a prompt redraw does not
/// prove that the harness admitted the message into its processing context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnCompletionEvidence {
    InputAcceptedOnly,
    ContextAccepted,
    Terminal,
}

/// Deliver one drained `<nexus-batch>` turn into a native harness's input. Implemented by both the
/// raw [`PtySession`] (cat-based tests) and the production [`TmuxHarness`] so [`PtyTransport`]'s
/// `inject_turn` is backend-agnostic — it renders the batch and calls [`send_turn`](HarnessInput::send_turn).
///
/// [`PtyTransport`]: ../nexus/daemon/pty_transport/struct.PtyTransport.html
#[async_trait::async_trait]
pub trait HarnessInput: Send + Sync {
    /// Deliver `text` as ONE submitted turn to the harness. The implementation owns the submit
    /// nuance (bracketed paste, verified Enter, etc.); callers just pass the rendered turn text.
    async fn send_turn(&self, text: &str) -> Result<(), String>;

    /// What the successful return from [`send_turn`](Self::send_turn) proves.
    fn turn_completion_evidence(&self) -> TurnCompletionEvidence {
        TurnCompletionEvidence::InputAcceptedOnly
    }

    /// Interrupt the active terminal turn before a redirect-now send.
    async fn interrupt_active_turn(&self) -> Result<(), String> {
        Err("this terminal backend does not support active-turn interruption".into())
    }

    /// Run the harness's native context-compaction command.
    ///
    /// Most headed harnesses use `/compact`; runtimes with different command vocabulary override
    /// this method while keeping ordinary prompt delivery unchanged.
    async fn compact(&self) -> Result<(), String> {
        self.send_turn("/compact").await
    }

    /// Is the harness process/session still alive? The daemon uses this for TRUTHFUL presence (a
    /// dead harness must read offline, not online) and to decide when to respawn before delivering.
    /// `TmuxHarness` checks `tmux has-session`; a raw [`PtySession`] checks its child hasn't exited.
    /// Defaults to `true` for backends that don't track liveness (none in production).
    fn is_alive(&self) -> bool {
        true
    }
}

/// Raw byte stream reader returned by a [`TerminalBackend`] attachment.
pub type ByteReader = broadcast::Receiver<Vec<u8>>;

/// Raw byte stream writer returned by a [`TerminalBackend`] attachment.
pub type ByteWriter = Arc<dyn TerminalWriter>;

/// One active web-terminal attachment to a runtime terminal backend.
pub struct TerminalAttachment {
    /// Raw terminal output bytes. Gateway websocket owners relay these as binary frames.
    pub reader: ByteReader,
    /// Raw terminal input writer. Gateway websocket owners feed binary frames here.
    pub writer: ByteWriter,
}

/// Write raw terminal bytes into a backend.
pub trait TerminalWriter: Send + Sync {
    /// Write exactly the bytes produced by a terminal client.
    fn write_bytes(&self, bytes: &[u8]) -> Result<(), String>;
}

/// A coherent repaint of a terminal's CURRENT screen plus its authoritative size. A fresh viewer
/// feeds `bytes` to an empty engine sized `cols`×`rows` and lands on exactly what the terminal
/// shows now — the attach contract that makes mid-life attach render correctly (a raw byte tap
/// joins mid-escape-sequence and paints garbage).
pub struct TerminalSnapshot {
    pub cols: u16,
    pub rows: u16,
    pub bytes: Vec<u8>,
}

/// Web-terminal backend for a launched runtime.
///
/// This sits beside [`HarnessInput`]: `HarnessInput` owns Nexus turn delivery, while
/// `TerminalBackend` owns human terminal bytes. tmux is one implementation, not the abstraction.
pub trait TerminalBackend: Send + Sync {
    /// Attach to the runtime terminal as raw output reader plus raw input writer.
    fn attach(&self) -> TerminalAttachment;

    /// Resize the runtime terminal. Arguments are terminal order: columns, then rows.
    fn resize(&self, cols: u16, rows: u16) -> Result<(), String>;

    /// The terminal's authoritative size (columns, rows), when the backend knows it.
    fn current_size(&self) -> Option<(u16, u16)> {
        None
    }

    /// Serialize the current screen for a fresh attacher. `None` when the backend keeps no
    /// screen state; wrap it in [`ScreenModelBackend`] to get one.
    fn snapshot(&self) -> Option<TerminalSnapshot> {
        None
    }

    /// Subscribe to live winsize changes (cols, rows), so every attached viewer can adopt a
    /// resize made by any one of them. `None` when the backend doesn't track size changes.
    fn subscribe_size(&self) -> Option<broadcast::Receiver<(u16, u16)>> {
        None
    }
}

/// Raw-PTY delivery: write `text` as a single bracketed-paste block followed by a carriage return.
/// `cat` echoes this back on its output stream, which is exactly what the deterministic cat-based
/// integration tests assert on (`pty_agent_to_agent`, `pty_launch_autobinds`). Kept so those tests
/// stay green; production harness delivery uses [`TmuxHarness`].
#[async_trait::async_trait]
impl HarnessInput for PtySession {
    async fn send_turn(&self, text: &str) -> Result<(), String> {
        self.write(&bracketed_paste(text))
            .map_err(|e| e.to_string())?;
        tokio::time::sleep(RAW_PASTE_SETTLE).await;
        self.write(b"\r").map_err(|e| e.to_string())
    }

    async fn interrupt_active_turn(&self) -> Result<(), String> {
        self.write(&[0x03]).map_err(|e| e.to_string())
    }

    fn is_alive(&self) -> bool {
        self.child_alive()
    }
}

impl TerminalWriter for PtySession {
    fn write_bytes(&self, bytes: &[u8]) -> Result<(), String> {
        self.write(bytes).map_err(|e| e.to_string())
    }
}

impl TerminalBackend for PtySession {
    fn attach(&self) -> TerminalAttachment {
        TerminalAttachment {
            reader: self.subscribe(),
            writer: Arc::new(self.clone()),
        }
    }

    fn resize(&self, cols: u16, rows: u16) -> Result<(), String> {
        PtySession::resize(self, rows, cols).map_err(|e| e.to_string())
    }

    fn current_size(&self) -> Option<(u16, u16)> {
        self.winsize().map(|size| (size.cols, size.rows))
    }
}

/// Wrap `s` in `ESC[200~ … ESC[201~` so a native-TUI harness treats a multi-line batch as one pasted
/// block (no per-line submit). The caller appends the trailing `\r` to submit exactly one turn.
pub fn bracketed_paste(s: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(s.len() + 12);
    v.extend_from_slice(b"\x1b[200~");
    v.extend_from_slice(s.as_bytes());
    v.extend_from_slice(b"\x1b[201~");
    v
}
