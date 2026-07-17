//! Daemon-side terminal screen model — the "lite tmux" attach guarantee.
//!
//! Wraps any [`TerminalBackend`] and continuously feeds its output byte stream into a
//! `vt100::Parser`, so the daemon always holds the terminal's CURRENT screen (grid, cursor,
//! attributes, modes) independent of any client. [`snapshot`](ScreenModelBackend::snapshot)
//! serializes that state as one repaint burst; the terminal socket sends it to every fresh
//! attacher before live bytes, so a viewer joining mid-life starts from a coherent screen
//! instead of decoding a stream that began mid-escape-sequence (the historical garble +
//! blank-until-first-output defects).
//!
//! The model is the daemon's OWN screen state — it does not shell out to tmux for it. That
//! keeps the attach contract backend-agnostic (tmux today, raw/ConPTY tomorrow) and is the
//! first step of removing tmux from the attach path entirely.

use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

use crate::{TerminalAttachment, TerminalBackend, TerminalSnapshot};

const DEFAULT_COLS: u16 = 80;
const DEFAULT_ROWS: u16 = 24;

pub struct ScreenModelBackend {
    inner: Arc<dyn TerminalBackend>,
    parser: Arc<Mutex<vt100::Parser>>,
    // Live winsize changes, fanned out to every attached socket client so all viewers
    // adopt a resize immediately (tmux-client semantics: one viewer resizes, the rest
    // stay coherent viewports at the new authoritative size).
    size_events: broadcast::Sender<(u16, u16)>,
}

impl ScreenModelBackend {
    /// Wrap `inner`, seeding the model at the backend's current size and subscribing to its
    /// output stream on a dedicated reader thread. For tmux the first subscription also seeds
    /// the stream with the current visible pane (see `TmuxTerminal::pipe_output`), so a model
    /// created on daemon adoption starts from the live screen, not blank.
    pub fn wrap(inner: Arc<dyn TerminalBackend>) -> Arc<Self> {
        let (cols, rows) = inner.current_size().unwrap_or((DEFAULT_COLS, DEFAULT_ROWS));
        let parser = Arc::new(Mutex::new(vt100::Parser::new(rows, cols, 0)));
        let (size_events, _) = broadcast::channel(16);

        let mut reader = inner.attach().reader;
        let weak = Arc::downgrade(&parser);
        std::thread::Builder::new()
            .name("terminal-screen-model".into())
            .spawn(move || loop {
                match reader.blocking_recv() {
                    Ok(bytes) => {
                        // The parser Arc dying means this model was replaced (rebind) — stop.
                        let Some(parser) = weak.upgrade() else { break };
                        parser.lock().unwrap().process(&bytes);
                    }
                    // Lag skips bytes; the screen may drift until the TUI's next full redraw.
                    // Better than blocking the broadcast for every other subscriber.
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            })
            .expect("spawn terminal screen model reader");

        Arc::new(Self {
            inner,
            parser,
            size_events,
        })
    }

    /// Make the daemon the "attached terminal emulator" for this pty: answer the harness
    /// TUI's terminal queries (cursor position, device attributes, colors, mode probes)
    /// from the screen model. This is what lets an UNATTENDED full-screen TUI on a raw
    /// daemon-owned pty run to completion — under tmux, tmux answered these.
    ///
    /// Enable ONLY for raw pty backends: tmux answers its own pane's queries, and a second
    /// responder would double-reply into the harness's input.
    pub fn spawn_query_responder(self: &Arc<Self>) {
        let attached = self.inner.attach();
        let mut reader = attached.reader;
        let writer = attached.writer;
        let weak = Arc::downgrade(&self.parser);
        std::thread::Builder::new()
            .name("terminal-query-responder".into())
            .spawn(move || {
                let mut scanner = crate::query_responder::QueryScanner::new();
                loop {
                    match reader.blocking_recv() {
                        Ok(bytes) => {
                            let queries = scanner.scan(&bytes);
                            if queries.is_empty() {
                                continue;
                            }
                            let Some(parser) = weak.upgrade() else { break };
                            let cursor = parser.lock().unwrap().screen().cursor_position();
                            for query in &queries {
                                let reply = crate::query_responder::reply_for(query, cursor);
                                if writer.write_bytes(&reply).is_err() {
                                    return;
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            })
            .expect("spawn terminal query responder");
    }

    /// Plain visible terminal contents held by the daemon-side screen model.
    ///
    /// Native input drivers use this to verify that a TUI prompt is ready and that a submitted
    /// draft actually left its composer. This reads the same authoritative model used for fresh
    /// terminal attachments; it does not require a human viewer to be connected.
    pub fn contents(&self) -> String {
        self.parser.lock().unwrap().screen().contents()
    }
}

impl TerminalBackend for ScreenModelBackend {
    fn attach(&self) -> TerminalAttachment {
        self.inner.attach()
    }

    fn resize(&self, cols: u16, rows: u16) -> Result<(), String> {
        // Keep the model's grid in lockstep with the pty winsize; the TUI's SIGWINCH redraw
        // then repaints both the same way.
        self.parser.lock().unwrap().set_size(rows, cols);
        let result = self.inner.resize(cols, rows);
        if result.is_ok() {
            let _ = self.size_events.send((cols, rows));
        }
        result
    }

    fn subscribe_size(&self) -> Option<broadcast::Receiver<(u16, u16)>> {
        Some(self.size_events.subscribe())
    }

    fn current_size(&self) -> Option<(u16, u16)> {
        let parser = self.parser.lock().unwrap();
        let (rows, cols) = parser.screen().size();
        Some((cols, rows))
    }

    fn snapshot(&self) -> Option<TerminalSnapshot> {
        let parser = self.parser.lock().unwrap();
        let screen = parser.screen();
        let (rows, cols) = screen.size();
        // Clear first: state_formatted paints the full grid but assumes a clean canvas.
        let mut bytes = b"\x1b[H\x1b[2J".to_vec();
        bytes.extend(screen.state_formatted());
        // state_formatted restores contents/attributes/modes; pin the cursor explicitly so
        // typed input lands where the harness expects it.
        let (cursor_row, cursor_col) = screen.cursor_position();
        bytes.extend(format!("\x1b[{};{}H", cursor_row + 1, cursor_col + 1).into_bytes());
        Some(TerminalSnapshot { cols, rows, bytes })
    }
}
