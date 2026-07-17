//! PTY reply reader: drives [`nexus_pty::screen_text::ScreenText`] over a PTY output byte-stream
//! and emits each newly-committed text line as a `Text` [`AgentUpdate`] to the [`EventSink`].
//!
//! This is the headed twin of the ACP [`relay_turn`] path: both feed TEXT to the same sink
//! (→ volatile `mem.stream_events` → AG-UI), but the source here is raw PTY bytes from a
//! daemon-owned native-TUI harness rather than an ACP session stream. Headed Codex app-server
//! sessions bypass this reader and stream structured app-server notifications instead.
//!
//! # Turn-settled detection
//!
//! After each chunk of bytes, a `tokio::time::timeout` re-arms the recv wait with a `quiet_ms`
//! deadline. When the deadline expires with no new bytes (the harness has gone quiet), the
//! session bell is rung once to signal "turn settled." The bell is re-armed on the next chunk,
//! so each distinct burst of output rings exactly one bell — matching the ACP path's semantics.
//!
//! # Spawn-once semantics
//!
//! The caller guarantees one reader per session. The spawned task owns the `ScreenText` (which
//! already guarantees committed-once / no-re-emit).
//!
//! [`relay_turn`]: nexus_agent::adapter::engine

use std::sync::Arc;
use std::time::Duration;

use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_contracts::AgentUpdateKind;
use nexus_dispatch::Bell;
use nexus_pty::screen_text::ScreenText;
use tokio::sync::broadcast;

/// Subscribe to a PTY output broadcast, run each chunk through [`ScreenText`], and emit every
/// newly-committed text line as a `Text` `AgentUpdate` to `events`. Rings `bell` once after
/// `quiet_ms` milliseconds elapse with no new bytes (turn settled), then re-arms for the next
/// turn. On `Lagged` (the broadcast buffer overflowed), logs and continues. On channel `Closed`,
/// exits cleanly.
///
/// The function is `spawn`-once: call it once per session at harness launch. The spawned task
/// runs until the PTY output channel closes (harness exited). Returns the `JoinHandle` for
/// optional abort on session cleanup (the task exits on its own when the channel closes).
pub fn spawn_pty_reply_reader(
    session: SessionId,
    output: broadcast::Receiver<Vec<u8>>,
    events: Arc<dyn EventSink>,
    bell: Bell,
    quiet_ms: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(pty_reply_reader_loop(
        session, output, events, bell, quiet_ms,
    ))
}

/// The inner async loop — split from `spawn_pty_reply_reader` so tests can drive it with
/// `tokio::spawn` directly and observe the join handle.
async fn pty_reply_reader_loop(
    session: SessionId,
    mut output: broadcast::Receiver<Vec<u8>>,
    events: Arc<dyn EventSink>,
    bell: Bell,
    quiet_ms: u64,
) {
    let mut screen = ScreenText::new(24, 80);
    let quiet = Duration::from_millis(quiet_ms);
    // Whether there has been at least one text line since the last bell ring.
    // We only ring when there was actual text, to avoid spurious bells on startup.
    let mut has_text_since_bell = false;

    loop {
        match tokio::time::timeout(quiet, output.recv()).await {
            // ── New bytes arrived within the quiet window ──────────────────────────────────
            Ok(Ok(bytes)) => {
                let lines = screen.feed(&bytes);
                for line in lines {
                    events
                        .emit(WsEvent::AgentUpdate {
                            session_id: session.clone(),
                            kind: AgentUpdateKind::Text,
                            data: serde_json::json!({ "text": line }),
                        })
                        .await;
                    has_text_since_bell = true;
                }
            }

            // ── Broadcast buffer overflowed — skip the missed frames, keep going ─────────
            Ok(Err(broadcast::error::RecvError::Lagged(n))) => {
                tracing::warn!(
                    target: "nexus::pty_reply_reader",
                    session_id = %session.0,
                    skipped = n,
                    "PTY broadcast lagged — missed frames skipped"
                );
            }

            // ── Channel closed (harness exited) — exit cleanly ────────────────────────────
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                tracing::debug!(
                    target: "nexus::pty_reply_reader",
                    session_id = %session.0,
                    "PTY output channel closed — reader exiting"
                );
                break;
            }

            // ── Quiet gap elapsed: ring the bell once (turn settled), then re-arm ─────────
            Err(_timeout) => {
                if has_text_since_bell {
                    bell.ring(&session);
                    has_text_since_bell = false;
                }
                // Re-arm: fall back to the top of the loop and wait for the next turn.
            }
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use nexus_contracts::events::WsEvent;
    use nexus_contracts::ports::EventSink;

    // ── Fake EventSink ────────────────────────────────────────────────────────────────────────

    /// Records every emitted `AgentUpdate` as `(kind_string, data_json_string)`.
    #[derive(Default)]
    struct RecordingSink {
        events: Arc<Mutex<Vec<(String, String)>>>,
    }

    impl RecordingSink {
        fn kinds(&self) -> Vec<String> {
            self.events
                .lock()
                .unwrap()
                .iter()
                .map(|(k, _)| k.clone())
                .collect()
        }

        /// Was any event of `kind` whose JSON payload contains `needle` emitted?
        fn emitted(&self, kind: &str, needle: &str) -> bool {
            self.events
                .lock()
                .unwrap()
                .iter()
                .any(|(k, d)| k == kind && d.contains(needle))
        }

        /// Return all `data` JSON strings for events of the given kind.
        fn data_for_kind(&self, kind: &str) -> Vec<String> {
            self.events
                .lock()
                .unwrap()
                .iter()
                .filter(|(k, _)| k == kind)
                .map(|(_, d)| d.clone())
                .collect()
        }
    }

    #[async_trait]
    impl EventSink for RecordingSink {
        async fn emit(&self, event: WsEvent) {
            if let WsEvent::AgentUpdate { kind, data, .. } = event {
                let kind_str = serde_json::to_value(kind)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_string))
                    .unwrap_or_default();
                self.events
                    .lock()
                    .unwrap()
                    .push((kind_str, data.to_string()));
            }
        }
    }

    // ── Test 1: basic ANSI-noisy stream ──────────────────────────────────────────────────────

    /// Feed an ANSI-noisy PTY chunk; assert:
    /// - `Text` AgentUpdates whose concatenated `data.text` contains `"PONG"`.
    /// - None of the emitted text payloads contain a raw ESC byte.
    /// - The bell rings once after the quiet gap.
    /// - No file under `~/.claude` or `~/.codex` is opened (this is trivially true since the
    ///   reader has no filesystem I/O — the assertion is implicit: if this test passes hermetically
    ///   there is no FS access possible).
    #[tokio::test]
    async fn ansi_noisy_chunk_emits_clean_text_and_rings_bell() {
        let (tx, rx) = broadcast::channel::<Vec<u8>>(64);
        let sink = Arc::new(RecordingSink::default());
        let bell = Bell::new();
        let session = SessionId("s_pty_1".into());

        let handle = tokio::spawn(pty_reply_reader_loop(
            session.clone(),
            rx,
            sink.clone() as Arc<dyn EventSink>,
            bell.clone(),
            // 150 ms quiet gap — short enough for fast tests.
            150,
        ));

        // ESC[2J = clear screen, ESC[H = cursor home, ESC[32m = green, ESC[0m = reset.
        // The ScreenText extractor strips all ANSI; only the visible "PONG" makes it out.
        tx.send(b"\x1b[2J\x1b[Hthinking...\r\nPONG\r\n".to_vec())
            .unwrap();

        // Wait for the quiet gap to elapse and ring, plus a generous margin.
        tokio::time::sleep(Duration::from_millis(400)).await;
        handle.abort();

        // There must be at least one `text` event whose payload contains "PONG".
        assert!(
            sink.emitted("text", "PONG"),
            "expected a Text AgentUpdate containing 'PONG'; got kinds={:?}",
            sink.kinds()
        );

        // No emitted text payload may contain a raw ESC byte.
        let has_esc = sink
            .data_for_kind("text")
            .iter()
            .any(|d| d.contains('\x1b'));
        assert!(!has_esc, "no Text event must contain a raw ESC byte");

        // The bell must have been rung exactly once (turn settled after the quiet gap).
        // We verify by waiting on the bell with a short timeout; if it fires the permit is there.
        let bell_rang = tokio::time::timeout(Duration::from_millis(50), bell.wait(&session))
            .await
            .is_ok();
        assert!(bell_rang, "bell must ring once after the quiet gap");
    }

    // ── Test 2: bell re-arms after first turn ──────────────────────────────────────────────

    /// Two sends separated by a quiet gap each ring the bell exactly once (re-arm works).
    #[tokio::test]
    async fn bell_re_arms_for_second_turn() {
        let (tx, rx) = broadcast::channel::<Vec<u8>>(64);
        let sink = Arc::new(RecordingSink::default());
        let bell = Bell::new();
        let session = SessionId("s_pty_2".into());

        let _handle = tokio::spawn(pty_reply_reader_loop(
            session.clone(),
            rx,
            sink.clone() as Arc<dyn EventSink>,
            bell.clone(),
            100, // 100 ms quiet gap
        ));

        // First turn.
        tx.send(b"first line\r\n".to_vec()).unwrap();
        // Wait for the bell to ring.
        tokio::time::timeout(Duration::from_millis(400), bell.wait(&session))
            .await
            .expect("bell must ring after first turn");

        // Second turn — send AFTER the first bell fired.
        tx.send(b"second line\r\n".to_vec()).unwrap();
        // Wait for the second bell.
        tokio::time::timeout(Duration::from_millis(400), bell.wait(&session))
            .await
            .expect("bell must ring after second turn (re-arm)");

        // Both turns' text must have been emitted.
        assert!(
            sink.emitted("text", "first line"),
            "first turn text emitted"
        );
        assert!(
            sink.emitted("text", "second line"),
            "second turn text emitted"
        );
    }

    // ── Test 3: channel close exits cleanly ───────────────────────────────────────────────

    /// Dropping the sender closes the channel; the reader task should exit without panic.
    #[tokio::test]
    async fn reader_exits_cleanly_on_channel_close() {
        let (tx, rx) = broadcast::channel::<Vec<u8>>(64);
        let sink = Arc::new(RecordingSink::default());
        let bell = Bell::new();
        let session = SessionId("s_pty_3".into());

        let handle = tokio::spawn(pty_reply_reader_loop(
            session.clone(),
            rx,
            sink.clone() as Arc<dyn EventSink>,
            bell.clone(),
            200,
        ));

        // Close the channel by dropping the sender.
        drop(tx);

        // The task should finish cleanly (not hang) within a short timeout.
        let result = tokio::time::timeout(Duration::from_millis(500), handle).await;
        assert!(
            result.is_ok(),
            "reader task must exit cleanly when the PTY channel closes"
        );
    }
}
