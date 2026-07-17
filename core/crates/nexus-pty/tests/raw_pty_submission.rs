use std::time::{Duration, Instant};

use nexus_pty::{HarnessInput as _, PtySession};
use portable_pty::{CommandBuilder, PtySize};

#[cfg(unix)]
fn passive_terminal_command() -> CommandBuilder {
    CommandBuilder::new("cat")
}

#[cfg(windows)]
fn passive_terminal_command() -> CommandBuilder {
    CommandBuilder::new("cmd.exe")
}

#[tokio::test]
async fn raw_pty_waits_for_a_large_paste_to_commit_before_submitting() {
    let session = PtySession::spawn(
        passive_terminal_command(),
        PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        },
    )
    .expect("spawn deterministic raw PTY fixture");
    let payload = "one complete Nexus turn\n".repeat(128);

    let started = Instant::now();
    session.send_turn(&payload).await.expect("submit turn");

    assert!(
        started.elapsed() >= Duration::from_millis(75),
        "send_turn returned before the TUI had a paste-settlement window"
    );
}
