#![cfg(windows)]

use std::time::Duration;

use nexus_pty::PtySession;
use portable_pty::{CommandBuilder, PtySize};

#[tokio::test]
async fn conpty_carries_interactive_input_and_output() {
    let session = PtySession::spawn(
        CommandBuilder::new("cmd.exe"),
        PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        },
    )
    .expect("spawn cmd.exe in ConPTY");
    let mut output = session.subscribe();

    session
        .write(b"echo NEXUS_CONPTY_IO\r\n")
        .expect("write to ConPTY");

    let observed = tokio::time::timeout(Duration::from_secs(5), async {
        let mut bytes = Vec::new();
        while let Ok(chunk) = output.recv().await {
            bytes.extend_from_slice(&chunk);
            if String::from_utf8_lossy(&bytes).contains("NEXUS_CONPTY_IO") {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false);

    let alive = session.child_alive();
    let _ = session.kill();
    assert!(
        observed,
        "ConPTY emitted no echoed input; child_alive={alive}"
    );
}
