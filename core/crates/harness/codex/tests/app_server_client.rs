//! Integration test for `CodexAppServerClient`.
//!
//! Uses the hermetic `fake_codex_app_server` binary so no real codex
//! installation is required.
//!
//! Run: `cargo test -p nexus-harness-codex --test app_server_client`

use nexus_harness_codex::{
    app_server::method, CodexAppServer, CodexAppServerClient, SupervisorOpts,
};

/// Path to the hermetic fake binary, injected by Cargo.
const FAKE_BIN: &str = env!("CARGO_BIN_EXE_fake_codex_app_server");

/// Build a unique temp directory for this test run.
fn tempdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nexus-client-test-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
        tag,
    ));
    std::fs::create_dir_all(&dir).expect("create test tempdir");
    dir
}

/// Clean up a temp directory after the test.
fn cleanup(dir: &std::path::Path) {
    let _ = std::fs::remove_dir_all(dir);
}

// ---------------------------------------------------------------------------
// RED → GREEN test
// ---------------------------------------------------------------------------

/// Drives the full connect → thread_start → turn_start pipeline against the
/// fake server and asserts the first notification is an agentMessage/delta.
///
/// IMPORTANT: `notifications()` is taken BEFORE `turn_start` so no frames
/// emitted during the turn are missed.
#[tokio::test]
async fn turn_start_streams_a_delta_then_turn_end() {
    let dir = tempdir("client");

    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: Some(dir.join("codex-home")),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![],
    })
    .await
    .expect("fake app-server should start");

    let cli = CodexAppServerClient::connect(srv.socket(), "nexus")
        .await
        .expect("client should connect and initialize");

    let thread = cli
        .thread_start()
        .await
        .expect("thread_start should succeed");
    assert_eq!(thread, "fake-thread", "fake server returns 'fake-thread'");

    // Take the receiver BEFORE turn_start so notifications emitted during the
    // turn are not lost.
    let mut notes = cli.notifications();

    cli.turn_start(&thread, "hello")
        .await
        .expect("turn_start should succeed");

    // First rendered notification must be item/agentMessage/delta with delta="hello".
    let first = notes
        .recv()
        .await
        .expect("at least one notification expected");
    assert_eq!(
        first.method,
        method::AGENT_MESSAGE_DELTA,
        "first notification method should be AGENT_MESSAGE_DELTA"
    );
    assert_eq!(
        first.params["delta"], "hello",
        "delta field should equal the submitted text"
    );

    srv.shutdown().await;
    cleanup(&dir);
}
