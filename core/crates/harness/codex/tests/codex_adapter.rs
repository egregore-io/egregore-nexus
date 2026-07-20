//! Codex ACP adapter — full-turn over the hermetic fake (moved from nexus-agent).
use std::ffi::OsString;
use std::sync::Mutex;

use nexus_agent::adapter::engine::HarnessCommand;
use nexus_agent::Adapter;
use nexus_contracts::AgentUpdateKind;
use nexus_harness_codex::{codex_command, CodexAdapter};

/// The compiled fake harness binary. `CARGO_BIN_EXE_<name>` is injected by Cargo because the
/// harness is a `[[bin]]` of this crate, so it is always built before these tests run.
const FAKE_HARNESS: &str = env!("CARGO_BIN_EXE_codex_fake_acp_agent");

static COMMAND_ENV_LOCK: Mutex<()> = Mutex::new(());
const COMMAND_ENV_KEYS: [&str; 5] = [
    "NEXUS_CODEX_ACP_CMD",
    "NEXUS_CODEX_ACP_ARGS",
    "NEXUS_CODEX_ACP_PACKAGE",
    "NEXUS_CODEX_REASONING_SUMMARY",
    "NEXUS_CODEX_RAW_REASONING",
];

struct EnvGuard(Vec<(&'static str, Option<OsString>)>);

impl EnvGuard {
    fn clear() -> Self {
        let saved = COMMAND_ENV_KEYS
            .into_iter()
            .map(|key| {
                let value = std::env::var_os(key);
                std::env::remove_var(key);
                (key, value)
            })
            .collect();
        Self(saved)
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, value) in self.0.drain(..) {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

fn reply_chunks(events: Vec<nexus_agent::StreamEvent>) -> Vec<String> {
    events
        .into_iter()
        .filter(|e| e.kind == AgentUpdateKind::Text)
        .filter_map(|e| {
            e.data
                .get("text")
                .and_then(|t| t.as_str())
                .map(str::to_owned)
        })
        .collect()
}

fn fake_command() -> HarnessCommand {
    HarnessCommand {
        program: FAKE_HARNESS.to_string(),
        args: vec![],
        cwd: None,
        ..Default::default()
    }
}

#[tokio::test]
async fn codex_adapter_full_turn_over_acp() {
    let adapter = CodexAdapter::with_command(fake_command());
    adapter.open_session().await.expect("open_session");
    adapter.inject("status?".to_string()).await.expect("inject");
    let chunks = reply_chunks(adapter.stream_updates().await.expect("stream_updates"));
    assert_eq!(chunks, vec!["echo: ".to_string(), "status?".to_string()]);
}

#[tokio::test]
async fn codex_adapter_compact_uses_the_acp_native_command() {
    let adapter = CodexAdapter::with_command(fake_command());
    adapter.open_session().await.expect("open_session");

    adapter.compact().await.expect("compact");

    let chunks = reply_chunks(adapter.stream_updates().await.expect("stream_updates"));
    assert_eq!(
        chunks,
        vec!["echo: ".to_string(), "/compact".to_string()],
        "the pinned codex-acp bridge recognizes /compact and maps it to thread/compact/start"
    );
}

#[test]
fn codex_resolves_pinned_app_server_acp_command_without_legacy_flags() {
    let _lock = COMMAND_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::clear();

    let cmd = codex_command(None);

    assert_eq!(
        cmd.program,
        nexus_harness_core::native_npm_runner(nexus_harness_core::NativeProcessPlatform::current())
    );
    assert_eq!(cmd.args, ["-y", "@agentclientprotocol/codex-acp@1.1.2"]);
}

#[test]
fn codex_package_override_is_passed_without_package_specific_arguments() {
    let _lock = COMMAND_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::clear();
    std::env::set_var("NEXUS_CODEX_ACP_PACKAGE", "example-codex-acp@4.2.0");
    std::env::set_var("NEXUS_CODEX_REASONING_SUMMARY", "detailed");
    std::env::set_var("NEXUS_CODEX_RAW_REASONING", "true");

    let cmd = codex_command(Some("/tmp/project".into()));

    assert_eq!(
        cmd.program,
        nexus_harness_core::native_npm_runner(nexus_harness_core::NativeProcessPlatform::current())
    );
    assert_eq!(cmd.args, ["-y", "example-codex-acp@4.2.0"]);
    assert_eq!(cmd.cwd.as_deref(), Some("/tmp/project"));
}

#[test]
fn codex_complete_command_override_remains_authoritative() {
    let _lock = COMMAND_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::clear();
    std::env::set_var("NEXUS_CODEX_ACP_CMD", "/opt/codex-acp");
    std::env::set_var("NEXUS_CODEX_ACP_ARGS", "--stdio --verbose");

    let cmd = codex_command(None);

    assert_eq!(cmd.program, "/opt/codex-acp");
    assert_eq!(cmd.args, ["--stdio", "--verbose"]);
}
