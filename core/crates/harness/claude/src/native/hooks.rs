//! Launch-local Claude hook settings for the native headed bridge.
//!
//! The hook commands append Claude's hook stdin to object-stream files inside the per-runtime
//! bridge directory. The forwarder can later tail those files without relying on PTY screen
//! scraping.

use std::path::{Path, PathBuf};

use serde_json::{json, Value};

/// Claude hook events captured by the native bridge.
pub const HOOK_EVENTS: &[&str] = &[
    "SessionStart",
    "Stop",
    "StopFailure",
    "UserPromptSubmit",
    "PreCompact",
    "MessageDisplay",
];

/// Append-only JSON object stream for lifecycle/user-prompt hook records.
pub const HOOK_LOG_FILE: &str = "hooks.jsonl";

/// Append-only JSON object stream for Claude message-display delta records.
pub const MESSAGE_DELTA_LOG_FILE: &str = "message_display.jsonl";

/// Launch-local Nexus identity stamped into a Claude native bridge directory.
pub const BRIDGE_IDENTITY_FILE: &str = "identity.env";

/// Return the lifecycle hook log path for `bridge_dir`.
pub fn hook_log_path(bridge_dir: &Path) -> PathBuf {
    bridge_dir.join(HOOK_LOG_FILE)
}

/// Return the message-display delta log path for `bridge_dir`.
pub fn message_delta_log_path(bridge_dir: &Path) -> PathBuf {
    bridge_dir.join(MESSAGE_DELTA_LOG_FILE)
}

/// Return the launch-local identity path for `bridge_dir`.
pub fn identity_path(bridge_dir: &Path) -> PathBuf {
    bridge_dir.join(BRIDGE_IDENTITY_FILE)
}

/// Build the `hooks` object for a launch-local Claude settings file.
pub fn settings_hooks(bridge_dir: &Path) -> Value {
    let mut hooks = serde_json::Map::new();
    for event in HOOK_EVENTS {
        hooks.insert((*event).to_string(), hook_entries_for(bridge_dir, event));
    }
    Value::Object(hooks)
}

/// Build the hook entry list for one Claude hook event.
pub fn hook_entries_for(bridge_dir: &Path, event: &str) -> Value {
    json!([
        {
            "hooks": [
                {
                    "type": "command",
                    "command": hook_command(bridge_dir, event)
                }
            ]
        }
    ])
}

/// Return the shell command Claude should run for a hook event.
///
/// The command writes one JSON object per invocation with an `event` field and the hook stdin as
/// the `payload`. `MessageDisplay` records are separated because they are high-volume text deltas.
/// A file lock serializes concurrent hook subprocesses; the daemon-side parser also refuses to
/// advance past a partial trailing object, so a poll that races the hook writer retries safely.
/// `SessionStart` also verifies the Nexus-owned launch-local bridge identity before writing
/// anything, so a resumed Claude cwd cannot silently register as a different Nexus runtime. This
/// guard does not inspect or claim Claude Code's native session identity.
pub fn hook_command(bridge_dir: &Path, event: &str) -> String {
    let log_path = if event == "MessageDisplay" {
        message_delta_log_path(bridge_dir)
    } else {
        hook_log_path(bridge_dir)
    };
    let script = if event == "SessionStart" {
        format!("{SESSION_START_IDENTITY_GUARD}\n{APPEND_HOOK_RECORD}")
    } else {
        APPEND_HOOK_RECORD.to_string()
    };
    format!(
        "NEXUS_CLAUDE_BRIDGE={} NEXUS_CLAUDE_HOOK_EVENT={} NEXUS_CLAUDE_HOOK_LOG={} /usr/bin/env sh -lc {}",
        shell_quote(&path_to_text(bridge_dir)),
        shell_quote(event),
        shell_quote(&path_to_text(&log_path)),
        shell_quote(&script)
    )
}

const SESSION_START_IDENTITY_GUARD: &str = r#"if [ "$NEXUS_CLAUDE_HOOK_EVENT" = "SessionStart" ] && [ -n "${NEXUS_CLAUDE_BRIDGE:-}" ] && [ -f "$NEXUS_CLAUDE_BRIDGE/identity.env" ]; then
  . "$NEXUS_CLAUDE_BRIDGE/identity.env"
  NEXUS_ENV_IDENTITY="${NEXUS_NAME:-${NEXUS_AGENT_ID:-}}"
  if [ "${NEXUS_BRIDGE_NAME:-}" != "$NEXUS_ENV_IDENTITY" ] || [ "${NEXUS_BRIDGE_SESSION_ID:-}" != "${NEXUS_SESSION_ID:-}" ]; then
    echo "Nexus Claude SessionStart identity mismatch" >&2
    echo "bridge identity: name=${NEXUS_BRIDGE_NAME:-<missing>} session=${NEXUS_BRIDGE_SESSION_ID:-<missing>}" >&2
    echo "environment identity: name=${NEXUS_NAME:-<missing>} agent_id=${NEXUS_AGENT_ID:-<missing>} session=${NEXUS_SESSION_ID:-<missing>}" >&2
    echo "Refusing to register this Claude cwd as the environment identity. Resume from the owning agent cwd/session, or override intentionally with NEXUS_NAME=${NEXUS_BRIDGE_NAME:-<name>} NEXUS_SESSION_ID=${NEXUS_BRIDGE_SESSION_ID:-<session>} NEXUS_CLIENT_KEY=<matching-client-key>." >&2
    exit 64
  fi
fi"#;

const APPEND_HOOK_RECORD: &str = r#"mkdir -p "$NEXUS_CLAUDE_BRIDGE"; exec 9>>"$NEXUS_CLAUDE_HOOK_LOG.lock"; flock 9; printf '{"event":"%s","payload":' "$NEXUS_CLAUDE_HOOK_EVENT" >> "$NEXUS_CLAUDE_HOOK_LOG"; cat >> "$NEXUS_CLAUDE_HOOK_LOG"; printf '}\n' >> "$NEXUS_CLAUDE_HOOK_LOG""#;

fn path_to_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}
