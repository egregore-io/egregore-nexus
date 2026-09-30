//! Per-runtime bridge directory and launch-local Claude settings.
//!
//! Claude's headed path does not have a Codex-style app-server socket. The native bridge uses
//! launch-local Claude hooks and append-only JSONL files under Nexus state so the daemon can later
//! forward structured activity without scraping the terminal.

use std::path::{Path, PathBuf};

use nexus_contracts::SessionId;
use serde_json::{json, Value};

use super::hooks;

/// File layout for one Claude native bridge runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeNativeBridgePaths {
    pub bridge_dir: PathBuf,
    pub settings_path: PathBuf,
    pub hook_log_path: PathBuf,
    pub message_delta_log_path: PathBuf,
    pub identity_path: PathBuf,
}

impl ClaudeNativeBridgePaths {
    /// Construct paths under the Nexus state root for `runtime_id`.
    pub fn new(state_root: &Path, runtime_id: &SessionId) -> Self {
        let bridge_dir = bridge_dir(state_root, runtime_id);
        Self {
            settings_path: bridge_dir.join("settings.json"),
            hook_log_path: hooks::hook_log_path(&bridge_dir),
            message_delta_log_path: hooks::message_delta_log_path(&bridge_dir),
            identity_path: hooks::identity_path(&bridge_dir),
            bridge_dir,
        }
    }
}

/// Deterministic bridge directory for a headed Claude runtime.
pub fn bridge_dir(state_root: &Path, runtime_id: &SessionId) -> PathBuf {
    state_root
        .join("claude-sessions")
        .join(runtime_id.0.as_str())
        .join("bridge")
}

/// Build the launch-local Claude settings JSON for the bridge.
pub fn launch_settings(
    paths: &ClaudeNativeBridgePaths,
    name: &str,
    project: &str,
    nexus_exe: &str,
) -> Value {
    launch_settings_with_identity(paths, name, project, nexus_exe, "", "")
}

/// Build launch-local Claude settings with an explicit Nexus runtime identity for the MCP server.
///
/// Empty `client_key`/`agent` preserves the legacy local shape. Daemon-owned headed launches pass
/// both values so the MCP command is authenticated as this runtime rather than by ambient shell env.
pub fn launch_settings_with_identity(
    paths: &ClaudeNativeBridgePaths,
    name: &str,
    project: &str,
    nexus_exe: &str,
    client_key: &str,
    agent: &str,
) -> Value {
    let mut mcp_args = vec![
        "mcp".to_string(),
        "--as".to_string(),
        name.to_string(),
        "--project".to_string(),
        project.to_string(),
    ];
    if !client_key.is_empty() {
        mcp_args.push("--client-key".to_string());
        mcp_args.push(client_key.to_string());
    }
    if !agent.is_empty() {
        mcp_args.push("--agent".to_string());
        mcp_args.push(agent.to_string());
    }
    json!({
        "hooks": hooks::settings_hooks(&paths.bridge_dir),
        "mcpServers": {
            "nexus-bus": {
                "command": nexus_exe,
                "args": mcp_args
            }
        }
    })
}

/// Prepare the bridge directory and write its launch-local Claude settings.
pub fn write_launch_settings(
    paths: &ClaudeNativeBridgePaths,
    name: &str,
    project: &str,
    nexus_exe: &str,
) -> std::io::Result<Value> {
    write_launch_settings_with_identity(paths, name, None, project, nexus_exe, "", "")
}

/// Prepare the bridge directory and write settings with an explicit Nexus runtime identity.
pub fn write_launch_settings_with_identity(
    paths: &ClaudeNativeBridgePaths,
    name: &str,
    session_id: Option<&SessionId>,
    project: &str,
    nexus_exe: &str,
    client_key: &str,
    agent: &str,
) -> std::io::Result<Value> {
    std::fs::create_dir_all(&paths.bridge_dir)?;
    if let Some(session_id) = session_id {
        write_bridge_identity(paths, name, session_id)?;
    }
    let settings =
        launch_settings_with_identity(paths, name, project, nexus_exe, client_key, agent);
    let body = serde_json::to_vec_pretty(&settings)?;
    std::fs::write(&paths.settings_path, body)?;
    Ok(settings)
}

/// Write the bridge-local identity manifest consumed by the `SessionStart` guard.
pub fn write_bridge_identity(
    paths: &ClaudeNativeBridgePaths,
    name: &str,
    session_id: &SessionId,
) -> std::io::Result<()> {
    std::fs::create_dir_all(&paths.bridge_dir)?;
    let body = format!(
        "NEXUS_BRIDGE_NAME={}\nNEXUS_BRIDGE_SESSION_ID={}\n",
        shell_quote(name),
        shell_quote(&session_id.0)
    );
    std::fs::write(&paths.identity_path, body)
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}
