//! Headed Claude command builders.
//!
//! Claude accepts MCP configuration as one `--mcp-config <json>` argument. The
//! daemon-owned PTY/tmux launch also needs a permission bypass mode so a fresh
//! cwd can process injected turns without waiting at interactive permission or
//! trust gates. `bypassPermissions` is the complete mode; the redundant
//! `--dangerously-skip-permissions` switch is deliberately omitted because Claude rejects it for
//! root-owned container gates.

/// Headed PTY args for a daemon-owned Claude TUI.
pub fn headed_pty_args(name: &str, project: &str, nexus_exe: &str) -> Vec<String> {
    let mut args = headed_cli_args(name, project, nexus_exe);
    args.push("--permission-mode".into());
    args.push("bypassPermissions".into());
    args
}

/// Headed PTY args for a daemon-owned Claude TUI with a verified Nexus runtime identity.
pub fn headed_pty_args_with_identity(
    name: &str,
    project: &str,
    nexus_exe: &str,
    client_key: &str,
    agent: &str,
) -> Vec<String> {
    let mut args = headed_cli_args_with_identity(name, project, nexus_exe, client_key, agent);
    args.push("--permission-mode".into());
    args.push("bypassPermissions".into());
    args
}

/// Headed CLI args for `nexus launch claude`.
pub fn headed_cli_args(name: &str, project: &str, nexus_exe: &str) -> Vec<String> {
    headed_cli_args_with_identity(name, project, nexus_exe, "", "")
}

/// Headed CLI args for `nexus launch claude` with explicit Nexus MCP authentication.
///
/// Empty `client_key`/`agent` preserves the legacy local CLI shape for non-daemon callers. Daemon
/// launches must pass both so the MCP server cannot fall back to an ambient identity.
pub fn headed_cli_args_with_identity(
    name: &str,
    project: &str,
    nexus_exe: &str,
    client_key: &str,
    agent: &str,
) -> Vec<String> {
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
    let mcp = serde_json::json!({
        "mcpServers": {
            "nexus-bus": {
                "command": nexus_exe,
                "args": mcp_args
            }
        }
    })
    .to_string();
    vec!["--mcp-config".into(), mcp]
}
