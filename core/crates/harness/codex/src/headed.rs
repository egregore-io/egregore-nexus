/// Quote a single string as a TOML basic string (double-quoted, with the TOML escapes). Used to
/// build codex's `-c` inline-table MCP override so paths/args with spaces or quotes stay valid TOML.
fn toml_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Render `items` as a TOML array of basic strings: `["a", "b"]`.
fn toml_str_array(items: &[&str]) -> String {
    let inner = items
        .iter()
        .map(|s| toml_quote(s))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{inner}]")
}

/// Headed (TUI) PTY args for codex: nexus-bus MCP via a `-c` inline-table TOML override
/// (codex rejects claude's `--mcp-config`) + approvals/sandbox bypass for a non-interactive
/// fresh-cwd PTY launch. IDENTICAL to the former nexus-pty codex arm.
pub fn headed_pty_args(name: &str, project: &str, nexus_exe: &str) -> Vec<String> {
    headed_pty_args_with_identity(name, project, nexus_exe, "", "")
}

/// Headed (TUI) PTY args for a daemon-owned codex runtime with verified Nexus MCP identity.
pub fn headed_pty_args_with_identity(
    name: &str,
    project: &str,
    nexus_exe: &str,
    client_key: &str,
    agent: &str,
) -> Vec<String> {
    let mcp_args = mcp_args(name, project, client_key, agent);
    let args_refs = mcp_args.iter().map(String::as_str).collect::<Vec<_>>();
    let args_toml = toml_str_array(&args_refs);
    let inline = format!(
        "mcp_servers.nexus-bus={{ command = {}, args = {} }}",
        toml_quote(nexus_exe),
        args_toml,
    );
    vec![
        "-c".into(),
        inline,
        "--dangerously-bypass-approvals-and-sandbox".into(),
    ]
}

/// Headed CLI (`nexus launch codex`) args. DISTINCT from `headed_pty_args`: two
/// separate `-c` overrides, NO bypass flag. IDENTICAL to the former lifecycle codex
/// arm (build_harness_command). Preserved as-is; not unified with the PTY form.
pub fn headed_cli_args(name: &str, project: &str, nexus_exe: &str) -> Vec<String> {
    headed_cli_args_with_identity(name, project, nexus_exe, "", "")
}

/// Headed CLI (`nexus launch codex`) args with explicit Nexus MCP authentication.
pub fn headed_cli_args_with_identity(
    name: &str,
    project: &str,
    nexus_exe: &str,
    client_key: &str,
    agent: &str,
) -> Vec<String> {
    let mcp_args = mcp_args(name, project, client_key, agent);
    let args_toml = toml_str_array(&mcp_args.iter().map(String::as_str).collect::<Vec<_>>());
    vec![
        "-c".into(),
        format!(r#"mcp_servers.nexus-bus.command="{nexus_exe}""#),
        "-c".into(),
        format!("mcp_servers.nexus-bus.args={args_toml}"),
    ]
}

fn mcp_args(name: &str, project: &str, client_key: &str, agent: &str) -> Vec<String> {
    let mut args = vec![
        "mcp".to_string(),
        "--as".to_string(),
        name.to_string(),
        "--project".to_string(),
        project.to_string(),
    ];
    if !client_key.is_empty() {
        args.push("--client-key".to_string());
        args.push(client_key.to_string());
    }
    if !agent.is_empty() {
        args.push("--agent".to_string());
        args.push(agent.to_string());
    }
    args
}
