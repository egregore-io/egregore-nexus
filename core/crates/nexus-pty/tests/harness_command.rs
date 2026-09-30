use nexus_pty::command::harness_command;

fn argv(command: portable_pty::CommandBuilder) -> Vec<String> {
    command
        .get_argv()
        .iter()
        .map(|argument| argument.to_string_lossy().into_owned())
        .collect()
}

#[test]
fn claude_command_wires_the_nexus_bus_mcp_and_is_interactive() {
    let args = argv(harness_command(
        "claude",
        "ada",
        "default",
        "/usr/bin/nexus",
        None,
    ));

    assert!(!args.iter().any(|argument| argument == "--print"));
    assert!(!args.iter().any(|argument| argument == "--output-format"));
    let joined = args.join(" ");
    assert!(joined.contains("--mcp-config"));
    assert!(joined.contains("\"--as\""));
    assert!(joined.contains("ada"));
}

#[test]
fn codex_command_uses_c_override_not_mcp_config() {
    let args = argv(harness_command(
        "codex",
        "ada",
        "lens",
        "/usr/bin/nexus",
        None,
    ));

    let override_index = args
        .iter()
        .position(|argument| argument == "-c")
        .expect("Codex must pass a -c override");
    assert!(args[override_index + 1].starts_with("mcp_servers.nexus-bus="));
    assert!(args
        .iter()
        .any(|argument| argument == "--dangerously-bypass-approvals-and-sandbox"));
    assert!(!args.iter().any(|argument| argument == "--mcp-config"));
}

#[test]
fn opencode_command_is_bare() {
    let args = argv(harness_command(
        "opencode",
        "ada",
        "qa",
        "/usr/bin/nexus",
        None,
    ));

    assert_eq!(args, ["opencode"]);
}

#[test]
fn hermes_command_uses_gateway_run() {
    let args = argv(harness_command(
        "hermes",
        "ada",
        "qa",
        "/usr/bin/nexus",
        None,
    ));

    assert_eq!(args, ["hermes", "gateway", "run"]);
}
