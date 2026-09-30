use nexus_harness_codex::{headed_cli_args, headed_pty_args};

#[test]
fn codex_headed_pty_args_escape_toml_values() {
    let argv = headed_pty_args("a\"b\\c", "lens", "C:\\Program Files\\Nexus\\nexus.exe");
    let inline = argv
        .windows(2)
        .find_map(|pair| (pair[0] == "-c").then(|| pair[1].as_str()))
        .expect("-c override");
    let parsed: toml::Value = toml::from_str(inline).expect("valid TOML override");
    let server = &parsed["mcp_servers"]["nexus-bus"];

    assert_eq!(
        server["command"].as_str(),
        Some("C:\\Program Files\\Nexus\\nexus.exe")
    );
    let arguments = server["args"].as_array().expect("argument array");
    assert_eq!(arguments[2].as_str(), Some("a\"b\\c"));
}

#[test]
fn codex_headed_cli_args_two_c_flags_no_bypass() {
    let argv = headed_cli_args("ada", "lens", "/usr/bin/nexus");

    assert!(argv.contains(&"-c".to_string()), "must contain -c flag");
    assert!(
        argv.iter()
            .any(|a| a.contains("mcp_servers.nexus-bus.command=")),
        "some arg must contain mcp_servers.nexus-bus.command="
    );
    assert!(
        argv.iter()
            .any(|a| a.contains("mcp_servers.nexus-bus.args=")
                && a.contains("--as")
                && a.contains("ada")),
        "some arg must contain mcp_servers.nexus-bus.args= and --as and ada"
    );
    assert!(
        !argv
            .iter()
            .any(|a| a == "--dangerously-bypass-approvals-and-sandbox"),
        "CLI form must NOT include --dangerously-bypass-approvals-and-sandbox"
    );
}

#[test]
fn codex_headed_pty_args_uses_c_override_not_mcp_config() {
    let argv = headed_pty_args("ada", "lens", "/usr/bin/nexus");

    assert!(
        !argv.iter().any(|a| a == "--mcp-config"),
        "codex must NOT get claude's --mcp-config"
    );
    let idx = argv
        .iter()
        .position(|a| a == "-c")
        .expect("codex must pass a -c override");
    let val = &argv[idx + 1];
    assert!(
        val.starts_with("mcp_servers.nexus-bus="),
        "the -c override targets the nexus-bus server: {val}"
    );
    assert!(
        val.contains("command = \"/usr/bin/nexus\""),
        "carries the nexus exe: {val}"
    );
    assert!(
        val.contains("\"--as\"") && val.contains("\"ada\""),
        "carries --as <name>: {val}"
    );
    assert!(val.contains("\"lens\""), "carries the project: {val}");
    assert!(
        !val.contains("\"--socket\""),
        "store-backed MCP must not carry a daemon socket arg: {val}"
    );
    assert!(
        argv.iter()
            .any(|a| a == "--dangerously-bypass-approvals-and-sandbox"),
        "codex keeps its approval/sandbox bypass"
    );
}
