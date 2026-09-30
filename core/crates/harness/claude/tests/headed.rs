#[test]
fn claude_headed_cli_args_wire_nexus_bus_mcp_only() {
    let argv = nexus_harness_claude::headed_cli_args("ada", "lens", "/usr/bin/nexus");
    let idx = argv
        .iter()
        .position(|a| a == "--mcp-config")
        .expect("--mcp-config not found");
    let json_str = &argv[idx + 1];
    let v: serde_json::Value = serde_json::from_str(json_str).expect("invalid JSON");
    assert!(v["mcpServers"]["nexus-bus"].is_object());
    assert_eq!(v["mcpServers"]["nexus-bus"]["command"], "/usr/bin/nexus");
    let mcp_args = v["mcpServers"]["nexus-bus"]["args"].as_array().unwrap();
    assert!(mcp_args.iter().any(|a| a == "--as"));
    assert!(mcp_args.iter().any(|a| a == "ada"));
    assert!(
        !argv.iter().any(|a| a == "--dangerously-skip-permissions"),
        "CLI form must not add PTY bypass flags"
    );
}

#[test]
fn claude_headed_pty_args_add_permission_bypass() {
    let argv = nexus_harness_claude::headed_pty_args("ada", "lens", "/usr/bin/nexus");
    assert!(argv.iter().any(|a| a == "--mcp-config"));
    assert!(argv.iter().any(|a| a == "--permission-mode"));
    assert!(argv.iter().any(|a| a == "bypassPermissions"));
    assert!(
        !argv.iter().any(|a| a == "--dangerously-skip-permissions"),
        "bypassPermissions is sufficient and remains valid for root-owned Docker gates"
    );
    assert!(!argv.iter().any(|a| a == "--print"));
    assert!(!argv.iter().any(|a| a == "--output-format"));
}

#[test]
fn claude_headed_pty_args_carry_verified_runtime_identity() {
    let argv = nexus_harness_claude::headed_pty_args_with_identity(
        "hugo",
        "default",
        "/usr/bin/nexus",
        "nexus_ck_hugo",
        "claude",
    );
    let idx = argv
        .iter()
        .position(|a| a == "--mcp-config")
        .expect("--mcp-config not found");
    let json_str = &argv[idx + 1];
    let v: serde_json::Value = serde_json::from_str(json_str).expect("invalid JSON");
    let args: Vec<_> = v["mcpServers"]["nexus-bus"]["args"]
        .as_array()
        .unwrap()
        .iter()
        .map(|arg| arg.as_str().unwrap())
        .collect();

    assert!(args.windows(2).any(|pair| pair == ["--as", "hugo"]));
    assert!(args.windows(2).any(|pair| pair == ["--project", "default"]));
    assert!(args
        .windows(2)
        .any(|pair| pair == ["--client-key", "nexus_ck_hugo"]));
    assert!(args.windows(2).any(|pair| pair == ["--agent", "claude"]));
}
