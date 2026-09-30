#[test]
fn tmux_launch_shell_command_scrubs_inherited_identity_and_exports_runtime_identity() {
    let env = vec![
        ("NEXUS_NAME".to_string(), "hugo".to_string()),
        ("NEXUS_CLIENT_KEY".to_string(), "nexus_ck_hugo".to_string()),
        ("NEXUS_PROJECT".to_string(), "default".to_string()),
        ("NEXUS_AGENT".to_string(), "claude".to_string()),
        ("UNSAFE-NAME".to_string(), "ignored".to_string()),
    ];

    let shell = nexus_pty::tmux_launch_shell_command(
        "claude",
        &["--settings".to_string(), "/tmp/settings.json".to_string()],
        "/work/project",
        &env,
    );

    assert!(
        shell.contains("grep -E '^(NEXUS_|CODEX_|CLAUDE_CONFIG_DIR$|HERMES_HOME$|OPENCODE_HOME$|OPENCODE_CONFIG_DIR$)'"),
        "shell must scrub inherited identity/harness env: {shell}"
    );
    assert!(shell.contains("export NEXUS_NAME='hugo';"), "{shell}");
    assert!(
        shell.contains("export NEXUS_CLIENT_KEY='nexus_ck_hugo';"),
        "{shell}"
    );
    assert!(shell.contains("export NEXUS_PROJECT='default';"), "{shell}");
    assert!(shell.contains("export NEXUS_AGENT='claude';"), "{shell}");
    assert!(
        !shell.contains("UNSAFE-NAME"),
        "invalid env keys must not be exported: {shell}"
    );
    assert!(
        shell.contains("cd '/work/project' && exec 'claude'"),
        "{shell}"
    );
}
