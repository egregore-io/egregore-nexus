use nexus_pty::command::harness_command;

fn argv(cmd: portable_pty::CommandBuilder) -> Vec<String> {
    cmd.get_argv()
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect()
}

#[test]
fn hermes_headed_uses_gateway_run_not_cli_tui() {
    let args = argv(harness_command(
        "hermes",
        "ada",
        "default",
        "/usr/bin/nexus",
        Some("/work/project"),
    ));

    assert_eq!(args.first().map(String::as_str), Some("hermes"));
    assert_eq!(args.get(1).map(String::as_str), Some("gateway"));
    assert_eq!(args.get(2).map(String::as_str), Some("run"));
    assert!(
        !args
            .iter()
            .any(|arg| arg == "--cli" || arg == "--accept-hooks"),
        "Hermes headed uses gateway extension points, not the old CLI TUI args: {args:?}"
    );
}
