fn argv_contains_pair(args: &[String], key: &str, value: &str) -> bool {
    args.windows(2).any(|pair| pair == [key, value])
}

#[test]
fn codex_headed_pty_args_carry_verified_runtime_identity() {
    let argv = nexus_harness_codex::headed_pty_args_with_identity(
        "beatrice",
        "default",
        "/usr/bin/nexus",
        "nexus_ck_beatrice",
        "codex",
    );
    let inline = argv
        .windows(2)
        .find_map(|pair| (pair[0] == "-c").then(|| pair[1].as_str()))
        .expect("-c override");

    assert!(inline.contains(r#""--as""#), "{inline}");
    assert!(inline.contains(r#""beatrice""#), "{inline}");
    assert!(inline.contains(r#""--project""#), "{inline}");
    assert!(inline.contains(r#""default""#), "{inline}");
    assert!(inline.contains(r#""--client-key""#), "{inline}");
    assert!(inline.contains(r#""nexus_ck_beatrice""#), "{inline}");
    assert!(inline.contains(r#""--agent""#), "{inline}");
    assert!(inline.contains(r#""codex""#), "{inline}");
}

#[test]
fn codex_headed_cli_args_carry_verified_runtime_identity() {
    let argv = nexus_harness_codex::headed_cli_args_with_identity(
        "beatrice",
        "default",
        "/usr/bin/nexus",
        "nexus_ck_beatrice",
        "codex",
    );
    assert!(argv_contains_pair(
        &argv,
        "-c",
        r#"mcp_servers.nexus-bus.command="/usr/bin/nexus""#
    ));
    let args_override = argv
        .windows(2)
        .find_map(|pair| {
            (pair[0] == "-c" && pair[1].starts_with("mcp_servers.nexus-bus.args="))
                .then(|| pair[1].as_str())
        })
        .expect("args override");
    assert!(args_override.contains(r#""--client-key""#));
    assert!(args_override.contains(r#""nexus_ck_beatrice""#));
    assert!(args_override.contains(r#""--agent""#));
    assert!(args_override.contains(r#""codex""#));
}
