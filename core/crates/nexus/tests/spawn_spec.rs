use nexus::spawn_spec::parse_spawn_spec;

#[test]
fn minimal_manifest_gets_defaults() {
    let spec = parse_spawn_spec("goose", "[command]\nprogram = \"goose\"\n").unwrap();
    assert_eq!(spec.id.as_str(), "goose");
    assert_eq!(spec.command.program, "goose");
    assert!(spec.command.args.is_empty());
    assert!(spec.command.cwd.is_none());
    assert!(spec.command.env.is_empty());
    assert_eq!(spec.contract.agent_token(), "goose");
    assert_eq!(
        spec.contract.program(),
        "",
        "manifest-defined ACP harnesses must not advertise a headed TUI binary"
    );
}

#[test]
fn full_manifest_maps_every_field() {
    let spec = parse_spawn_spec(
        "goose",
        r#"
[command]
program = "goose"
args = ["acp", "--verbose"]
cwd = "/tmp/goose"

[command.env]
GOOSE_MODE = "acp"
"#,
    )
    .unwrap();
    assert_eq!(spec.command.args, vec!["acp", "--verbose"]);
    assert_eq!(spec.command.cwd.as_deref(), Some("/tmp/goose"));
    assert_eq!(
        spec.command.env,
        vec![("GOOSE_MODE".to_string(), "acp".to_string())]
    );
}

#[test]
fn invalid_id_stem_is_rejected() {
    let err = parse_spawn_spec("Not A Token!", "[command]\nprogram = \"x\"\n").unwrap_err();
    assert!(err.contains("invalid harness id"), "got: {err}");
}

#[test]
fn empty_program_is_rejected() {
    let err = parse_spawn_spec("goose", "[command]\nprogram = \"  \"\n").unwrap_err();
    assert!(err.contains("non-empty"), "got: {err}");
}

#[test]
fn unknown_keys_are_rejected() {
    let err =
        parse_spawn_spec("goose", "[command]\nprogram = \"goose\"\nbootsrap = true\n").unwrap_err();
    assert!(err.contains("invalid manifest"), "got: {err}");
}
