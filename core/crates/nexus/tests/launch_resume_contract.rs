use clap::Parser;
use nexus::cli::commands::lifecycle::{
    build_spawn_request, launch_mode, launch_mode_for_resume, resolve_launch_resume,
    validate_launch_harness_args, validate_launch_resume,
};
use nexus::cli::{Cli, Command};

const RESUME_KEY: &str = "codex-thread-test";
const CLAUDE_SESSION_ID: &str = "claude-session-test";
const NATIVE_FLAG: &str = "--native-flag";
const NATIVE_VALUE: &str = "value";

fn test_cwd() -> String {
    std::env::current_dir()
        .expect("test cwd should be available")
        .to_string_lossy()
        .into_owned()
}

#[test]
fn build_spawn_request_carries_resume_key() {
    let mode = launch_mode(false, true, true, None);
    let req = build_spawn_request(
        hid("codex"),
        Some("resumed-codex".into()),
        "resumed-codex".into(),
        Some(test_cwd()),
        "default".into(),
        None,
        None,
        Some(RESUME_KEY.into()),
        Vec::new(),
        mode,
    );

    assert_eq!(req.resume.as_deref(), Some(RESUME_KEY));
    assert!(req.harness_args.is_empty());
    assert!(!req.headless);
}

#[test]
fn build_spawn_request_carries_harness_args() {
    let mode = launch_mode(false, true, true, None);
    let req = build_spawn_request(
        hid("claude"),
        Some("resumed-claude".into()),
        "resumed-claude".into(),
        Some(test_cwd()),
        "default".into(),
        None,
        None,
        None,
        vec!["--resume".into(), CLAUDE_SESSION_ID.into()],
        mode,
    );

    assert_eq!(
        req.harness_args,
        ["--resume".to_string(), CLAUDE_SESSION_ID.to_string()]
    );
    assert_eq!(req.resume, None);
    assert!(!req.headless);
}

#[test]
fn codex_tail_resume_parses_as_harness_args() {
    let cli = Cli::try_parse_from([
        "nexus", "launch", "--name", "chloe", "codex", "resume", RESUME_KEY,
    ])
    .expect("parse launch tail resume");

    let Command::Launch(args) = cli.command else {
        panic!("expected launch command");
    };
    assert_eq!(args.name.as_deref(), Some("chloe"));
    assert_eq!(args.kind, hid("codex"));
    assert_eq!(args.harness_args, ["resume", RESUME_KEY]);
}

#[test]
fn resume_command_parses_name_or_session_id_targets() {
    let cli = Cli::try_parse_from(["nexus", "resume", "chloe"]).expect("parse resume by name");
    let Command::Resume(args) = cli.command else {
        panic!("expected resume command");
    };
    assert_eq!(args.target, "chloe");

    let cli =
        Cli::try_parse_from(["nexus", "resume", "s_chloe"]).expect("parse resume by session id");
    let Command::Resume(args) = cli.command else {
        panic!("expected resume command");
    };
    assert_eq!(args.target, "s_chloe");
}

#[test]
fn codex_tail_resume_normalizes_to_resume_key() {
    let args = vec!["resume".to_string(), RESUME_KEY.to_string()];
    let resolved = resolve_launch_resume(&hid("codex"), &args).expect("tail resume should resolve");
    assert_eq!(resolved.resume.as_deref(), Some(RESUME_KEY));
    assert!(resolved.harness_args.is_empty());
    assert!(resolved.requires_tui);
}

#[test]
fn codex_non_resume_tail_passes_through_as_harness_args() {
    let cli = Cli::try_parse_from([
        "nexus",
        "launch",
        "--name",
        "chloe",
        "codex",
        "--model",
        "gpt-5-codex",
    ])
    .expect("parse codex native tail");

    let Command::Launch(args) = cli.command else {
        panic!("expected launch command");
    };
    assert_eq!(args.name.as_deref(), Some("chloe"));
    assert_eq!(args.kind, hid("codex"));
    assert_eq!(args.harness_args, ["--model", "gpt-5-codex"]);

    let resolved = resolve_launch_resume(&args.kind, &args.harness_args)
        .expect("codex native tail should pass through");
    assert_eq!(resolved.resume, None);
    assert_eq!(resolved.harness_args, ["--model", "gpt-5-codex"]);
    assert!(resolved.requires_tui);
}

#[test]
fn all_tui_harness_tails_parse_as_harness_native_args() {
    for (kind, harness) in [
        (hid("claude"), "claude"),
        (hid("codex"), "codex"),
        (hid("opencode"), "opencode"),
        (hid("hermes"), "hermes"),
    ] {
        let cli = Cli::try_parse_from([
            "nexus",
            "launch",
            "--name",
            "probe",
            harness,
            NATIVE_FLAG,
            NATIVE_VALUE,
        ])
        .unwrap_or_else(|error| panic!("{harness} tail should parse: {error}"));

        let Command::Launch(args) = cli.command else {
            panic!("expected launch command for {harness}");
        };
        assert_eq!(args.kind, kind);
        assert_eq!(args.harness_args, [NATIVE_FLAG, NATIVE_VALUE]);
    }
}

#[test]
fn all_tui_harness_non_resume_tails_forward_to_spawn_request() {
    for kind in [hid("claude"), hid("codex"), hid("opencode"), hid("hermes")] {
        let harness_args = vec![NATIVE_FLAG.to_string(), NATIVE_VALUE.to_string()];
        let resolved = resolve_launch_resume(&kind, &harness_args)
            .unwrap_or_else(|error| panic!("{kind:?} tail should resolve: {error}"));
        assert_eq!(resolved.resume, None);
        assert_eq!(resolved.harness_args, harness_args);
        assert!(resolved.requires_tui);

        let req = build_spawn_request(
            kind.clone(),
            Some(format!("{kind:?}-probe")),
            format!("{kind:?}-probe"),
            Some(test_cwd()),
            "default".into(),
            None,
            None,
            resolved.resume,
            resolved.harness_args,
            launch_mode_for_resume(false, true, true, None, resolved.requires_tui),
        );
        assert_eq!(req.harness_args, [NATIVE_FLAG, NATIVE_VALUE]);
        assert!(!req.headless);
    }
}

#[test]
fn claude_tail_resume_flag_parses_as_harness_args() {
    let cli = Cli::try_parse_from([
        "nexus",
        "launch",
        "--name",
        "hugo",
        "claude",
        "--resume",
        CLAUDE_SESSION_ID,
    ])
    .expect("parse claude native resume tail");

    let Command::Launch(args) = cli.command else {
        panic!("expected launch command");
    };
    assert_eq!(args.name.as_deref(), Some("hugo"));
    assert_eq!(args.kind, hid("claude"));
    assert_eq!(args.harness_args, ["--resume", CLAUDE_SESSION_ID]);
}

#[test]
fn claude_tail_resume_command_passes_through_as_native_harness_args() {
    let args = vec!["resume".to_string(), CLAUDE_SESSION_ID.to_string()];
    let resolved =
        resolve_launch_resume(&hid("claude"), &args).expect("claude native tail should resolve");

    assert_eq!(resolved.resume, None);
    assert_eq!(resolved.harness_args, args);
    assert!(resolved.requires_tui);
}

#[test]
fn claude_tail_continue_command_passes_through_as_native_harness_args() {
    let args = vec!["continue".to_string()];
    let resolved =
        resolve_launch_resume(&hid("claude"), &args).expect("claude native tail should resolve");

    assert_eq!(resolved.resume, None);
    assert_eq!(resolved.harness_args, args);
    assert!(resolved.requires_tui);
}

#[test]
fn codex_dash_resume_tail_passes_through_as_native_args() {
    let cli = Cli::try_parse_from([
        "nexus", "launch", "--name", "chloe", "codex", "--resume", RESUME_KEY,
    ])
    .expect("dash resume after harness is parsed as harness-native tail");

    let Command::Launch(args) = cli.command else {
        panic!("expected launch command");
    };
    let resolved = resolve_launch_resume(&args.kind, &args.harness_args)
        .expect("dash resume is a native Codex tail, not Nexus syntax");
    assert_eq!(resolved.resume, None);
    assert_eq!(resolved.harness_args, ["--resume", RESUME_KEY]);
    assert!(resolved.requires_tui);
}

#[test]
fn resume_defaults_to_tui_mode_even_in_non_interactive_shell() {
    assert_eq!(
        launch_mode_for_resume(false, false, false, None, true),
        nexus::cli::commands::lifecycle::Mode::Tui
    );
}

#[test]
fn codex_tui_resume_is_valid() {
    let mode = launch_mode(false, true, false, None);
    assert_eq!(
        validate_launch_resume(&hid("codex"), Some(RESUME_KEY), mode),
        Ok(())
    );
}

#[test]
fn resume_rejects_non_codex_harness() {
    let mode = launch_mode(false, true, false, None);
    assert!(validate_launch_resume(&hid("claude"), Some(RESUME_KEY), mode).is_err());
}

#[test]
fn resume_rejects_headless_codex_launch() {
    let mode = launch_mode(true, false, false, None);
    assert_eq!(
        validate_launch_resume(&hid("codex"), Some(RESUME_KEY), mode),
        Err("codex resume requires a headed launch; pass --tui when running non-interactively")
    );
}

#[test]
fn claude_native_tail_rejects_headless_launch() {
    let args = vec!["--resume".to_string(), CLAUDE_SESSION_ID.to_string()];
    let resolved = resolve_launch_resume(&hid("claude"), &args)
        .expect("claude native resume tail should resolve");
    let mode = launch_mode(true, false, false, None);

    assert_eq!(
        validate_launch_harness_args(&hid("claude"), &resolved, mode),
        Err(
            "harness launch arguments require a headed launch; pass --tui when running non-interactively"
        )
    );
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
