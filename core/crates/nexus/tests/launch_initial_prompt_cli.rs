use nexus::cli::commands::lifecycle::{
    build_spawn_request, launch_mode, resolve_launch_cwd_for, resolve_launch_resume,
    validate_initial_prompt_launch, Mode,
};
use nexus_contracts::SpawnIdentityPolicy;

#[test]
fn build_spawn_request_carries_initial_prompt_and_role() {
    let req = build_spawn_request(
        hid("codex"),
        Some("ada".into()),
        "unused-generated".into(),
        None,
        "default".into(),
        Some("reviewer".into()),
        Some("Review as <var.name>".into()),
        None,
        Vec::new(),
        launch_mode(false, true, true, None),
    );

    assert_eq!(req.name.as_deref(), Some("ada"));
    assert_eq!(req.identity_policy, Some(SpawnIdentityPolicy::ExplicitName));
    assert_eq!(req.role.as_deref(), Some("reviewer"));
    assert_eq!(req.initial_prompt.as_deref(), Some("Review as <var.name>"));
}

#[test]
fn build_spawn_request_keeps_initial_prompt_empty_for_existing_launches() {
    let req = build_spawn_request(
        hid("claude"),
        Some("ben".into()),
        "unused-generated".into(),
        None,
        "default".into(),
        None,
        None,
        None,
        Vec::new(),
        Mode::Tui,
    );

    assert_eq!(req.initial_prompt, None);
}

#[test]
fn initial_prompt_rejects_codex_resume_before_daemon_request() {
    let resolved = resolve_launch_resume(
        &hid("codex"),
        &["resume".to_string(), "019f3374-thread".to_string()],
    )
    .expect("codex resume tail resolves");

    assert_eq!(
        validate_initial_prompt_launch(Some("You are <var.name>."), &resolved),
        Err("--initial-prompt is fresh-launch only; remove resume/reuse harness arguments")
    );
}

#[test]
fn initial_prompt_rejects_native_resume_and_session_tails_before_daemon_request() {
    for (kind, args) in [
        (
            hid("claude"),
            vec!["--resume".to_string(), "claude-session".to_string()],
        ),
        (
            hid("opencode"),
            vec!["--session".to_string(), "opencode-session".to_string()],
        ),
        (
            hid("hermes"),
            vec!["-s".to_string(), "hermes-session".to_string()],
        ),
        (hid("claude"), vec!["continue".to_string()]),
    ] {
        let resolved = resolve_launch_resume(&kind, &args).expect("native tail resolves");
        assert_eq!(
            validate_initial_prompt_launch(Some("You are <var.name>."), &resolved),
            Err("--initial-prompt is fresh-launch only; remove resume/reuse harness arguments"),
            "kind={kind:?} args={args:?}"
        );
    }
}

#[test]
fn initial_prompt_allows_fresh_native_launch_args() {
    let resolved = resolve_launch_resume(
        &hid("opencode"),
        &[
            "--model".to_string(),
            "anthropic/claude-sonnet-4".to_string(),
        ],
    )
    .expect("fresh native tail resolves");

    assert_eq!(
        validate_initial_prompt_launch(Some("You are <var.name>."), &resolved),
        Ok(())
    );
}

#[test]
fn claude_launch_without_explicit_cwd_keeps_cli_process_cwd() {
    let expected = std::env::current_dir().unwrap().to_str().map(String::from);
    assert_eq!(resolve_launch_cwd_for(&hid("claude"), None, &[]), expected);
}

#[test]
fn claude_launch_preserves_explicit_cwd() {
    assert_eq!(
        resolve_launch_cwd_for(&hid("claude"), Some("/work/claude-bianca".into()), &[]),
        Some("/work/claude-bianca".into())
    );
}

#[test]
fn claude_resume_without_explicit_cwd_keeps_cli_process_cwd_for_native_lookup() {
    let expected = std::env::current_dir().unwrap().to_str().map(String::from);
    assert_eq!(
        resolve_launch_cwd_for(
            &hid("claude"),
            None,
            &["--resume".to_string(), "claude-native-session".to_string()],
        ),
        expected
    );
}

#[test]
fn non_claude_launch_without_explicit_cwd_keeps_cli_process_cwd_default() {
    let expected = std::env::current_dir().unwrap().to_str().map(String::from);
    assert_eq!(resolve_launch_cwd_for(&hid("codex"), None, &[]), expected);
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
