//! Per-harness launch policy (daemon::harness_launch) — the segregation seam.

use nexus::daemon::harness_launch::harness_launch_spec;

fn home() -> String {
    std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string())
}

#[test]
fn claude_fresh_launch_gets_private_per_session_cwd_and_add_dir_target() {
    let spec = harness_launch_spec(
        &hid("claude"),
        "a_123",
        "s_abc",
        Some("/home/tester/Projects/egregore-lens".to_string()),
        false,
    );
    assert_eq!(
        spec.cwd,
        format!("{}/.nexus/agents/a_123/sessions/s_abc", home())
    );
    assert!(spec.private_cwd);
    assert_eq!(
        spec.extra_args,
        vec![
            "--add-dir".to_string(),
            "/home/tester/Projects/egregore-lens".to_string()
        ]
    );
}

#[test]
fn claude_fresh_launch_without_target_grants_nothing() {
    let spec = harness_launch_spec(&hid("claude"), "a_123", "s_abc", None, false);
    assert_eq!(
        spec.cwd,
        format!("{}/.nexus/agents/a_123/sessions/s_abc", home())
    );
    assert!(spec.private_cwd);
    assert!(spec.extra_args.is_empty());
}

#[test]
fn two_claude_sessions_of_one_agent_get_distinct_cwds() {
    let first = harness_launch_spec(&hid("claude"), "a_1", "s_one", None, false);
    let second = harness_launch_spec(&hid("claude"), "a_1", "s_two", None, false);
    assert_ne!(first.cwd, second.cwd);
}

#[test]
fn claude_resume_reuses_the_requested_cwd_verbatim() {
    // The transcript jsonl lives under the ORIGINAL cwd's slug — resume must
    // never be redirected into a fresh per-session folder.
    let original = format!("{}/.nexus/agents/a_1/sessions/s_old", home());
    let spec = harness_launch_spec(&hid("claude"), "a_1", "s_new", Some(original.clone()), true);
    assert_eq!(spec.cwd, original);
    assert!(!spec.private_cwd);
    assert!(spec.extra_args.is_empty());
}

#[test]
fn other_harnesses_keep_requested_cwd_and_no_extra_args() {
    for harness in [hid("codex"), hid("opencode"), hid("hermes")] {
        let spec = harness_launch_spec(
            &harness,
            "a_9",
            "s_9",
            Some("/work/project".to_string()),
            false,
        );
        assert_eq!(spec.cwd, "/work/project");
        assert!(!spec.private_cwd);
        assert!(spec.extra_args.is_empty());
    }
}

#[test]
fn other_harnesses_default_to_the_per_agent_private_folder() {
    let spec = harness_launch_spec(&hid("codex"), "a_9", "s_9", None, false);
    assert_eq!(spec.cwd, format!("{}/.nexus/agents/a_9", home()));
    assert!(spec.private_cwd);
}

#[test]
fn blank_requested_cwd_counts_as_absent() {
    let spec = harness_launch_spec(&hid("hermes"), "a_9", "s_9", Some("  ".into()), false);
    assert_eq!(spec.cwd, format!("{}/.nexus/agents/a_9", home()));
    assert!(spec.private_cwd);
    let claude = harness_launch_spec(&hid("claude"), "a_9", "s_9", Some("".into()), false);
    assert!(claude.extra_args.is_empty());
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
