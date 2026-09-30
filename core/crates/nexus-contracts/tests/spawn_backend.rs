//! `SpawnRequest::resolved_backend` — the headed-viewer backend resolution contract
//! Backend precedence: explicit flag > Hermes-forced tmux > operator config > "pty".

use nexus_contracts::{HarnessId, SpawnRequest};

fn spawn(kind: &str, backend: Option<&str>) -> SpawnRequest {
    SpawnRequest {
        kind: HarnessId::new(kind).unwrap(),
        name: Some("t".into()),
        identity_policy: None,
        cwd: None,
        project: None,
        role: None,
        initial_prompt: None,
        resume: None,
        harness_args: Vec::new(),
        headless: false,
        backend: backend.map(str::to_string),
    }
}

#[test]
fn unset_defaults_to_pty() {
    assert_eq!(spawn("claude", None).resolved_backend(None), "pty");
    assert_eq!(spawn("codex", None).resolved_backend(None), "pty");
}

#[test]
fn operator_config_sets_the_default() {
    assert_eq!(spawn("claude", None).resolved_backend(Some("tmux")), "tmux");
    assert_eq!(spawn("claude", None).resolved_backend(Some("pty")), "pty");
}

#[test]
fn explicit_backend_beats_config() {
    assert_eq!(
        spawn("claude", Some("pty")).resolved_backend(Some("tmux")),
        "pty"
    );
    assert_eq!(spawn("claude", Some("tmux")).resolved_backend(None), "tmux");
}

#[test]
fn hermes_is_always_tmux_unless_explicitly_overridden() {
    // Hermes' gateway bridge is welded to the tmux viewer profile: config cannot demote it...
    assert_eq!(spawn("hermes", None).resolved_backend(None), "tmux");
    assert_eq!(spawn("hermes", None).resolved_backend(Some("pty")), "tmux");
    // ...but an explicit flag still wins (and the raw-pty launch path rejects it loudly).
    assert_eq!(spawn("hermes", Some("pty")).resolved_backend(None), "pty");
}
