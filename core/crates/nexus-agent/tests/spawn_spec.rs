use nexus_agent::adapter::{HarnessCommand, SpawnSpecAdapter};
use nexus_agent::LaunchCtx;
use nexus_contracts::HarnessId;

fn id(s: &str) -> HarnessId {
    HarnessId::new(s).expect("valid test harness id")
}

#[test]
fn launch_cwd_applies_when_manifest_does_not_pin_one() {
    let adapter = SpawnSpecAdapter::new(
        id("acme-agent"),
        HarnessCommand {
            program: "acme".into(),
            args: vec!["--acp".into()],
            ..Default::default()
        },
        LaunchCtx {
            cwd: Some("/tmp/launch".into()),
            ..Default::default()
        },
    );
    assert_eq!(adapter.command().cwd.as_deref(), Some("/tmp/launch"));
}

#[test]
fn manifest_cwd_wins_over_launch_cwd() {
    let adapter = SpawnSpecAdapter::new(
        id("acme-agent"),
        HarnessCommand {
            program: "acme".into(),
            cwd: Some("/opt/pinned".into()),
            ..Default::default()
        },
        LaunchCtx {
            cwd: Some("/tmp/launch".into()),
            ..Default::default()
        },
    );
    assert_eq!(adapter.command().cwd.as_deref(), Some("/opt/pinned"));
}

#[test]
fn launch_env_is_appended_after_manifest_env() {
    let adapter = SpawnSpecAdapter::new(
        id("acme-agent"),
        HarnessCommand {
            program: "acme".into(),
            env: vec![("ACME_MODE".into(), "acp".into())],
            ..Default::default()
        },
        LaunchCtx {
            env: vec![("NEXUS_NAME".into(), "worker".into())],
            ..Default::default()
        },
    );
    assert_eq!(
        adapter.command().env,
        vec![
            ("ACME_MODE".to_string(), "acp".to_string()),
            ("NEXUS_NAME".to_string(), "worker".to_string()),
        ]
    );
}
