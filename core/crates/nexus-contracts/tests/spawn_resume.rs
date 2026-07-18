use nexus_contracts::{HarnessId, SpawnRequest};

const RESUME_KEY: &str = "codex-thread-test";

fn resumed_codex_spawn() -> SpawnRequest {
    SpawnRequest {
        kind: HarnessId::new("codex").unwrap(),
        name: Some("resumed-codex".into()),
        identity_policy: None,
        cwd: None,
        project: Some("default".into()),
        role: None,
        initial_prompt: None,
        resume: Some(RESUME_KEY.into()),
        harness_args: Vec::new(),
        headless: false,
        backend: None,
    }
}

#[test]
fn spawn_request_resume_roundtrips() {
    let req = resumed_codex_spawn();

    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["resume"], RESUME_KEY);

    let back: SpawnRequest = serde_json::from_value(json).unwrap();
    assert_eq!(back, req);
}

#[test]
fn spawn_request_harness_args_roundtrips() {
    let req = SpawnRequest {
        kind: HarnessId::new("claude").unwrap(),
        name: Some("resumed-claude".into()),
        identity_policy: None,
        cwd: Some("/tmp/project".into()),
        project: Some("default".into()),
        role: None,
        initial_prompt: None,
        resume: None,
        harness_args: vec!["--resume".into(), "claude-session-test".into()],
        headless: false,
        backend: None,
    };

    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(
        json["harnessArgs"],
        serde_json::json!(["--resume", "claude-session-test"])
    );

    let back: SpawnRequest = serde_json::from_value(json).unwrap();
    assert_eq!(back, req);
}

#[test]
fn spawn_request_harness_args_default_empty_when_absent() {
    let json = serde_json::json!({
        "kind": "claude",
        "name": "fresh-claude",
        "cwd": null,
        "project": "default",
        "role": null,
        "resume": null,
        "headless": false
    });

    let parsed: SpawnRequest = serde_json::from_value(json).unwrap();
    assert!(parsed.harness_args.is_empty());
}
