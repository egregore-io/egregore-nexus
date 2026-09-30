use nexus_contracts::SessionId;
use nexus_harness_claude::native::bridge::{
    bridge_dir, launch_settings, launch_settings_with_identity, write_bridge_identity,
    write_launch_settings, write_launch_settings_with_identity, ClaudeNativeBridgePaths,
};

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("nexus-claude-native-bridge-{label}-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn bridge_dir_is_deterministic_under_state_root() {
    let state_root = temp_dir("state-root");
    let runtime_id = SessionId("s_claude_native".into());

    let dir = bridge_dir(&state_root, &runtime_id);

    assert_eq!(
        dir,
        state_root
            .join("claude-sessions")
            .join("s_claude_native")
            .join("bridge")
    );
}

#[test]
fn launch_settings_include_local_hooks_and_nexus_bus_mcp() {
    let state_root = temp_dir("settings-json");
    let paths = ClaudeNativeBridgePaths::new(&state_root, &SessionId("s_settings".into()));

    let settings = launch_settings(&paths, "bianca", "nexus", "/usr/bin/nexus");

    assert!(settings["hooks"]["SessionStart"].is_array());
    assert!(settings["hooks"]["Stop"].is_array());
    assert!(settings["hooks"]["StopFailure"].is_array());
    assert!(settings["hooks"]["UserPromptSubmit"].is_array());
    assert!(settings["hooks"]["PreCompact"].is_array());
    assert!(settings["hooks"]["MessageDisplay"].is_array());
    assert_eq!(
        settings["mcpServers"]["nexus-bus"]["command"],
        "/usr/bin/nexus"
    );
    assert_eq!(
        settings["mcpServers"]["nexus-bus"]["args"],
        serde_json::json!(["mcp", "--as", "bianca", "--project", "nexus"])
    );
}

#[test]
fn launch_settings_can_pin_nexus_bus_to_runtime_identity() {
    let state_root = temp_dir("settings-identity-json");
    let paths = ClaudeNativeBridgePaths::new(&state_root, &SessionId("s_settings_identity".into()));

    let settings = launch_settings_with_identity(
        &paths,
        "hugo",
        "default",
        "/usr/bin/nexus",
        "nexus_ck_hugo",
        "claude",
    );

    assert_eq!(
        settings["mcpServers"]["nexus-bus"]["args"],
        serde_json::json!([
            "mcp",
            "--as",
            "hugo",
            "--project",
            "default",
            "--client-key",
            "nexus_ck_hugo",
            "--agent",
            "claude"
        ])
    );
}

#[test]
fn write_launch_settings_only_writes_inside_bridge_dir() {
    let state_root = temp_dir("write");
    let paths = ClaudeNativeBridgePaths::new(&state_root, &SessionId("s_write".into()));

    write_launch_settings(&paths, "blake", "nexus", "/bin/nexus").unwrap();

    assert!(paths.settings_path.exists());
    assert!(paths.settings_path.starts_with(&paths.bridge_dir));
    assert!(paths.hook_log_path.starts_with(&paths.bridge_dir));
    assert!(paths.message_delta_log_path.starts_with(&paths.bridge_dir));
    assert!(paths.identity_path.starts_with(&paths.bridge_dir));
    assert!(!state_root.join(".claude/settings.json").exists());
}

#[test]
fn bridge_identity_manifest_is_shell_sourceable_and_launch_local() {
    let state_root = temp_dir("identity");
    let session = SessionId("s_identity".into());
    let paths = ClaudeNativeBridgePaths::new(&state_root, &session);

    write_bridge_identity(&paths, "kai", &session).unwrap();

    let body = std::fs::read_to_string(&paths.identity_path).unwrap();
    assert_eq!(
        body,
        "NEXUS_BRIDGE_NAME='kai'\nNEXUS_BRIDGE_SESSION_ID='s_identity'\n"
    );
    assert!(paths.identity_path.starts_with(&paths.bridge_dir));
}

#[test]
fn daemon_launch_settings_write_bridge_identity_when_session_is_known() {
    let state_root = temp_dir("identity-with-settings");
    let session = SessionId("s_settings_identity".into());
    let paths = ClaudeNativeBridgePaths::new(&state_root, &session);

    write_launch_settings_with_identity(
        &paths,
        "violet",
        Some(&session),
        "default",
        "/usr/bin/nexus",
        "nexus_ck_violet",
        "claude",
    )
    .unwrap();

    let body = std::fs::read_to_string(&paths.identity_path).unwrap();
    assert!(body.contains("NEXUS_BRIDGE_NAME='violet'"));
    assert!(body.contains("NEXUS_BRIDGE_SESSION_ID='s_settings_identity'"));
}
