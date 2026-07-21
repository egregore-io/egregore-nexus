use std::path::{Path, PathBuf};

use nexus_harness_codex::app_server::supervisor::{
    config_overrides, mcp_env_override, nexus_identity_developer_instructions,
    resolve_machine_codex_home,
};
use nexus_harness_codex::BusMcp;

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nexus-app-server-config-test-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
        tag,
    ));
    std::fs::create_dir_all(&dir).expect("create test tempdir");
    dir
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn config_overrides_include_features_model_bus_trust_and_identity() {
    let cwd = PathBuf::from("/tmp/nexus codex/project");
    let bus = BusMcp {
        command: "nexus".to_string(),
        args: vec!["mcp".to_string(), "--as".to_string(), "bot".to_string()],
    };
    let developer_instructions = "Nexus says \"stay\" on C:\\tmp\\nexus";

    let overrides = config_overrides(
        Some("gpt-5"),
        Some(developer_instructions),
        Some(&bus),
        Some(&cwd),
    );

    assert!(overrides.contains(&"features.apps=false".to_string()));
    assert!(overrides.contains(&"features.enable_mcp_apps=false".to_string()));
    assert!(overrides.contains(&"model=\"gpt-5\"".to_string()));
    assert!(overrides.contains(&"mcp_servers.nexus-bus.command=\"nexus\"".to_string()));
    assert!(
        overrides.contains(&"mcp_servers.nexus-bus.args=[\"mcp\", \"--as\", \"bot\"]".to_string())
    );
    assert!(overrides
        .contains(&"projects.\"/tmp/nexus codex/project\".trust_level=\"trusted\"".to_string()));
    assert!(overrides.contains(
        &"developer_instructions=\"Nexus says \\\"stay\\\" on C:\\\\tmp\\\\nexus\"".to_string()
    ));
}

#[test]
fn config_override_values_parse_as_toml_scalars_or_arrays() {
    let cwd = PathBuf::from("/tmp/nexus codex/project");
    let bus = BusMcp {
        command: "nexus".to_string(),
        args: vec!["mcp".to_string(), "--as".to_string(), "bot".to_string()],
    };
    let overrides = config_overrides(
        Some("gpt-5"),
        Some("brief with \"quotes\" and \\slashes"),
        Some(&bus),
        Some(&cwd),
    );

    for override_arg in overrides {
        let value = override_arg
            .split_once('=')
            .expect("override must be key=value")
            .1;
        let document = format!("value = {value}\n");
        toml::from_str::<toml::Value>(&document).unwrap_or_else(|e| {
            panic!("override value should parse as TOML: {override_arg:?}: {e}")
        });
    }
}

#[test]
fn mcp_env_override_carries_only_daemon_ipc_discovery() {
    let env = vec![
        ("NEXUS_HOME".to_string(), "/tmp/nexus-home".to_string()),
        (
            "NEXUS_DB_URL".to_string(),
            "http://127.0.0.1:8086".to_string(),
        ),
        (
            "NEXUS_STREAM_DB_PATH".to_string(),
            "/dev/shm/nexus-stream.db".to_string(),
        ),
        ("NEXUS_NO_AUTOSTART".to_string(), "1".to_string()),
        ("TOKIO_WORKER_THREADS".to_string(), "2".to_string()),
        ("UNRELATED_SECRET".to_string(), "must-not-leak".to_string()),
    ];

    let override_arg = mcp_env_override(&env).expect("transport env override");
    assert!(override_arg.starts_with("mcp_servers.nexus-bus.env="));
    assert!(override_arg.contains("NEXUS_HOME"));
    assert!(override_arg.contains("NEXUS_NO_AUTOSTART"));
    assert!(override_arg.contains("TOKIO_WORKER_THREADS"));
    assert!(!override_arg.contains("NEXUS_DB_URL"));
    assert!(!override_arg.contains("NEXUS_STREAM_DB_PATH"));
    assert!(!override_arg.contains("UNRELATED_SECRET"));

    let value = override_arg.split_once('=').unwrap().1;
    toml::from_str::<toml::Value>(&format!("value = {value}\n"))
        .expect("MCP env must be a valid TOML inline table");
}

#[test]
fn nexus_identity_env_becomes_developer_instructions() {
    let env = vec![
        ("NEXUS_NAME".to_string(), "milo".to_string()),
        ("NEXUS_SESSION_ID".to_string(), "s_milo".to_string()),
        ("NEXUS_PROJECT".to_string(), "default".to_string()),
    ];
    let instructions = nexus_identity_developer_instructions(&env).expect("identity instructions");

    assert!(instructions.contains("You are milo (session s_milo)"));
    assert!(instructions.contains("project default"));
    assert!(instructions.contains("Never sign messages as anyone else"));
    assert!(instructions.contains("nexus whoami"));
}

#[test]
fn nexus_identity_instructions_require_name_and_session() {
    let no_session = vec![("NEXUS_NAME".to_string(), "milo".to_string())];
    assert!(nexus_identity_developer_instructions(&no_session).is_none());

    let no_name = vec![("NEXUS_SESSION_ID".to_string(), "s_milo".to_string())];
    assert!(nexus_identity_developer_instructions(&no_name).is_none());
}

#[test]
fn machine_codex_home_prefers_external_machine_selection() {
    let tmp = tempdir("machine-home");
    let session_dir = tmp.join(".nexus/codex-sessions/s_current");
    let selected = tmp.join("profiles/operator-codex");
    let home = tmp.join("home");

    let resolved = resolve_machine_codex_home(&session_dir, Some(&selected), Some(&home))
        .expect("resolve selected machine home");

    assert_eq!(resolved, selected);
    cleanup(&tmp);
}

#[test]
fn machine_codex_home_falls_back_to_home_default() {
    let tmp = tempdir("home-default");
    let session_dir = tmp.join(".nexus/codex-sessions/s_current");
    let home = tmp.join("home");

    let resolved =
        resolve_machine_codex_home(&session_dir, None, Some(&home)).expect("resolve HOME/.codex");

    assert_eq!(resolved, home.join(".codex"));
    cleanup(&tmp);
}

#[test]
fn machine_codex_home_is_shared_across_nexus_sessions() {
    let tmp = tempdir("shared-machine-home");
    let home = tmp.join("home");
    let first = resolve_machine_codex_home(
        &tmp.join(".nexus/codex-sessions/s_first"),
        None,
        Some(&home),
    )
    .expect("resolve first session");
    let second = resolve_machine_codex_home(
        &tmp.join(".nexus/codex-sessions/s_second"),
        None,
        Some(&home),
    )
    .expect("resolve second session");

    assert_eq!(first, home.join(".codex"));
    assert_eq!(second, first);
    cleanup(&tmp);
}

#[test]
fn nexus_session_codex_home_is_not_machine_authority() {
    let tmp = tempdir("reject-session-home");
    let session_dir = tmp.join(".nexus/codex-sessions/s_current");
    let inherited = tmp.join(".nexus/codex-sessions/s_other/codex-home");
    let home = tmp.join("home");

    let resolved = resolve_machine_codex_home(&session_dir, Some(&inherited), Some(&home))
        .expect("fall back from Nexus-owned home");

    assert_eq!(resolved, home.join(".codex"));
    cleanup(&tmp);
}

#[test]
fn machine_codex_home_requires_absolute_nonempty_authority() {
    let tmp = tempdir("missing-authority");
    let session_dir = tmp.join(".nexus/codex-sessions/s_current");

    let missing = resolve_machine_codex_home(&session_dir, None, None)
        .expect_err("missing machine authority must fail");
    assert!(missing.contains("CODEX_HOME") && missing.contains("HOME"));

    let relative = resolve_machine_codex_home(
        &session_dir,
        Some(Path::new("relative-codex-home")),
        Some(Path::new("relative-home")),
    )
    .expect_err("relative authority must fail");
    assert!(relative.contains("absolute"));
    cleanup(&tmp);
}
