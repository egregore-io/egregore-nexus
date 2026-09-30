use std::path::{Path, PathBuf};

use nexus_harness_codex::app_server::supervisor::{
    config_overrides, mcp_env_override, nexus_identity_developer_instructions, seed_auth,
    seed_user_config,
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
fn seed_auth_copies_when_src_exists_and_dst_missing() {
    let tmp = tempdir("auth-copy");
    let src = tmp.join("src");
    let dst = tmp.join("dst");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::create_dir_all(&dst).unwrap();

    let auth_bytes = b"fake-auth-token-content";
    std::fs::write(src.join("auth.json"), auth_bytes).unwrap();

    seed_auth(&src, &dst);

    let dst_auth = dst.join("auth.json");
    assert!(
        dst_auth.exists(),
        "auth.json must be copied into codex_home"
    );
    assert_eq!(std::fs::read(&dst_auth).unwrap(), auth_bytes);

    cleanup(&tmp);
}

#[test]
fn seed_auth_skips_when_dst_already_exists() {
    let tmp = tempdir("auth-skip");
    let src = tmp.join("src");
    let dst = tmp.join("dst");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::create_dir_all(&dst).unwrap();

    std::fs::write(src.join("auth.json"), b"src-bytes").unwrap();
    std::fs::write(dst.join("auth.json"), b"dst-bytes-existing").unwrap();

    seed_auth(&src, &dst);

    assert_eq!(
        std::fs::read(dst.join("auth.json")).unwrap(),
        b"dst-bytes-existing"
    );

    cleanup(&tmp);
}

#[test]
fn seed_auth_noop_when_src_auth_missing() {
    let tmp = tempdir("auth-nosrc");
    let src = tmp.join("src");
    let dst = tmp.join("dst");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::create_dir_all(&dst).unwrap();

    seed_auth(&src, &dst);

    assert!(!dst.join("auth.json").exists());

    cleanup(&tmp);
}

#[test]
fn seed_user_config_copies_user_bytes_into_fresh_home() {
    let tmp = tempdir("config-copy");
    let src_home = tmp.join("operator");
    let codex_home = tmp.join("session-codex-home");
    std::fs::create_dir_all(&src_home).unwrap();
    std::fs::create_dir_all(&codex_home).unwrap();
    std::fs::write(src_home.join("config.toml"), b"model = \"gpt-5\"\n").unwrap();

    seed_user_config(&src_home, &codex_home, false).expect("seed user config");

    assert_eq!(
        std::fs::read(codex_home.join("config.toml")).unwrap(),
        b"model = \"gpt-5\"\n"
    );

    cleanup(&tmp);
}

#[test]
fn seed_user_config_recopies_when_user_config_changes() {
    let tmp = tempdir("config-recopy");
    let src_home = tmp.join("operator");
    let codex_home = tmp.join("session-codex-home");
    std::fs::create_dir_all(&src_home).unwrap();
    std::fs::create_dir_all(&codex_home).unwrap();

    std::fs::write(src_home.join("config.toml"), b"model = \"gpt-5\"\n").unwrap();
    seed_user_config(&src_home, &codex_home, false).expect("first seed");

    std::fs::write(
        src_home.join("config.toml"),
        b"model = \"gpt-5.1\"\napproval_policy = \"never\"\n",
    )
    .unwrap();
    seed_user_config(&src_home, &codex_home, false).expect("second seed");

    assert_eq!(
        std::fs::read(codex_home.join("config.toml")).unwrap(),
        b"model = \"gpt-5.1\"\napproval_policy = \"never\"\n"
    );

    cleanup(&tmp);
}

#[test]
fn seed_user_config_writes_empty_file_when_user_config_missing() {
    let tmp = tempdir("config-missing");
    let src_home = tmp.join("operator");
    let codex_home = tmp.join("session-codex-home");
    std::fs::create_dir_all(&src_home).unwrap();
    std::fs::create_dir_all(&codex_home).unwrap();

    seed_user_config(&src_home, &codex_home, false).expect("seed empty config");

    assert_eq!(std::fs::read(codex_home.join("config.toml")).unwrap(), b"");

    cleanup(&tmp);
}

#[test]
fn seed_user_config_leaves_external_home_untouched() {
    let tmp = tempdir("config-external");
    let src_home = tmp.join("operator");
    let codex_home = tmp.join("external-codex-home");
    std::fs::create_dir_all(&src_home).unwrap();
    std::fs::create_dir_all(&codex_home).unwrap();
    std::fs::write(src_home.join("config.toml"), b"model = \"gpt-5\"\n").unwrap();

    seed_user_config(&src_home, &codex_home, true).expect("external seed no-op");

    assert!(
        !codex_home.join("config.toml").exists(),
        "external CODEX_HOME config.toml must be untouched"
    );

    cleanup(&tmp);
}
