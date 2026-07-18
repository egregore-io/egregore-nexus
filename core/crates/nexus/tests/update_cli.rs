use std::process::Command;

#[test]
fn development_binary_refuses_mutation_with_stable_json() {
    let output = Command::new(env!("CARGO_BIN_EXE_nexus"))
        .args(["--json", "update", "--check"])
        .env_remove("NEXUS_CLIENT_KEY")
        .env_remove("NEXUS_SESSION_ID")
        .env_remove("NEXUS_NAME")
        .env_remove("NEXUS_INSTALL_METHOD")
        .env_remove("NEXUS_MANAGED_PACKAGE")
        .env_remove("NEXUS_MANAGED_PACKAGE_ROOT")
        .env_remove("NEXUS_LAUNCHER_PATH")
        .env_remove("NEXUS_NATIVE_BIN")
        .env("NEXUS_TEST_SECRET", "never-serialize-this")
        .output()
        .expect("run update check");

    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8(output.stdout).expect("utf-8 JSON");
    let value: serde_json::Value = serde_json::from_str(&stdout).expect("valid JSON");
    assert_eq!(value["version"], 1);
    assert_eq!(value["status"], "failed");
    assert_eq!(value["error"]["code"], "UNMANAGED_INSTALL");
    assert!(!stdout.contains("never-serialize-this"));
}

#[test]
fn development_binary_explains_the_supported_update_channels() {
    let output = Command::new(env!("CARGO_BIN_EXE_nexus"))
        .args(["update", "--check"])
        .env_remove("NEXUS_CLIENT_KEY")
        .env_remove("NEXUS_SESSION_ID")
        .env_remove("NEXUS_NAME")
        .env_remove("NEXUS_INSTALL_METHOD")
        .output()
        .expect("run update check");

    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).expect("utf-8 error");
    assert!(stderr.contains("npm or Cargo"), "{stderr}");
}

#[test]
fn daemon_launched_agents_cannot_update_the_operator_installation() {
    let output = Command::new(env!("CARGO_BIN_EXE_nexus"))
        .args(["--json", "update", "--check"])
        .env("NEXUS_NAME", "agent")
        .env("NEXUS_SESSION_ID", "s_agent")
        .env("NEXUS_CLIENT_KEY", "ck_agent")
        .output()
        .expect("run guarded update");

    assert_eq!(output.status.code(), Some(1));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid JSON");
    assert_eq!(value["error"]["code"], "OPERATOR_ONLY");
}
