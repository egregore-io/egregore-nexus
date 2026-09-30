use std::process::Command;

#[test]
fn missing_webconsole_has_stable_machine_guidance() {
    let output = Command::new(env!("CARGO_BIN_EXE_nexus"))
        .args(["--json", "webconsole", "status"])
        .env_remove("NEXUS_WEBUI_BIN")
        .env("PATH", "")
        .output()
        .expect("run Webconsole status");

    assert_eq!(output.status.code(), Some(3));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid JSON");
    assert_eq!(value["error"]["code"], "WEBCONSOLE_NOT_INSTALLED");
    assert!(value["error"]["hint"]
        .as_str()
        .unwrap()
        .contains("npm install -g @egregore/nexus"));
}
