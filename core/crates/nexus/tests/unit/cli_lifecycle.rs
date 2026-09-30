#![cfg(test)]

use super::launch_handle_output;

#[test]
fn detached_launch_output_honors_machine_json() {
    let rendered = launch_handle_output("codex-probe", "s_probe", true);
    let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    assert_eq!(value["name"], "codex-probe");
    assert_eq!(value["sessionId"], "s_probe");
    assert_eq!(
        launch_handle_output("codex-probe", "s_probe", false),
        "codex-probe:s_probe"
    );
}
