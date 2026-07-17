use nexus_contracts::SessionId;
use nexus_harness_claude::native::bridge::{write_bridge_identity, ClaudeNativeBridgePaths};
use nexus_harness_claude::native::hooks::{
    hook_command, message_delta_log_path, settings_hooks, HOOK_EVENTS,
};
use std::process::{Command, Stdio};

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("nexus-claude-native-hooks-{label}-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn hook_commands_target_files_inside_bridge_dir() {
    let paths = ClaudeNativeBridgePaths::new(&temp_dir("paths"), &SessionId("s_hooks".into()));

    for event in HOOK_EVENTS {
        let command = hook_command(&paths.bridge_dir, event);
        assert!(
            command.contains(paths.bridge_dir.to_str().unwrap()),
            "command for {event} should mention bridge dir: {command}"
        );
        assert!(
            command.contains("NEXUS_CLAUDE_HOOK_LOG"),
            "command for {event} should route through hook log env: {command}"
        );
        assert!(
            command.contains("flock"),
            "command for {event} should serialize concurrent hook writers: {command}"
        );
    }
}

#[test]
fn message_display_uses_delta_log() {
    let paths = ClaudeNativeBridgePaths::new(&temp_dir("delta"), &SessionId("s_delta".into()));
    let command = hook_command(&paths.bridge_dir, "MessageDisplay");

    assert!(command.contains(message_delta_log_path(&paths.bridge_dir).to_str().unwrap()));
}

#[test]
fn settings_hooks_include_expected_event_names() {
    let paths =
        ClaudeNativeBridgePaths::new(&temp_dir("settings"), &SessionId("s_settings".into()));
    let hooks = settings_hooks(&paths.bridge_dir);

    for event in HOOK_EVENTS {
        assert!(hooks[event].is_array(), "missing hook event {event}");
    }
}

#[test]
fn session_start_hook_logs_when_bridge_identity_matches_env() {
    let paths = ClaudeNativeBridgePaths::new(&temp_dir("match"), &SessionId("s_violet".into()));
    write_bridge_identity(&paths, "violet", &SessionId("s_violet".into())).unwrap();

    let output = Command::new("sh")
        .arg("-lc")
        .arg(hook_command(&paths.bridge_dir, "SessionStart"))
        .env("NEXUS_NAME", "violet")
        .env("NEXUS_SESSION_ID", "s_violet")
        .stdin(Stdio::piped())
        .output_with_stdin(r#"{"session_id":"claude-real"}"#);

    assert!(output.status.success(), "stderr: {}", output.stderr);
    let hook_log = std::fs::read_to_string(&paths.hook_log_path).unwrap();
    assert!(hook_log.contains(r#""event":"SessionStart""#));
    assert!(hook_log.contains(r#""session_id":"claude-real""#));
}

#[test]
fn session_start_hook_accepts_staged_agent_id_without_name() {
    let paths = ClaudeNativeBridgePaths::new(&temp_dir("agent-id"), &SessionId("s_stage".into()));
    write_bridge_identity(&paths, "a_s_stage", &SessionId("s_stage".into())).unwrap();

    let output = Command::new("sh")
        .arg("-lc")
        .arg(hook_command(&paths.bridge_dir, "SessionStart"))
        .env_remove("NEXUS_NAME")
        .env("NEXUS_AGENT_ID", "a_s_stage")
        .env("NEXUS_SESSION_ID", "s_stage")
        .stdin(Stdio::piped())
        .output_with_stdin(r#"{"session_id":"claude-real"}"#);

    assert!(output.status.success(), "stderr: {}", output.stderr);
    let hook_log = std::fs::read_to_string(&paths.hook_log_path).unwrap();
    assert!(hook_log.contains(r#""event":"SessionStart""#));
    assert!(hook_log.contains(r#""session_id":"claude-real""#));
}

#[test]
fn session_start_hook_fails_loudly_when_bridge_identity_does_not_match_env() {
    let paths = ClaudeNativeBridgePaths::new(&temp_dir("mismatch"), &SessionId("s_violet".into()));
    write_bridge_identity(&paths, "violet", &SessionId("s_violet".into())).unwrap();

    let output = Command::new("sh")
        .arg("-lc")
        .arg(hook_command(&paths.bridge_dir, "SessionStart"))
        .env("NEXUS_NAME", "kai")
        .env_remove("NEXUS_AGENT_ID")
        .env("NEXUS_SESSION_ID", "s_kai")
        .stdin(Stdio::piped())
        .output_with_stdin(r#"{"session_id":"claude-real"}"#);

    assert!(!output.status.success());
    assert!(output.stderr.contains("SessionStart identity mismatch"));
    assert!(output
        .stderr
        .contains("bridge identity: name=violet session=s_violet"));
    assert!(output
        .stderr
        .contains("environment identity: name=kai agent_id=<missing> session=s_kai"));
    assert!(output
        .stderr
        .contains("NEXUS_NAME=violet NEXUS_SESSION_ID=s_violet"));
    assert!(
        !paths.hook_log_path.exists()
            || std::fs::read_to_string(&paths.hook_log_path)
                .unwrap()
                .is_empty()
    );
}

trait CommandOutputExt {
    fn output_with_stdin(&mut self, stdin: &str) -> CommandOutput;
}

struct CommandOutput {
    status: std::process::ExitStatus,
    stderr: String,
}

impl CommandOutputExt for Command {
    fn output_with_stdin(&mut self, stdin: &str) -> CommandOutput {
        let mut child = self
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        {
            use std::io::Write;
            child
                .stdin
                .as_mut()
                .unwrap()
                .write_all(stdin.as_bytes())
                .unwrap();
        }
        let output = child.wait_with_output().unwrap();
        CommandOutput {
            status: output.status,
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
    }
}
