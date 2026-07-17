use nexus_harness_codex::{remote_command_with_executable, remote_resume_command_with_executable};

fn argv(command: portable_pty::CommandBuilder) -> Vec<String> {
    command
        .get_argv()
        .iter()
        .map(|argument| argument.to_string_lossy().into_owned())
        .collect()
}

#[test]
fn native_windows_executable_preserves_loopback_endpoint() {
    let args = argv(remote_command_with_executable(
        "C:\\tools\\codex.exe",
        "ws://127.0.0.1:43127",
        Some("C:\\workspace"),
        &[],
    ));

    assert_eq!(
        args.first().map(String::as_str),
        Some("C:\\tools\\codex.exe")
    );
    assert!(args
        .windows(2)
        .any(|pair| pair == ["--remote", "ws://127.0.0.1:43127"]));
}

#[test]
fn resume_places_native_tail_before_thread_id() {
    let thread_id = "019f13d7-b5fb-7d33-aa42-e0109552887a";
    let args = argv(remote_resume_command_with_executable(
        "/usr/bin/codex",
        "/tmp/codex.sock",
        thread_id,
        Some("/work/project"),
        &["--model".to_string(), "gpt-5-codex".to_string()],
    ));

    let model = args
        .iter()
        .position(|argument| argument == "--model")
        .expect("model flag");
    let thread = args
        .iter()
        .position(|argument| argument == thread_id)
        .expect("thread id");
    assert!(model < thread);
    assert_eq!(args.get(model + 1).map(String::as_str), Some("gpt-5-codex"));
    assert!(args
        .windows(2)
        .any(|pair| pair == ["--remote", "unix:///tmp/codex.sock"]));
}
