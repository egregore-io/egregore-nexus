use nexus_pty::command::{
    codex_remote_command, codex_remote_command_with_harness_args, codex_remote_resume_command,
    codex_remote_resume_command_with_harness_args,
};

fn argv(cmd: portable_pty::CommandBuilder) -> Vec<String> {
    cmd.get_argv()
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect()
}

#[test]
fn codex_remote_command_carries_workspace_override() {
    let args = argv(codex_remote_command(
        "/tmp/codex.sock",
        Some("/work/project"),
    ));

    assert!(args.windows(2).any(|pair| pair == ["-C", "/work/project"]));
    assert!(args
        .windows(2)
        .any(|pair| pair == ["--remote", "unix:///tmp/codex.sock"]));
    assert!(args
        .windows(2)
        .any(|pair| pair == ["-c", "check_for_update_on_startup=false"]));
}

#[test]
fn codex_remote_command_forwards_native_tail() {
    let args = argv(codex_remote_command_with_harness_args(
        "/tmp/codex.sock",
        Some("/work/project"),
        &["--model".to_string(), "gpt-5-codex".to_string()],
    ));

    assert!(args
        .windows(2)
        .any(|pair| pair == ["--model", "gpt-5-codex"]));
    assert!(args
        .windows(2)
        .any(|pair| pair == ["--remote", "unix:///tmp/codex.sock"]));
}

#[test]
fn codex_remote_resume_command_carries_workspace_override_before_thread_id() {
    let args = argv(codex_remote_resume_command(
        "/tmp/codex.sock",
        "019f13d7-b5fb-7d33-aa42-e0109552887a",
        Some("/work/project"),
    ));

    assert_eq!(args.first().map(String::as_str), Some("codex"));
    assert_eq!(args.get(1).map(String::as_str), Some("resume"));
    assert!(args.windows(2).any(|pair| pair == ["-C", "/work/project"]));
    assert!(args
        .windows(2)
        .any(|pair| pair == ["-c", "check_for_update_on_startup=false"]));
    assert_eq!(
        args.last().map(String::as_str),
        Some("019f13d7-b5fb-7d33-aa42-e0109552887a")
    );
}

#[test]
fn codex_remote_resume_command_forwards_native_tail_before_thread_id() {
    let args = argv(codex_remote_resume_command_with_harness_args(
        "/tmp/codex.sock",
        "019f13d7-b5fb-7d33-aa42-e0109552887a",
        Some("/work/project"),
        &["--model".to_string(), "gpt-5-codex".to_string()],
    ));

    let model = args
        .iter()
        .position(|arg| arg == "--model")
        .expect("model flag");
    let thread_id = args
        .iter()
        .position(|arg| arg == "019f13d7-b5fb-7d33-aa42-e0109552887a")
        .expect("thread id");
    assert!(
        model < thread_id,
        "native tail should stay before thread id: {args:?}"
    );
    assert_eq!(args.get(model + 1).map(String::as_str), Some("gpt-5-codex"));
}
