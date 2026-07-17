use nexus_pty::command::apply_headed_terminal_environment;
use nexus_pty::tmux_launch_shell_command;
use portable_pty::CommandBuilder;
use std::ffi::OsStr;

#[test]
fn raw_headed_terminal_replaces_service_presentation_environment() {
    let mut command = CommandBuilder::new("harness");
    command.env("TERM", "dumb");
    command.env("COLORTERM", "");
    command.env("NO_COLOR", "1");

    apply_headed_terminal_environment(&mut command);

    assert_eq!(command.get_env("TERM"), Some(OsStr::new("xterm-256color")));
    assert_eq!(command.get_env("COLORTERM"), Some(OsStr::new("truecolor")));
    assert_eq!(command.get_env("NO_COLOR"), None);
}

#[test]
fn tmux_headed_terminal_unsets_no_color_before_launch() {
    let command = tmux_launch_shell_command(
        "harness",
        &[],
        "/tmp",
        &[
            ("TERM".to_string(), "screen-256color".to_string()),
            ("COLORTERM".to_string(), "truecolor".to_string()),
        ],
    );

    assert!(command.contains("unset NO_COLOR;"), "{command}");
    assert!(
        command.contains("export TERM='screen-256color';"),
        "{command}"
    );
    assert!(
        command.contains("export COLORTERM='truecolor';"),
        "{command}"
    );
}
