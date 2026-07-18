use nexus_harness_codex::CodexHarness;
use nexus_harness_core::{
    harness_conformance, Harness as _, HarnessError, HarnessIdentity, SlashCommand,
    SlashCommandAction,
};

harness_conformance!(CodexHarness);

#[test]
fn codex_resume_alias_becomes_sidecar_metadata_not_tui_argv() {
    let tail = vec!["resume".to_string(), "thread-123".to_string()];
    let resolved = CodexHarness
        .resolve_tail(&tail)
        .expect("codex resume alias should resolve");
    assert_eq!(resolved.resume.as_deref(), Some("thread-123"));
    assert!(resolved.argv.is_empty());
    assert!(resolved.requires_tui);

    let command = CodexHarness
        .headed_cli_command(
            &HarnessIdentity::legacy("ada", "default"),
            "/usr/bin/nexus",
            &tail,
        )
        .expect("codex command should build");
    assert!(
        !command
            .args
            .windows(2)
            .any(|pair| pair == ["resume", "thread-123"]),
        "compat resume sidecar must not be appended to the TUI argv: {:?}",
        command.args
    );
}

#[test]
fn codex_compact_slash_maps_to_app_server_compaction() {
    let command = SlashCommand::parse("/compact").expect("slash command");
    let action = CodexHarness
        .translate_slash_command(&command)
        .expect("codex should translate compact");

    assert_eq!(action, SlashCommandAction::NativeCompact);
}

#[test]
fn codex_rejects_unmapped_slash_commands() {
    let command = SlashCommand::parse("/clear").expect("slash command");
    let err = CodexHarness.translate_slash_command(&command).unwrap_err();

    assert_eq!(
        err,
        HarnessError::UnsupportedSlashCommand {
            harness: "codex".to_string(),
            command: "/clear".to_string(),
        }
    );
}
