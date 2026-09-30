use nexus_harness_claude::ClaudeHarness;
use nexus_harness_core::{harness_conformance, Harness as _, SlashCommand, SlashCommandAction};

harness_conformance!(ClaudeHarness);

#[test]
fn claude_slash_commands_inject_verbatim() {
    let command = SlashCommand::parse("/compact").expect("slash command");
    let action = ClaudeHarness
        .translate_slash_command(&command)
        .expect("claude should accept slash commands verbatim");

    assert_eq!(action, SlashCommandAction::InjectVerbatim);
}

#[test]
fn claude_harness_rejects_generic_revive_tail() {
    let err = ClaudeHarness
        .revive_tail()
        .expect_err("Claude revive must require an exact --resume id");

    assert!(
        err.to_string().contains("--resume"),
        "error should explain exact Claude resume requirement: {err}"
    );
}
