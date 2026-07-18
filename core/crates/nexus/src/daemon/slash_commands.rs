//! Slash-command routing for direct operator prompts.
//!
//! The generic `prompt` path is text injection. Slash commands are different: if a harness cannot
//! execute a slash command natively, dispatch must fail the command intent immediately instead of
//! queueing text that can hang the serial HarnessPrompt lane.

use nexus_contracts::{codes, ContractError};
use nexus_harness_core::{SlashCommand, SlashCommandAction};
use nexus_store::types::SessionRow;

use crate::daemon::app::harness_from_token;
use crate::harness_registry::harness_registry_by_id;

/// Native prompt handling decision for a parsed slash command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PromptSlashAction {
    /// Send the prompt text through the normal harness prompt injection path.
    PromptVerbatim,
    /// Execute the runtime's native context-compaction operation.
    NativeCompact,
}

/// Return the slash-command action for this prompt target.
pub(crate) fn prompt_slash_action(
    row: &SessionRow,
    text: &str,
) -> Result<Option<PromptSlashAction>, ContractError> {
    let Some(command) = SlashCommand::parse(text) else {
        return Ok(None);
    };
    let kind = harness_from_token(row.agent.as_deref());
    let harness = harness_registry_by_id(&kind);
    let translated = harness
        .translate_slash_command(&command)
        .map_err(|_| unsupported(row, &command, "not declared by harness adapter"))?;

    match translated {
        SlashCommandAction::InjectVerbatim => {
            if row.transport.as_deref() == Some("pty") {
                Ok(Some(PromptSlashAction::PromptVerbatim))
            } else {
                Err(unsupported(
                    row,
                    &command,
                    "verbatim slash injection is supported only for headed PTY sessions",
                ))
            }
        }
        SlashCommandAction::NativeCompact => {
            if (kind.as_str() == "codex" && row.transport.as_deref() == Some("codex-appserver"))
                || (kind.as_str() == "hermes" && row.transport.as_deref() == Some("pty"))
            {
                Ok(Some(PromptSlashAction::NativeCompact))
            } else {
                Err(unsupported(
                    row,
                    &command,
                    "requires a native compact-capable transport",
                ))
            }
        }
    }
}

fn unsupported(row: &SessionRow, command: &SlashCommand, reason: &str) -> ContractError {
    ContractError {
        code: codes::INVALID_PARAMS,
        message: format!(
            "unsupported slash command {} for {} transport {}: {}",
            command.display_name(),
            row.agent.as_deref().unwrap_or("claude"),
            row.transport.as_deref().unwrap_or("unknown"),
            reason
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_contracts::ids::SessionId;

    fn row(agent: Option<&str>, transport: Option<&str>) -> SessionRow {
        SessionRow {
            session_id: SessionId("s_test".into()),
            name: Some("target".to_string()),
            agent: agent.map(str::to_string),
            kind: "agent".to_string(),
            role: None,
            tier: "agent".to_string(),
            harness_session_id: None,
            client_key: None,
            cwd: None,
            project: "default".to_string(),
            current_work: None,
            presence: Some("online".to_string()),
            paused: false,
            paused_by: None,
            callback_url: None,
            last_heartbeat: None,
            created_at: 0,
            transport: transport.map(str::to_string),
            metadata_json: None,
            agent_id: None,
        }
    }

    #[test]
    fn non_slash_prompt_uses_normal_prompt_path() {
        assert_eq!(
            prompt_slash_action(&row(Some("codex"), Some("codex-appserver")), "hello").unwrap(),
            None
        );
    }

    #[test]
    fn claude_pty_slash_injects_verbatim() {
        assert_eq!(
            prompt_slash_action(&row(Some("claude"), Some("pty")), "/compact").unwrap(),
            Some(PromptSlashAction::PromptVerbatim)
        );
    }

    #[test]
    fn claude_acp_slash_fails_fast() {
        let err = prompt_slash_action(&row(Some("claude"), Some("acp")), "/compact").unwrap_err();
        assert!(err
            .message
            .contains("unsupported slash command /compact for claude"));
        assert!(err.message.contains("headed PTY"));
    }

    #[test]
    fn codex_appserver_compact_maps_to_native_action() {
        assert_eq!(
            prompt_slash_action(&row(Some("codex"), Some("codex-appserver")), "/compact").unwrap(),
            Some(PromptSlashAction::NativeCompact)
        );
    }

    #[test]
    fn codex_acp_compact_fails_fast_instead_of_text_injection() {
        let err = prompt_slash_action(&row(Some("codex"), Some("acp")), "/compact").unwrap_err();
        assert!(err
            .message
            .contains("unsupported slash command /compact for codex"));
        assert!(err.message.contains("native compact-capable transport"));
    }

    #[test]
    fn opencode_slash_commands_are_unsupported() {
        let err = prompt_slash_action(&row(Some("opencode"), Some("opencode-plugin")), "/compact")
            .unwrap_err();
        assert!(err
            .message
            .contains("unsupported slash command /compact for opencode"));
    }

    #[test]
    fn hermes_compact_maps_to_native_action() {
        assert_eq!(
            prompt_slash_action(&row(Some("hermes"), Some("pty")), "/compact").unwrap(),
            Some(PromptSlashAction::NativeCompact)
        );
        assert_eq!(
            prompt_slash_action(&row(Some("hermes"), Some("pty")), "/compress").unwrap(),
            Some(PromptSlashAction::NativeCompact)
        );
    }

    #[test]
    fn hermes_non_pty_compact_fails_fast() {
        let err = prompt_slash_action(&row(Some("hermes"), Some("acp")), "/compact").unwrap_err();
        assert!(err
            .message
            .contains("unsupported slash command /compact for hermes"));
        assert!(err.message.contains("native compact-capable transport"));
    }
}
