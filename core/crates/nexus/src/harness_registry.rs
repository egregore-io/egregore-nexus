//! Integration-layer registry for headed harness contracts.
//!
//! Harness crates own their native launch-tail behavior. The `nexus` composition crate owns the
//! kind-to-contract registry because it is the only layer allowed to depend on every harness crate.

use nexus_contracts::Harness as HarnessKind;
use nexus_harness_core::{
    native_harness_program, GenericHarness, Harness as HarnessContract, HarnessIdentity,
    HeadedCommand, HeadedRuntimeKind, NativeProcessPlatform, SlashCommand, SlashCommandAction,
};

#[derive(Debug, Clone, Copy)]
struct OpenCodeHarness;

impl HarnessContract for OpenCodeHarness {
    fn kind(&self) -> HarnessKind {
        HarnessKind::OpenCode
    }

    fn program(&self) -> &'static str {
        native_harness_program(self.kind(), NativeProcessPlatform::current())
            .expect("OpenCode has a native headed executable")
    }

    fn headed_runtime_kind(&self) -> HeadedRuntimeKind {
        HeadedRuntimeKind::OpenCodePlugin
    }
}

#[derive(Debug, Clone, Copy)]
struct HermesHarness;

impl HarnessContract for HermesHarness {
    fn kind(&self) -> HarnessKind {
        HarnessKind::Hermes
    }

    fn program(&self) -> &'static str {
        native_harness_program(self.kind(), NativeProcessPlatform::current())
            .expect("Hermes has a native headed executable")
    }

    fn headed_runtime_kind(&self) -> HeadedRuntimeKind {
        HeadedRuntimeKind::HermesGateway
    }

    fn headed_cli_command(
        &self,
        _identity: &HarnessIdentity<'_>,
        _nexus_exe: &str,
        tail: &[String],
    ) -> Result<HeadedCommand, nexus_harness_core::HarnessError> {
        let mut args = vec!["gateway".to_string(), "run".to_string()];
        args.extend(tail.iter().cloned());
        Ok(HeadedCommand {
            program: self.program().to_string(),
            args,
        })
    }

    fn translate_slash_command(
        &self,
        command: &SlashCommand,
    ) -> Result<SlashCommandAction, nexus_harness_core::HarnessError> {
        if matches!(command.verb.as_str(), "compact" | "compress") && command.args.is_empty() {
            Ok(SlashCommandAction::NativeCompact)
        } else {
            Err(nexus_harness_core::HarnessError::UnsupportedSlashCommand {
                harness: self.kind(),
                command: command.display_name(),
            })
        }
    }
}

static CLAUDE: nexus_harness_claude::ClaudeHarness = nexus_harness_claude::ClaudeHarness;
static CODEX: nexus_harness_codex::CodexHarness = nexus_harness_codex::CodexHarness;
static OPENCODE: OpenCodeHarness = OpenCodeHarness;
static HERMES: HermesHarness = HermesHarness;
static PI: GenericHarness = GenericHarness::new(HarnessKind::Pi, "");
static OTHER: GenericHarness = GenericHarness::new(HarnessKind::Other, "");

/// Return the harness contract for a contract harness kind.
///
/// Empty-program entries are non-headed harness kinds. They still provide default launch-tail
/// conformance for CLI/request parsing while `harness_program` rejects headed launches.
pub fn harness_registry(kind: HarnessKind) -> &'static dyn HarnessContract {
    match kind {
        HarnessKind::Claude => &CLAUDE,
        HarnessKind::Codex => &CODEX,
        HarnessKind::OpenCode => &OPENCODE,
        HarnessKind::Hermes => &HERMES,
        HarnessKind::Pi => &PI,
        HarnessKind::Other => &OTHER,
    }
}
