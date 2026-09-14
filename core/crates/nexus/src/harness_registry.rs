//! Integration-layer registry for headed harness contracts.
//!
//! Harness crates own their native launch-tail behavior. The `nexus` composition crate owns the
//! kind-to-contract registry because it is the only layer allowed to depend on every harness crate.

use std::collections::HashMap;
use std::sync::OnceLock;

use nexus_contracts::HarnessId;
use nexus_harness_core::{
    native_harness_program, GenericHarness, Harness as HarnessContract, HarnessIdentity,
    HeadedCommand, HeadedRuntimeKind, NativeProcessPlatform, ResumeStyle, SlashCommand,
    SlashCommandAction,
};

#[derive(Debug, Clone, Copy)]
struct OpenCodeHarness;

impl HarnessContract for OpenCodeHarness {
    fn program(&self) -> &'static str {
        native_harness_program(self.agent_token(), NativeProcessPlatform::current())
            .expect("built-in harness has a native headed executable")
    }

    fn agent_token(&self) -> &'static str {
        "opencode"
    }

    fn headed_runtime_kind(&self) -> HeadedRuntimeKind {
        HeadedRuntimeKind::OpenCodePlugin
    }

    fn display_name(&self) -> &'static str {
        "OpenCode"
    }

    fn has_native_thread_binding(&self) -> bool {
        true
    }

    fn resume_style(&self) -> ResumeStyle {
        ResumeStyle::Flag(&["-s"])
    }

    /// OpenCode's non-interactive run mode, used only for short courtesy replies.
    fn oneshot_command(&self, prompt: &str) -> Option<HeadedCommand> {
        Some(HeadedCommand {
            program: self.program().to_string(),
            args: vec!["run".to_string(), prompt.to_string()],
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct HermesHarness;

impl HarnessContract for HermesHarness {
    fn program(&self) -> &'static str {
        native_harness_program(self.agent_token(), NativeProcessPlatform::current())
            .expect("built-in harness has a native headed executable")
    }

    fn agent_token(&self) -> &'static str {
        "hermes"
    }

    fn headed_runtime_kind(&self) -> HeadedRuntimeKind {
        HeadedRuntimeKind::HermesGateway
    }

    fn display_name(&self) -> &'static str {
        "Hermes"
    }

    fn attach_backend(&self) -> Option<&'static str> {
        Some("tmux")
    }

    fn has_native_thread_binding(&self) -> bool {
        true
    }

    fn resume_style(&self) -> ResumeStyle {
        ResumeStyle::Flag(&["--session"])
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
                harness: self.agent_token().to_string(),
                command: command.display_name(),
            })
        }
    }
}

static PI: GenericHarness = GenericHarness::new("pi", "");
static OTHER: GenericHarness = GenericHarness::new("other", "");

/// The built-in harness contracts. This list is the only enumeration of in-tree harnesses.
fn builtin_contracts() -> [&'static dyn HarnessContract; 6] {
    [
        &nexus_harness_claude::ClaudeHarness,
        &nexus_harness_codex::CodexHarness,
        &OpenCodeHarness,
        &HermesHarness,
        &PI,
        &OTHER,
    ]
}

static REGISTRY: OnceLock<HashMap<String, &'static dyn HarnessContract>> = OnceLock::new();

fn build_registry(
    extra: &[(HarnessId, &'static dyn HarnessContract)],
) -> HashMap<String, &'static dyn HarnessContract> {
    let mut map: HashMap<String, &'static dyn HarnessContract> = HashMap::new();
    for contract in builtin_contracts() {
        map.insert(contract.agent_token().to_owned(), contract);
    }
    for (id, contract) in extra {
        map.insert(id.as_str().to_owned(), *contract);
    }
    map
}

/// Install the harness registry at the composition root, adding `extra` contracts on top of
/// the built-ins. Returns `false` if the registry was already initialized (first install wins;
/// lazy default installs the built-ins only).
pub fn init_harness_registry(extra: &[(HarnessId, &'static dyn HarnessContract)]) -> bool {
    REGISTRY.set(build_registry(extra)).is_ok()
}

fn registry() -> &'static HashMap<String, &'static dyn HarnessContract> {
    REGISTRY.get_or_init(|| build_registry(&[]))
}

/// Return the harness contract for an open-set harness id.
///
/// Lookup-miss policy: unknown ids deterministically fall back to the generic non-headed
/// contract. The fallback still
/// provides default launch-tail conformance for CLI/request parsing, while its empty
/// `program` means `harness_program` rejects headed launches.
pub fn harness_registry_by_id(id: &HarnessId) -> &'static dyn HarnessContract {
    registry().get(id.as_str()).copied().unwrap_or(&OTHER)
}
