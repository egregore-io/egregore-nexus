//! Shared headed-harness contract.
//!
//! `nexus-harness-core` is the common seam for launch-tail handling and headed command
//! construction. Harness-specific crates implement [`Harness`] so daemon/CLI layers can stop
//! re-deriving per-runtime argument rules. The default launch-tail behavior is deliberately
//! boring: everything after `nexus launch <harness>` is the harness's native argv and is forwarded
//! verbatim. A harness overrides only when it needs a compatibility sidecar such as Codex's
//! `resume <thread>` app-server metadata.

use thiserror::Error;

pub mod native_executable;
pub mod native_resume;

pub use native_resume::{NativeResumeEvidence, NativeResumePlan, NativeResumeStore};

/// Process naming convention used when resolving provider-owned harness executables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeProcessPlatform {
    /// Linux and macOS executable names are exposed without a suffix.
    Unix,
    /// Windows harness packages expose native `.exe` entry points.
    Windows,
}

impl NativeProcessPlatform {
    /// Platform of the currently compiled Nexus binary.
    pub const fn current() -> Self {
        if cfg!(windows) {
            Self::Windows
        } else {
            Self::Unix
        }
    }
}

/// Return the native headed executable name for a stable harness token and platform.
///
/// Keyed by the open-set harness token (for example `claude` or `codex`); tokens without a
/// provider-owned native executable return `None`.
pub fn native_harness_program(
    harness: &str,
    platform: NativeProcessPlatform,
) -> Option<&'static str> {
    match (harness, platform) {
        ("claude", NativeProcessPlatform::Unix) => Some("claude"),
        ("claude", NativeProcessPlatform::Windows) => Some("claude.exe"),
        ("codex", NativeProcessPlatform::Unix) => Some("codex"),
        ("codex", NativeProcessPlatform::Windows) => Some("codex.exe"),
        ("opencode", NativeProcessPlatform::Unix) => Some("opencode"),
        ("opencode", NativeProcessPlatform::Windows) => Some("opencode.exe"),
        ("hermes", NativeProcessPlatform::Unix) => Some("hermes"),
        ("hermes", NativeProcessPlatform::Windows) => Some("hermes.exe"),
        _ => None,
    }
}

/// Return the OS-native npm command used to start provider ACP bridge packages.
pub const fn native_npm_runner(platform: NativeProcessPlatform) -> &'static str {
    match platform {
        NativeProcessPlatform::Unix => "npx",
        NativeProcessPlatform::Windows => "npx.cmd",
    }
}

/// Result of resolving a harness-native launch tail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTail {
    /// Optional legacy sidecar resume key consumed by a structured headed backend.
    ///
    /// Today only Codex sets this for the compatibility spelling `codex resume <thread>`.
    pub resume: Option<String>,
    /// Native argv tail to append to the harness's real TUI command.
    pub argv: Vec<String>,
    /// Whether this tail requires a headed launch. Any non-empty native tail and any sidecar resume
    /// require a TUI because headless ACP adapters do not consume TUI argv.
    pub requires_tui: bool,
}

impl ResolvedTail {
    /// No native tail and no sidecar metadata.
    pub fn fresh() -> Self {
        Self {
            resume: None,
            argv: Vec::new(),
            requires_tui: false,
        }
    }

    /// Forward the harness-native argv exactly as supplied.
    pub fn passthrough(tail: &[String]) -> Self {
        Self {
            resume: None,
            argv: tail.to_vec(),
            requires_tui: !tail.is_empty(),
        }
    }

    /// Split a compatibility resume key out of the native argv.
    pub fn sidecar_resume(resume: String) -> Self {
        Self {
            resume: Some(resume),
            argv: Vec::new(),
            requires_tui: true,
        }
    }
}

/// Bus identity and runtime credential used to build launch-local MCP args.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessIdentity<'a> {
    /// Nexus bus name.
    pub name: &'a str,
    /// Nexus project scope.
    pub project: &'a str,
    /// Runtime client key. Empty is allowed only for legacy local builders.
    pub client_key: &'a str,
    /// Harness token carried by `nexus mcp --agent`.
    pub agent: &'a str,
}

impl<'a> HarnessIdentity<'a> {
    /// Identity for legacy local command builders that do not yet carry a runtime client key.
    pub fn legacy(name: &'a str, project: &'a str) -> Self {
        Self {
            name,
            project,
            client_key: "",
            agent: "",
        }
    }
}

/// Resolved headed TUI command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadedCommand {
    /// Program to execute.
    pub program: String,
    /// Arguments to pass to the program.
    pub args: Vec<String>,
}

/// Parsed leading slash command from an operator prompt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlashCommand {
    /// Full trimmed slash-command text.
    pub raw: String,
    /// Command verb without the leading slash.
    pub verb: String,
    /// Remainder after the verb, with leading whitespace removed.
    pub args: String,
}

impl SlashCommand {
    /// Parse a prompt as a slash command when its first non-whitespace character is `/`.
    pub fn parse(text: &str) -> Option<Self> {
        let raw = text.trim_start();
        let body = raw.strip_prefix('/')?;
        let mut parts = body.splitn(2, char::is_whitespace);
        let verb = parts.next()?.trim();
        if verb.is_empty() {
            return None;
        }
        Some(Self {
            raw: raw.to_string(),
            verb: verb.to_string(),
            args: parts.next().unwrap_or("").trim_start().to_string(),
        })
    }

    /// Display name used in operator-facing errors.
    pub fn display_name(&self) -> String {
        format!("/{}", self.verb)
    }
}

/// Harness-native slash-command handling decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlashCommandAction {
    /// Deliver the command text exactly as typed into the headed harness.
    InjectVerbatim,
    /// Execute the runtime's native context-compaction operation.
    NativeCompact,
}

/// Headed runtime family used by the daemon/gateway composition layer.
///
/// This keeps launch, revive, terminal attachment, and stream-forwarder wiring
/// keyed by the harness contract instead of by scattered binary-name checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeadedRuntimeKind {
    /// Plain terminal program: inject turns through PTY stdin and scrape screen output.
    Screen,
    /// Claude's native hooks/transcript bridge.
    ClaudeNative,
    /// Codex app-server bridge.
    CodexAppServer,
    /// OpenCode native plugin bridge.
    OpenCodePlugin,
    /// Hermes gateway bridge.
    HermesGateway,
}

/// How a harness consumes a stored native resume key during attach revive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeStyle {
    /// The harness cannot be revived from a stored key.
    Unsupported,
    /// The key travels out-of-band as sidecar resume metadata consumed by a
    /// structured headed backend (Codex app-server threads).
    Sidecar,
    /// The key is appended to the native argv behind a flag prefix,
    /// e.g. `["--resume"]` or `["-s"]`.
    Flag(&'static [&'static str]),
}

/// One launch's resolved cwd/argv policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HarnessLaunchSpec {
    /// The directory the harness process spawns in.
    pub cwd: String,
    /// Daemon-owned private folder: created 0700 by the caller.
    pub private_cwd: bool,
    /// Appended to the harness argv (e.g. claude `--add-dir <target>`).
    pub extra_args: Vec<String>,
}

impl HarnessLaunchSpec {
    /// Status-quo policy: requested cwd verbatim, else the per-agent private default.
    pub fn status_quo(agent_root: &str, requested_cwd: Option<String>) -> Self {
        match requested_cwd {
            Some(cwd) if !cwd.trim().is_empty() => Self {
                cwd,
                private_cwd: false,
                extra_args: Vec::new(),
            },
            _ => Self {
                cwd: agent_root.to_string(),
                private_cwd: true,
                extra_args: Vec::new(),
            },
        }
    }
}

/// Harness launch contract shared by the daemon, CLI, and harness crates.
pub trait Harness: Send + Sync {
    /// TUI program token, for example `claude` or `codex`.
    ///
    /// Harness kinds that do not support headed launch may return an empty string; callers should
    /// reject those before spawning.
    fn program(&self) -> &'static str;

    /// Stable, platform-neutral runtime token stored in Nexus identity rows and exported as
    /// `NEXUS_AGENT`.
    ///
    /// This must not be derived from [`Harness::program`]: native executable names are
    /// platform-specific (`codex` on Unix, `codex.exe` on Windows), while identity and registry
    /// keys must remain identical across platforms.
    fn agent_token(&self) -> &'static str;

    /// Runtime integration profile for headed launches.
    fn headed_runtime_kind(&self) -> HeadedRuntimeKind {
        HeadedRuntimeKind::Screen
    }

    /// Whether output is delivered by a structured bridge rather than PTY scraping.
    fn uses_structured_output(&self) -> bool {
        !matches!(self.headed_runtime_kind(), HeadedRuntimeKind::Screen)
    }

    /// Resolve the native tail after `nexus launch <harness>`.
    ///
    /// Default behavior is verbatim passthrough. Overrides must be narrow and documented in the
    /// harness crate; otherwise Nexus does not interpret harness-native arguments.
    fn resolve_tail(&self, tail: &[String]) -> Result<ResolvedTail, HarnessError> {
        Ok(ResolvedTail::passthrough(tail))
    }

    /// Native tail used when the daemon revives an existing headed runtime.
    ///
    /// Most harnesses need no argv to reconnect to their previous state. Harnesses whose native
    /// state is keyed by an explicit session/thread id should resolve that id at the caller layer
    /// and fail closed when it is unavailable.
    fn revive_tail(&self) -> Result<ResolvedTail, HarnessError> {
        Ok(ResolvedTail::fresh())
    }

    /// Human-facing label used in operator-visible errors, e.g. `Claude`.
    ///
    /// Default is the lowercase agent token; headed harnesses override with
    /// their branded spelling.
    fn display_name(&self) -> &'static str {
        self.agent_token()
    }

    /// Preferred local attach backend when `nexus attach` revives this harness.
    ///
    /// `None` means the transport recorded on the session row already implies
    /// the backend and attach should not force one.
    fn attach_backend(&self) -> Option<&'static str> {
        None
    }

    /// Whether a headless ACP row may be repaired into a headed launch by
    /// `nexus attach` using the stored native session id.
    fn acp_attach_revivable(&self) -> bool {
        false
    }

    /// Whether `nexus attach` can revive this harness into a headed runtime at all.
    ///
    /// Requires a PTY-native interactive binary; ACP-pure kinds (empty
    /// [`Harness::program`]) fail closed at the CLI layer.
    fn attach_revivable(&self) -> bool {
        !self.program().is_empty()
    }

    /// Whether Nexus records native thread bindings for this harness.
    ///
    /// When true, the binding rows are keyed by [`Harness::agent_token`].
    fn has_native_thread_binding(&self) -> bool {
        false
    }

    /// How a revive launch consumes a stored native resume key.
    fn resume_style(&self) -> ResumeStyle {
        ResumeStyle::Unsupported
    }

    /// Validate runtime-scoped native evidence and describe a safe resume operation.
    ///
    /// The caller gathers evidence and validates Nexus ownership; the concrete harness owns
    /// native key agreement, argv, and store selection. Existing harnesses opt in explicitly.
    fn native_resume_plan(
        &self,
        _evidence: NativeResumeEvidence<'_>,
    ) -> Result<NativeResumePlan, HarnessError> {
        Err(HarnessError::UnsupportedResume {
            harness: self.agent_token().to_string(),
        })
    }

    /// Choose the native key to persist in a runtime's resurrection capsule after readiness.
    ///
    /// The default preserves existing requested-key behavior; harnesses with a native ready
    /// result may require that observation and validate it against the original request.
    fn capture_resurrection_key(
        &self,
        requested: Option<&str>,
        _reported: Option<&str>,
    ) -> Result<Option<String>, HarnessError> {
        Ok(requested.map(str::to_string))
    }

    /// Parse an explicit native resume request, rejecting malformed flags when supported.
    ///
    /// The default does not interpret native arguments. A parsed request is a constraint, not
    /// proof that an existing runtime owns that native conversation.
    fn requested_native_resume_key<'a>(
        &self,
        _args: &'a [String],
    ) -> Result<Option<&'a str>, HarnessError> {
        Ok(None)
    }

    /// Operator-facing description of the stored key named in revive errors,
    /// e.g. `--resume session id` or `thread id`.
    fn resume_key_description(&self) -> &'static str {
        "native session id"
    }

    /// Build the local CLI/TUI command for `nexus launch --tui`.
    fn headed_cli_command(
        &self,
        _identity: &HarnessIdentity<'_>,
        _nexus_exe: &str,
        tail: &[String],
    ) -> Result<HeadedCommand, HarnessError> {
        Ok(HeadedCommand {
            program: self.program().to_string(),
            args: tail.to_vec(),
        })
    }

    /// Build the daemon-owned PTY command.
    fn headed_pty_command(
        &self,
        identity: &HarnessIdentity<'_>,
        nexus_exe: &str,
        tail: &[String],
    ) -> Result<HeadedCommand, HarnessError> {
        self.headed_cli_command(identity, nexus_exe, tail)
    }

    /// Resolve the launch cwd/argv policy for one (agent, session) pair.
    ///
    /// * `agent_root` — the per-agent private default folder (daemon-owned).
    /// * `requested_cwd` — the folder the caller asked to work in.
    /// * `is_resume` — a resume launch; harnesses whose fresh-launch policy
    ///   diverges must fall back to [`HarnessLaunchSpec::status_quo`] so the
    ///   session's persisted original cwd is reused verbatim.
    ///
    /// Default: status quo — requested cwd verbatim, else the private root.
    fn launch_spec(
        &self,
        agent_root: &str,
        _session_id: &str,
        requested_cwd: Option<String>,
        _is_resume: bool,
    ) -> HarnessLaunchSpec {
        HarnessLaunchSpec::status_quo(agent_root, requested_cwd)
    }

    /// Translate an operator slash command into a harness-native action.
    ///
    /// The default is fail-fast unsupported. Harnesses opt in only for commands whose native
    /// transport can complete without being injected as ordinary text.
    fn translate_slash_command(
        &self,
        command: &SlashCommand,
    ) -> Result<SlashCommandAction, HarnessError> {
        Err(HarnessError::UnsupportedSlashCommand {
            harness: self.agent_token().to_string(),
            command: command.display_name(),
        })
    }
}

/// Minimal generic harness for runtimes that need only the default contract.
#[derive(Debug, Clone, Copy)]
pub struct GenericHarness {
    token: &'static str,
    program: &'static str,
}

impl GenericHarness {
    /// Build a default passthrough harness identified by its runtime token.
    pub const fn new(token: &'static str, program: &'static str) -> Self {
        Self { token, program }
    }
}

impl Harness for GenericHarness {
    fn program(&self) -> &'static str {
        self.program
    }

    fn agent_token(&self) -> &'static str {
        self.token
    }
}

/// Harness-level launch contract error.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum HarnessError {
    /// The harness-specific compatibility tail was malformed.
    #[error("{0}")]
    InvalidTail(String),
    /// Native resume evidence or an explicit request is missing, malformed, or inconsistent.
    #[error("{0}")]
    InvalidResume(String),
    /// The harness has not opted into runtime-scoped native resume planning.
    #[error("native resume planning is unsupported for {harness}")]
    UnsupportedResume {
        /// Stable runtime token of the harness.
        harness: String,
    },
    /// The harness cannot execute this slash command natively.
    #[error("unsupported slash command {command} for {harness}")]
    UnsupportedSlashCommand {
        /// Runtime agent token of the harness that rejected the command.
        harness: String,
        /// Slash command display name, e.g. `/compact`.
        command: String,
    },
}

/// Shared conformance checks for harness-tail behavior.
///
/// Each harness crate should instantiate this macro in a test module. It proves the universal
/// default shape: arbitrary native tail args are not reinterpreted by Nexus.
#[macro_export]
macro_rules! harness_conformance {
    ($harness:expr) => {
        #[test]
        fn native_tail_passes_through() {
            use nexus_harness_core::Harness as _;
            use nexus_harness_core::HarnessIdentity;

            let harness = $harness;
            let tail = vec!["--native-flag".to_string(), "value".to_string()];
            let resolved = harness
                .resolve_tail(&tail)
                .expect("native harness tail should resolve");

            assert_eq!(resolved.resume, None);
            assert_eq!(resolved.argv, tail);
            assert!(resolved.requires_tui);
            assert!(!harness.program().is_empty());

            let identity = HarnessIdentity::legacy("ada", "default");
            let command = harness
                .headed_cli_command(&identity, "/usr/bin/nexus", &resolved.argv)
                .expect("headed CLI command should build");
            assert_eq!(command.program, harness.program());
            assert!(
                command.args.ends_with(&tail),
                "headed command must append the native tail verbatim: {:?}",
                command.args
            );
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slash_command_parse_trims_leading_space_and_keeps_args() {
        let parsed = SlashCommand::parse("  /compact now please").expect("slash command");

        assert_eq!(parsed.raw, "/compact now please");
        assert_eq!(parsed.verb, "compact");
        assert_eq!(parsed.args, "now please");
        assert_eq!(parsed.display_name(), "/compact");
    }

    #[test]
    fn slash_command_parse_ignores_non_commands_and_empty_slash() {
        assert_eq!(SlashCommand::parse("hello /compact"), None);
        assert_eq!(SlashCommand::parse("/"), None);
        assert_eq!(SlashCommand::parse("   "), None);
    }

    #[test]
    fn generic_harness_rejects_slash_commands_by_default() {
        let harness = GenericHarness::new("other", "future");
        let command = SlashCommand::parse("/compact").expect("slash command");
        let err = harness.translate_slash_command(&command).unwrap_err();

        assert_eq!(
            err,
            HarnessError::UnsupportedSlashCommand {
                harness: "other".to_string(),
                command: "/compact".to_string(),
            }
        );
    }
}
