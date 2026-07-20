//! Lifecycle commands: `launch`, `register`, `whoami` (cli-spec §2). The `daemon` subcommand stays
//! in `main.rs` / the backend plan. Identity writes are daemon-managed command intents; `whoami` is
//! a direct store read view.

use std::process::ExitCode;

use clap::Args;
use nexus_contracts::{
    codes, AgentId, ContractError, HarnessId, Kind, RegisterRequest, RegisterResponse,
    RenameRequest, RenameResponse, SpawnIdentityPolicy, SpawnRequest, SpawnResponse, Tier, Whoami,
};
use nexus_harness_core::HarnessIdentity;

use crate::cli::ambient::stable_harness_session_id_from_env;
use crate::cli::commands::parse;
use crate::cli::read_client::ReadClient;
use crate::cli::render::finish_with;
use crate::cli::store_client::StoreClient;
use crate::harness_registry::harness_registry_by_id;

/// Launch mode decision — factored out so it can be unit-tested without touching stdin or exec.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Mode {
    /// Delegate to the daemon command worker (ACP agent + MCP spawned headlessly).
    Headless,
    /// Exec the real harness TUI (build_harness_command + spawn).
    Tui,
}

/// Pure decision function for the headless/TUI split.
///
/// Arguments:
/// - `headless`: `--headless` flag was set.
/// - `tui`: `--tui` flag was set.
/// - `is_tty`: stdin is an interactive terminal (`IsTerminal::is_terminal(&stdin())`).
/// - `prompt_answer`: result of the interactive `[y/N]` prompt (`Some(true)` = headless,
///   `Some(false)` = TUI, `None` = not prompted / non-interactive path).
pub fn launch_mode(headless: bool, tui: bool, is_tty: bool, prompt_answer: Option<bool>) -> Mode {
    if headless {
        return Mode::Headless;
    }
    if tui {
        return Mode::Tui;
    }
    if is_tty {
        // Interactive: use whatever the user answered (None means prompt wasn't shown yet,
        // treated as TUI = default N).
        match prompt_answer {
            Some(true) => Mode::Headless,
            _ => Mode::Tui,
        }
    } else {
        // Non-interactive (CI/scripts): default headless so callers are never blocked.
        Mode::Headless
    }
}

/// Validate the currently-supported launch resume surface.
///
/// A resume key is harness-native. Today only headed Codex app-server launches know how to use it:
/// the daemon starts `codex app-server` and opens the TUI with `codex resume --remote ... <thread>`.
/// Reject unsupported combinations so callers never think a resume key was honored when it was
/// silently ignored by a different backend.
pub fn validate_launch_resume(
    kind: &HarnessId,
    resume: Option<&str>,
    mode: Mode,
) -> Result<(), &'static str> {
    if resume.is_none() {
        return Ok(());
    }
    if kind.as_str() != "codex" {
        return Err("resume is currently supported only for codex launches");
    }
    if mode == Mode::Headless {
        return Err(
            "codex resume requires a headed launch; pass --tui when running non-interactively",
        );
    }
    Ok(())
}

/// Resolved harness launch tail carried by [`SpawnRequest`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedLaunchArgs {
    /// Codex app-server resume key, kept in the legacy dedicated field until the daemon-side Codex
    /// path is converted to generic harness args.
    pub resume: Option<String>,
    /// Harness-native argv tail to pass to the headed runtime.
    pub harness_args: Vec<String>,
    /// Tail args that only make sense for a headed runtime should force TUI mode unless `--headless`
    /// was explicitly requested, in which case validation rejects the launch.
    pub requires_tui: bool,
}

impl ResolvedLaunchArgs {
    fn fresh() -> Self {
        Self {
            resume: None,
            harness_args: Vec::new(),
            requires_tui: false,
        }
    }
}

/// Resolve harness-specific launch tail arguments into the shared launch contract.
///
/// The stable launch shape is `nexus launch [launch flags] <harness> [harness args...]`. Harness
/// crates own this resolution through `nexus-harness-core`; the default is verbatim native argv.
/// Codex keeps the one compatibility normalization, `codex resume <thread-id>`, because the
/// app-server bridge stores that thread id separately.
pub fn resolve_launch_resume(
    kind: &HarnessId,
    harness_args: &[String],
) -> Result<ResolvedLaunchArgs, String> {
    if harness_args.is_empty() {
        return Ok(ResolvedLaunchArgs::fresh());
    }

    let resolved = resolve_tail_for(kind, harness_args).map_err(|e| e.to_string())?;
    Ok(ResolvedLaunchArgs {
        resume: resolved.resume,
        harness_args: resolved.argv,
        requires_tui: resolved.requires_tui,
    })
}

/// Validate the resolved harness tail against the selected run mode.
pub fn validate_launch_harness_args(
    kind: &HarnessId,
    resolved: &ResolvedLaunchArgs,
    mode: Mode,
) -> Result<(), &'static str> {
    validate_launch_resume(kind, resolved.resume.as_deref(), mode)?;
    if resolved.requires_tui && mode == Mode::Headless {
        return Err(
            "harness launch arguments require a headed launch; pass --tui when running non-interactively",
        );
    }
    Ok(())
}

fn looks_like_native_resume_or_reuse_tail(arg: &str) -> bool {
    matches!(
        arg,
        "resume" | "continue" | "--resume" | "--continue" | "--session" | "-s"
    ) || arg.starts_with("--resume=")
        || arg.starts_with("--session=")
}

/// `--initial-prompt` is a boot-only contract. Reject resume/reuse tails before enqueueing the
/// daemon command so a boot prompt cannot be replayed into an existing native conversation.
pub fn validate_initial_prompt_launch(
    initial_prompt: Option<&str>,
    resolved: &ResolvedLaunchArgs,
) -> Result<(), &'static str> {
    if initial_prompt.is_none() {
        return Ok(());
    }
    if resolved.resume.is_some()
        || resolved
            .harness_args
            .iter()
            .any(|arg| looks_like_native_resume_or_reuse_tail(arg))
    {
        return Err("--initial-prompt is fresh-launch only; remove resume/reuse harness arguments");
    }
    Ok(())
}

/// Resolve launch mode, with headed-only harness args defaulting to TUI because headless ACP runners
/// do not consume native TUI argv.
pub fn launch_mode_for_resume(
    headless: bool,
    tui: bool,
    is_tty: bool,
    prompt_answer: Option<bool>,
    requires_tui: bool,
) -> Mode {
    if requires_tui && !headless {
        return Mode::Tui;
    }
    launch_mode(headless, tui, is_tty, prompt_answer)
}

/// `nexus launch <kind>` — submit a daemon-owned harness launch; the agent then self-registers.
#[derive(Args, Debug)]
pub struct LaunchArgs {
    /// Harness id (`claude`|`codex`|`opencode`|`hermes`|`pi`|any registered id).
    #[arg(value_parser = parse::harness)]
    pub kind: HarnessId,
    #[arg(long)]
    pub cwd: Option<String>,
    #[arg(long)]
    pub name: Option<String>,
    #[arg(long)]
    pub project: Option<String>,
    #[arg(long)]
    pub role: Option<String>,
    #[arg(long = "initial-prompt")]
    pub initial_prompt: Option<String>,
    /// Harness-specific launch arguments after `<kind>`. Put Nexus launch flags before `<kind>`.
    #[arg(num_args = 0.., trailing_var_arg = true, allow_hyphen_values = true)]
    pub harness_args: Vec<String>,
    /// Skip auto-attach: print the session id and return immediately (agent keeps running).
    #[arg(long)]
    pub detach: bool,
    /// Escape hatch: attach an opaque JSON-object metadata bag to the launched agent
    /// immediately after spawn (e.g. `--meta '{"lens-owned":true}'`). Stored via the
    /// ungated entity-metadata surface; Nexus never interprets the shape.
    #[arg(long, value_name = "json")]
    pub meta: Option<String>,
    /// Spawn headlessly via the daemon command worker (ACP agent + MCP); no TUI.
    #[arg(long)]
    pub headless: bool,
    /// Exec the harness TUI (the existing build_harness_command path).
    #[arg(long)]
    pub tui: bool,
    /// Local terminal backend for headed launches: `pty` (raw daemon-owned PTY, the default —
    /// attach/view it from any terminal emulator, e.g. `alacritty -e nexus attach <name>`) or
    /// `tmux` (the legacy multiplexer backend).
    #[arg(long, default_value = "pty", value_parser = ["pty", "tmux"])]
    pub backend: String,
    /// Use the legacy tmux headed backend instead of the default daemon-owned raw PTY.
    #[arg(long, conflicts_with = "backend")]
    pub tmux: bool,
}

/// `nexus register` — bind an ALREADY-RUNNING harness process to the bus (register-once;
/// idempotent on `--client-key`). Registration creates the bus row only — no inbound delivery
/// path exists unless the harness brings its own transport (the `nexus mcp` client). Internal:
/// invoked by the daemon-installed `.nexus/bootstrap-register.sh` hook; humans use `nexus launch`.
#[derive(Args, Debug)]
pub struct RegisterArgs {
    #[arg(long)]
    pub name: String,
    /// The harness/runtime (`claude`|`codex`|…); defaults to claude.
    #[arg(long, value_parser = parse::harness)]
    pub agent: Option<HarnessId>,
    /// `agent` | `app`.
    #[arg(long, value_parser = parse::kind)]
    pub kind: Option<Kind>,
    #[arg(long)]
    pub project: Option<String>,
    #[arg(long)]
    pub role: Option<String>,
    #[arg(long)]
    pub cwd: Option<String>,
    /// Stable per-harness-session key; resume reuses the session.
    #[arg(long = "client-key")]
    pub client_key: Option<String>,
    /// Adopt an existing bound identity: the durable `a_*` agent id to claim.
    /// Must be paired with `--runtime-credential` (or `NEXUS_RUNTIME_CREDENTIAL`);
    /// the daemon verifies the credential before any write. This is the repair
    /// path the "name already bound" register error asks for.
    #[arg(long = "agent-id")]
    pub agent_id: Option<String>,
    /// Runtime credential secret proving ownership of `--agent-id`
    /// (minted via `nexus agents credentials create`). Falls back to
    /// `NEXUS_RUNTIME_CREDENTIAL`.
    #[arg(long = "runtime-credential")]
    pub runtime_credential: Option<String>,
}

/// Resolve the `RegisterRequest` payload for `nexus register` in tests.
///
/// The stable harness-native session id is preferred when the runtime exports one, but callers
/// must provide the daemon-issued client key explicitly.
pub fn register_request(a: &RegisterArgs) -> RegisterRequest {
    let client_key = a
        .client_key
        .clone()
        .expect("register_request test helper requires explicit client_key");
    RegisterRequest {
        agent_id: None,
        name: Some(a.name.clone()),
        harness: a
            .agent
            .clone()
            .unwrap_or_else(|| HarnessId::new("claude").expect("builtin harness id is valid")),
        harness_session_id: stable_harness_session_id_from_env()
            .unwrap_or_else(|| format!("hs_{client_key}")),
        project: a.project.clone().unwrap_or_else(|| "default".into()),
        client_key,
        runtime_credential: None,
        tier: Tier::Agent,
        kind: a.kind,
        role: a.role.clone(),
        cwd: a.cwd.clone(),
    }
}

/// Resolve the working directory to send with a daemon-owned launch.
///
/// Explicit `--cwd` wins. Otherwise the CLI captures its own current directory, because the daemon
/// may be running elsewhere and cannot infer the shell folder that invoked `nexus launch`.
pub fn resolve_launch_cwd(explicit: Option<String>) -> Option<String> {
    explicit.or_else(|| {
        std::env::current_dir()
            .ok()
            .and_then(|p| p.to_str().map(String::from))
    })
}

/// Resolve the launch working directory for a concrete harness.
///
/// Explicit `--cwd` remains authoritative. Otherwise the CLI sends its shell cwd for every harness,
/// including Claude, so launched agents work in the folder the operator chose. Revival uses
/// an exact best-effort `--resume` hint, but Nexus does not own or validate harness identity.
pub fn resolve_launch_cwd_for(
    kind: &HarnessId,
    explicit: Option<String>,
    harness_args: &[String],
) -> Option<String> {
    let _ = (kind, harness_args);
    if explicit.is_some() {
        return explicit;
    }
    resolve_launch_cwd(None)
}

/// Build the harness program + args for `nexus launch <kind>`.
///
/// Returns `None` for unsupported kinds (pi/other).
pub fn build_harness_command(
    kind: &HarnessId,
    name: &str,
    project: &str,
    nexus_exe: &str,
    _cwd: Option<&str>,
) -> Option<(String, Vec<String>)> {
    let command = harness_registry_by_id(kind)
        .headed_cli_command(&HarnessIdentity::legacy(name, project), nexus_exe, &[])
        .ok()?;
    (!command.program.is_empty()).then_some((command.program, command.args))
}

fn resolve_tail_for(
    kind: &HarnessId,
    harness_args: &[String],
) -> Result<nexus_harness_core::ResolvedTail, nexus_harness_core::HarnessError> {
    harness_registry_by_id(kind).resolve_tail(harness_args)
}

/// `nexus launch <kind>` — ask the daemon command worker to spawn the harness runtime, then
/// optionally attach to the local terminal view for headed runtimes.
pub async fn launch(store: &StoreClient, read: &ReadClient, a: LaunchArgs, json: bool) -> ExitCode {
    // Resolve fields. Fresh launches with no `--name` now stage an unnamed durable identity; only
    // `whoami` or an explicit admin rename assigns the public handle.
    let project = a
        .project
        .clone()
        .or_else(|| {
            std::env::var("NEXUS_PROJECT")
                .ok()
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| "default".into());

    // The daemon ALWAYS owns the harness: the launch command intent spawns the ACP agent + MCP and
    // binds the session under a daemon-chosen id. The CLI never execs its own harness (that
    // produced an MCP-attached session the daemon couldn't inject into — the whole "DM didn't
    // reach it" bug).
    // Launch in the folder where `nexus launch` was invoked unless an explicit `--cwd` overrides.
    // Claude's native resume id is an opaque provider hint; it is not Nexus identity.
    let cwd = resolve_launch_cwd_for(&a.kind, a.cwd.clone(), &a.harness_args);
    let resolved = match resolve_launch_resume(&a.kind, &a.harness_args) {
        Ok(resolved) => resolved,
        Err(message) => {
            eprintln!("error: {message}");
            return ExitCode::from(1);
        }
    };

    // Resolve the launch mode to carry `headless` onto the SpawnRequest so the daemon knows
    // whether to start an ACP runner or a PTY harness. The `--detach` / attach path below is
    // NOT changed here — it stays driven by `a.detach` / `a.headless` as before.
    use std::io::IsTerminal as _;
    let mode = launch_mode_for_resume(
        a.headless,
        a.tui,
        std::io::stdin().is_terminal(),
        None,
        resolved.requires_tui,
    );
    if let Err(message) = validate_launch_harness_args(&a.kind, &resolved, mode) {
        eprintln!("error: {message}");
        return ExitCode::from(1);
    }
    let launch_meta = match parse_launch_meta(a.meta.as_deref()) {
        Ok(meta) => meta,
        Err(message) => {
            eprintln!("error: {message}");
            return ExitCode::from(2);
        }
    };
    if let Err(message) = validate_initial_prompt_launch(a.initial_prompt.as_deref(), &resolved) {
        eprintln!("error: {message}");
        return ExitCode::from(1);
    }
    let (request_name, identity_policy) =
        launch_identity_fields(&a.kind, a.name.clone(), resolved.resume.as_deref());
    let backend = if a.tmux {
        "tmux".to_string()
    } else {
        a.backend.clone()
    };
    let spawn_req = SpawnRequest {
        kind: a.kind.clone(),
        name: request_name.clone(),
        identity_policy: Some(identity_policy),
        cwd,
        project: Some(project.clone()),
        role: a.role.clone(),
        initial_prompt: a.initial_prompt.clone(),
        resume: resolved.resume,
        harness_args: resolved.harness_args,
        headless: matches!(mode, Mode::Headless),
        backend: Some(backend.clone()),
    };
    // The raw-PTY backend keeps the harness on a daemon-owned PTY with no terminal emulator of
    // its own. Detached, a full-screen TUI (claude) may hold an injected turn incomplete until a
    // viewer attaches — tmux does not have that failure mode because it IS a terminal emulator.
    if a.detach && backend == "pty" && !matches!(mode, Mode::Headless) {
        eprintln!(
            "warning: --detach with the raw pty backend leaves the TUI unattended; \
             injected turns may not complete until you attach (`nexus attach <name>`). \
             Use `--tmux` for a fully detached headed launch."
        );
    }
    let session_id = match store
        .command::<_, SpawnResponse>(nexus_store::command_kinds::harness::LAUNCH, &spawn_req)
        .await
    {
        Ok(resp) => resp.session_id,
        Err(e) => {
            eprintln!("error: daemon launch command failed: {}", e.message);
            return ExitCode::from(1);
        }
    };

    let name = launch_display_name(read, request_name.as_deref(), &session_id).await;

    // --meta escape hatch: attach the opaque bag to the launched agent (by public name or a_*
    // id — both are valid Agent metadata ids). The agent is already running; a metadata write
    // failure is reported loudly but does not undo or fail the launch.
    if let Some(metadata) = launch_meta {
        let res: Result<nexus_contracts::MetadataResponse, _> = store
            .command(
                nexus_store::command_kinds::metadata::SET,
                &nexus_contracts::MetadataSetRequest {
                    entity: nexus_contracts::MetadataEntityKind::Agent,
                    id: name.clone(),
                    metadata,
                },
            )
            .await;
        if let Err(e) = res {
            eprintln!(
                "warning: agent launched but --meta attach failed ({}); \
                 retry via the metadata.set API against agent {name}",
                e.message
            );
        }
    }

    // `--detach` / `--headless` / non-interactive stdin: print the addressable handle
    // (`<name>:<session_id>`) and return; the agent keeps running daemon-owned. Re-attach a TUI
    // view later with `nexus attach <name>`, or open the web view at `/agent/<name>:<session_id>`.
    let attach = !a.detach && !a.headless && std::io::stdin().is_terminal();
    if !attach {
        println!("{}", launch_handle_output(&name, &session_id.0, json));
        return ExitCode::SUCCESS;
    }

    // Interactive: drop into the daemon-owned headed terminal. Session output in the web UI is
    // store-backed; terminal attach is local through the backend descriptor.
    eprintln!(
        "[launch] {name} ({:?}) is daemon-owned as {name}:{} - attaching (detach: Ctrl-b d; agent keeps running).",
        a.kind, session_id.0
    );
    super::pty_attach::attach_session(store, read, session_id).await
}

fn launch_handle_output(name: &str, session_id: &str, json: bool) -> String {
    if json {
        serde_json::to_string_pretty(&serde_json::json!({
            "name": name,
            "sessionId": session_id,
        }))
        .expect("launch handle is JSON serializable")
    } else {
        format!("{name}:{session_id}")
    }
}

#[path = "../../../tests/unit/cli_lifecycle.rs"]
mod cli_lifecycle_contracts;

/// Parse and validate `--meta`: must be a JSON object (an opaque bag, not a scalar/array),
/// rejected BEFORE the spawn is submitted so a typo cannot launch an untagged agent.
pub fn parse_launch_meta(raw: Option<&str>) -> Result<Option<serde_json::Value>, String> {
    let Some(raw) = raw else { return Ok(None) };
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|e| format!("--meta must be valid JSON: {e}"))?;
    if !value.is_object() {
        return Err("--meta must be a JSON object, e.g. --meta '{\"lens-owned\":true}'".into());
    }
    Ok(Some(value))
}

pub fn launch_identity_fields(
    kind: &HarnessId,
    explicit_name: Option<String>,
    resume: Option<&str>,
) -> (Option<String>, SpawnIdentityPolicy) {
    let _ = (kind, resume);
    match explicit_name {
        Some(name) => {
            let policy = if name.starts_with("a_") {
                SpawnIdentityPolicy::ExplicitAgentId
            } else {
                SpawnIdentityPolicy::ExplicitName
            };
            (Some(name), policy)
        }
        None => (None, SpawnIdentityPolicy::Implicit),
    }
}

async fn launch_display_name(
    read: &ReadClient,
    request_name: Option<&str>,
    session_id: &nexus_contracts::SessionId,
) -> String {
    if let Some(name) = request_name {
        return name.to_string();
    }
    read.members(nexus_contracts::MemberListRequest {
        project: None,
        include_offline: Some(true),
        include_dead: None,
    })
    .await
    .ok()
    .and_then(|members| {
        members
            .members
            .into_iter()
            .find(|member| member.session_id == *session_id)
            .and_then(|member| member.name.or(member.agent_id.map(|id| id.0)))
    })
    .unwrap_or_else(|| session_id.0.clone())
}

/// `nexus register` — bind a name (register-once, idempotent on the client key).
///
/// Identity is implicit downstream — register is the one command that *establishes* identity, so it
/// carries the name + harness + keys. `harness_session_id` uses Nexus-owned runtime identity from
/// `NEXUS_SESSION_ID`, then the daemon-issued `NEXUS_CLIENT_KEY`. Provider-native ids such as
/// `CLAUDE_CODE_SESSION_ID` are deliberately ignored. A name without a key is refused instead of
/// minting a predictable compatibility key.
pub async fn register(client: &StoreClient, a: RegisterArgs, json: bool) -> ExitCode {
    let res: Result<RegisterResponse, _> =
        match build_register_request(a, std::env::var("NEXUS_CLIENT_KEY").ok()) {
            Ok(req) => {
                client
                    .command(nexus_store::command_kinds::identity::REGISTER, &req)
                    .await
            }
            Err(error) => Err(error),
        };
    finish_with(res, json, |r| {
        format!("registered session {}\n{}", r.session_id.0, r.directive)
    })
}

fn build_register_request(
    a: RegisterArgs,
    env_client_key: Option<String>,
) -> Result<RegisterRequest, ContractError> {
    let env_client_key = env_client_key.filter(|key| !key.is_empty());
    let explicit_or_env_client_key = a.client_key.clone().or(env_client_key);
    let client_key = explicit_or_env_client_key
        .clone()
        .ok_or_else(|| ContractError {
            code: codes::UNAUTHORIZED,
            message: "nexus register requires --client-key or NEXUS_CLIENT_KEY".into(),
        })?;
    let harness_session_id = stable_harness_session_id_from_env()
        .or(explicit_or_env_client_key)
        .unwrap_or_else(|| format!("hs_{}", a.name));
    let runtime_credential = a
        .runtime_credential
        .clone()
        .or_else(|| std::env::var("NEXUS_RUNTIME_CREDENTIAL").ok())
        .filter(|v| !v.is_empty());
    Ok(RegisterRequest {
        agent_id: a.agent_id.clone().map(AgentId),
        name: Some(a.name.clone()),
        harness: a
            .agent
            .unwrap_or_else(|| HarnessId::new("claude").expect("builtin harness id is valid")),
        harness_session_id,
        project: a.project.unwrap_or_else(|| "default".into()),
        client_key,
        runtime_credential,
        tier: Tier::Agent,
        kind: a.kind,
        role: a.role,
        cwd: a.cwd,
    })
}

/// `nexus rename <new>` — the caller renames itself on the bus.
#[derive(Args, Debug)]
pub struct RenameArgs {
    /// The new name to bind to your session.
    pub name: String,
}

/// `nexus rename <new>` — change the caller's own display name (an agent renames himself).
pub async fn rename(client: &StoreClient, a: RenameArgs, json: bool) -> ExitCode {
    let req = RenameRequest { name: a.name };
    let res: Result<RenameResponse, _> = client
        .command(nexus_store::command_kinds::identity::RENAME, &req)
        .await;
    finish_with(res, json, |r| match r.previous.as_deref() {
        Some(previous) => format!("renamed {} -> {}", previous, r.name),
        None => format!("named {} (first name claimed)", r.name),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::ambient::with_test_env_vars;

    #[test]
    fn register_request_prefers_stable_session_id_over_client_key() {
        with_test_env_vars(
            &[
                ("NEXUS_SESSION_ID", Some("stable-register-session")),
                ("CLAUDE_CODE_SESSION_ID", Some("claude-session-ignored")),
            ],
            || {
                let req = register_request(&RegisterArgs {
                    name: "bianca".into(),
                    agent: Some(HarnessId::new("claude").unwrap()),
                    kind: Some(Kind::Human),
                    project: Some("default".into()),
                    role: Some("operator".into()),
                    cwd: Some("/repo".into()),
                    client_key: Some("ck_bianca_rotating".into()),
                    agent_id: None,
                    runtime_credential: None,
                });

                assert_eq!(req.client_key, "ck_bianca_rotating");
                assert_eq!(req.harness_session_id, "stable-register-session");
                assert_eq!(req.project, "default");
                assert_eq!(req.role.as_deref(), Some("operator"));
            },
        );
    }
}

/// `nexus whoami` — the caller's resolved bound identity (no args; identity is implicit).
/// The implicit local-operator identity is labeled loudly so an escalated shell is visible at a
/// glance (B21): it is the local zero-auth human default, not a registered agent.
pub async fn whoami(client: &ReadClient, json: bool) -> ExitCode {
    let res: Result<Whoami, _> = client.whoami().await;
    finish_with(res, json, |w| {
        let operator_label = if w.session_id.0 == "local-operator" {
            "\n  ^ LOCAL OPERATOR — implicit admin (local zero-auth shell), not a registered agent"
        } else {
            ""
        };
        format!(
            "{}  agent={}  session={}  tier={:?}  project={}  presence={:?}{}",
            w.name.as_deref().unwrap_or("<unnamed>"),
            w.agent_id.as_ref().map(|id| id.0.as_str()).unwrap_or("-"),
            w.session_id.0,
            w.tier,
            w.project,
            w.presence,
            operator_label
        )
    })
}

#[cfg(test)]
mod builder_tests {
    use super::*;
    use crate::cli::ambient::with_test_env_vars;
    use nexus_harness_core::{native_harness_program, NativeProcessPlatform};

    #[test]
    fn launch_meta_accepts_objects_and_rejects_everything_else() {
        assert_eq!(parse_launch_meta(None).unwrap(), None);
        assert_eq!(
            parse_launch_meta(Some("{\"lens-owned\":true}")).unwrap(),
            Some(serde_json::json!({"lens-owned": true}))
        );
        assert!(parse_launch_meta(Some("not json")).is_err());
        assert!(
            parse_launch_meta(Some("[1,2]")).is_err(),
            "arrays are not a bag"
        );
        assert!(
            parse_launch_meta(Some("42")).is_err(),
            "scalars are not a bag"
        );
    }

    fn register_args(name: &str) -> RegisterArgs {
        RegisterArgs {
            name: name.into(),
            agent: None,
            kind: None,
            project: None,
            role: None,
            cwd: None,
            client_key: None,
            agent_id: None,
            runtime_credential: None,
        }
    }

    #[test]
    fn build_register_request_carries_adopt_identity() {
        let mut args = register_args("remy");
        args.client_key = Some("ck_test".into());
        args.agent_id = Some("a_s_abc".into());
        args.runtime_credential = Some("nexus_rt_secret".into());
        let req = build_register_request(args, None).expect("request");
        assert_eq!(
            req.agent_id.as_ref().map(|id| id.0.as_str()),
            Some("a_s_abc")
        );
        assert_eq!(req.runtime_credential.as_deref(), Some("nexus_rt_secret"));
    }

    #[test]
    fn build_register_request_reads_runtime_credential_env() {
        with_test_env_vars(
            &[("NEXUS_RUNTIME_CREDENTIAL", Some("nexus_rt_env"))],
            || {
                let mut args = register_args("remy");
                args.client_key = Some("ck_test".into());
                let req = build_register_request(args, None).expect("request");
                assert_eq!(req.runtime_credential.as_deref(), Some("nexus_rt_env"));
            },
        );
    }

    #[test]
    fn codex_command_and_mcp_args() {
        let kind = HarnessId::new("codex").unwrap();
        let (prog, args) =
            build_harness_command(&kind, "ada", "lens", "/usr/bin/nexus", None).unwrap();
        assert_eq!(
            prog,
            native_harness_program(kind.as_str(), NativeProcessPlatform::current())
                .expect("built-in harness has a native headed executable")
        );
        assert!(args.contains(&"-c".to_string()));
        assert!(args
            .iter()
            .any(|a| a.contains("mcp_servers.nexus-bus.command=")));
        assert!(args
            .iter()
            .any(|a| a.contains("mcp_servers.nexus-bus.args=")
                && a.contains("--as")
                && a.contains("ada")));
    }

    #[test]
    fn claude_command_and_mcp_config() {
        let kind = HarnessId::new("claude").unwrap();
        let (prog, args) =
            build_harness_command(&kind, "ada", "lens", "/usr/bin/nexus", None).unwrap();
        assert_eq!(
            prog,
            native_harness_program(kind.as_str(), NativeProcessPlatform::current())
                .expect("built-in harness has a native headed executable")
        );
        let idx = args
            .iter()
            .position(|a| a == "--mcp-config")
            .expect("--mcp-config not found");
        let json_str = &args[idx + 1];
        let v: serde_json::Value = serde_json::from_str(json_str).expect("invalid JSON");
        assert!(
            v["mcpServers"]["nexus-bus"].is_object(),
            "nexus-bus key must exist"
        );
        assert_eq!(v["mcpServers"]["nexus-bus"]["command"], "/usr/bin/nexus");
        // verify --as ada is in the args array
        let mcp_args = v["mcpServers"]["nexus-bus"]["args"].as_array().unwrap();
        assert!(
            mcp_args.iter().any(|a| a == "--as"),
            "--as missing from MCP args"
        );
        assert!(
            mcp_args.iter().any(|a| a == "ada"),
            "name missing from MCP args"
        );
    }

    #[test]
    fn pi_returns_none() {
        assert!(
            build_harness_command(&HarnessId::new("pi").unwrap(), "x", "p", "/e", None).is_none()
        );
    }

    #[test]
    fn register_uses_env_client_key_when_flag_is_omitted() {
        with_test_env_vars(
            &[("NEXUS_SESSION_ID", None), ("CLAUDE_CODE_SESSION_ID", None)],
            || {
                let req =
                    build_register_request(register_args("hugo"), Some("ck_hugo".into())).unwrap();

                assert_eq!(req.client_key, "ck_hugo");
                assert_eq!(req.harness_session_id, "ck_hugo");
            },
        )
    }

    #[test]
    fn register_client_key_flag_wins_over_env_client_key() {
        let mut args = register_args("hugo");
        args.client_key = Some("ck_explicit".into());

        with_test_env_vars(
            &[("NEXUS_SESSION_ID", None), ("CLAUDE_CODE_SESSION_ID", None)],
            || {
                let req = build_register_request(args, Some("ck_hugo".into())).unwrap();

                assert_eq!(req.client_key, "ck_explicit");
                assert_eq!(req.harness_session_id, "ck_explicit");
            },
        )
    }

    #[test]
    fn register_without_flag_or_env_fails_loud() {
        with_test_env_vars(
            &[("NEXUS_SESSION_ID", None), ("CLAUDE_CODE_SESSION_ID", None)],
            || {
                let error = build_register_request(register_args("hugo"), None).unwrap_err();

                assert_eq!(error.code, codes::UNAUTHORIZED);
                assert!(error.message.contains("client-key"));
            },
        )
    }
}

/// Build a `SpawnRequest` from launch args and a resolved mode, without any I/O. Used for unit
/// testing `headless` wiring without submitting a command intent.
pub fn build_spawn_request(
    kind: nexus_contracts::HarnessId,
    explicit_name: Option<String>,
    generated_name: String,
    cwd: Option<String>,
    project: String,
    role: Option<String>,
    initial_prompt: Option<String>,
    resume: Option<String>,
    harness_args: Vec<String>,
    mode: Mode,
) -> SpawnRequest {
    let _ = generated_name;
    let (name, identity_policy) = launch_identity_fields(&kind, explicit_name, resume.as_deref());
    SpawnRequest {
        kind,
        name,
        identity_policy: Some(identity_policy),
        cwd,
        project: Some(project),
        role,
        initial_prompt,
        resume,
        harness_args,
        headless: matches!(mode, Mode::Headless),
        backend: None,
    }
}

// ── launch_mode unit tests ────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod launch_mode_tests {
    use super::{build_spawn_request, launch_mode, resolve_launch_cwd, Mode};
    use nexus_contracts::{HarnessId, SpawnIdentityPolicy};

    #[test]
    fn headless_flag_forces_headless() {
        assert_eq!(launch_mode(true, false, true, None), Mode::Headless);
        assert_eq!(launch_mode(true, false, false, None), Mode::Headless);
    }

    #[test]
    fn tui_flag_forces_tui() {
        assert_eq!(launch_mode(false, true, true, None), Mode::Tui);
        assert_eq!(launch_mode(false, true, false, None), Mode::Tui);
    }

    #[test]
    fn no_flags_tty_answer_yes_is_headless() {
        assert_eq!(launch_mode(false, false, true, Some(true)), Mode::Headless);
    }

    #[test]
    fn no_flags_tty_answer_no_is_tui() {
        assert_eq!(launch_mode(false, false, true, Some(false)), Mode::Tui);
    }

    #[test]
    fn no_flags_non_interactive_is_headless() {
        // non-tty stdin, no flags → headless (safe default for scripts/CI)
        assert_eq!(launch_mode(false, false, false, None), Mode::Headless);
    }

    #[test]
    fn explicit_cwd_wins_over_process_cwd() {
        assert_eq!(
            resolve_launch_cwd(Some("/tmp/nexus-project".into())),
            Some("/tmp/nexus-project".into())
        );
    }

    #[test]
    fn omitted_cwd_uses_cli_process_cwd() {
        let expected = std::env::current_dir().unwrap().to_str().map(String::from);
        assert_eq!(resolve_launch_cwd(None), expected);
    }

    /// `--headless` flag → `SpawnRequest.headless == true`.
    #[test]
    fn spawn_request_headless_true_when_mode_is_headless() {
        let mode = launch_mode(true, false, true, None); // --headless
        assert_eq!(mode, Mode::Headless);
        let req = build_spawn_request(
            HarnessId::new("claude").unwrap(),
            Some("ada".into()),
            "ada".into(),
            None,
            "egregore".into(),
            None,
            None,
            None,
            Vec::new(),
            mode,
        );
        assert!(
            req.headless,
            "SpawnRequest.headless must be true when mode is Headless"
        );
    }

    /// TUI mode → `SpawnRequest.headless == false`.
    #[test]
    fn spawn_request_headless_false_when_mode_is_tui() {
        let mode = launch_mode(false, true, true, None); // --tui
        assert_eq!(mode, Mode::Tui);
        let req = build_spawn_request(
            HarnessId::new("codex").unwrap(),
            Some("ben".into()),
            "ben".into(),
            None,
            "egregore".into(),
            None,
            None,
            None,
            Vec::new(),
            mode,
        );
        assert!(
            !req.headless,
            "SpawnRequest.headless must be false when mode is Tui"
        );
    }

    #[test]
    fn codex_resume_without_user_name_preserves_implicit_identity_intent() {
        let mode = launch_mode(false, true, true, None);
        let req = build_spawn_request(
            HarnessId::new("codex").unwrap(),
            None,
            "generated-celia".into(),
            None,
            "default".into(),
            None,
            None,
            Some("019f3374-78e7-7763-9730-9e89f84bf2eb".into()),
            Vec::new(),
            mode,
        );

        assert_eq!(req.name, None);
        assert_eq!(req.identity_policy, Some(SpawnIdentityPolicy::Implicit));
    }

    #[test]
    fn fresh_launch_without_user_name_sends_no_name() {
        let mode = launch_mode(false, true, true, None);
        let req = build_spawn_request(
            HarnessId::new("codex").unwrap(),
            None,
            "generated-celia".into(),
            None,
            "default".into(),
            None,
            None,
            None,
            Vec::new(),
            mode,
        );

        assert_eq!(req.name, None);
        assert_eq!(req.identity_policy, Some(SpawnIdentityPolicy::Implicit));
    }

    #[test]
    fn explicit_agent_id_name_sets_explicit_agent_id_policy() {
        let mode = launch_mode(false, true, true, None);
        let req = build_spawn_request(
            HarnessId::new("codex").unwrap(),
            Some("a_s_otto".into()),
            "unused-generated".into(),
            None,
            "default".into(),
            None,
            None,
            Some("019f3374-78e7-7763-9730-9e89f84bf2eb".into()),
            Vec::new(),
            mode,
        );

        assert_eq!(req.name.as_deref(), Some("a_s_otto"));
        assert_eq!(
            req.identity_policy,
            Some(SpawnIdentityPolicy::ExplicitAgentId)
        );
    }
}

// ── Arg-parse tests ───────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod arg_tests {
    use clap::CommandFactory;
    use clap::Parser;

    use crate::cli::{Cli, Command};

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("nexus").chain(args.iter().copied())).unwrap()
    }

    #[test]
    fn launch_detach_flag_parses() {
        let cli = parse(&["launch", "claude", "--detach"]);
        match cli.command {
            crate::cli::Command::Launch(a) => {
                assert!(a.detach, "--detach flag must be parsed as true");
            }
            other => panic!("expected Launch, got {:?}", other),
        }
    }

    #[test]
    fn launch_default_is_not_detached() {
        let cli = parse(&["launch", "claude"]);
        match cli.command {
            crate::cli::Command::Launch(a) => {
                assert!(!a.detach, "default launch must not be detached");
            }
            other => panic!("expected Launch, got {:?}", other),
        }
    }

    #[test]
    fn launch_with_name_and_detach() {
        let cli = parse(&["launch", "codex", "--name", "ada", "--detach"]);
        match cli.command {
            crate::cli::Command::Launch(a) => {
                assert_eq!(a.name, Some("ada".into()));
                assert!(a.detach);
            }
            other => panic!("expected Launch, got {:?}", other),
        }
    }

    #[test]
    fn launch_headless_flag_parses() {
        let cli = parse(&["launch", "codex", "--headless"]);
        match cli.command {
            crate::cli::Command::Launch(a) => {
                assert!(a.headless, "--headless must be parsed as true");
                assert!(!a.tui);
            }
            other => panic!("expected Launch, got {:?}", other),
        }
    }

    #[test]
    fn launch_tui_flag_parses() {
        let cli = parse(&["launch", "claude", "--tui"]);
        match cli.command {
            Command::Launch(a) => {
                assert!(a.tui, "--tui must be parsed as true");
                assert!(!a.headless);
            }
            other => panic!("expected Launch, got {:?}", other),
        }
    }

    #[test]
    fn launch_help_lists_every_supported_harness_kind() {
        let mut command = Cli::command();
        let launch = command
            .find_subcommand_mut("launch")
            .expect("launch subcommand");
        let mut help = Vec::new();
        launch.write_long_help(&mut help).unwrap();
        let help = String::from_utf8(help).unwrap();

        assert!(help.contains("claude"), "{help}");
        assert!(help.contains("codex"), "{help}");
        assert!(help.contains("opencode"), "{help}");
        assert!(help.contains("hermes"), "{help}");
        assert!(help.contains("pi"), "{help}");
        // Open-set migration: `other` is no longer a kind literal; the help must
        // instead advertise that any registered id is accepted.
        assert!(help.contains("any registered id"), "{help}");
    }

    #[test]
    fn launch_default_backend_is_raw_pty() {
        let cli = parse(&["launch", "claude"]);
        match cli.command {
            crate::cli::Command::Launch(a) => {
                assert_eq!(a.backend, "pty", "raw pty is the launch default");
            }
            other => panic!("expected Launch, got {:?}", other),
        }
    }

    #[test]
    fn launch_backend_tmux_opt_in_parses() {
        let cli = parse(&["launch", "--backend", "tmux", "claude"]);
        match cli.command {
            crate::cli::Command::Launch(a) => assert_eq!(a.backend, "tmux"),
            other => panic!("expected Launch, got {:?}", other),
        }
    }

    #[test]
    fn launch_tmux_flag_is_backend_tmux_alias() {
        let cli = parse(&["launch", "--tmux", "codex"]);
        match cli.command {
            crate::cli::Command::Launch(a) => {
                assert!(a.tmux);
                assert_eq!(a.backend, "pty");
            }
            other => panic!("expected Launch, got {:?}", other),
        }
    }

    #[test]
    fn admin_remove_kill_parses() {
        let cli = parse(&["admin", "remove", "dylan", "--kill"]);
        match cli.command {
            crate::cli::Command::Admin(crate::cli::commands::admin::AdminCmd::Remove {
                name,
                kill,
            }) => {
                assert_eq!(name, "dylan");
                assert!(kill, "--kill must be true");
            }
            other => panic!("expected Admin(Remove), got {:?}", other),
        }
    }

    #[test]
    fn admin_remove_default_no_kill() {
        let cli = parse(&["admin", "remove", "dylan"]);
        match cli.command {
            crate::cli::Command::Admin(crate::cli::commands::admin::AdminCmd::Remove {
                name,
                kill,
            }) => {
                assert_eq!(name, "dylan");
                assert!(!kill, "kill must default to false");
            }
            other => panic!("expected Admin(Remove), got {:?}", other),
        }
    }

    #[test]
    fn admin_delete_parses() {
        let cli = parse(&["admin", "delete", "dylan"]);
        match cli.command {
            crate::cli::Command::Admin(crate::cli::commands::admin::AdminCmd::Delete { name }) => {
                assert_eq!(name, "dylan");
            }
            other => panic!("expected Admin(Delete), got {:?}", other),
        }
    }
}
