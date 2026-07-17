//! Command construction helpers for spawning native harness binaries in a PTY.

use portable_pty::CommandBuilder;

/// Give daemon-owned headed terminals a color-capable environment independent of the daemon's.
pub fn apply_headed_terminal_environment(cmd: &mut CommandBuilder) {
    cmd.env("TERM", "xterm-256color");
    cmd.env("COLORTERM", "truecolor");
    cmd.env_remove("NO_COLOR");
}

/// Verified Nexus runtime identity injected into daemon-owned headed harnesses.
///
/// `client_key` is the stored runtime key, not the raw session id. The command builders use it for
/// harness-local MCP config; tmux launch uses the same value for the child process environment.
#[derive(Debug, Clone, Copy)]
pub struct HarnessIdentity<'a> {
    pub name: &'a str,
    pub project: &'a str,
    pub client_key: &'a str,
    pub agent: &'a str,
}

/// Build the INTERACTIVE native-harness invocation (the real TUI), with the nexus-bus MCP wired so
/// the agent can send on the bus. Mirrors the daemon's headless `build_harness_command`, but
/// deliberately omits `--print`/`--output-format` so the harness renders its native TUI.
pub fn harness_command(
    program: &str,
    name: &str,
    project: &str,
    nexus_exe: &str,
    cwd: Option<&str>,
) -> CommandBuilder {
    harness_command_with_identity(
        program,
        HarnessIdentity {
            name,
            project,
            client_key: "",
            agent: "",
        },
        nexus_exe,
        cwd,
    )
}

/// Build the interactive native-harness invocation with an explicit Nexus runtime identity.
///
/// Daemon-owned launches must use this form so every MCP command spawned by the harness carries the
/// same verified runtime key that is stored on the Nexus session row.
pub fn harness_command_with_identity(
    program: &str,
    identity: HarnessIdentity<'_>,
    nexus_exe: &str,
    cwd: Option<&str>,
) -> CommandBuilder {
    let mut cmd = CommandBuilder::new(program);
    apply_headed_terminal_environment(&mut cmd);

    // `cat` is the deterministic, offline harness STAND-IN used by tests (it echoes its PTY input
    // back out). It is not a real harness and rejects `--mcp-config`/flags (it would treat them as
    // filenames and exit). So the stand-in gets a BARE `cat`; only real harnesses get MCP +
    // permission flags below. Provably inert in production: `harness_program` only ever returns
    // `claude`/`codex`, never `cat`. (A cleaner test-only `launch_with_command` seam is a tracked
    // follow-up; this carve-out is safe as-is.)
    if program == "cat" {
        if let Some(dir) = cwd {
            cmd.cwd(dir);
        }
        return cmd;
    }

    // Wire the nexus-bus MCP server — but the FLAG is per-harness. claude takes a single
    // `--mcp-config <json>` (the claude-style `{ "mcpServers": { … } }` blob); codex does NOT accept
    // that flag (`error: unexpected argument '--mcp-config' found`) and instead configures MCP via
    // `-c <key=value>` TOML overrides of `~/.codex/config.toml` (`mcp_servers.<name>.command` /
    // `.args`). Mixing them up is the bug this fixes — codex launches were dying on `--mcp-config`.
    // Also bypass the harness's interactive permission/trust onboarding: a native `claude`/`codex`
    // spawned in a FRESH PTY+cwd sits at a "trust this folder?" / permission gate and never
    // processes the injected turn until a human accepts it — so without these flags the daemon's
    // injected `<nexus-batch>` is silently swallowed and `stream_events` stays empty (proven by the
    // live e2e). These flags make a fresh-cwd launch process input non-interactively, mirroring how
    // the headless SDK launch already runs un-gated.
    match program {
        "claude" => {
            for a in nexus_harness_claude::headed_pty_args_with_identity(
                identity.name,
                identity.project,
                nexus_exe,
                identity.client_key,
                identity.agent,
            ) {
                cmd.arg(a);
            }
            // claude v2.x: bypassPermissions skips per-tool permission prompts;
            // --dangerously-skip-permissions additionally bypasses ALL checks (and, with it, the
            // workspace-trust dialog) so an interactive PTY launch in an untrusted fresh cwd starts
            // processing immediately. Both are accepted by claude 2.1.191 (verified via --help).
        }
        "codex" => {
            for a in nexus_harness_codex::headed_pty_args_with_identity(
                identity.name,
                identity.project,
                nexus_exe,
                identity.client_key,
                identity.agent,
            ) {
                cmd.arg(a);
            }
        }
        "opencode" => {
            // opencode: BARE `opencode` — its `nexus-bus` MCP comes from OpenCode config outside
            // this argv (`OPENCODE_CONFIG_CONTENT` for headless ACP, project config for headed
            // compatibility). opencode does NOT accept claude's `--mcp-config` (it would
            // usage-error on the unknown flag), so we pass NO MCP flag here. No `-c`/TOML override
            // either — that's a codex-ism.
        }
        "hermes" => {
            // hermes headed runs the gateway, not the old Ink/CLI TUI. Nexus installs a launch-local
            // plugin/platform into an isolated HERMES_HOME and drives delivery/streaming over that
            // gateway bridge, so there is no tmux typing and no ~/.hermes mutation.
            cmd.arg("gateway");
            cmd.arg("run");
        }
        _ => {}
    }

    if let Some(dir) = cwd {
        cmd.cwd(dir);
    }
    cmd
}

/// Build the headed TUI for a running Codex app-server session.
///
/// Produces: `codex --remote unix://<sock> --dangerously-bypass-approvals-and-sandbox
/// --dangerously-bypass-hook-trust -c check_for_update_on_startup=false`
/// with an optional `-C <cwd>` workspace override applied to the command.
///
/// This connects a FRESH codex TUI to the shared app-server "brain" over the unix socket — a
/// NORMAL codex boot (no `resume`, no pre-created thread). We deliberately do NOT `resume <thread>`:
/// resume-by-id reads a rollout from disk that does not exist until a thread has run a turn, so it
/// failed on launch (the `[exited]` the operator saw) and forced an ugly warmup turn. Letting codex
/// start its own thread keeps the TUI normal; the daemon discovers that thread (from the on-disk
/// rollout) and binds its forwarder/inject to it for the web console.
///
/// `--remote` is a top-level flag ("Connect the TUI to a remote app server endpoint", verified via
/// `codex --help`). The approval/sandbox bypass mirrors the prior headed-codex behaviour so the
/// agent's tool calls don't block on approval prompts. The hook-trust bypass remains available for
/// operator-owned hooks, while daemon-owned app-server launches avoid installing a redundant Nexus
/// registration hook. The update override prevents a startup modal from hiding the live session.
pub fn codex_remote_command(sock: &str, cwd: Option<&str>) -> CommandBuilder {
    codex_remote_command_with_harness_args(sock, cwd, &[])
}

/// Build [`codex_remote_command`] and append the user-supplied Codex-native launch tail.
pub fn codex_remote_command_with_harness_args(
    sock: &str,
    cwd: Option<&str>,
    harness_args: &[String],
) -> CommandBuilder {
    nexus_harness_codex::remote_command_with_executable("codex", sock, cwd, harness_args)
}

/// Build the headed TUI for a running Codex app-server session, resuming an existing Codex thread.
///
/// This is the revive path counterpart to [`codex_remote_command`]. A revived Nexus session already
/// has a Codex thread id from `harness_session_id` (or an existing rollout file), so the TUI should
/// attach to the same app-server socket and open that same thread instead of creating a fresh one.
/// Codex resume restores the original session workspace unless `-C` is supplied, so Nexus passes
/// the launch cwd explicitly to keep `nexus launch --cwd ... codex resume ...` truthful.
pub fn codex_remote_resume_command(
    sock: &str,
    thread_id: &str,
    cwd: Option<&str>,
) -> CommandBuilder {
    codex_remote_resume_command_with_harness_args(sock, thread_id, cwd, &[])
}

/// Build [`codex_remote_resume_command`] and append a Codex-native launch tail before the thread id.
pub fn codex_remote_resume_command_with_harness_args(
    sock: &str,
    thread_id: &str,
    cwd: Option<&str>,
    harness_args: &[String],
) -> CommandBuilder {
    nexus_harness_codex::remote_resume_command_with_executable(
        "codex",
        sock,
        thread_id,
        cwd,
        harness_args,
    )
}
