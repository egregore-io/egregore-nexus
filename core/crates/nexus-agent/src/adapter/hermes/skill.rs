//! Hermes-specific `nexus-bus` skill + bootstrap-register script + `nexus-bus` MCP wiring.
//!
//! Unlike claude/codex (which got their skill from the shared `bootstrap.rs`), Hermes previously
//! got NOTHING — the legacy bootstrap installer skipped the `hermes` harness, so a launched
//! Hermes had no bus skill and (together with the forced stdio MCP over ACP) never came online.
//! This installs the full set:
//!
//! - `<HERMES_HOME>/skills/nexus-bus/SKILL.md` — the bus how-to (Hermes discovers skills under
//!   `HERMES_HOME/skills`, per `hermes_constants.get_skills_dir`).
//! - `<cwd>/.nexus/bootstrap-register.sh` — the idempotent `nexus register` fallback script.
//! - `<HERMES_HOME>/config.yaml` `mcp_servers.nexus-bus` — the **OPT-IN** MCP wiring (default OFF;
//!   see below). Hermes REJECTS a stdio MCP server over ACP `session/new` (`-32602`); instead its
//!   ACP session reads `mcp_servers` from `config.yaml` at `session/new` time
//!   (`acp_adapter/session.py`), so when enabled the bus tools are wired there. The server command
//!   mirrors the ACP/headed wiring exactly:
//!   `nexus mcp --as <name> --project <project> [--client-key <key>] [--agent <agent>]`, with
//!   `enabled: true` (a key-preserving merge that leaves every other config key untouched) because
//!   Hermes only enumerates servers whose `enabled` is not `false`.
//!
//! ## Why the MCP wiring is OPT-IN (and the bus is the `nexus` CLI by default)
//!
//! Hermes connects its configured `mcp_servers` **eagerly during ACP startup, before it answers
//! `initialize`**. Verified live against `hermes 0.17.0`: with a `nexus-bus` entry in `config.yaml`
//! — even pointed at a fully live, fast (`< 1ms` to `tools/list`) `nexus mcp` stdio server — hermes
//! never reaches "ACP client connected" and the daemon's `initialize` times out at 30s → the agent
//! NEVER comes online. The same hermes, with no `mcp_servers` entry, comes online in ~1.7s and
//! replies over ACP. So the config-MCP wiring is mutually exclusive with coming online for this
//! hermes build; wiring it on by default would re-introduce the exact "never came online" bug.
//!
//! Therefore the default (online + reply, proven) leaves MCP OUT of hermes's config, and hermes
//! sends on the bus via the **`nexus` CLI** (on its `PATH`, documented in the installed skill —
//! exactly like the claude adapter). The config-MCP merge is retained behind an explicit opt-in
//! (`NEXUS_HERMES_BUS_MCP=1`) for environments/hermes builds where eager MCP connect is non-blocking
//! and the operator wants the tools over MCP instead of the CLI.
//!
//! Hermes has no claude/codex-style *command* SessionStart hook in the headless ACP path (the
//! daemon mints + connects the session itself), so the script is the manual/operator fallback
//! rather than an auto-fired hook.
//!
//! All operations honor the same `NEXUS_SKIP_AGENT_*` env flags the shared bootstrap used, in the
//! same order. `SKILL_MD` is sourced from `skill.md` alongside this file so the markdown stays in
//! one place and is never duplicated in a Rust string literal.

use std::path::{Path, PathBuf};

use super::super::engine::LaunchCtx;

/// The `nexus-bus` `SKILL.md` content (frontmatter + body). Loaded at compile time from the
/// adjacent `skill.md`.
pub const SKILL_MD: &str = include_str!("skill.md");

/// Install all launch-local bootstrap files for a Hermes agent launched in `cwd`.
///
/// Respects the same env-flag escape hatches as the other adapters:
/// - `NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL=1` → skip everything
/// - `NEXUS_SKIP_AGENT_SKILL_INSTALL=1`     → skip the skill but still write the script + MCP config
/// - `NEXUS_SKIP_AGENT_HOOK_INSTALL=1`      → skip the script + MCP config but still write the skill
pub fn install(cwd: &str, ctx: &LaunchCtx) {
    if env_flag("NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL") {
        return;
    }

    if !env_flag("NEXUS_SKIP_AGENT_SKILL_INSTALL") {
        let _ = install_bus_skill_in_home(&hermes_home());
    }

    if env_flag("NEXUS_SKIP_AGENT_HOOK_INSTALL") {
        return;
    }

    write_script(cwd);
    write_hermes_mcp_config(ctx);
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn env_flag(name: &str) -> bool {
    std::env::var(name).ok().as_deref() == Some("1")
}

/// Resolve Hermes' home directory the same way Hermes does: the `HERMES_HOME` env var, else
/// `~/.hermes`. This is where Hermes reads `config.yaml` and discovers `skills/`.
fn hermes_home() -> PathBuf {
    if let Ok(h) = std::env::var("HERMES_HOME") {
        if !h.trim().is_empty() {
            return PathBuf::from(h);
        }
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    Path::new(&home).join(".hermes")
}

/// Install the `nexus-bus` skill under `<hermes_home>/skills/nexus-bus/SKILL.md`.
///
/// Headless Hermes calls this through [`install`]. Headed Hermes uses an isolated profile and calls
/// it directly so both launch modes discover the same bus contract and launch-pinned Nexus binary.
/// The operation is idempotent.
pub fn install_bus_skill_in_home(hermes_home: &Path) -> std::io::Result<()> {
    let dir = hermes_home.join("skills/nexus-bus");
    std::fs::create_dir_all(&dir)?;
    let nexus_exe = std::env::current_exe()
        .ok()
        .and_then(|path| path.to_str().map(str::to_string))
        .unwrap_or_else(|| "nexus".to_string());
    let rendered = SKILL_MD.replace("{{NEXUS_CLI}}", &shell_single_quote(&nexus_exe));
    std::fs::write(dir.join("SKILL.md"), rendered)
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn write_script(cwd: &str) {
    let dir = Path::new(cwd).join(".nexus");
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("bootstrap-register.sh");
    let _ = std::fs::write(&path, BOOTSTRAP_REGISTER_SH);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = std::fs::metadata(&path) {
            let mut perms = meta.permissions();
            perms.set_mode(0o755);
            let _ = std::fs::set_permissions(&path, perms);
        }
    }
}

/// Merge the `nexus-bus` MCP server into Hermes' `config.yaml` the **hermes way**: a
/// `mcp_servers.nexus-bus` stdio entry (`{ command, args, enabled: true }`) that Hermes' ACP session
/// loads at `session/new` time. This is a **key-preserving** merge — every other config key is read
/// back and re-written untouched — so it does not clobber the user's model/auth/etc. config.
///
/// The server command mirrors the ACP/headed wiring exactly:
/// `nexus mcp --as <name> --project <project> [--client-key <key>] [--agent <agent>]`.
///
/// **OPT-IN.** No-op unless `NEXUS_HERMES_BUS_MCP=1` is set: hermes connects configured MCP servers
/// eagerly during ACP startup and hangs `initialize` if a `nexus-bus` entry is present (verified
/// live — see the module docs), so by default the bus is reached via the `nexus` CLI and this writes
/// nothing. When opted in, it merges the entry as documented.
///
/// Also a no-op (and no file written) unless the bus identity (`bus_name`/`bus_project`) is
/// present, so test/admin paths without bus context don't drop a stray entry.
pub fn write_hermes_mcp_config(ctx: &LaunchCtx) {
    if !env_flag("NEXUS_HERMES_BUS_MCP") {
        return;
    }
    let (Some(name), Some(project)) = (&ctx.bus_name, &ctx.bus_project) else {
        return;
    };
    let nexus_exe = std::env::current_exe()
        .ok()
        .and_then(|p| p.to_str().map(str::to_string))
        .unwrap_or_else(|| "nexus".to_string());

    let path = hermes_home().join("config.yaml");

    // Read the existing config (if any) so the merge preserves all other keys; tolerate a
    // missing/empty/garbage file by starting from an empty mapping.
    let mut root: serde_yaml::Value = std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_yaml::from_str(&s).ok())
        .unwrap_or_else(|| serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));

    if !root.is_mapping() {
        root = serde_yaml::Value::Mapping(serde_yaml::Mapping::new());
    }

    let mut args = vec![
        "mcp".to_string(),
        "--as".to_string(),
        name.clone(),
        "--project".to_string(),
        project.clone(),
    ];
    if let Some(key) = &ctx.bus_client_key {
        args.push("--client-key".to_string());
        args.push(key.clone());
    }
    if let Some(agent) = &ctx.bus_agent {
        args.push("--agent".to_string());
        args.push(agent.clone());
    }

    let server = serde_yaml::to_value(serde_json::json!({
        "command": nexus_exe,
        "args": args,
        "enabled": true,
    }))
    .unwrap_or(serde_yaml::Value::Null);

    if let serde_yaml::Value::Mapping(map) = &mut root {
        let servers_key = serde_yaml::Value::String("mcp_servers".to_string());
        let servers = map
            .entry(servers_key)
            .or_insert_with(|| serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
        if !servers.is_mapping() {
            *servers = serde_yaml::Value::Mapping(serde_yaml::Mapping::new());
        }
        if let serde_yaml::Value::Mapping(servers_map) = servers {
            servers_map.insert(serde_yaml::Value::String("nexus-bus".to_string()), server);
        }
    }

    if let Ok(parent) = path.parent().ok_or(()) {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(serialized) = serde_yaml::to_string(&root) {
        let _ = std::fs::write(&path, serialized);
    }
}

const BOOTSTRAP_REGISTER_SH: &str = r#"#!/usr/bin/env bash
set -u

if [ -z "${NEXUS_NAME:-}" ] || [ -z "${NEXUS_CLIENT_KEY:-}" ]; then
  echo "Nexus bootstrap skipped: NEXUS_NAME or NEXUS_CLIENT_KEY is missing" >&2
  exit 0
fi

nexus register \
  --name "$NEXUS_NAME" \
  --agent "${NEXUS_AGENT:-other}" \
  --project "${NEXUS_PROJECT:-default}" \
  --client-key "$NEXUS_CLIENT_KEY" >/dev/null
"#;
