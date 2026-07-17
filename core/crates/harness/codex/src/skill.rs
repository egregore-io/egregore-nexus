//! Codex-specific `nexus-bus` skill + SessionStart hook bootstrap.
//!
//! `install(cwd)` is the exact equivalent of the old
//! `adapter::bootstrap::install(cwd, Harness::Codex)`: it writes the `nexus-bus` skill into
//! `<cwd>/.codex/skills/nexus-bus/SKILL.md`, writes the bootstrap shell script into
//! `<cwd>/.nexus/bootstrap-register.sh`, and writes the SessionStart hook into
//! `<cwd>/.codex/hooks.json`. All three operations are guarded by the same
//! `NEXUS_SKIP_AGENT_*` env flags the shared bootstrap used, in the same order.
//!
//! `SKILL_MD` is sourced from `skill.md` alongside this file so the markdown stays in one place
//! and is never duplicated in a Rust string literal.

use std::path::Path;

/// The `nexus-bus` `SKILL.md` content (frontmatter + body). Loaded at compile time from the
/// adjacent `skill.md`.
pub const SKILL_MD: &str = include_str!("skill.md");

/// Install all launch-local bootstrap files for a Codex agent launched in `cwd`.
///
/// Equivalent to the old `adapter::bootstrap::install(cwd, Harness::Codex)`.
/// Respects the same env-flag escape hatches:
/// - `NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL=1` → skip everything
/// - `NEXUS_SKIP_AGENT_SKILL_INSTALL=1`     → skip the skill but still write hook + script
/// - `NEXUS_SKIP_AGENT_HOOK_INSTALL=1`      → skip hook + script but still write skill
pub fn install(cwd: &str) {
    if env_flag("NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL") {
        return;
    }

    if !env_flag("NEXUS_SKIP_AGENT_SKILL_INSTALL") {
        install_skill(&format!("{cwd}/.codex/skills"));
        write_bus_launcher(cwd);
    }

    if env_flag("NEXUS_SKIP_AGENT_HOOK_INSTALL") {
        return;
    }

    write_script(cwd);
    write_codex_hook(cwd);
}

/// Install only the model-facing bus skill for a daemon-owned Codex app-server runtime.
///
/// The daemon has already registered these runtimes before attaching a TUI. Adding a project
/// SessionStart hook would duplicate registration and force a fresh TUI through Codex's hook-review
/// screen, so the native app-server path deliberately uses this narrower installer.
pub fn install_skill_only(cwd: &str) {
    if env_flag("NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL") || env_flag("NEXUS_SKIP_AGENT_SKILL_INSTALL")
    {
        return;
    }
    install_skill(&format!("{cwd}/.codex/skills"));
    write_bus_launcher(cwd);
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn env_flag(name: &str) -> bool {
    std::env::var(name).ok().as_deref() == Some("1")
}

/// Install the `nexus-bus` skill under `<skills_dir>/nexus-bus/SKILL.md`. Idempotent, best-effort.
fn install_skill(skills_dir: &str) {
    let dir = format!("{skills_dir}/nexus-bus");
    let _ = std::fs::create_dir_all(&dir);
    let _ = std::fs::write(format!("{dir}/SKILL.md"), SKILL_MD);
}

/// Install a model-facing command with no shell-variable quoting at the call site. The wrapper
/// still resolves the daemon-pinned CLI from `NEXUS_CLI`; it merely keeps that quoting inside
/// trusted generated code instead of asking a model to reproduce it in a nested tool-call string.
fn write_bus_launcher(cwd: &str) {
    let dir = Path::new(cwd).join(".nexus");
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("bus");
    let _ = std::fs::write(&path, BUS_LAUNCHER_SH);
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

fn write_codex_hook(cwd: &str) {
    let dir = Path::new(cwd).join(".codex");
    let _ = std::fs::create_dir_all(&dir);
    let body = r#"{
  "hooks": {
    "SessionStart": [
      {
        "matcher": "startup|resume",
        "hooks": [
          {
            "type": "command",
            "command": "./.nexus/bootstrap-register.sh",
            "statusMessage": "Registering Nexus session"
          }
        ]
      }
    ]
  }
}
"#;
    let _ = std::fs::write(dir.join("hooks.json"), body);
}

const BOOTSTRAP_REGISTER_SH: &str = r#"#!/usr/bin/env bash
set -u

if [ -z "${NEXUS_NAME:-}" ] || [ -z "${NEXUS_CLIENT_KEY:-}" ]; then
  echo "Nexus bootstrap skipped: NEXUS_NAME or NEXUS_CLIENT_KEY is missing" >&2
  exit 0
fi

NEXUS_CLI=${NEXUS_CLI:-nexus}
"$NEXUS_CLI" register \
  --name "$NEXUS_NAME" \
  --agent "${NEXUS_AGENT:-other}" \
  --project "${NEXUS_PROJECT:-default}" \
  --client-key "$NEXUS_CLIENT_KEY" >/dev/null
"#;

const BUS_LAUNCHER_SH: &str = r#"#!/usr/bin/env bash
set -euo pipefail

exec "${NEXUS_CLI:-nexus}" "$@"
"#;
