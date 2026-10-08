//! OpenCode-specific `nexus-bus` skill + bootstrap-register script install.
//!
//! Unlike claude/codex (which previously got NOTHING from the shared `bootstrap.rs` — the `_ => {}`
//! arm — and so launched without the bus skill, part of why opencode failed), this installs the
//! `nexus-bus` skill into opencode's own skills location plus the shared bootstrap-register script:
//!
//! - `<cwd>/.opencode/skills/nexus-bus/SKILL.md` — the bus how-to (opencode discovers skills here).
//! - `<cwd>/.nexus/bootstrap-register.sh` — the idempotent `nexus register` fallback script.
//!
//! opencode has no claude/codex-style *command* SessionStart hook (its extension surface is JS
//! plugins), and in the headless ACP path the daemon mints + connects the session itself, so the
//! script is the manual/operator fallback rather than an auto-fired hook. The **load-bearing** MCP
//! wiring (the `nexus-bus` MCP server) is supplied separately by [`super::harness`] — headless ACP
//! and headed launches use per-process `OPENCODE_CONFIG_CONTENT`, never a shared project config
//! write. opencode REJECTS a stdio MCP server over ACP `session/new`, so the bus must
//! come from opencode's own config channel instead.
//!
//! All operations honor the same `NEXUS_SKIP_AGENT_*` env flags the shared bootstrap used, in the
//! same order. `SKILL_MD` is sourced from `skill.md` alongside this file so the markdown stays in
//! one place and is never duplicated in a Rust string literal.

use std::path::Path;

/// The `nexus-bus` `SKILL.md` content (frontmatter + body). Loaded at compile time from the
/// adjacent `skill.md`.
pub const SKILL_MD: &str = include_str!("skill.md");

/// Install all launch-local bootstrap files for an OpenCode agent launched in `cwd`.
///
/// Respects the same env-flag escape hatches as the other adapters:
/// - `NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL=1` → skip everything
/// - `NEXUS_SKIP_AGENT_SKILL_INSTALL=1`     → skip the skill but still write the script
/// - `NEXUS_SKIP_AGENT_HOOK_INSTALL=1`      → skip the script but still write the skill
pub fn install(cwd: &str) {
    if env_flag("NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL") {
        return;
    }

    if !env_flag("NEXUS_SKIP_AGENT_SKILL_INSTALL") {
        install_skill(&format!("{cwd}/.opencode/skills"));
    }

    if env_flag("NEXUS_SKIP_AGENT_HOOK_INSTALL") {
        return;
    }

    write_script(cwd);
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
