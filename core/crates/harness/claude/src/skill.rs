//! Claude-specific `nexus-bus` skill + SessionStart hook bootstrap.
//!
//! `install(cwd)` writes the `nexus-bus` skill into
//! `<cwd>/.claude/skills/nexus-bus/SKILL.md`, writes the bootstrap shell script into
//! `<cwd>/.nexus/bootstrap-register.sh`. `launch_settings()` supplies the SessionStart hook
//! as a launch-only overlay, never writing `<cwd>/.claude/settings.json`. Bootstrap operations use the same
//! `NEXUS_SKIP_AGENT_*` env flags the shared bootstrap uses, in the same order.
//!
//! `SKILL_MD` is sourced from `skill.md` alongside this file so the markdown stays in one place
//! and is never duplicated in a Rust string literal.

use std::path::Path;

/// The `nexus-bus` `SKILL.md` content (frontmatter + body). Loaded at compile time from the
/// adjacent `skill.md`.
pub const SKILL_MD: &str = include_str!("skill.md");

/// Install all launch-local bootstrap files for a Claude Code agent launched in `cwd`.
///
/// Respects the same env-flag escape hatches:
/// - `NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL=1` → skip everything
/// - `NEXUS_SKIP_AGENT_SKILL_INSTALL=1`     → skip the skill but still write hook + script
/// - `NEXUS_SKIP_AGENT_HOOK_INSTALL=1`      → skip hook + script but still write skill
pub fn install(cwd: &str) {
    if env_flag("NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL") {
        return;
    }

    install_bus_skill(cwd);

    if env_flag("NEXUS_SKIP_AGENT_HOOK_INSTALL") {
        return;
    }

    write_script(cwd);
}

/// Install only the model-facing bus contract for a daemon-owned headed Claude runtime.
///
/// Headed registration is already owned by the daemon's launch-local native bridge, so this path
/// deliberately does not add the project SessionStart registration hook a second time.
#[doc(hidden)]
pub fn install_bus_skill(cwd: &str) {
    if env_flag("NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL") || env_flag("NEXUS_SKIP_AGENT_SKILL_INSTALL")
    {
        return;
    }
    install_skill(&format!("{cwd}/.claude/skills"));
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

/// Additional per-launch settings. Provider-managed user/project settings remain in their
/// original layers, so existing hooks run once and no project settings file is rewritten.
pub fn launch_settings() -> Option<serde_json::Value> {
    if env_flag("NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL") || env_flag("NEXUS_SKIP_AGENT_HOOK_INSTALL") {
        return None;
    }
    Some(serde_json::json!({
    "hooks": {
      "SessionStart": [
        {
          "matcher": "startup|resume",
          "hooks": [
            {
              "type": "command",
              "command": "./.nexus/bootstrap-register.sh"
            }
          ]
        }
      ]
    }
      }))
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
