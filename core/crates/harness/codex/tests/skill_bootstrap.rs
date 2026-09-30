use nexus_agent::adapter::bootstrap::SKIP_ENV_LOCK;
use nexus_harness_codex::skill::{install, SKILL_MD};

const SKIP_ENV: &[&str] = &[
    "NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL",
    "NEXUS_SKIP_AGENT_HOOK_INSTALL",
    "NEXUS_SKIP_AGENT_SKILL_INSTALL",
];

struct EnvGuard {
    saved: Vec<(&'static str, Option<String>)>,
}

impl EnvGuard {
    fn clear() -> Self {
        let saved = SKIP_ENV
            .iter()
            .map(|&name| {
                let value = std::env::var(name).ok();
                std::env::remove_var(name);
                (name, value)
            })
            .collect();
        Self { saved }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (name, value) in self.saved.drain(..) {
            match value {
                Some(v) => std::env::set_var(name, v),
                None => std::env::remove_var(name),
            }
        }
    }
}

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("nexus-codex-skill-{label}-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn skill_md_contains_required_content() {
    assert!(SKILL_MD.contains("SessionStart"));
    assert!(SKILL_MD.contains("./.nexus/bus register"));
    assert!(SKILL_MD.contains("NEXUS_NAME"));
    assert!(SKILL_MD.contains("NEXUS_CLIENT_KEY"));
    assert!(SKILL_MD.contains("idempotent"));
}

#[test]
fn skill_md_guides_codex_deferred_mcp_and_stdin_posting() {
    assert!(SKILL_MD.contains("tool discovery/search"));
    assert!(SKILL_MD.contains("mcp__nexus_bus"));
    assert!(SKILL_MD.contains("nexus-bus reply dm post members threads"));
    assert!(SKILL_MD.contains("./.nexus/bus reply --stdin"));
    assert!(SKILL_MD.contains("./.nexus/bus post <thread> --stdin"));
    assert!(!SKILL_MD.contains("\"$NEXUS_CLI\" post <thread> --stdin"));
    assert!(SKILL_MD.contains("shell-sensitive"));
}

fn assert_bus_launcher(dir: &std::path::Path) {
    let launcher = dir.join(".nexus/bus");
    assert!(
        launcher.exists(),
        "launch-local bus command should be written"
    );
    let body = std::fs::read_to_string(&launcher).unwrap();
    assert!(body.contains("exec \"${NEXUS_CLI:-nexus}\" \"$@\""));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_ne!(
            std::fs::metadata(&launcher).unwrap().permissions().mode() & 0o111,
            0,
            "launch-local bus command should be executable"
        );
    }
}

#[test]
fn codex_install_writes_session_start_hook_script_and_skill() {
    let _lock = SKIP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::clear();
    let dir = temp_dir("codex");

    install(dir.to_str().unwrap());

    let hooks = std::fs::read_to_string(dir.join(".codex/hooks.json")).unwrap();
    assert!(hooks.contains("\"SessionStart\""));
    assert!(hooks.contains("startup|resume"));
    assert!(hooks.contains("./.nexus/bootstrap-register.sh"));

    let script = dir.join(".nexus/bootstrap-register.sh");
    assert!(script.exists(), "bootstrap script should be written");
    let script_body = std::fs::read_to_string(&script).unwrap();
    assert!(script_body.contains("NEXUS_CLI"));
    assert!(script_body.contains("\"$NEXUS_CLI\" register"));
    assert!(script_body.contains("NEXUS_CLIENT_KEY"));
    assert!(dir.join(".codex/skills/nexus-bus/SKILL.md").exists());
    assert_bus_launcher(&dir);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn skill_only_install_avoids_redundant_session_start_hook() {
    let _lock = SKIP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::clear();
    let dir = temp_dir("skill-only");

    nexus_harness_codex::skill::install_skill_only(dir.to_str().unwrap());

    assert!(dir.join(".codex/skills/nexus-bus/SKILL.md").exists());
    assert_bus_launcher(&dir);
    assert!(!dir.join(".nexus/bootstrap-register.sh").exists());
    assert!(!dir.join(".codex/hooks.json").exists());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn install_can_be_fully_skipped() {
    let _lock = SKIP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::clear();
    std::env::set_var("NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL", "1");
    let dir = temp_dir("skip-all");

    install(dir.to_str().unwrap());

    assert!(!dir.join(".nexus/bootstrap-register.sh").exists());
    assert!(!dir.join(".nexus/bus").exists());
    assert!(!dir.join(".codex/hooks.json").exists());
    assert!(!dir.join(".codex/skills/nexus-bus/SKILL.md").exists());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hook_install_can_be_skipped_without_skipping_skill() {
    let _lock = SKIP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::clear();
    std::env::set_var("NEXUS_SKIP_AGENT_HOOK_INSTALL", "1");
    let dir = temp_dir("skip-hook");

    install(dir.to_str().unwrap());

    assert!(!dir.join(".nexus/bootstrap-register.sh").exists());
    assert!(!dir.join(".codex/hooks.json").exists());
    assert!(dir.join(".codex/skills/nexus-bus/SKILL.md").exists());
    assert_bus_launcher(&dir);
    let _ = std::fs::remove_dir_all(&dir);
}
