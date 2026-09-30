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
        EnvGuard { saved }
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
    let dir = std::env::temp_dir().join(format!("nexus-claude-skill-{label}-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn skill_md_contains_required_content() {
    assert!(nexus_harness_claude::skill::SKILL_MD.contains("SessionStart"));
    assert!(nexus_harness_claude::skill::SKILL_MD.contains("nexus register"));
    assert!(nexus_harness_claude::skill::SKILL_MD.contains("NEXUS_NAME"));
    assert!(nexus_harness_claude::skill::SKILL_MD.contains("NEXUS_CLIENT_KEY"));
    assert!(nexus_harness_claude::skill::SKILL_MD.contains("idempotent"));
    assert!(nexus_harness_claude::skill::SKILL_MD
        .contains("The `Skill` tool only loads these instructions"));
    assert!(nexus_harness_claude::skill::SKILL_MD
        .contains("do not pass `post`, `dm`, or `reply` as `Skill` tool arguments"));
    assert!(nexus_harness_claude::skill::SKILL_MD
        .contains("`Launching skill: nexus-bus` is not a delivery receipt"));
    assert!(nexus_harness_claude::skill::SKILL_MD.contains("message ID beginning with `m_`"));
}

#[test]
fn claude_install_writes_session_start_hook_script_and_skill() {
    let _lock = nexus_agent::adapter::bootstrap::SKIP_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::clear();
    let dir = temp_dir("claude");

    nexus_harness_claude::skill::install(dir.to_str().unwrap());

    let settings = std::fs::read_to_string(dir.join(".claude/settings.json")).unwrap();
    assert!(settings.contains("\"SessionStart\""));
    assert!(settings.contains("startup|resume"));
    assert!(settings.contains("./.nexus/bootstrap-register.sh"));

    let script = dir.join(".nexus/bootstrap-register.sh");
    assert!(script.exists(), "bootstrap script should be written");
    let script_body = std::fs::read_to_string(&script).unwrap();
    assert!(script_body.contains("nexus register"));
    assert!(script_body.contains("NEXUS_CLIENT_KEY"));

    assert!(dir.join(".claude/skills/nexus-bus/SKILL.md").exists());
}

#[test]
fn install_can_be_fully_skipped() {
    let _lock = nexus_agent::adapter::bootstrap::SKIP_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::clear();
    std::env::set_var("NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL", "1");
    let dir = temp_dir("skip-all");

    nexus_harness_claude::skill::install(dir.to_str().unwrap());

    assert!(!dir.join(".nexus/bootstrap-register.sh").exists());
    assert!(!dir.join(".claude/settings.json").exists());
    assert!(!dir.join(".claude/skills/nexus-bus/SKILL.md").exists());
}

#[test]
fn hook_install_can_be_skipped_without_skipping_skill() {
    let _lock = nexus_agent::adapter::bootstrap::SKIP_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::clear();
    std::env::set_var("NEXUS_SKIP_AGENT_HOOK_INSTALL", "1");
    let dir = temp_dir("skip-hook");

    nexus_harness_claude::skill::install(dir.to_str().unwrap());

    assert!(!dir.join(".nexus/bootstrap-register.sh").exists());
    assert!(!dir.join(".claude/settings.json").exists());
    assert!(dir.join(".claude/skills/nexus-bus/SKILL.md").exists());
}
