use std::path::{Path, PathBuf};

use nexus_agent::adapter::bootstrap::SKIP_ENV_LOCK;
use nexus_agent::adapter::hermes::{harness::hermes_command, skill as hermes_skill};
use nexus_agent::adapter::opencode::{harness as opencode_harness, skill as opencode_skill};
use nexus_agent::{write_hermes_mcp_config, write_opencode_mcp_config, LaunchCtx};

const SKIP_ENV: &[&str] = &[
    "NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL",
    "NEXUS_SKIP_AGENT_HOOK_INSTALL",
    "NEXUS_SKIP_AGENT_SKILL_INSTALL",
    "NEXUS_HERMES_BUS_MCP",
];

struct EnvGuard {
    saved: Vec<(&'static str, Option<String>)>,
    saved_home: Option<String>,
}

impl EnvGuard {
    fn isolated(hermes_home: Option<&Path>, bus_mcp: bool) -> Self {
        let saved = SKIP_ENV
            .iter()
            .map(|&name| {
                let value = std::env::var(name).ok();
                std::env::remove_var(name);
                (name, value)
            })
            .collect();
        let saved_home = std::env::var("HERMES_HOME").ok();
        if let Some(home) = hermes_home {
            std::env::set_var("HERMES_HOME", home);
        } else {
            std::env::remove_var("HERMES_HOME");
        }
        if bus_mcp {
            std::env::set_var("NEXUS_HERMES_BUS_MCP", "1");
        }
        Self { saved, saved_home }
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
        match self.saved_home.take() {
            Some(v) => std::env::set_var("HERMES_HOME", v),
            None => std::env::remove_var("HERMES_HOME"),
        }
    }
}

fn temp_dir(label: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("nexus-agent-bootstrap-{label}-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn bus_ctx(cwd: Option<String>) -> LaunchCtx {
    LaunchCtx {
        cwd,
        bus_name: Some("qa".into()),
        bus_project: Some("qa".into()),
        ..Default::default()
    }
}

#[test]
fn opencode_command_defaults_to_opencode_acp() {
    std::env::remove_var("NEXUS_OPENCODE_ACP_CMD");
    std::env::remove_var("NEXUS_OPENCODE_ACP_ARGS");

    let cmd = opencode_harness::opencode_command(None);

    assert_eq!(
        cmd.program,
        nexus_harness_core::native_harness_program(
            nexus_contracts::Harness::OpenCode,
            nexus_harness_core::NativeProcessPlatform::current(),
        )
        .unwrap()
    );
    assert_eq!(cmd.args, vec!["acp".to_string()]);
}

#[test]
fn opencode_writes_project_config_with_nexus_bus_mcp() {
    let _lock = SKIP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::isolated(None, false);
    let dir = temp_dir("opencode-cfg");
    let cwd = dir.to_string_lossy().into_owned();

    write_opencode_mcp_config(&cwd, &bus_ctx(Some(cwd.clone())));

    let body = std::fs::read_to_string(dir.join("opencode.json")).unwrap();
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let server = &v["mcp"]["nexus-bus"];
    assert_eq!(server["type"], "local");
    assert_eq!(server["enabled"], true);
    let argv = server["command"].as_array().unwrap();
    let joined: Vec<String> = argv
        .iter()
        .map(|a| a.as_str().unwrap().to_string())
        .collect();
    assert!(joined.iter().any(|a| a == "mcp"));
    assert!(joined.iter().any(|a| a == "--as"));
    assert!(joined.iter().any(|a| a == "qa"));
    assert!(!joined.iter().any(|a| a == "--socket"));
    assert_eq!(v["default_agent"], "nexus");
    assert_eq!(v["agent"]["nexus"]["mode"], "primary");
    let prompt = v["agent"]["nexus"]["prompt"].as_str().unwrap();
    assert!(prompt.contains("You are \"qa\" on the Nexus bus"));
    assert!(prompt.contains("nexus-bus MCP"));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn opencode_writes_no_project_config_without_bus_identity() {
    let dir = temp_dir("opencode-nocfg");
    let cwd = dir.to_string_lossy().into_owned();

    write_opencode_mcp_config(
        &cwd,
        &LaunchCtx {
            cwd: Some(cwd.clone()),
            ..Default::default()
        },
    );

    assert!(!dir.join("opencode.json").exists());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn opencode_skill_md_contains_required_content() {
    assert!(opencode_skill::SKILL_MD.contains("SessionStart"));
    assert!(opencode_skill::SKILL_MD.contains("nexus register"));
    assert!(opencode_skill::SKILL_MD.contains("NEXUS_NAME"));
    assert!(opencode_skill::SKILL_MD.contains("NEXUS_CLIENT_KEY"));
    assert!(opencode_skill::SKILL_MD.contains("nexus-bus"));
}

#[test]
fn opencode_install_writes_skill_and_script() {
    let _lock = SKIP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::isolated(None, false);
    let dir = temp_dir("opencode-install");

    opencode_skill::install(dir.to_str().unwrap());

    let script = dir.join(".nexus/bootstrap-register.sh");
    assert!(script.exists(), "bootstrap script should be written");
    let script_body = std::fs::read_to_string(&script).unwrap();
    assert!(script_body.contains("nexus register"));
    assert!(script_body.contains("NEXUS_CLIENT_KEY"));
    assert!(dir.join(".opencode/skills/nexus-bus/SKILL.md").exists());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn opencode_install_can_be_fully_skipped() {
    let _lock = SKIP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::isolated(None, false);
    std::env::set_var("NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL", "1");
    let dir = temp_dir("opencode-skip-all");

    opencode_skill::install(dir.to_str().unwrap());

    assert!(!dir.join(".nexus/bootstrap-register.sh").exists());
    assert!(!dir.join(".opencode/skills/nexus-bus/SKILL.md").exists());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn opencode_hook_install_can_be_skipped_without_skipping_skill() {
    let _lock = SKIP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::isolated(None, false);
    std::env::set_var("NEXUS_SKIP_AGENT_HOOK_INSTALL", "1");
    let dir = temp_dir("opencode-skip-hook");

    opencode_skill::install(dir.to_str().unwrap());

    assert!(!dir.join(".nexus/bootstrap-register.sh").exists());
    assert!(dir.join(".opencode/skills/nexus-bus/SKILL.md").exists());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hermes_command_defaults_to_hermes_acp_accept_hooks() {
    std::env::remove_var("NEXUS_HERMES_ACP_CMD");
    std::env::remove_var("NEXUS_HERMES_ACP_ARGS");

    let cmd = hermes_command(None);

    assert_eq!(
        cmd.program,
        nexus_harness_core::native_harness_program(
            nexus_contracts::Harness::Hermes,
            nexus_harness_core::NativeProcessPlatform::current(),
        )
        .unwrap()
    );
    assert_eq!(
        cmd.args,
        vec!["acp".to_string(), "--accept-hooks".to_string()]
    );
}

#[test]
fn hermes_skill_md_contains_required_content() {
    assert!(hermes_skill::SKILL_MD.contains("SessionStart"));
    assert!(hermes_skill::SKILL_MD.contains("nexus register"));
    assert!(hermes_skill::SKILL_MD.contains("NEXUS_NAME"));
    assert!(hermes_skill::SKILL_MD.contains("NEXUS_CLIENT_KEY"));
    assert!(hermes_skill::SKILL_MD.contains("nexus-bus"));
}

#[test]
fn hermes_install_writes_skill_script_and_mcp_config() {
    let _lock = SKIP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let home = temp_dir("hermes-home");
    let cwd = temp_dir("hermes-cwd");
    let _env = EnvGuard::isolated(Some(&home), true);

    hermes_skill::install(
        cwd.to_str().unwrap(),
        &bus_ctx(Some(cwd.to_string_lossy().into_owned())),
    );

    let skill = home.join("skills/nexus-bus/SKILL.md");
    assert!(skill.exists());
    let skill_body = std::fs::read_to_string(&skill).unwrap();
    let nexus_exe = std::env::current_exe().unwrap();
    assert!(
        skill_body.contains(nexus_exe.to_string_lossy().as_ref()),
        "Hermes terminal tools sanitize PATH, so the installed skill must carry the absolute Nexus binary: {skill_body}"
    );
    assert!(
        skill_body.contains("\"$NEXUS_CLI\" dm"),
        "outbound examples must invoke the pinned binary: {skill_body}"
    );
    let script = cwd.join(".nexus/bootstrap-register.sh");
    assert!(script.exists(), "bootstrap script should be written");
    let script_body = std::fs::read_to_string(&script).unwrap();
    assert!(script_body.contains("nexus register"));
    assert!(script_body.contains("NEXUS_CLIENT_KEY"));

    let body = std::fs::read_to_string(home.join("config.yaml")).unwrap();
    let v: serde_yaml::Value = serde_yaml::from_str(&body).unwrap();
    let server = &v["mcp_servers"]["nexus-bus"];
    assert_eq!(server["enabled"], serde_yaml::Value::Bool(true));
    let argv: Vec<String> = server["args"]
        .as_sequence()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap().to_string())
        .collect();
    assert!(argv.iter().any(|a| a == "mcp"));
    assert!(argv.iter().any(|a| a == "--as"));
    assert!(argv.iter().any(|a| a == "qa"));
    assert!(!argv.iter().any(|a| a == "--socket"));

    let _ = std::fs::remove_dir_all(&home);
    let _ = std::fs::remove_dir_all(&cwd);
}

#[test]
fn hermes_mcp_merge_preserves_existing_config_keys() {
    let _lock = SKIP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let home = temp_dir("hermes-merge-home");
    let cwd = temp_dir("hermes-merge-cwd");
    let _env = EnvGuard::isolated(Some(&home), true);
    std::fs::write(
        home.join("config.yaml"),
        "model:\n  default: gpt-5.5\nmcp_servers:\n  other:\n    command: foo\n    enabled: true\n",
    )
    .unwrap();

    write_hermes_mcp_config(&bus_ctx(Some(cwd.to_string_lossy().into_owned())));

    let body = std::fs::read_to_string(home.join("config.yaml")).unwrap();
    let v: serde_yaml::Value = serde_yaml::from_str(&body).unwrap();
    assert_eq!(v["model"]["default"].as_str(), Some("gpt-5.5"));
    assert_eq!(v["mcp_servers"]["other"]["command"].as_str(), Some("foo"));
    assert_eq!(
        v["mcp_servers"]["nexus-bus"]["enabled"],
        serde_yaml::Value::Bool(true)
    );

    let _ = std::fs::remove_dir_all(&home);
    let _ = std::fs::remove_dir_all(&cwd);
}

#[test]
fn hermes_no_mcp_config_without_bus_identity() {
    let _lock = SKIP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let home = temp_dir("hermes-nobus-home");
    let _env = EnvGuard::isolated(Some(&home), true);

    write_hermes_mcp_config(&LaunchCtx::default());

    assert!(!home.join("config.yaml").exists());
    let _ = std::fs::remove_dir_all(&home);
}

#[test]
fn hermes_no_mcp_config_without_opt_in_flag() {
    let _lock = SKIP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let home = temp_dir("hermes-noflag-home");
    let cwd = temp_dir("hermes-noflag-cwd");
    let _env = EnvGuard::isolated(Some(&home), false);

    write_hermes_mcp_config(&bus_ctx(Some(cwd.to_string_lossy().into_owned())));

    assert!(!home.join("config.yaml").exists());
    let _ = std::fs::remove_dir_all(&home);
    let _ = std::fs::remove_dir_all(&cwd);
}

#[test]
fn hermes_install_can_be_fully_skipped() {
    let _lock = SKIP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let home = temp_dir("hermes-skip-home");
    let cwd = temp_dir("hermes-skip-all");
    let _env = EnvGuard::isolated(Some(&home), true);
    std::env::set_var("NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL", "1");

    hermes_skill::install(
        cwd.to_str().unwrap(),
        &bus_ctx(Some(cwd.to_string_lossy().into_owned())),
    );

    assert!(!cwd.join(".nexus/bootstrap-register.sh").exists());
    assert!(!home.join("skills/nexus-bus/SKILL.md").exists());
    assert!(!home.join("config.yaml").exists());
    let _ = std::fs::remove_dir_all(&home);
    let _ = std::fs::remove_dir_all(&cwd);
}

#[test]
fn hermes_hook_install_can_be_skipped_without_skipping_skill() {
    let _lock = SKIP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let home = temp_dir("hermes-skip-hook-home");
    let cwd = temp_dir("hermes-skip-hook");
    let _env = EnvGuard::isolated(Some(&home), true);
    std::env::set_var("NEXUS_SKIP_AGENT_HOOK_INSTALL", "1");

    hermes_skill::install(
        cwd.to_str().unwrap(),
        &bus_ctx(Some(cwd.to_string_lossy().into_owned())),
    );

    assert!(!cwd.join(".nexus/bootstrap-register.sh").exists());
    assert!(!home.join("config.yaml").exists());
    assert!(home.join("skills/nexus-bus/SKILL.md").exists());
    let _ = std::fs::remove_dir_all(&home);
    let _ = std::fs::remove_dir_all(&cwd);
}
