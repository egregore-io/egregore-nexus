//! The OpenCode harness adapter (backend spec §5): injection over **ACP** — the one uniform
//! transport every harness uses. OpenCode speaks ACP natively via its `opencode acp` subcommand
//! (stdio JSON-RPC), so — unlike claude/codex which shell out to an npm ACP bridge — the spawn
//! command is just the `opencode` binary itself. All real-protocol work lives in [`AcpEngine`].
//!
//! **Why opencode previously never came online, and the fix.** The shared ACP `session/new` path
//! injects a *stdio* `nexus-bus` MCP server into the request (see
//! [`super::super::engine::build_new_session_request`]). opencode REJECTS that: its `initialize`
//! advertises only `mcpCapabilities { http, sse }` (no stdio), and a stdio entry makes `session/new`
//! fail with `-32602 Invalid params` — so the session never opens and the agent never reports
//! online. The fix is two-sided:
//!   1. The adapter sets [`LaunchCtx::suppress_acp_mcp`] so the engine does NOT put the stdio MCP
//!      server into opencode's `session/new`.
//!   2. The bus is instead wired the **opencode way** — an OpenCode config whose `mcp.nexus-bus`
//!      server is the same `nexus mcp --as <name> --project <project> [--client-key <key>]
//!      [--agent <agent>]` stdio command. Headless ACP launches pass that config through
//!      `OPENCODE_CONFIG_CONTENT` so multiple daemon-owned agents can share a cwd without racing on
//!      one `opencode.json`; headed compatibility callers may still write the project-local file via
//!      [`write_opencode_mcp_config`]. opencode loads the `mcp` section over its own MCP channel,
//!      NOT over ACP. The same config also sets a launch-local primary agent prompt that names the
//!      Nexus bus identity; otherwise a fresh OpenCode ACP session may answer as generic "opencode"
//!      instead of the daemon-owned agent name. Headless ACP launches also force `OPENCODE_DB` under
//!      a per-identity directory inside the launch cwd so `session/new` cannot accidentally reuse
//!      the operator's global OpenCode DB or a stale same-cwd session from another daemon-owned
//!      agent.
//! The HEADED (TUI) launch builder (`nexus-pty::command::harness_command`) likewise gives opencode a
//! BARE `opencode` (no claude `--mcp-config`, which opencode would usage-error on); its MCP comes
//! from the same project config file.
//!
//! The **real-binary spawn** ([`OpenCodeAdapter::new`] / the [`crate::AdapterRegistry`] built-in)
//! is gated behind the `live` cargo feature: without `live`, `open_session` returns
//! [`NexusError::Adapter`] rather than launching a process. The always-enabled
//! [`OpenCodeAdapter::with_command`] constructor lets the hermetic tests drive the full
//! `open → inject → stream` path against the fake ACP harness without `live`.

use std::path::{Path, PathBuf};

use async_trait::async_trait;

use nexus_common::NexusError;
use nexus_contracts::{Harness, SteerCapability};
#[cfg(windows)]
use nexus_harness_core::native_executable::resolve_opencode_executable;
use nexus_harness_core::{native_harness_program, NativeProcessPlatform};
use serde_json::Value;

use super::super::engine::{AcpEngine, HarnessCommand, LaunchCtx};
use super::super::provider_limit::classify_structured_provider_payload;
use super::super::{Adapter, AdapterInjectError, StreamEvent};

/// OpenCode ACP adapter. Owns the resolved spawn command and the live ACP engine.
pub struct OpenCodeAdapter {
    command: HarnessCommand,
    engine: AcpEngine,
    ctx: LaunchCtx,
}

impl OpenCodeAdapter {
    /// Construct an OpenCode adapter that will spawn the **real** `opencode acp` process for the
    /// given working directory.
    pub fn new(mut ctx: LaunchCtx) -> Self {
        // Install launch-local bootstrap into opencode's project cwd: the `nexus-bus` skill (under
        // `.opencode/skills`) and the bootstrap-register script. Headless ACP config is scoped to
        // this child process via OPENCODE_CONFIG_CONTENT below, so agents sharing a cwd cannot
        // overwrite each other's identity prompt in one project-local opencode.json. Quarantine a
        // malformed project config first: OpenCode still parses it before applying env config.
        if let Some(cwd) = &ctx.cwd {
            super::skill::install(cwd);
            quarantine_invalid_project_opencode_config(cwd);
        }
        // opencode rejects a stdio MCP server in ACP `session/new` (`-32602`), so suppress the
        // engine's stdio injection: the bus is wired via opencode's own config channel.
        ctx.suppress_acp_mcp = true;
        let mut command = opencode_command(ctx.cwd.clone());
        command.env = ctx.env.clone();
        command
            .env
            .retain(|(key, _)| key != "OPENCODE_CONFIG_CONTENT");
        if let Some(config_content) = opencode_mcp_config_content(&ctx) {
            command
                .env
                .push(("OPENCODE_CONFIG_CONTENT".to_string(), config_content));
        }
        if let Some(cwd) = &ctx.cwd {
            if let Some(db_path) = launch_local_opencode_db(cwd, &ctx) {
                command.env.retain(|(key, _)| key != "OPENCODE_DB");
                command.env.push(("OPENCODE_DB".to_string(), db_path));
            }
        }
        Self {
            command,
            engine: AcpEngine::for_harness(Harness::OpenCode),
            ctx,
        }
    }

    /// Construct an OpenCode adapter over an explicit [`HarnessCommand`] (the hermetic tests point
    /// this at the fake ACP harness). The protocol path is identical to [`OpenCodeAdapter::new`].
    pub fn with_command(command: HarnessCommand) -> Self {
        let ctx = LaunchCtx {
            cwd: command.cwd.clone(),
            suppress_acp_mcp: true,
            ..Default::default()
        };
        Self {
            command,
            engine: AcpEngine::for_harness(Harness::OpenCode),
            ctx,
        }
    }

    /// The resolved spawn command this adapter will run (program + args + cwd).
    pub fn command(&self) -> &HarnessCommand {
        &self.command
    }
}

/// Resolve the exact subprocess to put **OpenCode** into ACP mode.
///
/// Default: `opencode acp` (ACP is built into the binary; no npm bridge). Overrides:
/// - `NEXUS_OPENCODE_ACP_CMD` — full program path (e.g. an absolute `opencode`); when set,
///   `NEXUS_OPENCODE_ACP_ARGS` (whitespace-split) supplies its args (default `acp`).
pub fn opencode_command(cwd: Option<String>) -> HarnessCommand {
    if let Ok(program) = std::env::var("NEXUS_OPENCODE_ACP_CMD") {
        let args = std::env::var("NEXUS_OPENCODE_ACP_ARGS")
            .ok()
            .map(|s| s.split_whitespace().map(str::to_string).collect())
            .unwrap_or_else(|| vec!["acp".to_string()]);
        return HarnessCommand {
            program,
            args,
            cwd,
            ..Default::default()
        };
    }
    HarnessCommand {
        program: default_native_program(),
        args: vec!["acp".into()],
        cwd,
        ..Default::default()
    }
}

fn default_native_program() -> String {
    #[cfg(windows)]
    if let Ok(program) = resolve_opencode_executable(None, std::env::var("PATH").ok().as_deref()) {
        return program;
    }

    native_harness_program(Harness::OpenCode, NativeProcessPlatform::current())
        .expect("supported harness has a native executable")
        .into()
}

/// Write a project-local `opencode.json` into `cwd` wiring the `nexus-bus` MCP server the **opencode
/// way** (a `mcp.<name>` local-command entry opencode reads from the working directory) and setting
/// a launch-local primary agent prompt that names the Nexus identity. This is how opencode gets both
/// the bus tools and the durable "you are `<name>`" context — opencode rejects a stdio MCP server
/// over ACP `session/new` and has no `--append-system-prompt` equivalent.
///
/// The server command mirrors the ACP/headed wiring exactly:
/// `nexus mcp --as <name> --project <project> [--client-key <key>] [--agent <agent>]`.
///
/// No-op (and no file written) unless the bus identity (`bus_name`/`bus_project`) is present, so
/// test/admin paths without bus context don't drop a stray config file. Honors
/// `NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL` / `NEXUS_SKIP_AGENT_HOOK_INSTALL` (the MCP wiring is part
/// of the launch-local bootstrap the hook flag governs).
pub fn write_opencode_mcp_config(cwd: &str, ctx: &LaunchCtx) {
    let Some(config) = opencode_mcp_config(ctx) else {
        return;
    };

    let path = std::path::Path::new(cwd).join("opencode.json");
    let _ = std::fs::write(
        path,
        serde_json::to_string_pretty(&config).unwrap_or_default(),
    );
}

fn opencode_mcp_config_content(ctx: &LaunchCtx) -> Option<String> {
    let config = opencode_mcp_config(ctx)?;
    serde_json::to_string(&config).ok()
}

fn opencode_mcp_config(ctx: &LaunchCtx) -> Option<serde_json::Value> {
    if env_flag("NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL") || env_flag("NEXUS_SKIP_AGENT_HOOK_INSTALL") {
        return None;
    }
    let (Some(name), Some(project)) = (&ctx.bus_name, &ctx.bus_project) else {
        return None;
    };
    let nexus_exe = std::env::current_exe()
        .ok()
        .and_then(|p| p.to_str().map(str::to_string))
        .unwrap_or_else(|| "nexus".to_string());

    let mut command = vec![
        nexus_exe,
        "mcp".to_string(),
        "--as".to_string(),
        name.clone(),
        "--project".to_string(),
        project.clone(),
    ];
    if let Some(key) = &ctx.bus_client_key {
        command.push("--client-key".to_string());
        command.push(key.clone());
    }
    if let Some(agent) = &ctx.bus_agent {
        command.push("--agent".to_string());
        command.push(agent.clone());
    }

    let prompt = opencode_identity_prompt(name, project);
    Some(serde_json::json!({
        "$schema": "https://opencode.ai/config.json",
        "default_agent": "nexus",
        "agent": {
            "nexus": {
                "mode": "primary",
                "description": "Nexus bus runtime identity",
                "prompt": prompt
            }
        },
        "mcp": {
            "nexus-bus": {
                "type": "local",
                "command": command,
                "enabled": true
            }
        }
    }))
}

fn opencode_identity_prompt(name: &str, project: &str) -> String {
    format!(
        "You are \"{name}\" on the Nexus bus in project \"{project}\".\n\
         Messages arrive through the nexus-bus MCP tools; use those tools to reply, DM, post, read, \
         list members, and list threads.\n\
         Do not claim another agent identity. If asked who you are, answer as \"{name}\"."
    )
}

fn quarantine_invalid_project_opencode_config(cwd: &str) {
    let path = Path::new(cwd).join("opencode.json");
    let Ok(body) = std::fs::read_to_string(&path) else {
        return;
    };
    if serde_json::from_str::<serde_json::Value>(&body).is_ok() {
        return;
    }

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let quarantine = Path::new(cwd).join(format!("opencode.json.invalid-nexus-{nanos}"));
    if std::fs::rename(&path, quarantine).is_err() {
        let _ = std::fs::remove_file(path);
    }
}

fn env_flag(name: &str) -> bool {
    std::env::var(name).ok().as_deref() == Some("1")
}

/// Classify a structured OpenCode provider-error payload preserved by a non-ACP bridge surface.
///
/// Visible message text must not be passed here; only machine-readable provider fields are used.
pub fn classify_opencode_provider_error_payload(
    data: &Value,
    source: &'static str,
) -> Option<AdapterInjectError> {
    classify_structured_provider_payload(Harness::OpenCode, data, source)
}

/// Resolve the launch-local OpenCode session DB used by headless ACP launches.
///
/// OpenCode's ACP `session/new` creates a fresh native session, but the binary normally stores it
/// in the operator's global OpenCode data directory. For daemon-owned Nexus agents that is too
/// broad: a stale global DB/config can make a newly launched agent inherit another identity's
/// session state. Pinning `OPENCODE_DB` under a per-identity directory inside the agent cwd keeps
/// the working session store scoped to the Nexus bus identity while leaving OpenCode's normal auth
/// store available. Callers without bus identity keep the legacy cwd-local path for tests and
/// operator-driven probes.
fn launch_local_opencode_db(cwd: &str, ctx: &LaunchCtx) -> Option<String> {
    let cwd = Path::new(cwd);
    let root = std::fs::canonicalize(cwd).unwrap_or_else(|_| PathBuf::from(cwd));
    let mut dir = root.join(".nexus").join("opencode");
    if let (Some(project), Some(name)) = (&ctx.bus_project, &ctx.bus_name) {
        dir = dir
            .join("acp")
            .join(safe_path_component(project))
            .join(safe_path_component(name));
    }
    if std::fs::create_dir_all(&dir).is_err() {
        return None;
    }
    Some(dir.join("opencode.db").to_string_lossy().into_owned())
}

fn safe_path_component(value: &str) -> String {
    let safe: String = value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' | '-' => ch,
            _ => '_',
        })
        .collect();
    if safe.is_empty() {
        "_".to_string()
    } else {
        safe
    }
}

#[async_trait]
impl Adapter for OpenCodeAdapter {
    async fn open_session(&self) -> Result<(), NexusError> {
        self.engine.spawn_and_initialize(&self.command).await?;
        self.engine
            .new_session(self.ctx.cwd.as_deref(), &self.ctx)
            .await
    }

    async fn resume(&self, resume_key: &str) -> Result<(), NexusError> {
        self.engine.spawn_and_initialize(&self.command).await?;
        self.engine
            .load_session(resume_key, self.ctx.cwd.as_deref(), &self.ctx)
            .await
    }

    async fn inject(&self, prompt: String) -> Result<(), AdapterInjectError> {
        self.engine.inject(prompt).await
    }

    fn steer_capability(&self) -> SteerCapability {
        SteerCapability::InterruptAndSend
    }

    async fn interrupt_active_turn(&self) -> Result<(), NexusError> {
        self.engine.cancel_active_turn().await
    }

    async fn inject_with_accepted_event(
        &self,
        prompt: String,
        accepted_event: Option<StreamEvent>,
    ) -> Result<(), AdapterInjectError> {
        self.engine
            .inject_with_accepted_event(prompt, accepted_event)
            .await
    }

    async fn stream_updates(&self) -> Result<Vec<StreamEvent>, NexusError> {
        Ok(self.engine.take_updates())
    }

    async fn kill(&self) {
        self.engine.kill().await
    }

    fn install_live(&self) -> Option<tokio::sync::mpsc::UnboundedReceiver<StreamEvent>> {
        Some(self.engine.install_live())
    }

    fn clear_live(&self) {
        self.engine.clear_live()
    }

    fn runtime_process_ids(&self) -> Option<nexus_common::RuntimeProcessIds> {
        self.engine.runtime_process_ids()
    }

    async fn acp_session_id(&self) -> Option<String> {
        self.engine.acp_session_id().await
    }

    async fn new_session_only(&self) -> Result<(), NexusError> {
        self.engine
            .new_session(self.ctx.cwd.as_deref(), &self.ctx)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opencode_adapter_suppresses_acp_mcp() {
        // The adapter must mark its ctx so the engine drops the stdio MCP server from session/new.
        let adapter = OpenCodeAdapter::with_command(HarnessCommand {
            program: "opencode".into(),
            args: vec!["acp".into()],
            cwd: None,
            ..Default::default()
        });
        assert!(
            adapter.ctx.suppress_acp_mcp,
            "opencode must suppress ACP-injected MCP"
        );
    }

    #[test]
    fn opencode_adapter_pins_session_db_under_launch_cwd() {
        let dir = std::env::temp_dir().join(format!(
            "nexus-opencode-db-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let cwd = dir.to_string_lossy().into_owned();

        let adapter = OpenCodeAdapter::new(LaunchCtx {
            cwd: Some(cwd.clone()),
            env: vec![("OPENCODE_DB".into(), "/tmp/stale-opencode.db".into())],
            ..Default::default()
        });

        let db_values: Vec<_> = adapter
            .command()
            .env
            .iter()
            .filter_map(|(key, value)| (key == "OPENCODE_DB").then_some(value.as_str()))
            .collect();
        assert_eq!(db_values.len(), 1);
        assert_eq!(
            db_values[0],
            dir.join(".nexus/opencode/opencode.db")
                .to_string_lossy()
                .as_ref()
        );
        assert!(dir.join(".nexus/opencode").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn opencode_adapter_isolates_session_db_per_bus_identity() {
        let dir = temp_dir("opencode-db-identity");
        let cwd = dir.to_string_lossy().into_owned();

        let dean = OpenCodeAdapter::new(LaunchCtx {
            cwd: Some(cwd.clone()),
            bus_name: Some("dean".to_string()),
            bus_project: Some("default".to_string()),
            bus_client_key: Some("ck_dean".to_string()),
            bus_agent: Some("opencode".to_string()),
            ..Default::default()
        });
        let oscar = OpenCodeAdapter::new(LaunchCtx {
            cwd: Some(cwd.clone()),
            bus_name: Some("oscar".to_string()),
            bus_project: Some("default".to_string()),
            bus_client_key: Some("ck_oscar".to_string()),
            bus_agent: Some("opencode".to_string()),
            ..Default::default()
        });

        let dean_db = only_env(dean.command(), "OPENCODE_DB");
        let oscar_db = only_env(oscar.command(), "OPENCODE_DB");
        assert_ne!(dean_db, oscar_db);
        assert_eq!(
            dean_db,
            dir.join(".nexus/opencode/acp/default/dean/opencode.db")
                .to_string_lossy()
                .as_ref()
        );
        assert_eq!(
            oscar_db,
            dir.join(".nexus/opencode/acp/default/oscar/opencode.db")
                .to_string_lossy()
                .as_ref()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn opencode_adapter_scopes_config_per_process_env() {
        let _env = EnvGuard::new(&[
            "NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL",
            "NEXUS_SKIP_AGENT_HOOK_INSTALL",
        ]);
        let dir = temp_dir("opencode-env-config");
        let cwd = dir.to_string_lossy().into_owned();
        let stale_file = dir.join("opencode.json");
        std::fs::write(&stale_file, "{\"default_agent\":\"stale\"}").unwrap();

        let dean = OpenCodeAdapter::new(LaunchCtx {
            cwd: Some(cwd.clone()),
            bus_name: Some("dean".to_string()),
            bus_project: Some("default".to_string()),
            bus_client_key: Some("ck_dean".to_string()),
            bus_agent: Some("opencode".to_string()),
            env: vec![(
                "OPENCODE_CONFIG_CONTENT".to_string(),
                "{\"default_agent\":\"stale\"}".to_string(),
            )],
            ..Default::default()
        });
        let oscar = OpenCodeAdapter::new(LaunchCtx {
            cwd: Some(cwd.clone()),
            bus_name: Some("oscar".to_string()),
            bus_project: Some("default".to_string()),
            bus_client_key: Some("ck_oscar".to_string()),
            bus_agent: Some("opencode".to_string()),
            ..Default::default()
        });

        assert_eq!(
            std::fs::read_to_string(&stale_file).unwrap(),
            "{\"default_agent\":\"stale\"}"
        );
        let dean_config = only_env_json(dean.command(), "OPENCODE_CONFIG_CONTENT");
        let oscar_config = only_env_json(oscar.command(), "OPENCODE_CONFIG_CONTENT");
        assert!(dean_config["agent"]["nexus"]["prompt"]
            .as_str()
            .unwrap()
            .contains("You are \"dean\" on the Nexus bus"));
        assert!(oscar_config["agent"]["nexus"]["prompt"]
            .as_str()
            .unwrap()
            .contains("You are \"oscar\" on the Nexus bus"));
        assert_ne!(dean_config, oscar_config);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn opencode_adapter_quarantines_invalid_project_config() {
        let _env = EnvGuard::new(&[
            "NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL",
            "NEXUS_SKIP_AGENT_HOOK_INSTALL",
        ]);
        let dir = temp_dir("opencode-invalid-config");
        let cwd = dir.to_string_lossy().into_owned();
        std::fs::write(
            dir.join("opencode.json"),
            "{\"default_agent\":\"dean\"}\n{\"oops\":true}",
        )
        .unwrap();

        let _adapter = OpenCodeAdapter::new(LaunchCtx {
            cwd: Some(cwd),
            bus_name: Some("oscar".to_string()),
            bus_project: Some("default".to_string()),
            bus_client_key: Some("ck_oscar".to_string()),
            bus_agent: Some("opencode".to_string()),
            ..Default::default()
        });

        assert!(!dir.join("opencode.json").exists());
        let quarantined: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("opencode.json.invalid-nexus-")
            })
            .collect();
        assert_eq!(quarantined.len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    fn only_env<'a>(command: &'a HarnessCommand, name: &str) -> &'a str {
        let values: Vec<_> = command
            .env
            .iter()
            .filter_map(|(key, value)| (key == name).then_some(value.as_str()))
            .collect();
        assert_eq!(values.len(), 1);
        values[0]
    }

    fn only_env_json(command: &HarnessCommand, name: &str) -> serde_json::Value {
        serde_json::from_str(only_env(command, name)).unwrap()
    }

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("nexus-agent-{label}-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    struct EnvGuard {
        saved: Vec<(&'static str, Option<String>)>,
    }

    impl EnvGuard {
        fn new(keys: &[&'static str]) -> Self {
            let saved = keys
                .iter()
                .map(|&key| {
                    let value = std::env::var(key).ok();
                    std::env::remove_var(key);
                    (key, value)
                })
                .collect();
            Self { saved }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, value) in self.saved.drain(..) {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }
}
