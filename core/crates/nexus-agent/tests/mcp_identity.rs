use agent_client_protocol::schema::v1::McpServer;
use nexus_agent::adapter::engine::{build_load_session_request, build_new_session_request};
use nexus_agent::{write_hermes_mcp_config, write_opencode_mcp_config, LaunchCtx};

fn bus_ctx(cwd: Option<String>) -> LaunchCtx {
    LaunchCtx {
        cwd,
        bus_name: Some("hugo".to_string()),
        bus_project: Some("default".to_string()),
        bus_client_key: Some("nexus_ck_hugo".to_string()),
        bus_agent: Some("claude".to_string()),
        ..Default::default()
    }
}

#[test]
fn acp_session_new_mcp_uses_verified_runtime_identity() {
    let req = build_new_session_request(None, &bus_ctx(None));
    assert_eq!(req.mcp_servers.len(), 1);
    let McpServer::Stdio(stdio) = &req.mcp_servers[0] else {
        panic!("expected stdio MCP server");
    };

    assert!(stdio.args.windows(2).any(|pair| pair == ["--as", "hugo"]));
    assert!(stdio
        .args
        .windows(2)
        .any(|pair| pair == ["--project", "default"]));
    assert!(stdio
        .args
        .windows(2)
        .any(|pair| pair == ["--client-key", "nexus_ck_hugo"]));
    assert!(stdio
        .args
        .windows(2)
        .any(|pair| pair == ["--agent", "claude"]));
}

#[test]
fn acp_session_load_rebinds_mcp_with_verified_runtime_identity() {
    let req = build_load_session_request("provider-session", None, &bus_ctx(None));
    assert_eq!(req.mcp_servers.len(), 1);
    let McpServer::Stdio(stdio) = &req.mcp_servers[0] else {
        panic!("expected stdio MCP server");
    };

    assert!(stdio.args.windows(2).any(|pair| pair == ["--as", "hugo"]));
    assert!(stdio
        .args
        .windows(2)
        .any(|pair| pair == ["--client-key", "nexus_ck_hugo"]));
    assert!(stdio
        .args
        .windows(2)
        .any(|pair| pair == ["--agent", "claude"]));
}

#[test]
fn acp_session_new_mcp_discovers_only_daemon_ipc() {
    let mut ctx = bus_ctx(None);
    ctx.env = vec![
        ("NEXUS_HOME".into(), "/tmp/nexus-home".into()),
        ("NEXUS_NO_AUTOSTART".into(), "1".into()),
        ("TOKIO_WORKER_THREADS".into(), "2".into()),
        ("NEXUS_DB_URL".into(), "http://127.0.0.1:4141".into()),
        ("NEXUS_DB_AUTH_TOKEN".into(), "store-token".into()),
        (
            "NEXUS_STREAM_DB_PATH".into(),
            "/dev/shm/nexus-stream.db".into(),
        ),
        ("NEXUS_NAME".into(), "must-not-be-copied-as-mcp-env".into()),
    ];

    let req = build_new_session_request(None, &ctx);
    let McpServer::Stdio(stdio) = &req.mcp_servers[0] else {
        panic!("expected stdio MCP server");
    };
    let env = stdio
        .env
        .iter()
        .map(|item| (item.name.as_str(), item.value.as_str()))
        .collect::<std::collections::HashMap<_, _>>();

    assert_eq!(env.get("NEXUS_HOME"), Some(&"/tmp/nexus-home"));
    assert_eq!(env.get("NEXUS_NO_AUTOSTART"), Some(&"1"));
    assert_eq!(env.get("TOKIO_WORKER_THREADS"), Some(&"2"));
    assert!(!env.contains_key("NEXUS_DB_URL"));
    assert!(!env.contains_key("NEXUS_DB_AUTH_TOKEN"));
    assert!(!env.contains_key("NEXUS_STREAM_DB_PATH"));
    assert!(!env.contains_key("NEXUS_NAME"));
}

#[test]
fn acp_session_new_has_no_mcp_server_without_bus_identity() {
    let req = build_new_session_request(None, &LaunchCtx::default());

    assert_eq!(req.mcp_servers.len(), 0);
}

#[test]
fn acp_session_new_suppresses_mcp_when_flag_set() {
    let mut ctx = bus_ctx(Some("/tmp/worker".to_string()));
    ctx.suppress_acp_mcp = true;

    let req = build_new_session_request(ctx.cwd.as_deref(), &ctx);

    assert_eq!(req.mcp_servers.len(), 0);
}

#[test]
fn opencode_project_config_uses_verified_runtime_identity() {
    let _env = EnvGuard::new(&[
        "NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL",
        "NEXUS_SKIP_AGENT_HOOK_INSTALL",
    ]);
    let dir = temp_dir("opencode");
    let cwd = dir.to_string_lossy().into_owned();

    write_opencode_mcp_config(&cwd, &bus_ctx(Some(cwd.clone())));

    let body = std::fs::read_to_string(dir.join("opencode.json")).unwrap();
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    let command: Vec<String> = value["mcp"]["nexus-bus"]["command"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item.as_str().unwrap().to_string())
        .collect();

    assert!(command.windows(2).any(|pair| pair == ["--as", "hugo"]));
    assert!(command
        .windows(2)
        .any(|pair| pair == ["--client-key", "nexus_ck_hugo"]));
    assert!(command.windows(2).any(|pair| pair == ["--agent", "claude"]));
    assert_eq!(value["default_agent"], "nexus");
    assert_eq!(value["agent"]["nexus"]["mode"], "primary");
    let prompt = value["agent"]["nexus"]["prompt"].as_str().unwrap();
    assert!(prompt.contains("You are \"hugo\" on the Nexus bus"));
    assert!(prompt.contains("project \"default\""));
    assert!(prompt.contains("nexus-bus MCP"));
    assert!(prompt.contains("Do not claim another agent identity"));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hermes_project_config_uses_verified_runtime_identity_when_enabled() {
    let home = temp_dir("hermes-home");
    let _env = EnvGuard::new(&[
        "NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL",
        "NEXUS_SKIP_AGENT_HOOK_INSTALL",
        "NEXUS_HERMES_BUS_MCP",
        "HERMES_HOME",
    ]);
    std::env::set_var("HERMES_HOME", &home);
    std::env::set_var("NEXUS_HERMES_BUS_MCP", "1");

    write_hermes_mcp_config(&bus_ctx(None));

    let body = std::fs::read_to_string(home.join("config.yaml")).unwrap();
    let value: serde_yaml::Value = serde_yaml::from_str(&body).unwrap();
    let args: Vec<String> = value["mcp_servers"]["nexus-bus"]["args"]
        .as_sequence()
        .unwrap()
        .iter()
        .map(|item| item.as_str().unwrap().to_string())
        .collect();

    assert!(args.windows(2).any(|pair| pair == ["--as", "hugo"]));
    assert!(args
        .windows(2)
        .any(|pair| pair == ["--client-key", "nexus_ck_hugo"]));
    assert!(args.windows(2).any(|pair| pair == ["--agent", "claude"]));

    let _ = std::fs::remove_dir_all(&home);
}

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("nexus-agent-mcp-identity-{label}-{nanos}"));
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
