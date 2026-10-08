use nexus_agent::adapter::opencode::harness::OpenCodeAdapter;
use nexus_agent::{Adapter, LaunchCtx};

fn context(config: &str) -> LaunchCtx {
    LaunchCtx {
        bus_name: Some("first".into()),
        bus_project: Some("default".into()),
        bus_client_key: Some("first-key".into()),
        bus_agent: Some("opencode".into()),
        env: vec![("OPENCODE_CONFIG_CONTENT".into(), config.into())],
        ..Default::default()
    }
}

#[test]
fn preservation_opencode_keeps_caller_launch_configuration() {
    let config = serde_json::json!({
        "permission":{"edit":"deny"}, "model":"user-model",
        "plugin":["user-plugin"], "agent":{"custom":{"mode":"primary"},"nexus":{"permission":{"edit":"deny"},"model":"agent-model","tools":{"write":false}}},
        "mcp":{"user-tool":{"type":"local","command":["user-command"]}}
    });
    let adapter = OpenCodeAdapter::new(context(&config.to_string()));
    let values: Vec<_> = adapter
        .command()
        .env
        .iter()
        .filter(|(key, _)| key == "OPENCODE_CONFIG_CONTENT")
        .collect();
    assert_eq!(values.len(), 1);
    let actual: serde_json::Value = serde_json::from_str(&values[0].1).unwrap();
    for key in ["permission", "model", "plugin"] {
        assert_eq!(actual[key], config[key], "lost caller {key}");
    }
    assert_eq!(actual["agent"]["custom"], config["agent"]["custom"]);
    for key in ["permission", "model", "tools"] {
        assert_eq!(
            actual["agent"]["nexus"][key], config["agent"]["nexus"][key],
            "lost Nexus-agent {key}"
        );
    }
    assert_eq!(actual["mcp"]["user-tool"], config["mcp"]["user-tool"]);
}

#[tokio::test]
async fn malformed_opencode_launch_config_fails_before_native_spawn_without_echoing_content() {
    for input in [
        "{secret-fixture",
        "null",
        "[]",
        "{\"agent\":1}",
        "{\"mcp\":1}",
    ] {
        let adapter = OpenCodeAdapter::new(context(input));
        for error in [
            adapter.open_session().await.unwrap_err(),
            adapter.resume("fixture").await.unwrap_err(),
            adapter.new_session_only().await.unwrap_err(),
        ] {
            assert!(error.to_string().contains("OPENCODE_CONFIG_CONTENT"));
            assert!(!error.to_string().contains("secret-fixture"));
        }
        assert!(adapter.runtime_process_ids().is_none());
    }
}
