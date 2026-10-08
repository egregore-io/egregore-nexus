use nexus_agent::{Adapter, LaunchCtx};
use nexus_harness_claude::ClaudeAdapter;

struct Restore(Vec<(&'static str, Option<std::ffi::OsString>)>);
impl Drop for Restore {
    fn drop(&mut self) {
        for (key, value) in self.0.drain(..) {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

#[tokio::test]
async fn preservation_claude_new_and_load_have_launch_only_settings() {
    let root = std::env::temp_dir().join(format!(
        "nexus-claude-overlay-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let fixture = root.join("bridge.mjs");
    std::fs::write(&fixture, r#"
import {createInterface} from 'node:readline';
import {appendFileSync} from 'node:fs';
for await (const line of createInterface({input:process.stdin})) {
  const request=JSON.parse(line);
  if(request.id===undefined) continue;
  let result;
  if(request.method==='initialize') result={protocolVersion:1,agentCapabilities:{loadSession:true},authMethods:[]};
  else if(request.method==='session/new'||request.method==='session/load') {
    appendFileSync(process.env.FIXTURE_REQUESTS,JSON.stringify(request)+'\n');
    result=request.method==='session/new'?{sessionId:'fixture-session'}:{};
  } else result={};
  console.log(JSON.stringify({jsonrpc:'2.0',id:request.id,result}));
}
"#).unwrap();
    let keys = [
        "NEXUS_CLAUDE_ACP_CMD",
        "NEXUS_CLAUDE_ACP_ARGS",
        "NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL",
        "NEXUS_SKIP_AGENT_HOOK_INSTALL",
    ];
    let _restore = Restore(
        keys.into_iter()
            .map(|key| (key, std::env::var_os(key)))
            .collect(),
    );
    for key in keys {
        std::env::remove_var(key);
    }
    std::env::set_var("NEXUS_CLAUDE_ACP_CMD", "node");
    std::env::set_var("NEXUS_CLAUDE_ACP_ARGS", &fixture);
    let requests = root.join("requests.jsonl");
    for (resume, explicit) in [(false, false), (true, false), (false, true), (true, true)] {
        let adapter = ClaudeAdapter::new(LaunchCtx {
            cwd: Some(root.to_string_lossy().into_owned()),
            env: vec![("CLAUDE_MODEL_CONFIG".into(), serde_json::json!({"modelOverrides":{"sonnet":"fixture-bedrock-model"},"availableModels":["sonnet"]}).to_string()), (
                "FIXTURE_REQUESTS".into(),
                requests.to_string_lossy().into_owned(),
            )],
            session_meta: explicit.then(|| serde_json::json!({"fixture":"caller-metadata", "claudeCode":{"options":{"tools":["Read"],"settings":{"permissions":{"deny":["Edit"]},"modelOverrides":{"sonnet":"explicit-model"},"hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"fixture-user-hook"}]}]}}}}}).as_object().unwrap().clone()),
            ..Default::default()
        });
        if resume {
            adapter.resume("fixture-session").await.unwrap();
        } else {
            adapter.open_session().await.unwrap();
        }
        adapter.kill().await;
    }
    let lines = std::fs::read_to_string(requests).unwrap();
    let requests: Vec<serde_json::Value> = lines
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[0]["method"], "session/new");
    assert_eq!(requests[1]["method"], "session/load");
    for (index, request) in requests.into_iter().enumerate() {
        let options = &request["params"]["_meta"]["claudeCode"]["options"];
        let explicit = index >= 2;
        let hooks = options["settings"]["hooks"]["SessionStart"]
            .as_array()
            .unwrap();
        assert_eq!(hooks.len(), if explicit { 2 } else { 1 });
        assert_eq!(
            hooks.last().unwrap()["hooks"][0]["command"],
            "./.nexus/bootstrap-register.sh"
        );
        assert!(options.get("settingSources").is_none());
        assert_eq!(
            options["settings"]["modelOverrides"]["sonnet"],
            if explicit {
                "explicit-model"
            } else {
                "fixture-bedrock-model"
            }
        );
        if explicit {
            assert_eq!(hooks[0]["hooks"][0]["command"], "fixture-user-hook");
            assert_eq!(
                options["settings"]["permissions"]["deny"],
                serde_json::json!(["Edit"])
            );
            assert_eq!(options["tools"], serde_json::json!(["Read"]));
            assert_eq!(request["params"]["_meta"]["fixture"], "caller-metadata");
            assert!(options["settings"].get("availableModels").is_none());
        } else {
            assert_eq!(
                options["settings"]["availableModels"],
                serde_json::json!(["sonnet"])
            );
        }
    }
    assert!(!root.join(".claude/settings.json").exists());
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn invalid_claude_inline_configuration_fails_without_spawn_or_secret_contents() {
    let root = std::env::temp_dir().join(format!(
        "nexus-claude-invalid-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    for raw in ["{secret-fixture", "null", "[]"] {
        let adapter = ClaudeAdapter::new(LaunchCtx {
            cwd: Some(root.to_string_lossy().into_owned()),
            env: vec![("CLAUDE_MODEL_CONFIG".into(), raw.into())],
            ..Default::default()
        });
        for error in [
            adapter.open_session().await.unwrap_err(),
            adapter.resume("fixture").await.unwrap_err(),
            adapter.new_session_only().await.unwrap_err(),
        ] {
            assert!(error.to_string().contains("CLAUDE_MODEL_CONFIG"));
            assert!(!error.to_string().contains("secret-fixture"));
        }
        assert!(adapter.runtime_process_ids().is_none());
    }
    std::fs::remove_dir_all(root).unwrap();
}
