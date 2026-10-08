use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn run_fixture(mode: &str) {
    let root = std::env::temp_dir().join(format!(
        "nexus-opencode-config-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let source = root.join("serve.mjs");
    std::fs::write(&source, nexus_harness_opencode::SERVE_SOURCE).unwrap();
    let driver = r#"
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {EventEmitter} from 'node:events';
const mode=process.argv[3];
Object.assign(process.env, {
  NEXUS_OPENCODE_PLUGIN_PATH:'/fixture/nexus-plugin.mjs',
  NEXUS_OPENCODE_HOME:process.argv[2], NEXUS_OPENCODE_BIN:'fixture-native',
  NEXUS_CLI:'/fixture/nexus', NEXUS_NAME:'first', NEXUS_PROJECT:'project',
  NEXUS_CLIENT_KEY:'fixture-key', NEXUS_AGENT:'opencode',
  NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL:'', NEXUS_SKIP_AGENT_HOOK_INSTALL:'',
  OPENCODE_CONFIG_CONTENT:JSON.stringify({permission:{edit:'deny'},model:'user-model',
    agent:{custom:{mode:'primary'},nexus:{permission:{edit:'deny'},model:'agent-model',tools:{write:false}}},plugin:['user-plugin'],
    mcp:{'user-tool':{type:'local',command:['fixture-user']},'nexus-bus':{type:'local',command:['stale-key']}}}),
});
if(mode==='invalid') process.env.OPENCODE_CONFIG_CONTENT='{secret-fixture';
if(mode==='skip') process.env.NEXUS_SKIP_AGENT_HOOK_INSTALL='1';
let calls=0;
globalThis.capture = (_bin,_args,options) => {
  calls++;
  const config=JSON.parse(options.env.OPENCODE_CONFIG_CONTENT);
  assert.deepEqual(config.permission,{edit:'deny'});
  assert.equal(config.model,'user-model');
  assert.deepEqual(config.agent.custom,{mode:'primary'});
  assert.deepEqual(config.agent.nexus.permission,{edit:'deny'});
  assert.equal(config.agent.nexus.model,'agent-model');
  assert.deepEqual(config.agent.nexus.tools,{write:false});
  assert.deepEqual(config.plugin,calls===1?['user-plugin','/fixture/nexus-plugin.mjs']:['user-plugin']);
  assert.deepEqual(config.mcp['user-tool'],{type:'local',command:['fixture-user']});
  assert.deepEqual(config.mcp['nexus-bus'].command,mode==='skip'?['stale-key']:['/fixture/nexus','mcp','--as','first','--project','project','--client-key','fixture-key','--agent','opencode']);
  if(mode!=='skip') {
    assert.equal(config.default_agent,'nexus');
    assert.match(config.agent.nexus.prompt,/You are "first" on the Nexus bus/);
  }
  if(calls===1 && mode==='tui') {
    const child=new EventEmitter(); child.stdout=new EventEmitter(); child.stderr=new EventEmitter();
    child.pid=process.pid; child.exitCode=null; child.signalCode=null;
    setTimeout(()=>child.stdout.emit('data',Buffer.from('[nexus-opencode-session] fixture-session\n')),5);
    return child;
  }
  assert.equal(calls,mode==='tui'?2:1);
  if(mode==='tui') {
    assert.equal(_args[0],'attach');
    assert.equal(options.env.NEXUS_OPENCODE_PLUGIN_PATH,undefined);
    assert.equal(options.env.NEXUS_OPENCODE_BRIDGE_TOKEN,undefined);
  }
  throw new Error('fixture-captured');
};
globalThis.fetch=async()=>({});
process.on('unhandledRejection',error => {
  if(mode==='invalid') {
    assert.equal(calls,0);
    assert.equal(error.message,'invalid OPENCODE_CONFIG_CONTENT JSON');
    process.exit(0);
  }
  if(error.message==='fixture-captured') process.exit(0);
  console.error(error.message); process.exit(1);
});
const source=readFileSync(process.argv[1],'utf8').replace('import { spawn } from "node:child_process";','const spawn=globalThis.capture;');
await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
"#;
    let output = Command::new("node")
        .args(["--input-type=module", "-e", driver])
        .arg(&source)
        .arg(root.join("runtime"))
        .arg(mode)
        .current_dir(&root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn preservation_serve_keeps_caller_configuration_and_captured_identity() {
    run_fixture("serve");
}

#[test]
fn preservation_attach_keeps_user_settings_without_nexus_recording_plugin() {
    run_fixture("tui");
}

#[test]
fn invalid_serve_config_rejects_before_spawn_without_secret_content() {
    run_fixture("invalid");
}

#[test]
fn skipped_hooks_keep_the_callers_mcp_configuration() {
    run_fixture("skip");
}
