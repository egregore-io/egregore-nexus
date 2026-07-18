use std::{fs, sync::Arc};

use libsql::params;
use nexus::cli::read_client::ReadClient;
use nexus::daemon::AppState;
use nexus_common::Config;
use nexus_contracts::{
    codes, Kind, MemberListRequest, Presence, RegisterRequest, SearchMode, SearchRequest,
    SendRequest, SendTarget, Tier,
};
use nexus_harness_claude::storage::{ClaudeRuntimeLaunch, ClaudeRuntimeStateRepo};
use nexus_store::repos::{
    AgentRuntimes, NativeThreadBindings, NewAgentRuntime, NewNativeThreadBinding, NewSession,
    Sessions,
};
use nexus_store::Store;
use std::path::PathBuf;

async fn state() -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    AppState::wire(store, &Config::default())
}

fn human(name: &str, client_key: &str) -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some(name.into()),
        harness: hid("other"),
        harness_session_id: format!("hs_{client_key}"),
        project: "default".into(),
        client_key: client_key.into(),
        runtime_credential: None,
        tier: Tier::Admin,
        kind: Some(Kind::Human),
        role: None,
        cwd: None,
    }
}

fn agent(
    name: &str,
    client_key: &str,
    harness: nexus_contracts::HarnessId,
    cwd: &str,
) -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some(name.into()),
        harness,
        harness_session_id: format!("hs_{client_key}"),
        project: "default".into(),
        client_key: client_key.into(),
        runtime_credential: None,
        tier: Tier::Agent,
        kind: Some(Kind::Agent),
        role: None,
        cwd: Some(cwd.into()),
    }
}

fn write_claude_hooks(bridge_dir: &std::path::Path, body: &str) {
    fs::create_dir_all(bridge_dir).unwrap();
    fs::write(bridge_dir.join("hooks.jsonl"), body).unwrap();
}

#[tokio::test]
async fn read_client_renders_identity_roster_threads_topics_and_recall_from_store() {
    let state = state().await;
    let ben = state
        .identity
        .register(human("ben", "ck_ben"))
        .await
        .unwrap();
    state
        .identity
        .register(human("ada", "ck_ada"))
        .await
        .unwrap();
    let caller = state.identity.resolve("default", "ben").await.unwrap();

    state
        .bus
        .create_thread(
            &caller,
            nexus_contracts::CreateThreadRequest {
                name: "backend".into(),
                members: vec!["ada".into()],
            },
        )
        .await
        .unwrap();
    state
        .bus
        .subscribe(
            &caller,
            nexus_contracts::SubscribeRequest {
                topic: "ci".into(),
                group: None,
            },
        )
        .await
        .unwrap();
    state
        .bus
        .send(
            &caller,
            SendRequest {
                to: SendTarget::dm_name("ada"),
                summary: None,
                body: "store backed recall works".into(),
                mention: vec![],
                idempotency_key: None,
            },
        )
        .await
        .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "ben",
        "default",
        Some(ben.session_id.0.clone()),
        Some("ck_ben".into()),
        Tier::Admin,
    );

    assert_eq!(read.whoami().await.unwrap().name.as_deref(), Some("ben"));
    assert!(read
        .members(MemberListRequest {
            include_offline: Some(true),
            include_dead: None,
        })
        .await
        .unwrap()
        .members
        .iter()
        .any(|member| member.name.as_deref() == Some("ada")));
    assert!(read
        .threads()
        .await
        .unwrap()
        .threads
        .iter()
        .any(|thread| thread.name == "backend"));
    assert!(read
        .topics()
        .await
        .unwrap()
        .topics
        .iter()
        .any(|topic| topic.topic == "ci"));
    assert!(read
        .history(nexus_contracts::HistoryRequest {
            thread: None,
            with: Some("ada".into()),
            topic: None,
            limit: Some(10),
            before: None,
        })
        .await
        .unwrap()
        .entries
        .iter()
        .any(|entry| entry.body == "store backed recall works"));
    assert!(read
        .search(SearchRequest {
            query: "recall".into(),
            mode: SearchMode::Fts,
            limit: Some(10),
            thread: None,
            with: Some("ada".into()),
            since: None,
        })
        .await
        .unwrap()
        .hits
        .iter()
        .any(|hit| hit.snippet.contains("recall")));
}

#[tokio::test]
async fn read_client_fetches_full_message_body_by_id() {
    let state = state().await;
    let ben = state
        .identity
        .register(human("ben", "ck_ben"))
        .await
        .unwrap();
    state
        .identity
        .register(human("ada", "ck_ada"))
        .await
        .unwrap();
    let caller = state.identity.resolve("default", "ben").await.unwrap();
    let ack = state
        .bus
        .send(
            &caller,
            SendRequest {
                to: SendTarget::dm_name("ada"),
                summary: Some("long".into()),
                body: "this is the full message body that the drain view may truncate".into(),
                mention: vec![],
                idempotency_key: None,
            },
        )
        .await
        .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "ben",
        "default",
        Some(ben.session_id.0.clone()),
        Some("ck_ben".into()),
        Tier::Admin,
    );

    let message = read.message(&ack.message_id.0).await.unwrap();
    assert_eq!(
        message.body,
        "this is the full message body that the drain view may truncate"
    );
}

#[tokio::test]
async fn read_client_preserves_local_operator_whoami_without_session_row() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let read = ReadClient::from_store_with_caller_for_tests(
        store,
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );

    let who = read.whoami().await.unwrap();
    assert_eq!(who.name.as_deref(), Some("operator"));
    assert_eq!(who.session_id.0, "local-operator");
    assert_eq!(who.tier, Tier::Admin);
}

#[tokio::test]
async fn read_client_rejects_bad_key_instead_of_falling_back_to_name() {
    let state = state().await;
    state
        .identity
        .register(agent("remy", "ck_real_remy", hid("claude"), "/repo"))
        .await
        .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "remy",
        "default",
        None,
        Some("ck_wrong_remy".into()),
        Tier::Agent,
    );

    let error = read.whoami().await.unwrap_err();
    assert_eq!(error.code, codes::NOT_FOUND);
    assert!(error.message.contains("caller not registered"));
}

#[tokio::test]
async fn read_client_resolves_raw_pty_descriptor_when_endpoint_manifest_exists() {
    // De-tmux (#22) conformance: a transport=pty row whose session has a live terminal endpoint
    // manifest attaches through the socket terminal client, not tmux.
    let dir = std::env::temp_dir().join(format!("nexus-manifest-rc-{}", std::process::id()));
    let dir_s = dir.display().to_string();
    let _env = nexus::cli::ambient::TestEnvGuard::new(&[(
        "NEXUS_TERMINAL_MANIFEST_DIR",
        Some(dir_s.as_str()),
    )]);

    let state = state().await;
    let ada = state
        .identity
        .register(human("ada", "ck_ada_raw"))
        .await
        .unwrap();
    let sessions = Sessions::new(&state.store);
    sessions
        .set_transport(&ada.session_id, "pty")
        .await
        .unwrap();
    let manifest_path =
        nexus::daemon::terminal_socket::terminal_endpoint_manifest_path(&ada.session_id);
    std::fs::create_dir_all(manifest_path.parent().unwrap()).unwrap();
    std::fs::write(
        &manifest_path,
        format!(
            r#"{{"session_id":"{}","path":"/tmp/nexus-terminal-test.sock","token":"tok"}}"#,
            ada.session_id.0
        ),
    )
    .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );
    let descriptor = read
        .pty_attach_descriptor_for_session(&ada.session_id)
        .await
        .unwrap();
    assert_eq!(descriptor.backend, "raw-pty");
    assert_eq!(descriptor.argv[0], "nexus");
    assert_eq!(descriptor.argv[1], "terminal-client");
    assert_eq!(descriptor.argv[2], ada.session_id.0);
    assert!(descriptor.liveness_argv.is_none());

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn read_client_resolves_backend_neutral_pty_attach_descriptor_from_store() {
    let state = state().await;
    let ben = state
        .identity
        .register(human("ben", "ck_ben"))
        .await
        .unwrap();
    let codex = state
        .identity
        .register(human("codex", "ck_codex"))
        .await
        .unwrap();
    let opencode = state
        .identity
        .register(human("opencode", "ck_opencode"))
        .await
        .unwrap();
    let sessions = Sessions::new(&state.store);
    sessions
        .set_transport(&ben.session_id, "pty")
        .await
        .unwrap();
    sessions
        .set_transport(&codex.session_id, "codex-appserver")
        .await
        .unwrap();
    sessions
        .set_transport(&opencode.session_id, "opencode-plugin")
        .await
        .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );

    for (name, session) in [
        ("ben", &ben.session_id),
        ("codex", &codex.session_id),
        ("opencode", &opencode.session_id),
    ] {
        let descriptor = read.pty_attach_descriptor(name).await.unwrap();
        let by_target_session = read.pty_attach_descriptor(&session.0).await.unwrap();
        let by_session = read
            .pty_attach_descriptor_for_session(session)
            .await
            .unwrap();

        assert_eq!(descriptor, by_session);
        assert_eq!(by_target_session, by_session);
        assert_eq!(descriptor.backend, "tmux");
        assert_eq!(descriptor.argv[0], "tmux");
        assert_eq!(descriptor.argv[1], "-S");
        assert!(descriptor.argv[2].contains("nexus-tmux-nexus-"));
        assert_eq!(descriptor.argv[3], "attach");
        assert_eq!(descriptor.argv[4], "-t");
        assert_eq!(descriptor.argv[5], tmux_session_name_for_test(&session.0));
        assert_eq!(
            descriptor.liveness_argv.as_ref().unwrap(),
            &vec![
                "tmux".to_string(),
                "-S".to_string(),
                descriptor.argv[2].clone(),
                "has-session".to_string(),
                "-t".to_string(),
                descriptor.argv[5].clone(),
            ]
        );
    }
}

#[tokio::test]
async fn read_client_prefers_explicit_session_id_over_matching_name_for_attach() {
    let state = state().await;
    let target = state
        .identity
        .register(human("target", "ck_target"))
        .await
        .unwrap();
    let shadow_name = target.session_id.0.clone();
    let shadow = state
        .identity
        .register(human(&shadow_name, "ck_shadow"))
        .await
        .unwrap();
    let sessions = Sessions::new(&state.store);
    sessions
        .set_transport(&target.session_id, "pty")
        .await
        .unwrap();
    sessions
        .set_transport(&shadow.session_id, "pty")
        .await
        .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );

    let descriptor = read
        .pty_attach_descriptor(&target.session_id.0)
        .await
        .unwrap();

    assert_eq!(
        descriptor.argv[5],
        tmux_session_name_for_test(&target.session_id.0)
    );
    assert_ne!(
        descriptor.argv[5],
        tmux_session_name_for_test(&shadow.session_id.0)
    );
}

#[tokio::test]
async fn read_client_resolves_attach_name_to_the_stable_agents_active_runtime() {
    let state = state().await;
    let registered = state
        .identity
        .register(agent("codex", "ck_codex", hid("codex"), "/work/old"))
        .await
        .unwrap();
    let agent_id = registered.agent_id.unwrap();
    let sessions = Sessions::new(&state.store);
    sessions
        .set_transport(&registered.session_id, "acp")
        .await
        .unwrap();

    let active_session = nexus_contracts::SessionId("s_active_codex_runtime".into());
    sessions
        .create(NewSession {
            session_id: active_session.clone(),
            name: None,
            agent: Some("codex".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some("codex-active-thread".into()),
            client_key: Some("ck_codex_active_runtime".into()),
            cwd: Some("/work/active".into()),
            project: "default".into(),
            transport: Some("pty".into()),
        })
        .await
        .unwrap();
    sessions
        .set_agent_id(&active_session, &agent_id.0)
        .await
        .unwrap();
    AgentRuntimes::new(&state.store)
        .create(NewAgentRuntime {
            runtime_id: active_session.0.clone(),
            agent_id: agent_id.0,
            harness: "codex".into(),
            cwd: Some("/work/active".into()),
            transport: Some("pty".into()),
            presence: Some("online".into()),
            active: true,
        })
        .await
        .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );
    let descriptor = read.pty_attach_descriptor("codex").await.unwrap();

    assert_eq!(descriptor.session_id, active_session);
    assert_eq!(descriptor.backend, "tmux");
    assert_eq!(
        descriptor.argv[5],
        tmux_session_name_for_test(&descriptor.session_id.0)
    );
}

#[tokio::test]
async fn read_client_prefers_codex_sidecar_tmux_attach_target() {
    let state = state().await;
    let codex = state
        .identity
        .register(agent("codex", "ck_codex", hid("codex"), "/work/codex"))
        .await
        .unwrap();
    Sessions::new(&state.store)
        .set_transport(&codex.session_id, "codex-appserver")
        .await
        .unwrap();

    let sidecar = nexus_harness_codex::storage::CodexRuntimeStateRepo::new(&state.store);
    sidecar
        .upsert_launch(nexus_harness_codex::storage::CodexRuntimeLaunch {
            runtime_id: codex.session_id.clone(),
            codex_thread_id: None,
            codex_home: PathBuf::from("/tmp/codex-home"),
            app_server_sock: PathBuf::from("/tmp/codex.sock"),
            app_server_pid: None,
            mcp_sidecar_pids_json: None,
            app_server_adopted: false,
        })
        .await
        .unwrap();
    sidecar
        .set_tmux(
            &codex.session_id,
            Some("/tmp/nexus-explicit-tmux.sock"),
            "nexus-explicit-session",
        )
        .await
        .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );
    let descriptor = read.pty_attach_descriptor("codex").await.unwrap();

    assert_eq!(descriptor.backend, "tmux");
    assert_eq!(descriptor.argv[2], "/tmp/nexus-explicit-tmux.sock");
    assert_eq!(descriptor.argv[5], "nexus-explicit-session");
    assert_eq!(
        descriptor.liveness_argv.as_ref().unwrap(),
        &vec![
            "tmux".to_string(),
            "-S".to_string(),
            "/tmp/nexus-explicit-tmux.sock".to_string(),
            "has-session".to_string(),
            "-t".to_string(),
            "nexus-explicit-session".to_string(),
        ]
    );
}

#[tokio::test]
async fn read_client_prefers_codex_raw_terminal_manifest_over_sidecar_tmux() {
    let state = state().await;
    let codex = state
        .identity
        .register(agent("codex", "ck_codex", hid("codex"), "/work/codex"))
        .await
        .unwrap();
    Sessions::new(&state.store)
        .set_transport(&codex.session_id, "codex-appserver")
        .await
        .unwrap();

    let sidecar = nexus_harness_codex::storage::CodexRuntimeStateRepo::new(&state.store);
    sidecar
        .upsert_launch(nexus_harness_codex::storage::CodexRuntimeLaunch {
            runtime_id: codex.session_id.clone(),
            codex_thread_id: None,
            codex_home: PathBuf::from("/tmp/codex-home"),
            app_server_sock: PathBuf::from("/tmp/codex.sock"),
            app_server_pid: None,
            mcp_sidecar_pids_json: None,
            app_server_adopted: false,
        })
        .await
        .unwrap();
    sidecar
        .set_tmux(
            &codex.session_id,
            Some("/tmp/nexus-explicit-tmux.sock"),
            "nexus-explicit-session",
        )
        .await
        .unwrap();

    let dir = std::env::temp_dir().join(format!(
        "nexus-store-read-client-attach-{}",
        codex.session_id.0
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.display().to_string();
    let _env = nexus::cli::ambient::TestEnvGuard::new(&[(
        "NEXUS_TERMINAL_MANIFEST_DIR",
        Some(dir_s.as_str()),
    )]);
    let manifest_path =
        nexus::daemon::terminal_socket::terminal_endpoint_manifest_path(&codex.session_id);
    std::fs::create_dir_all(manifest_path.parent().unwrap()).unwrap();
    std::fs::write(
        &manifest_path,
        format!(
            r#"{{"session_id":"{}","path":"/tmp/nexus-terminal-test.sock","token":"tok"}}"#,
            codex.session_id.0
        ),
    )
    .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );
    let descriptor = read.pty_attach_descriptor("codex").await.unwrap();

    assert_eq!(descriptor.backend, "raw-pty");
    assert_eq!(descriptor.argv[1], "terminal-client");
    assert_eq!(descriptor.argv[2], codex.session_id.0);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn read_client_rejects_non_headed_runtime_for_attach_descriptor() {
    let state = state().await;
    let ben = state
        .identity
        .register(human("ben", "ck_ben"))
        .await
        .unwrap();
    Sessions::new(&state.store)
        .set_transport(&ben.session_id, "acp")
        .await
        .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );
    let err = read.pty_attach_descriptor("ben").await.unwrap_err();

    assert_eq!(err.code, codes::INVALID_PARAMS);
}

#[tokio::test]
async fn read_client_builds_claude_revive_plan_from_dead_headed_row() {
    let state = state().await;
    let hugo = state
        .identity
        .register(agent("hugo", "ck_hugo", hid("claude"), "/work/egregore"))
        .await
        .unwrap();
    let sessions = Sessions::new(&state.store);
    sessions
        .set_transport(&hugo.session_id, "pty")
        .await
        .unwrap();
    sessions
        .set_harness_session_id(&hugo.session_id, "claude-session-1")
        .await
        .unwrap();
    sessions
        .set_presence(&hugo.session_id, Presence::Offline)
        .await
        .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );
    let plan = read.attach_revive_plan("hugo").await.unwrap();
    let plan_by_session = read.attach_revive_plan(&hugo.session_id.0).await.unwrap();

    assert_eq!(plan.name, "hugo");
    assert_eq!(plan_by_session, plan);
    assert_eq!(plan.spawn.kind, hid("claude"));
    assert_eq!(plan.spawn.name.as_deref(), Some("hugo"));
    assert_eq!(plan.spawn.project.as_deref(), Some("default"));
    assert_eq!(plan.spawn.cwd.as_deref(), Some("/work/egregore"));
    assert_eq!(plan.spawn.resume, None);
    assert_eq!(plan.spawn.harness_args, ["--resume", "claude-session-1"]);
    assert_eq!(
        plan.spawn.backend.as_deref(),
        Some("pty"),
        "attach revive must preserve the raw-PTY launch default instead of falling through to tmux"
    );
    assert!(!plan.spawn.headless);
}

#[tokio::test]
async fn read_client_harvests_missing_claude_resume_id_before_revive_failure() {
    let state = state().await;
    let tmp = tempfile::tempdir().unwrap();
    let bridge_dir = tmp.path().join("claude-sessions/s_hugo/bridge");
    let hugo = state
        .identity
        .register(agent("hugo", "ck_hugo", hid("claude"), "/work/egregore"))
        .await
        .unwrap();
    let sessions = Sessions::new(&state.store);
    sessions
        .set_transport(&hugo.session_id, "pty")
        .await
        .unwrap();
    sessions
        .clear_harness_session_id(&hugo.session_id)
        .await
        .unwrap();
    sessions
        .set_presence(&hugo.session_id, Presence::Offline)
        .await
        .unwrap();
    ClaudeRuntimeStateRepo::new(&state.store)
        .upsert_launch(ClaudeRuntimeLaunch {
            runtime_id: hugo.session_id.clone(),
            bridge_dir: bridge_dir.clone(),
            claude_session_id: None,
            launch_cwd: PathBuf::from("/work/egregore"),
            transcript_path: None,
            bridge_pid: None,
            hook_pids_json: None,
        })
        .await
        .unwrap();
    write_claude_hooks(
        &bridge_dir,
        r#"{"event":"SessionStart","payload":{"session_id":"claude-native-inline","transcript_path":"/tmp/claude-inline.jsonl"}}"#,
    );

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );
    let plan = read.attach_revive_plan("hugo").await.unwrap();
    let state = ClaudeRuntimeStateRepo::new(&state.store)
        .find_by_runtime_id(&hugo.session_id)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(plan.spawn.kind, hid("claude"));
    assert_eq!(
        plan.spawn.harness_args,
        ["--resume", "claude-native-inline"]
    );
    assert_eq!(
        state.claude_session_id.as_deref(),
        Some("claude-native-inline")
    );
    assert_eq!(
        state.transcript_path,
        Some(PathBuf::from("/tmp/claude-inline.jsonl"))
    );
}

#[tokio::test]
async fn read_client_refuses_ambiguous_claude_resume_harvest_before_revive() {
    let state = state().await;
    let tmp = tempfile::tempdir().unwrap();
    let bridge_dir = tmp.path().join("claude-sessions/s_hugo/bridge");
    let hugo = state
        .identity
        .register(agent("hugo", "ck_hugo", hid("claude"), "/work/egregore"))
        .await
        .unwrap();
    let sessions = Sessions::new(&state.store);
    sessions
        .set_transport(&hugo.session_id, "pty")
        .await
        .unwrap();
    sessions
        .clear_harness_session_id(&hugo.session_id)
        .await
        .unwrap();
    sessions
        .set_presence(&hugo.session_id, Presence::Offline)
        .await
        .unwrap();
    ClaudeRuntimeStateRepo::new(&state.store)
        .upsert_launch(ClaudeRuntimeLaunch {
            runtime_id: hugo.session_id.clone(),
            bridge_dir: bridge_dir.clone(),
            claude_session_id: None,
            launch_cwd: PathBuf::from("/work/egregore"),
            transcript_path: None,
            bridge_pid: None,
            hook_pids_json: None,
        })
        .await
        .unwrap();
    write_claude_hooks(
        &bridge_dir,
        "\
{\"event\":\"SessionStart\",\"payload\":{\"session_id\":\"claude-native-a\"}}\n\
{\"event\":\"Stop\",\"payload\":{\"session_id\":\"claude-native-b\"}}\n",
    );

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );
    let err = read.attach_revive_plan("hugo").await.unwrap_err();
    let state = ClaudeRuntimeStateRepo::new(&state.store)
        .find_by_runtime_id(&hugo.session_id)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(err.code, codes::INVALID_PARAMS);
    assert!(
        err.message.contains("multiple Claude session ids"),
        "ambiguous bridge log must fail closed with a one-line revive truth: {}",
        err.message
    );
    assert!(state.claude_session_id.is_none());
}

#[tokio::test]
async fn read_client_does_not_treat_duplicate_claude_resume_hint_as_identity_conflict() {
    let state = state().await;
    let clara = state
        .identity
        .register(agent("clara", "ck_clara", hid("claude"), "/work/lens"))
        .await
        .unwrap();
    let alan = state
        .identity
        .register(agent("alan", "ck_alan", hid("claude"), "/work/lens"))
        .await
        .unwrap();
    let sessions = Sessions::new(&state.store);
    for session_id in [&clara.session_id, &alan.session_id] {
        sessions.set_transport(session_id, "pty").await.unwrap();
        sessions.clear_harness_session_id(session_id).await.unwrap();
        sessions
            .set_presence(session_id, Presence::Offline)
            .await
            .unwrap();
        ClaudeRuntimeStateRepo::new(&state.store)
            .upsert_launch(ClaudeRuntimeLaunch {
                runtime_id: session_id.clone(),
                bridge_dir: PathBuf::from(format!("/tmp/{}/bridge", session_id.0)),
                claude_session_id: None,
                launch_cwd: PathBuf::from("/work/lens"),
                transcript_path: None,
                bridge_pid: None,
                hook_pids_json: None,
            })
            .await
            .unwrap();
    }
    state
        .store
        .conn
        .execute(
            "UPDATE claude_runtime_state
             SET claude_session_id = ?1
             WHERE runtime_id IN (?2, ?3)",
            params![
                "claude-native-duplicate",
                clara.session_id.0,
                alan.session_id.0,
            ],
        )
        .await
        .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );
    let plan = read.attach_revive_plan("clara").await.unwrap();

    assert_eq!(plan.spawn.kind, hid("claude"));
    assert_eq!(
        plan.spawn.harness_args,
        ["--resume", "claude-native-duplicate"],
        "Claude's native id remains an opaque best-effort resume hint"
    );
}

#[tokio::test]
async fn read_client_uses_codex_resume_key_for_revive_plan() {
    let state = state().await;
    let rex = state
        .identity
        .register(agent("rex", "ck_rex", hid("codex"), "/work/codex"))
        .await
        .unwrap();
    let sessions = Sessions::new(&state.store);
    sessions
        .set_transport(&rex.session_id, "codex-appserver")
        .await
        .unwrap();
    sessions
        .set_harness_session_id(&rex.session_id, "codex-thread-7")
        .await
        .unwrap();
    sessions
        .set_presence(&rex.session_id, Presence::Offline)
        .await
        .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );
    let plan = read.attach_revive_plan("rex").await.unwrap();

    assert_eq!(plan.spawn.kind, hid("codex"));
    assert_eq!(plan.spawn.resume.as_deref(), Some("codex-thread-7"));
    assert!(plan.spawn.harness_args.is_empty());
}

#[tokio::test]
async fn read_client_rejects_codex_revive_when_harness_session_id_is_nexus_session_id() {
    let state = state().await;
    let otto = state
        .identity
        .register(agent("otto", "ck_otto", hid("codex"), "/work/codex"))
        .await
        .unwrap();
    let sessions = Sessions::new(&state.store);
    sessions
        .set_transport(&otto.session_id, "codex-appserver")
        .await
        .unwrap();
    sessions
        .set_harness_session_id(&otto.session_id, &otto.session_id.0)
        .await
        .unwrap();
    sessions
        .set_presence(&otto.session_id, Presence::Offline)
        .await
        .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );
    let err = read.attach_revive_plan("otto").await.unwrap_err();

    assert_eq!(err.code, codes::INVALID_PARAMS);
    assert!(
        err.message.contains("stored thread id"),
        "Codex attach must fail closed instead of using a Nexus session id as a native thread id: {}",
        err.message
    );
}

#[tokio::test]
async fn read_client_uses_codex_sidecar_thread_for_revive_plan() {
    let state = state().await;
    let rex = state
        .identity
        .register(agent("rex", "ck_rex", hid("codex"), "/work/codex"))
        .await
        .unwrap();
    let sessions = Sessions::new(&state.store);
    sessions
        .set_transport(&rex.session_id, "codex-appserver")
        .await
        .unwrap();
    sessions
        .clear_harness_session_id(&rex.session_id)
        .await
        .unwrap();
    sessions
        .set_presence(&rex.session_id, Presence::Offline)
        .await
        .unwrap();
    let codex_state = nexus_harness_codex::storage::CodexRuntimeStateRepo::new(&state.store);
    codex_state
        .upsert_launch(nexus_harness_codex::storage::CodexRuntimeLaunch {
            runtime_id: rex.session_id.clone(),
            codex_thread_id: None,
            codex_home: PathBuf::from("/tmp/codex-home"),
            app_server_sock: PathBuf::from("/tmp/codex.sock"),
            app_server_pid: None,
            mcp_sidecar_pids_json: None,
            app_server_adopted: false,
        })
        .await
        .unwrap();
    codex_state
        .set_thread(&rex.session_id, "codex-thread-sidecar", None)
        .await
        .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );
    let plan = read.attach_revive_plan("rex").await.unwrap();

    assert_eq!(plan.spawn.kind, hid("codex"));
    assert_eq!(plan.spawn.resume.as_deref(), Some("codex-thread-sidecar"));
    assert!(plan.spawn.harness_args.is_empty());
}

#[tokio::test]
async fn read_client_prefers_codex_sidecar_thread_over_contaminated_harness_session_id() {
    let state = state().await;
    let otto = state
        .identity
        .register(agent("otto", "ck_otto", hid("codex"), "/work/codex"))
        .await
        .unwrap();
    let sessions = Sessions::new(&state.store);
    sessions
        .set_transport(&otto.session_id, "codex-appserver")
        .await
        .unwrap();
    sessions
        .set_harness_session_id(&otto.session_id, &otto.session_id.0)
        .await
        .unwrap();
    sessions
        .set_presence(&otto.session_id, Presence::Offline)
        .await
        .unwrap();
    let codex_state = nexus_harness_codex::storage::CodexRuntimeStateRepo::new(&state.store);
    codex_state
        .upsert_launch(nexus_harness_codex::storage::CodexRuntimeLaunch {
            runtime_id: otto.session_id.clone(),
            codex_thread_id: None,
            codex_home: PathBuf::from("/tmp/codex-home"),
            app_server_sock: PathBuf::from("/tmp/codex.sock"),
            app_server_pid: None,
            mcp_sidecar_pids_json: None,
            app_server_adopted: false,
        })
        .await
        .unwrap();
    codex_state
        .set_thread(&otto.session_id, "codex-thread-real", None)
        .await
        .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );
    let plan = read.attach_revive_plan("otto").await.unwrap();

    assert_eq!(plan.spawn.kind, hid("codex"));
    assert_eq!(plan.spawn.resume.as_deref(), Some("codex-thread-real"));
    assert!(plan.spawn.harness_args.is_empty());
}

#[tokio::test]
async fn read_client_prefers_identity_owned_codex_thread_over_sidecar_thread() {
    let state = state().await;
    let rex = state
        .identity
        .register(agent("rex", "ck_rex", hid("codex"), "/work/codex"))
        .await
        .unwrap();
    let agent_id = rex.agent_id.as_ref().expect("agent id").0.clone();
    let sessions = Sessions::new(&state.store);
    sessions
        .set_transport(&rex.session_id, "codex-appserver")
        .await
        .unwrap();
    sessions
        .clear_harness_session_id(&rex.session_id)
        .await
        .unwrap();
    sessions
        .set_presence(&rex.session_id, Presence::Offline)
        .await
        .unwrap();
    let codex_state = nexus_harness_codex::storage::CodexRuntimeStateRepo::new(&state.store);
    codex_state
        .upsert_launch(nexus_harness_codex::storage::CodexRuntimeLaunch {
            runtime_id: rex.session_id.clone(),
            codex_thread_id: None,
            codex_home: PathBuf::from("/tmp/codex-home"),
            app_server_sock: PathBuf::from("/tmp/codex.sock"),
            app_server_pid: None,
            mcp_sidecar_pids_json: None,
            app_server_adopted: false,
        })
        .await
        .unwrap();
    codex_state
        .set_thread(&rex.session_id, "codex-thread-sidecar", None)
        .await
        .unwrap();
    NativeThreadBindings::new(&state.store)
        .mark_released_for_runtime("codex", &rex.session_id.0)
        .await
        .unwrap();
    NativeThreadBindings::new(&state.store)
        .claim(NewNativeThreadBinding {
            harness: "codex".into(),
            native_thread_id: "codex-thread-identity".into(),
            agent_id,
            project: "default".into(),
            runtime_id: Some(rex.session_id.0.clone()),
        })
        .await
        .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );
    let plan = read.attach_revive_plan("rex").await.unwrap();

    assert_eq!(plan.spawn.kind, hid("codex"));
    assert_eq!(plan.spawn.resume.as_deref(), Some("codex-thread-identity"));
    assert!(plan.spawn.harness_args.is_empty());
}

#[tokio::test]
async fn read_client_uses_native_opencode_session_for_revive_plan() {
    let state = state().await;
    let ada = state
        .identity
        .register(agent("ada", "ck_ada", hid("opencode"), "/work/opencode"))
        .await
        .unwrap();
    let sessions = Sessions::new(&state.store);
    sessions
        .set_transport(&ada.session_id, "opencode-plugin")
        .await
        .unwrap();
    sessions
        .set_harness_session_id(&ada.session_id, "ses_opencode_1")
        .await
        .unwrap();
    sessions
        .set_presence(&ada.session_id, Presence::Offline)
        .await
        .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );
    let plan = read.attach_revive_plan("ada").await.unwrap();

    assert_eq!(plan.spawn.kind, hid("opencode"));
    assert_eq!(plan.spawn.harness_args, ["-s", "ses_opencode_1"]);
}

#[tokio::test]
async fn read_client_uses_native_hermes_session_for_revive_plan() {
    let state = state().await;
    let ada = state
        .identity
        .register(agent("ada", "ck_ada", hid("hermes"), "/work/hermes"))
        .await
        .unwrap();
    let sessions = Sessions::new(&state.store);
    sessions
        .set_transport(&ada.session_id, "pty")
        .await
        .unwrap();
    sessions
        .clear_harness_session_id(&ada.session_id)
        .await
        .unwrap();
    sessions
        .set_presence(&ada.session_id, Presence::Offline)
        .await
        .unwrap();
    nexus::daemon::hermes_native_forwarder::HermesRuntimeStateRepo::new(&state.store)
        .upsert_launch(
            nexus::daemon::hermes_native_forwarder::HermesRuntimeLaunch {
                runtime_id: ada.session_id.clone(),
                hermes_db_path: PathBuf::from("/tmp/hermes/state.db"),
                hermes_session_id: Some("hermes-session-9".into()),
                launch_cwd: PathBuf::from("/work/hermes"),
                acp_child_pid: None,
                viewer_backend: "pty".into(),
            },
        )
        .await
        .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );
    let plan = read.attach_revive_plan("ada").await.unwrap();

    assert_eq!(plan.spawn.kind, hid("hermes"));
    assert_eq!(plan.spawn.harness_args, ["--session", "hermes-session-9"]);
}

#[tokio::test]
async fn read_client_builds_claude_revive_plan_from_acp_row() {
    let state = state().await;
    let ada = state
        .identity
        .register(agent("ada", "ck_ada", hid("claude"), "/work/acp"))
        .await
        .unwrap();
    let sessions = Sessions::new(&state.store);
    sessions
        .set_transport(&ada.session_id, "acp")
        .await
        .unwrap();
    sessions
        .set_harness_session_id(&ada.session_id, "claude-acp-session-1")
        .await
        .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );
    let plan = read.attach_revive_plan("ada").await.unwrap();

    assert_eq!(plan.name, "ada");
    assert_eq!(plan.spawn.kind, hid("claude"));
    assert_eq!(plan.spawn.name.as_deref(), Some("ada"));
    assert_eq!(plan.spawn.cwd.as_deref(), Some("/work/acp"));
    assert_eq!(plan.spawn.resume, None);
    assert_eq!(
        plan.spawn.harness_args,
        ["--resume", "claude-acp-session-1"]
    );
    assert!(!plan.spawn.headless);
}

#[tokio::test]
async fn read_client_does_not_build_headed_revive_plan_for_non_claude_acp_row() {
    let state = state().await;
    let rex = state
        .identity
        .register(agent("rex", "ck_rex", hid("codex"), "/work/acp"))
        .await
        .unwrap();
    Sessions::new(&state.store)
        .set_transport(&rex.session_id, "acp")
        .await
        .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    );
    let err = read.attach_revive_plan("rex").await.unwrap_err();

    assert_eq!(err.code, codes::INVALID_PARAMS);
    assert!(err.message.contains("headed"));
}

fn tmux_session_name_for_test(session_id: &str) -> String {
    let safe: String = session_id
        .chars()
        .map(|ch| match ch {
            '.' | ':' => '-',
            other => other,
        })
        .collect();
    format!("nexus-{safe}")
}

#[tokio::test]
async fn members_hides_dead_agents_unless_include_dead() {
    let state = state().await;
    let zed = state
        .identity
        .register(agent("zed", "ck_zed", hid("claude"), "/tmp"))
        .await
        .unwrap();
    let agent_id = zed.agent_id.expect("registered agent id");
    nexus_store::repos::Agents::new(&state.store)
        .mark_dead(&agent_id.0, "revive_exhausted")
        .await
        .unwrap();

    let read = ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "zed",
        "default",
        Some(zed.session_id.0.clone()),
        Some("ck_zed".into()),
        Tier::Admin,
    );

    // Default roster (even with offline rows) excludes the dead agent...
    let default_view = read
        .members(MemberListRequest {
            include_offline: Some(true),
            include_dead: None,
        })
        .await
        .unwrap();
    assert!(
        !default_view
            .members
            .iter()
            .any(|m| m.name.as_deref() == Some("zed")),
        "dead agent must leave the default roster"
    );

    // ...and the audit view names it, with lifecycle + reason.
    let audit_view = read
        .members(MemberListRequest {
            include_offline: Some(true),
            include_dead: Some(true),
        })
        .await
        .unwrap();
    let row = audit_view
        .members
        .iter()
        .find(|m| m.name.as_deref() == Some("zed"))
        .expect("audit view includes the dead agent");
    assert_eq!(row.lifecycle_state.as_deref(), Some("dead"));
    assert_eq!(row.dead_reason.as_deref(), Some("revive_exhausted"));
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
