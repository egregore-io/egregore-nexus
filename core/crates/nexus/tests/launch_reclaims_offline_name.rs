use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use nexus::cli::read_client::ReadClient;
use nexus::daemon::opencode_native_forwarder::{OpenCodeRuntimeLaunch, OpenCodeRuntimeStateRepo};
use nexus::daemon::{AppState, WsSink};
use nexus_common::Config;
use nexus_contracts::{
    Ack, AckRequest, AckResponse, AckThreadsRequest, AgentId, AssignProjectRequest,
    AssignProjectResponse, AssignRoleRequest, AssignRoleResponse, Caller, ChannelRequest,
    ConsumeRequest, CreateThreadRequest, HeartbeatResponse, HistoryRequest, HistoryResponse,
    JoinThreadRequest, LeaveThreadRequest, MemberListRequest, MemberListResponse, MessageId,
    MonitorRequest, NexusBatch, NotifyRequest, NotifyResponse, PortResult, RegisterRequest,
    RegisterResponse, RemoveRequest, RemoveResponse, RouteForwardRequest, SearchRequest,
    SearchResponse, SendRequest, SessionId, SpawnRequest, SpawnResponse, StatusRequest,
    StatusResponse, SubscribeRequest, SubscribeResponse, ThreadListResponse, ThreadMemberRequest,
    ThreadMembersRequest, ThreadMembersResponse, TopicListResponse, UnsubscribeRequest, Whoami,
};
use nexus_contracts::{SpawnIdentityPolicy, Tier};
use nexus_harness_claude::storage::{ClaudeRuntimeLaunch, ClaudeRuntimeStateRepo};
use nexus_harness_codex::storage::{CodexRuntimeLaunch, CodexRuntimeStateRepo};
use nexus_store::repos::{
    AgentRuntimes, Agents, NativeThreadBindings, NewAgent, NewAgentRuntime, NewNativeThreadBinding,
    NewSession, Sessions, TranscriptArchive,
};
use nexus_store::Store;

const PROJECT: &str = "default";
const AGENT_NAME: &str = "offline-codex";
const OLD_SESSION: &str = "s_offline_old";
const NEW_SESSION: &str = "s_launched_new";
const CODEX_THREAD: &str = "00000000-0000-4000-8000-000000000103";
const CLAUDE_NATIVE_SESSION: &str = "claude-native-owned";
const OPENCODE_NATIVE_SESSION: &str = "ses_native_owned";

fn temp_dir(label: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("nexus-launch-reclaim-{label}-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn passive_pty_program() -> &'static str {
    #[cfg(windows)]
    {
        "cmd.exe"
    }
    #[cfg(not(windows))]
    {
        "cat"
    }
}

struct MockAgent;

#[async_trait]
impl nexus_contracts::AgentTurnExecutionPort for MockAgent {
    async fn inject_turn(&self, _recipient: &SessionId, _batch: &NexusBatch) -> PortResult<()> {
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        Ok(SpawnResponse {
            session_id: SessionId(NEW_SESSION.into()),
        })
    }

    async fn remove(&self, req: RemoveRequest) -> PortResult<RemoveResponse> {
        Ok(RemoveResponse {
            name: Some(req.name),
            status: "removed".into(),
        })
    }

    fn is_harness_alive(&self, recipient: &SessionId) -> Option<bool> {
        Some(recipient.0 != OLD_SESSION)
    }
}

struct MockIdentity;

#[async_trait]
impl nexus_contracts::IdentityPort for MockIdentity {
    async fn register(&self, _req: RegisterRequest) -> PortResult<RegisterResponse> {
        unimplemented!()
    }

    async fn whoami(&self, _caller: &Caller) -> PortResult<Whoami> {
        unimplemented!()
    }

    async fn resolve(&self, _project: &str, _name: &str) -> PortResult<Caller> {
        unimplemented!()
    }

    async fn members(
        &self,
        _caller: &Caller,
        _req: MemberListRequest,
    ) -> PortResult<MemberListResponse> {
        unimplemented!()
    }

    async fn status(&self, _caller: &Caller, _req: StatusRequest) -> PortResult<StatusResponse> {
        unimplemented!()
    }

    async fn heartbeat(&self, _caller: &Caller) -> PortResult<HeartbeatResponse> {
        unimplemented!()
    }

    async fn assign_project(
        &self,
        _name: &str,
        _to_project: &str,
    ) -> PortResult<AssignProjectResponse> {
        unimplemented!()
    }
}

struct MockBus;

#[async_trait]
impl nexus_contracts::BusPort for MockBus {
    async fn preflight_send(&self, _caller: &Caller, _req: &SendRequest) -> PortResult<()> {
        Ok(())
    }

    async fn preflight_notify_target(
        &self,
        _caller: &Caller,
        target: &nexus_contracts::NotifyTarget,
    ) -> PortResult<Option<nexus_contracts::AgentId>> {
        Ok(match target {
            nexus_contracts::NotifyTarget::Agent { agent_id } => Some(agent_id.clone()),
            _ => None,
        })
    }

    async fn send(&self, _caller: &Caller, _req: SendRequest) -> PortResult<Ack> {
        Ok(Ack {
            message_id: MessageId("m_test".into()),
            fanout: None,
        })
    }

    async fn create_thread(&self, _caller: &Caller, _req: CreateThreadRequest) -> PortResult<()> {
        unimplemented!()
    }

    async fn join_thread(&self, _caller: &Caller, _req: JoinThreadRequest) -> PortResult<()> {
        unimplemented!()
    }

    async fn leave_thread(&self, _caller: &Caller, _req: LeaveThreadRequest) -> PortResult<()> {
        unimplemented!()
    }

    async fn add_thread_member(
        &self,
        _caller: &Caller,
        _req: ThreadMemberRequest,
    ) -> PortResult<()> {
        unimplemented!()
    }

    async fn remove_thread_member(
        &self,
        _caller: &Caller,
        _req: ThreadMemberRequest,
    ) -> PortResult<()> {
        unimplemented!()
    }

    async fn threads(&self, _caller: &Caller) -> PortResult<ThreadListResponse> {
        unimplemented!()
    }

    async fn thread_members(
        &self,
        _caller: &Caller,
        _req: ThreadMembersRequest,
    ) -> PortResult<ThreadMembersResponse> {
        unimplemented!()
    }

    async fn subscribe(
        &self,
        _caller: &Caller,
        _req: SubscribeRequest,
    ) -> PortResult<SubscribeResponse> {
        unimplemented!()
    }

    async fn unsubscribe(&self, _caller: &Caller, _req: UnsubscribeRequest) -> PortResult<()> {
        unimplemented!()
    }

    async fn topics(&self, _caller: &Caller) -> PortResult<TopicListResponse> {
        unimplemented!()
    }
}

struct MockRealtime;

#[async_trait]
impl nexus_contracts::DispatchPort for MockRealtime {
    async fn enqueue(&self, _recipient: &SessionId, _message: &MessageId) -> PortResult<()> {
        Ok(())
    }

    async fn consume(&self, _caller: &Caller, _req: ConsumeRequest) -> PortResult<NexusBatch> {
        unimplemented!()
    }

    async fn ack(&self, _caller: &Caller, _req: AckRequest) -> PortResult<AckResponse> {
        unimplemented!()
    }

    async fn ack_threads(
        &self,
        _caller: &Caller,
        _req: AckThreadsRequest,
    ) -> PortResult<AckResponse> {
        unimplemented!()
    }
}

struct MockSearch;

#[async_trait]
impl nexus_contracts::SearchPort for MockSearch {
    async fn search(&self, _caller: &Caller, _req: SearchRequest) -> PortResult<SearchResponse> {
        unimplemented!()
    }

    async fn history(&self, _caller: &Caller, _req: HistoryRequest) -> PortResult<HistoryResponse> {
        unimplemented!()
    }
}

struct MockNotify;

#[async_trait]
impl nexus_contracts::NotifyPort for MockNotify {
    async fn ingest(&self, _req: NotifyRequest, _hmac_ok: bool) -> PortResult<NotifyResponse> {
        unimplemented!()
    }

    async fn forward(&self, _caller: &Caller, _req: RouteForwardRequest) -> PortResult<()> {
        unimplemented!()
    }

    async fn channel(&self, _caller: &Caller, _req: ChannelRequest) -> PortResult<()> {
        unimplemented!()
    }
}

struct MockAdmin;

#[async_trait]
impl nexus_contracts::AdminPort for MockAdmin {
    async fn spawn(&self, _caller: &Caller, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        unimplemented!()
    }

    async fn remove(&self, _caller: &Caller, _req: RemoveRequest) -> PortResult<RemoveResponse> {
        unimplemented!()
    }

    async fn assign_role(
        &self,
        _caller: &Caller,
        _req: AssignRoleRequest,
    ) -> PortResult<AssignRoleResponse> {
        unimplemented!()
    }

    async fn channel(&self, _caller: &Caller, _req: ChannelRequest) -> PortResult<()> {
        unimplemented!()
    }

    async fn route(&self, _caller: &Caller, _req: RouteForwardRequest) -> PortResult<()> {
        unimplemented!()
    }

    async fn monitor(&self, _caller: &Caller, _req: MonitorRequest) -> PortResult<()> {
        unimplemented!()
    }

    async fn assign_project(
        &self,
        _caller: &Caller,
        _req: AssignProjectRequest,
    ) -> PortResult<AssignProjectResponse> {
        unimplemented!()
    }
}

async fn state_with_store(store: Arc<Store>) -> AppState {
    AppState::new(
        store,
        WsSink::new(16, None),
        Arc::new(MockIdentity),
        Arc::new(MockAgent),
        Arc::new(MockRealtime),
        Arc::new(MockBus),
        Arc::new(MockSearch),
        Arc::new(MockNotify),
        Arc::new(MockAdmin),
        PROJECT.into(),
    )
}

fn offline_row() -> NewSession {
    NewSession {
        session_id: SessionId(OLD_SESSION.into()),
        name: Some(AGENT_NAME.into()),
        agent: Some("codex".into()),
        kind: "agent".into(),
        role: None,
        tier: "agent".into(),
        harness_session_id: Some("old-codex-thread".into()),
        client_key: Some(OLD_SESSION.into()),
        cwd: Some("/tmp/old-cwd".into()),
        project: PROJECT.into(),
        transport: Some("codex-appserver".into()),
    }
}

fn spawn_request() -> SpawnRequest {
    SpawnRequest {
        kind: hid("codex"),
        name: Some(AGENT_NAME.into()),
        identity_policy: None,
        cwd: Some("/tmp/new-cwd".into()),
        project: Some(PROJECT.into()),
        role: None,
        initial_prompt: None,
        resume: Some("new-codex-thread".into()),
        harness_args: Vec::new(),
        headless: true,
        backend: None,
    }
}

fn id_launch_request(agent_id: &str) -> SpawnRequest {
    SpawnRequest {
        kind: hid("codex"),
        name: Some(agent_id.into()),
        identity_policy: None,
        cwd: Some("/tmp/should-not-replace-existing-cwd".into()),
        project: Some(PROJECT.into()),
        role: None,
        initial_prompt: None,
        resume: Some("should-not-replace-existing-thread".into()),
        harness_args: Vec::new(),
        headless: true,
        backend: None,
    }
}

fn new_agent(agent_id: &str, name: &str) -> NewAgent {
    NewAgent {
        agent_id: agent_id.into(),
        project: PROJECT.into(),
        name: Some(name.into()),
        default_harness: Some("codex".into()),
        role: None,
        tier: Some("agent".into()),
        owner: None,
    }
}

fn active_runtime(runtime_id: &str, agent_id: &str, cwd: &str, transport: &str) -> NewAgentRuntime {
    NewAgentRuntime {
        runtime_id: runtime_id.into(),
        agent_id: agent_id.into(),
        harness: "codex".into(),
        cwd: Some(cwd.into()),
        transport: Some(transport.into()),
        presence: Some("offline".into()),
        active: true,
    }
}

fn session_row(
    session_id: &str,
    name: &str,
    harness: &str,
    cwd: &str,
    transport: &str,
) -> NewSession {
    NewSession {
        session_id: SessionId(session_id.into()),
        name: Some(name.into()),
        agent: Some(harness.into()),
        kind: "agent".into(),
        role: None,
        tier: "agent".into(),
        harness_session_id: Some("native-resume-key".into()),
        client_key: Some(format!("ck_{session_id}")),
        cwd: Some(cwd.into()),
        project: PROJECT.into(),
        transport: Some(transport.into()),
    }
}

async fn stamp_session_agent_id(store: &Store, session_id: &str, agent_id: &str) {
    store
        .conn
        .execute(
            "UPDATE sessions SET agent_id = ?2 WHERE session_id = ?1",
            libsql::params![session_id, agent_id],
        )
        .await
        .unwrap();
}

fn codex_resume_request(name: &str) -> SpawnRequest {
    SpawnRequest {
        kind: hid("codex"),
        name: Some(name.into()),
        identity_policy: None,
        cwd: Some("/tmp/codex-cwd".into()),
        project: Some(PROJECT.into()),
        role: None,
        initial_prompt: None,
        resume: Some(CODEX_THREAD.into()),
        harness_args: Vec::new(),
        headless: false,
        backend: None,
    }
}

fn codex_resume_request_without_name() -> SpawnRequest {
    SpawnRequest {
        kind: hid("codex"),
        name: None,
        identity_policy: Some(SpawnIdentityPolicy::Implicit),
        cwd: Some("/tmp/codex-cwd".into()),
        project: Some(PROJECT.into()),
        role: None,
        initial_prompt: None,
        resume: Some(CODEX_THREAD.into()),
        harness_args: Vec::new(),
        headless: false,
        backend: None,
    }
}

fn claude_resume_request(name: &str) -> SpawnRequest {
    SpawnRequest {
        kind: hid("claude"),
        name: Some(name.into()),
        identity_policy: None,
        cwd: Some("/tmp/claude-cwd".into()),
        project: Some(PROJECT.into()),
        role: None,
        initial_prompt: None,
        resume: None,
        harness_args: vec!["--resume".into(), CLAUDE_NATIVE_SESSION.into()],
        headless: false,
        backend: None,
    }
}

fn opencode_resume_request(name: &str) -> SpawnRequest {
    SpawnRequest {
        kind: hid("opencode"),
        name: Some(name.into()),
        identity_policy: None,
        cwd: Some("/tmp/opencode-cwd".into()),
        project: Some(PROJECT.into()),
        role: None,
        initial_prompt: None,
        resume: None,
        harness_args: vec!["--session".into(), OPENCODE_NATIVE_SESSION.into()],
        headless: false,
        backend: None,
    }
}

#[tokio::test]
async fn launch_reclaims_a_dead_name_for_the_new_session() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let repo = Sessions::new(&store);
    repo.create(offline_row()).await.unwrap();

    let state = state_with_store(store.clone()).await;
    let response = state
        .launch_agent(spawn_request(), PROJECT, None)
        .await
        .unwrap();

    assert_eq!(response.session_id.0, NEW_SESSION);

    let row = repo
        .find_by_name(PROJECT, AGENT_NAME)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.session_id.0, NEW_SESSION);
    let client_key = row.client_key.as_deref().expect("client key");
    assert!(client_key.starts_with("nexus_ck_"));
    assert_ne!(client_key, NEW_SESSION);
    assert_eq!(row.presence.as_deref(), Some("online"));
    assert_eq!(row.agent.as_deref(), Some("codex"));
    assert_eq!(row.kind, "agent");
    assert_eq!(row.tier, "agent");
    assert_eq!(row.cwd.as_deref(), Some("/tmp/new-cwd"));
    assert_eq!(row.transport.as_deref(), Some("acp"));

    let agent = Agents::new(&store)
        .find_by_name(AGENT_NAME)
        .await
        .unwrap()
        .expect("stable agent exists");
    assert_eq!(agent.project, PROJECT);
    assert_eq!(agent.default_harness.as_deref(), Some("codex"));

    let runtime = AgentRuntimes::new(&store)
        .active_for_agent(&agent.agent_id)
        .await
        .unwrap()
        .expect("stable active runtime exists");
    assert_eq!(runtime.runtime_id, NEW_SESSION);
    assert_eq!(runtime.harness, "codex");
    assert_eq!(runtime.cwd.as_deref(), Some("/tmp/new-cwd"));
    assert_eq!(runtime.transport.as_deref(), Some("acp"));
}

#[tokio::test]
async fn launch_with_agent_id_reuses_existing_active_runtime() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    Agents::new(&store)
        .create(new_agent("a_ada", "ada-renamed"))
        .await
        .unwrap();
    AgentRuntimes::new(&store)
        .create(active_runtime(
            "s_ada_existing",
            "a_ada",
            "/tmp/original-cwd",
            "acp",
        ))
        .await
        .unwrap();
    Sessions::new(&store)
        .create(session_row(
            "s_ada_existing",
            "ada-renamed",
            "codex",
            "/tmp/original-cwd",
            "acp",
        ))
        .await
        .unwrap();
    stamp_session_agent_id(&store, "s_ada_existing", "a_ada").await;

    let state = state_with_store(store.clone()).await;
    let response = state
        .launch_agent(id_launch_request("a_ada"), PROJECT, None)
        .await
        .unwrap();

    assert_eq!(
        response.session_id.0, "s_ada_existing",
        "launch by agent_id revives the active runtime instead of minting a new identity"
    );
    assert!(Agents::new(&store)
        .find_by_name("a_ada")
        .await
        .unwrap()
        .is_none());
    let runtime = AgentRuntimes::new(&store)
        .active_for_agent("a_ada")
        .await
        .unwrap()
        .expect("active runtime remains attached to identity");
    assert_eq!(runtime.runtime_id, "s_ada_existing");
    assert_eq!(runtime.cwd.as_deref(), Some("/tmp/original-cwd"));
}

#[tokio::test]
async fn explicit_launch_rejects_cross_project_name_collision() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    Agents::new(&store)
        .create(NewAgent {
            project: "other-project".into(),
            ..new_agent("a_dylan_other", "dylan")
        })
        .await
        .unwrap();

    let state = state_with_store(store.clone()).await;
    let err = state
        .launch_agent(
            SpawnRequest {
                kind: hid("claude"),
                name: Some("dylan".into()),
                identity_policy: Some(SpawnIdentityPolicy::Implicit),
                cwd: Some("/tmp/implicit-cwd".into()),
                project: Some(PROJECT.into()),
                role: None,
                initial_prompt: None,
                resume: None,
                harness_args: Vec::new(),
                headless: true,
                backend: None,
            },
            PROJECT,
            None,
        )
        .await
        .expect_err("explicit launch name must honor global name uniqueness");

    assert_eq!(err.code, nexus_contracts::codes::DUPLICATE_NAME);
    assert!(err.message.contains("name already bound: dylan"));
    assert!(Sessions::new(&store)
        .find_by_session_id(&SessionId(NEW_SESSION.into()))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn attach_revive_plan_resolves_agent_id_before_stale_name() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let agents = Agents::new(&store);
    agents
        .create(new_agent("a_ada", "ada-renamed"))
        .await
        .unwrap();
    agents
        .create(new_agent("a_stale_label", "a_ada"))
        .await
        .unwrap();
    let runtimes = AgentRuntimes::new(&store);
    runtimes
        .create(active_runtime(
            "s_ada_existing",
            "a_ada",
            "/tmp/original-cwd",
            "pty",
        ))
        .await
        .unwrap();
    runtimes
        .create(active_runtime(
            "s_stale_label",
            "a_stale_label",
            "/tmp/stale-cwd",
            "pty",
        ))
        .await
        .unwrap();
    let sessions = Sessions::new(&store);
    sessions
        .create(session_row(
            "s_ada_existing",
            "ada-renamed",
            "codex",
            "/tmp/original-cwd",
            "pty",
        ))
        .await
        .unwrap();
    sessions
        .create(session_row(
            "s_stale_label",
            "a_ada",
            "codex",
            "/tmp/stale-cwd",
            "pty",
        ))
        .await
        .unwrap();
    stamp_session_agent_id(&store, "s_ada_existing", "a_ada").await;
    stamp_session_agent_id(&store, "s_stale_label", "a_stale_label").await;

    let read = ReadClient::from_store_with_caller_for_tests(
        store,
        "operator",
        PROJECT,
        None,
        None,
        Tier::Admin,
    );
    let plan = read.attach_revive_plan("a_ada").await.unwrap();

    assert_eq!(plan.session_id.0, "s_ada_existing");
    assert_eq!(plan.name, "ada-renamed");
    assert_eq!(plan.spawn.name.as_deref(), Some("ada-renamed"));
    assert_eq!(plan.spawn.cwd.as_deref(), Some("/tmp/original-cwd"));
}

#[tokio::test]
async fn codex_resume_reuses_existing_thread_owner_when_name_matches() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: SessionId("s_beatrice".into()),
            name: Some("beatrice".into()),
            agent: Some("codex".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some(CODEX_THREAD.into()),
            client_key: Some("s_beatrice".into()),
            cwd: Some("/tmp/beatrice".into()),
            project: PROJECT.into(),
            transport: Some("codex-appserver".into()),
        })
        .await
        .unwrap();

    let state = state_with_store(store).await;
    let response = state
        .launch_agent_with_program(codex_resume_request("beatrice"), PROJECT, "codex", None)
        .await
        .unwrap();

    assert_eq!(response.session_id.0, "s_beatrice");
}

#[tokio::test]
async fn codex_resume_rejects_second_identity_for_existing_thread() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: SessionId("s_beatrice".into()),
            name: Some("beatrice".into()),
            agent: Some("codex".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some(CODEX_THREAD.into()),
            client_key: Some("s_beatrice".into()),
            cwd: Some("/tmp/beatrice".into()),
            project: PROJECT.into(),
            transport: Some("codex-appserver".into()),
        })
        .await
        .unwrap();

    let state = state_with_store(store).await;
    let err = state
        .launch_agent_with_program(codex_resume_request("dora"), PROJECT, "codex", None)
        .await
        .expect_err("second identity should be rejected");

    assert_eq!(err.code, nexus_contracts::codes::INVALID_PARAMS);
    assert!(err.message.contains("already bound to beatrice:s_beatrice"));
}

#[tokio::test]
async fn codex_resume_rejects_second_identity_for_sidecar_thread_owner() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let owner = SessionId("s_beatrice".into());
    Sessions::new(&store)
        .create(NewSession {
            session_id: owner.clone(),
            name: Some("beatrice".into()),
            agent: Some("codex".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some("s_beatrice".into()),
            cwd: Some("/tmp/beatrice".into()),
            project: PROJECT.into(),
            transport: Some("codex-appserver".into()),
        })
        .await
        .unwrap();
    let sidecar = CodexRuntimeStateRepo::new(&store);
    sidecar
        .upsert_launch(CodexRuntimeLaunch {
            runtime_id: owner,
            codex_thread_id: None,
            codex_home: "/tmp/codex-home".into(),
            app_server_sock: "/tmp/codex.sock".into(),
            app_server_pid: None,
            mcp_sidecar_pids_json: None,
            app_server_adopted: true,
        })
        .await
        .unwrap();
    sidecar
        .set_thread(&SessionId("s_beatrice".into()), CODEX_THREAD, None)
        .await
        .unwrap();

    let state = state_with_store(store).await;
    let err = state
        .launch_agent_with_program(codex_resume_request("dora"), PROJECT, "codex", None)
        .await
        .expect_err("second identity should be rejected");

    assert_eq!(err.code, nexus_contracts::codes::INVALID_PARAMS);
    assert!(err.message.contains("already bound to beatrice:s_beatrice"));
}

#[tokio::test]
async fn codex_resume_without_name_reuses_durable_native_owner() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let owner = SessionId("s_old_otto".into());
    Agents::new(&store)
        .create(new_agent("a_otto", "otto"))
        .await
        .unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: owner.clone(),
            name: Some("otto".into()),
            agent: Some("codex".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some("s_old_otto".into()),
            cwd: Some("/tmp/otto".into()),
            project: PROJECT.into(),
            transport: Some("codex-appserver".into()),
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .set_agent_id(&owner, "a_otto")
        .await
        .unwrap();
    NativeThreadBindings::new(&store)
        .claim(NewNativeThreadBinding {
            harness: "codex".into(),
            native_thread_id: CODEX_THREAD.into(),
            agent_id: "a_otto".into(),
            project: PROJECT.into(),
            runtime_id: Some(owner.0.clone()),
        })
        .await
        .unwrap();

    let state = state_with_store(store.clone()).await;
    let response = state
        .launch_agent_with_program(codex_resume_request_without_name(), PROJECT, "codex", None)
        .await
        .unwrap();

    assert_eq!(response.session_id, owner);
}

#[tokio::test]
async fn codex_resume_after_remove_reuses_preserved_durable_thread_on_owner_row() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let owner = SessionId("s_removed_otto".into());
    Agents::new(&store)
        .create(new_agent("a_otto_removed", "otto-removed"))
        .await
        .unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: owner.clone(),
            name: Some("otto-removed".into()),
            agent: Some("codex".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some(CODEX_THREAD.into()),
            client_key: Some("s_removed_otto".into()),
            cwd: Some("/tmp/otto-removed".into()),
            project: PROJECT.into(),
            transport: Some("codex-appserver".into()),
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .set_agent_id(&owner, "a_otto_removed")
        .await
        .unwrap();
    NativeThreadBindings::new(&store)
        .claim(NewNativeThreadBinding {
            harness: "codex".into(),
            native_thread_id: CODEX_THREAD.into(),
            agent_id: "a_otto_removed".into(),
            project: PROJECT.into(),
            runtime_id: Some(owner.0.clone()),
        })
        .await
        .unwrap();

    let state = state_with_store(store.clone()).await;
    let admin = Caller {
        agent_id: None,
        session: SessionId("s_operator".into()),
        name: "operator".into(),
        project: PROJECT.into(),
        tier: nexus_contracts::Tier::Admin,
    };
    state
        .remove_agent(
            &admin,
            Some(&AgentId("a_otto_removed".into())),
            "otto-removed",
            false,
        )
        .await
        .unwrap();
    let removed = Sessions::new(&store)
        .find_by_session_id(&owner)
        .await
        .unwrap()
        .expect("remove keeps session row");
    assert_eq!(removed.harness_session_id.as_deref(), Some(CODEX_THREAD));
    assert_eq!(
        NativeThreadBindings::new(&store)
            .find("codex", CODEX_THREAD)
            .await
            .unwrap()
            .expect("binding preserved")
            .released_at,
        None
    );

    let response = state
        .launch_agent_with_program(codex_resume_request_without_name(), PROJECT, "codex", None)
        .await
        .unwrap();

    assert_eq!(response.session_id, owner);
    let resumed = Sessions::new(&store)
        .find_by_session_id(&owner)
        .await
        .unwrap()
        .expect("owner session row still exists");
    assert_eq!(resumed.harness_session_id.as_deref(), Some(CODEX_THREAD));
    assert_eq!(
        NativeThreadBindings::new(&store)
            .find("codex", CODEX_THREAD)
            .await
            .unwrap()
            .expect("binding remains")
            .released_at,
        None
    );
}

#[tokio::test]
async fn codex_resume_with_different_explicit_name_rejects_durable_owner() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    Agents::new(&store)
        .create(new_agent("a_otto", "otto"))
        .await
        .unwrap();
    NativeThreadBindings::new(&store)
        .claim(NewNativeThreadBinding {
            harness: "codex".into(),
            native_thread_id: CODEX_THREAD.into(),
            agent_id: "a_otto".into(),
            project: PROJECT.into(),
            runtime_id: Some("s_old_otto".into()),
        })
        .await
        .unwrap();

    let state = state_with_store(store).await;
    let mut req = codex_resume_request("celia");
    req.identity_policy = Some(SpawnIdentityPolicy::ExplicitName);
    let err = state
        .launch_agent_with_program(req, PROJECT, "codex", None)
        .await
        .expect_err("different explicit owner must reject");

    assert_eq!(err.code, nexus_contracts::codes::INVALID_PARAMS);
    assert!(err.message.contains("already bound to otto:a_otto"));
}

#[tokio::test]
async fn codex_launch_explicit_agent_id_resume_validates_thread_before_active_runtime_reuse() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    Agents::new(&store)
        .create(new_agent("a_otto", "otto"))
        .await
        .unwrap();
    Agents::new(&store)
        .create(new_agent("a_celia", "celia"))
        .await
        .unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: SessionId("s_active_otto".into()),
            name: Some("otto".into()),
            agent: Some("codex".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some("other-thread".into()),
            client_key: Some("s_active_otto".into()),
            cwd: Some("/tmp/otto".into()),
            project: PROJECT.into(),
            transport: Some("acp".into()),
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .set_agent_id(&SessionId("s_active_otto".into()), "a_otto")
        .await
        .unwrap();
    AgentRuntimes::new(&store)
        .create(active_runtime(
            "s_active_otto",
            "a_otto",
            "/tmp/otto",
            "acp",
        ))
        .await
        .unwrap();
    NativeThreadBindings::new(&store)
        .claim(NewNativeThreadBinding {
            harness: "codex".into(),
            native_thread_id: CODEX_THREAD.into(),
            agent_id: "a_celia".into(),
            project: PROJECT.into(),
            runtime_id: Some("s_celia".into()),
        })
        .await
        .unwrap();

    let state = state_with_store(store).await;
    let mut req = codex_resume_request("a_otto");
    req.identity_policy = Some(SpawnIdentityPolicy::ExplicitAgentId);
    req.headless = true;
    let err = state
        .launch_agent(req, PROJECT, None)
        .await
        .expect_err("resume thread owner must be validated before reusing active runtime");

    assert_eq!(err.code, nexus_contracts::codes::INVALID_PARAMS);
    assert!(err.message.contains("already bound to celia:a_celia"));
}

#[tokio::test]
async fn claude_resume_does_not_treat_provider_session_as_nexus_identity() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let owner = SessionId("s_hugo".into());
    let cwd = temp_dir("claude-opaque-native-id");
    Sessions::new(&store)
        .create(NewSession {
            session_id: owner.clone(),
            name: Some("hugo".into()),
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some(CLAUDE_NATIVE_SESSION.into()),
            client_key: Some("s_hugo".into()),
            cwd: Some(cwd.to_string_lossy().into_owned()),
            project: PROJECT.into(),
            transport: Some("pty".into()),
        })
        .await
        .unwrap();
    let sidecar = ClaudeRuntimeStateRepo::new(&store);
    sidecar
        .upsert_launch(ClaudeRuntimeLaunch {
            runtime_id: owner.clone(),
            bridge_dir: "/tmp/claude-bridge".into(),
            claude_session_id: None,
            launch_cwd: cwd.clone(),
            transcript_path: None,
            bridge_pid: None,
            hook_pids_json: None,
        })
        .await
        .unwrap();
    sidecar
        .set_session(&owner, CLAUDE_NATIVE_SESSION, None)
        .await
        .unwrap();

    let state = AppState::wire_pty(store.clone(), &Config::default());
    let mut request = claude_resume_request("felix");
    request.cwd = Some(cwd.to_string_lossy().into_owned());
    let response = state
        .launch_agent_with_program(request, PROJECT, passive_pty_program(), None)
        .await
        .unwrap();

    assert_ne!(response.session_id, owner);
    let felix = Sessions::new(&store)
        .find_by_name(PROJECT, "felix")
        .await
        .unwrap()
        .expect("Nexus should create Felix independently of Claude's native id");
    assert_eq!(felix.session_id, response.session_id);
    assert!(
        Sessions::new(&store)
            .find_by_session_id(&owner)
            .await
            .unwrap()
            .is_some(),
        "launching Felix must not rebind or delete Hugo's Nexus identity"
    );

    state.teardown_harness("felix", PROJECT).await;
}

#[tokio::test]
async fn claude_resume_replaces_same_identity_acp_owner_with_headed_runtime() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let owner = SessionId("s_hugo_acp".into());
    let cwd = temp_dir("claude-acp-to-headed");
    Sessions::new(&store)
        .create(NewSession {
            session_id: owner.clone(),
            name: Some("hugo".into()),
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some(CLAUDE_NATIVE_SESSION.into()),
            client_key: Some("s_hugo_acp".into()),
            cwd: Some(cwd.to_string_lossy().into_owned()),
            project: PROJECT.into(),
            transport: Some("acp".into()),
        })
        .await
        .unwrap();
    let sidecar = ClaudeRuntimeStateRepo::new(&store);
    sidecar
        .upsert_launch(ClaudeRuntimeLaunch {
            runtime_id: owner.clone(),
            bridge_dir: "/tmp/claude-bridge-acp".into(),
            claude_session_id: None,
            launch_cwd: cwd.clone(),
            transcript_path: None,
            bridge_pid: None,
            hook_pids_json: None,
        })
        .await
        .unwrap();
    sidecar
        .set_session(&owner, CLAUDE_NATIVE_SESSION, None)
        .await
        .unwrap();

    let state = AppState::wire_pty(store.clone(), &Config::default());
    let mut request = claude_resume_request("hugo");
    request.cwd = Some(cwd.to_string_lossy().into_owned());
    let response = state
        .launch_agent_with_program(request, PROJECT, passive_pty_program(), None)
        .await
        .unwrap();

    assert_ne!(response.session_id, owner);
    let row = Sessions::new(&store)
        .find_by_name(PROJECT, "hugo")
        .await
        .unwrap()
        .expect("hugo row rebound");
    assert_eq!(row.session_id, response.session_id);
    assert_eq!(row.transport.as_deref(), Some("pty"));
    assert_eq!(row.presence.as_deref(), Some("online"));
    assert_eq!(row.cwd.as_deref(), Some(cwd.to_string_lossy().as_ref()));

    state.teardown_harness("hugo", PROJECT).await;
}

#[tokio::test]
async fn daemon_launch_without_cwd_uses_stable_agent_id_fallback_workspace() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let state = AppState::wire_pty(store.clone(), &Config::default());
    let name = "claude-daemon-fallback-cwd";
    let request = SpawnRequest {
        kind: hid("claude"),
        name: Some(name.into()),
        identity_policy: Some(SpawnIdentityPolicy::ExplicitName),
        cwd: None,
        project: Some(PROJECT.into()),
        role: None,
        initial_prompt: None,
        resume: None,
        harness_args: Vec::new(),
        headless: false,
        backend: Some("pty".into()),
    };

    let response = state
        .launch_agent_with_program(request, PROJECT, passive_pty_program(), None)
        .await
        .unwrap();
    let row = Sessions::new(&store)
        .find_by_session_id(&response.session_id)
        .await
        .unwrap()
        .expect("launched Claude row is persisted");
    let agent_id = row.agent_id.clone().expect("stable agent id is persisted");
    let cwd = row.cwd.clone().expect("launch cwd is persisted");
    let runtime = AgentRuntimes::new(&store)
        .find_by_runtime_id(&response.session_id.0)
        .await
        .unwrap()
        .expect("stable runtime row is persisted");

    state.teardown_harness(name, PROJECT).await;
    let _ = std::fs::remove_dir_all(&cwd);
    if let Ok(home) = std::env::var("HOME") {
        let _ = std::fs::remove_dir_all(format!("{home}/.nexus/agents/{name}"));
    }

    // Claude launches get a per-SESSION private folder under the durable
    // agent id (transcript-slug isolation; daemon::harness_launch policy).
    let expected_suffix = format!(
        ".nexus/agents/{agent_id}/sessions/{}",
        response.session_id.0
    );
    assert!(
        cwd.ends_with(&expected_suffix),
        "claude fallback cwd should be the per-session folder under the durable agent id; cwd={cwd} agent_id={agent_id}"
    );
    assert_eq!(
        runtime.cwd.as_deref(),
        Some(cwd.as_str()),
        "runtime cwd should match the launch cwd"
    );
}

#[tokio::test]
async fn opencode_resume_rejects_second_identity_for_native_session_owner() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let owner = SessionId("s_hugo".into());
    Sessions::new(&store)
        .create(NewSession {
            session_id: owner.clone(),
            name: Some("hugo".into()),
            agent: Some("opencode".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some(OPENCODE_NATIVE_SESSION.into()),
            client_key: Some("s_hugo".into()),
            cwd: Some("/tmp/hugo".into()),
            project: PROJECT.into(),
            transport: Some("opencode-plugin".into()),
        })
        .await
        .unwrap();
    OpenCodeRuntimeStateRepo::new(&store)
        .upsert_launch(OpenCodeRuntimeLaunch {
            runtime_id: owner,
            opencode_db_path: "/tmp/opencode.db".into(),
            opencode_session_id: Some(OPENCODE_NATIVE_SESSION.into()),
            launch_cwd: "/tmp/hugo".into(),
            plugin_bridge_pid: None,
            viewer_backend: "tmux".into(),
        })
        .await
        .unwrap();

    let state = state_with_store(store).await;
    let err = state
        .launch_agent_with_program(opencode_resume_request("felix"), PROJECT, "opencode", None)
        .await
        .expect_err("second identity should be rejected before spawning OpenCode");

    assert_eq!(err.code, nexus_contracts::codes::INVALID_PARAMS);
    assert!(err.message.contains("already bound to hugo:s_hugo"));
}

#[tokio::test]
async fn admin_remove_preserves_codex_native_resume_state() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let owner = SessionId("s_beatrice".into());
    Agents::new(&store)
        .create(new_agent("a_beatrice", "beatrice"))
        .await
        .unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: owner.clone(),
            name: Some("beatrice".into()),
            agent: Some("codex".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some(CODEX_THREAD.into()),
            client_key: Some("s_beatrice".into()),
            cwd: Some("/tmp/beatrice".into()),
            project: PROJECT.into(),
            transport: Some("codex-appserver".into()),
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .set_agent_id(&owner, "a_beatrice")
        .await
        .unwrap();
    NativeThreadBindings::new(&store)
        .claim(NewNativeThreadBinding {
            harness: "codex".into(),
            native_thread_id: CODEX_THREAD.into(),
            agent_id: "a_beatrice".into(),
            project: PROJECT.into(),
            runtime_id: Some(owner.0.clone()),
        })
        .await
        .unwrap();
    let sidecar = CodexRuntimeStateRepo::new(&store);
    sidecar
        .upsert_launch(CodexRuntimeLaunch {
            runtime_id: owner.clone(),
            codex_thread_id: None,
            codex_home: "/tmp/codex-home".into(),
            app_server_sock: "/tmp/codex.sock".into(),
            app_server_pid: None,
            mcp_sidecar_pids_json: None,
            app_server_adopted: true,
        })
        .await
        .unwrap();
    sidecar
        .set_thread(&owner, CODEX_THREAD, None)
        .await
        .unwrap();

    let state = state_with_store(store.clone()).await;
    let admin = Caller {
        agent_id: None,
        session: SessionId("s_operator".into()),
        name: "operator".into(),
        project: PROJECT.into(),
        tier: nexus_contracts::Tier::Admin,
    };
    let removed = state
        .remove_agent(&admin, None, "beatrice", false)
        .await
        .unwrap();
    assert_eq!(removed.status, "removed");

    let row = Sessions::new(&store)
        .find_by_name(PROJECT, "beatrice")
        .await
        .unwrap()
        .expect("remove retains the session row");
    assert_eq!(row.presence.as_deref(), Some("offline"));
    assert_eq!(row.harness_session_id.as_deref(), Some(CODEX_THREAD));
    assert_eq!(
        sidecar.find_by_thread_id(CODEX_THREAD).await.unwrap().len(),
        1,
        "admin.remove must preserve the Codex sidecar needed for cold resume"
    );
    let binding = NativeThreadBindings::new(&store)
        .find("codex", CODEX_THREAD)
        .await
        .unwrap()
        .expect("remove preserves native owner");
    assert_eq!(binding.agent_id, "a_beatrice");
    assert_eq!(
        binding.released_at, None,
        "admin.remove must keep native ownership active for cold resume"
    );
}

#[tokio::test]
async fn admin_delete_removes_codex_native_thread_ownership() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let owner = SessionId("s_beatrice_delete_codex_owner".into());
    Agents::new(&store)
        .create(new_agent("a_beatrice_delete", "beatrice-delete"))
        .await
        .unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: owner.clone(),
            name: Some("beatrice-delete".into()),
            agent: Some("codex".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some(CODEX_THREAD.into()),
            client_key: Some("s_beatrice_delete_codex_owner".into()),
            cwd: Some("/tmp/beatrice-delete".into()),
            project: PROJECT.into(),
            transport: Some("codex-appserver".into()),
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .set_agent_id(&owner, "a_beatrice_delete")
        .await
        .unwrap();
    NativeThreadBindings::new(&store)
        .claim(NewNativeThreadBinding {
            harness: "codex".into(),
            native_thread_id: CODEX_THREAD.into(),
            agent_id: "a_beatrice_delete".into(),
            project: PROJECT.into(),
            runtime_id: Some(owner.0.clone()),
        })
        .await
        .unwrap();

    let state = state_with_store(store.clone()).await;
    let admin = Caller {
        agent_id: None,
        session: SessionId("s_operator".into()),
        name: "operator".into(),
        project: PROJECT.into(),
        tier: nexus_contracts::Tier::Admin,
    };
    state
        .delete_agent(
            &admin,
            Some(&AgentId("a_beatrice_delete".into())),
            "beatrice-delete",
        )
        .await
        .unwrap();

    assert!(
        NativeThreadBindings::new(&store)
            .find("codex", CODEX_THREAD)
            .await
            .unwrap()
            .is_none(),
        "admin.delete frees durable native thread ownership"
    );
}

#[tokio::test]
async fn admin_remove_final_flushes_codex_rollout_while_preserving_sidecar() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let owner = SessionId("s_beatrice_codex_archive".into());
    let rollout_path = temp_dir("codex-remove").join("rollout.jsonl");
    let rollout = r#"{"type":"response_item","text":"remove codex archive"}"#;
    std::fs::write(&rollout_path, rollout).unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: owner.clone(),
            name: Some("beatrice".into()),
            agent: Some("codex".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some(CODEX_THREAD.into()),
            client_key: Some("s_beatrice_codex_archive".into()),
            cwd: Some("/tmp/beatrice".into()),
            project: PROJECT.into(),
            transport: Some("codex-appserver".into()),
        })
        .await
        .unwrap();
    AgentRuntimes::new(&store)
        .create(NewAgentRuntime {
            runtime_id: owner.0.clone(),
            agent_id: "a_beatrice_codex_archive".into(),
            harness: "codex".into(),
            cwd: Some("/tmp/beatrice".into()),
            transport: Some("codex-appserver".into()),
            presence: Some("online".into()),
            active: true,
        })
        .await
        .unwrap();
    let sidecar = CodexRuntimeStateRepo::new(&store);
    sidecar
        .upsert_launch(CodexRuntimeLaunch {
            runtime_id: owner.clone(),
            codex_thread_id: None,
            codex_home: "/tmp/codex-home".into(),
            app_server_sock: "/tmp/codex.sock".into(),
            app_server_pid: None,
            mcp_sidecar_pids_json: None,
            app_server_adopted: true,
        })
        .await
        .unwrap();
    sidecar
        .set_thread(&owner, CODEX_THREAD, Some(rollout_path.clone()))
        .await
        .unwrap();

    let state = state_with_store(store.clone()).await;
    let admin = Caller {
        agent_id: None,
        session: SessionId("s_operator".into()),
        name: "operator".into(),
        project: PROJECT.into(),
        tier: nexus_contracts::Tier::Admin,
    };
    let removed = state
        .remove_agent(&admin, None, "beatrice", false)
        .await
        .unwrap();
    assert_eq!(removed.status, "removed");

    assert!(sidecar.find_by_runtime_id(&owner).await.unwrap().is_some());
    let archived = TranscriptArchive::new(&store)
        .find_by_runtime_id(&owner.0)
        .await
        .unwrap()
        .expect("admin.remove final-flushes Codex rollout before stopping");
    assert_eq!(archived.harness, "codex");
    assert_eq!(archived.source_kind, "file");
    assert_eq!(archived.source_path, rollout_path.to_string_lossy());
    assert_eq!(archived.bytes_archived, rollout.len() as i64);
    assert_eq!(archived.last_event.as_deref(), Some("stop"));
    assert_eq!(
        std::fs::read_to_string(&archived.archive_path).unwrap(),
        rollout
    );
    cleanup_archive(&archived.archive_path);
}

#[tokio::test]
async fn admin_remove_preserves_all_native_session_bindings() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let owner = SessionId("s_beatrice_remove".into());
    let transcript_path = temp_dir("remove").join("transcript.jsonl");
    let transcript = r#"{"type":"assistant","text":"remove archive"}"#;
    std::fs::write(&transcript_path, transcript).unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: owner.clone(),
            name: Some("beatrice".into()),
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some(CLAUDE_NATIVE_SESSION.into()),
            client_key: Some("s_beatrice".into()),
            cwd: Some("/tmp/beatrice".into()),
            project: PROJECT.into(),
            transport: Some("pty".into()),
        })
        .await
        .unwrap();
    let claude = ClaudeRuntimeStateRepo::new(&store);
    claude
        .upsert_launch(ClaudeRuntimeLaunch {
            runtime_id: owner.clone(),
            bridge_dir: "/tmp/claude-bridge".into(),
            claude_session_id: None,
            launch_cwd: "/tmp/beatrice".into(),
            transcript_path: Some(transcript_path),
            bridge_pid: None,
            hook_pids_json: None,
        })
        .await
        .unwrap();
    claude
        .set_session(&owner, CLAUDE_NATIVE_SESSION, None)
        .await
        .unwrap();
    OpenCodeRuntimeStateRepo::new(&store)
        .upsert_launch(OpenCodeRuntimeLaunch {
            runtime_id: owner.clone(),
            opencode_db_path: "/tmp/opencode.db".into(),
            opencode_session_id: Some(OPENCODE_NATIVE_SESSION.into()),
            launch_cwd: "/tmp/beatrice".into(),
            plugin_bridge_pid: None,
            viewer_backend: "tmux".into(),
        })
        .await
        .unwrap();

    let state = state_with_store(store.clone()).await;
    let admin = Caller {
        agent_id: None,
        session: SessionId("s_operator".into()),
        name: "operator".into(),
        project: PROJECT.into(),
        tier: nexus_contracts::Tier::Admin,
    };
    let removed = state
        .remove_agent(&admin, None, "beatrice", false)
        .await
        .unwrap();
    assert_eq!(removed.status, "removed");

    assert!(claude.find_by_runtime_id(&owner).await.unwrap().is_some());
    let archived = TranscriptArchive::new(&store)
        .find_by_runtime_id(&owner.0)
        .await
        .unwrap()
        .expect("admin.remove final-flushes Claude transcript before stopping");
    assert_eq!(archived.bytes_archived, transcript.len() as i64);
    assert_eq!(archived.last_event.as_deref(), Some("stop"));
    assert_eq!(
        std::fs::read_to_string(&archived.archive_path).unwrap(),
        transcript
    );
    cleanup_archive(&archived.archive_path);
    assert!(OpenCodeRuntimeStateRepo::new(&store)
        .find_by_runtime_id(&owner)
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn admin_delete_releases_all_native_session_bindings() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let owner = SessionId("s_beatrice_delete".into());
    let transcript_path = temp_dir("delete").join("transcript.jsonl");
    let codex_rollout_path = temp_dir("delete-codex").join("rollout.jsonl");
    let transcript = r#"{"type":"assistant","text":"delete archive"}"#;
    let codex_rollout =
        r#"{"type":"response_item","text":"stale sidecar must not replace Claude"}"#;
    std::fs::write(&transcript_path, transcript).unwrap();
    std::fs::write(&codex_rollout_path, codex_rollout).unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: owner.clone(),
            name: Some("beatrice".into()),
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some(CLAUDE_NATIVE_SESSION.into()),
            client_key: Some("s_beatrice".into()),
            cwd: Some("/tmp/beatrice".into()),
            project: PROJECT.into(),
            transport: Some("pty".into()),
        })
        .await
        .unwrap();
    let claude = ClaudeRuntimeStateRepo::new(&store);
    claude
        .upsert_launch(ClaudeRuntimeLaunch {
            runtime_id: owner.clone(),
            bridge_dir: "/tmp/claude-bridge".into(),
            claude_session_id: None,
            launch_cwd: "/tmp/beatrice".into(),
            transcript_path: Some(transcript_path),
            bridge_pid: None,
            hook_pids_json: None,
        })
        .await
        .unwrap();
    claude
        .set_session(&owner, CLAUDE_NATIVE_SESSION, None)
        .await
        .unwrap();
    CodexRuntimeStateRepo::new(&store)
        .upsert_launch(CodexRuntimeLaunch {
            runtime_id: owner.clone(),
            codex_thread_id: None,
            codex_home: "/tmp/codex-home".into(),
            app_server_sock: "/tmp/codex.sock".into(),
            app_server_pid: None,
            mcp_sidecar_pids_json: None,
            app_server_adopted: true,
        })
        .await
        .unwrap();
    CodexRuntimeStateRepo::new(&store)
        .set_thread(&owner, CODEX_THREAD, Some(codex_rollout_path))
        .await
        .unwrap();
    OpenCodeRuntimeStateRepo::new(&store)
        .upsert_launch(OpenCodeRuntimeLaunch {
            runtime_id: owner.clone(),
            opencode_db_path: "/tmp/opencode.db".into(),
            opencode_session_id: Some(OPENCODE_NATIVE_SESSION.into()),
            launch_cwd: "/tmp/beatrice".into(),
            plugin_bridge_pid: None,
            viewer_backend: "tmux".into(),
        })
        .await
        .unwrap();

    let state = state_with_store(store.clone()).await;
    let admin = Caller {
        agent_id: None,
        session: SessionId("s_operator".into()),
        name: "operator".into(),
        project: PROJECT.into(),
        tier: nexus_contracts::Tier::Admin,
    };
    state.delete_agent(&admin, None, "beatrice").await.unwrap();

    assert!(claude.find_by_runtime_id(&owner).await.unwrap().is_none());
    let archived = TranscriptArchive::new(&store)
        .find_by_runtime_id(&owner.0)
        .await
        .unwrap()
        .expect("admin.delete final-flushes Claude transcript before releasing sidecar");
    assert_eq!(archived.bytes_archived, transcript.len() as i64);
    assert_eq!(archived.last_event.as_deref(), Some("release"));
    assert_eq!(
        std::fs::read_to_string(&archived.archive_path).unwrap(),
        transcript
    );
    cleanup_archive(&archived.archive_path);
    assert!(CodexRuntimeStateRepo::new(&store)
        .find_by_runtime_id(&owner)
        .await
        .unwrap()
        .is_none());
    assert!(OpenCodeRuntimeStateRepo::new(&store)
        .find_by_runtime_id(&owner)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn admin_delete_releases_native_bindings_when_archive_final_flush_fails() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let owner = SessionId("s_beatrice_delete_stale_archive".into());
    let missing_transcript = temp_dir("delete-missing").join("missing-transcript.jsonl");
    Sessions::new(&store)
        .create(NewSession {
            session_id: owner.clone(),
            name: Some("beatrice".into()),
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some(CLAUDE_NATIVE_SESSION.into()),
            client_key: Some("s_beatrice".into()),
            cwd: Some("/tmp/beatrice".into()),
            project: PROJECT.into(),
            transport: Some("pty".into()),
        })
        .await
        .unwrap();
    let claude = ClaudeRuntimeStateRepo::new(&store);
    claude
        .upsert_launch(ClaudeRuntimeLaunch {
            runtime_id: owner.clone(),
            bridge_dir: "/tmp/claude-bridge".into(),
            claude_session_id: None,
            launch_cwd: "/tmp/beatrice".into(),
            transcript_path: Some(missing_transcript),
            bridge_pid: None,
            hook_pids_json: None,
        })
        .await
        .unwrap();
    claude
        .set_session(&owner, CLAUDE_NATIVE_SESSION, None)
        .await
        .unwrap();
    OpenCodeRuntimeStateRepo::new(&store)
        .upsert_launch(OpenCodeRuntimeLaunch {
            runtime_id: owner.clone(),
            opencode_db_path: "/tmp/opencode.db".into(),
            opencode_session_id: Some(OPENCODE_NATIVE_SESSION.into()),
            launch_cwd: "/tmp/beatrice".into(),
            plugin_bridge_pid: None,
            viewer_backend: "tmux".into(),
        })
        .await
        .unwrap();

    let state = state_with_store(store.clone()).await;
    let admin = Caller {
        agent_id: None,
        session: SessionId("s_operator".into()),
        name: "operator".into(),
        project: PROJECT.into(),
        tier: nexus_contracts::Tier::Admin,
    };
    state.delete_agent(&admin, None, "beatrice").await.unwrap();

    assert!(Sessions::new(&store)
        .find_by_session_id(&owner)
        .await
        .unwrap()
        .is_none());
    assert!(claude.find_by_runtime_id(&owner).await.unwrap().is_none());
    assert!(OpenCodeRuntimeStateRepo::new(&store)
        .find_by_runtime_id(&owner)
        .await
        .unwrap()
        .is_none());
}

fn cleanup_archive(path: &str) {
    let path = PathBuf::from(path);
    let _ = std::fs::remove_file(&path);
    if let Some(parent) = path.parent() {
        let _ = std::fs::remove_dir_all(parent);
    }
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
