//! Daemon-IPC CLI read views.
//!
//! Production CLI and MCP reads cross the daemon-owned local socket/pipe so the daemon remains
//! the sole durable-store client. Direct-store construction remains an explicit integration-test
//! seam for exercising the same view assembly against an in-memory store.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;

use nexus_common::{now, Config};
use nexus_contracts::{
    codes, AgentId, AgentRuntimeListRequest, AgentRuntimeListResponse, AgentRuntimeSummary,
    AgentShowRequest, AgentShowResponse, AgentSummary, Caller, ContractError, DaemonIpcCall,
    DaemonIpcCaller, DaemonIpcRequest, Harness, HeartbeatResponse, HistoryRequest, HistoryResponse,
    IdentityPort, Kind, MemberListRequest, MemberListResponse, MemberSummary, Message, MessageId,
    Presence, RegisterRequest, RegisterResponse, SearchPort, SearchRequest, SearchResponse,
    SessionId, Source, SourceListResponse, SourceRef, SpawnRequest, StatusRequest, StatusResponse,
    ThreadListResponse, ThreadMembersRequest, ThreadSummary, Tier, TopicListResponse, TopicSummary,
    Whoami, DAEMON_IPC_PROTOCOL_VERSION,
};
use nexus_store::repos::{
    AgentRef, AgentRuntimes, Agents, DeveloperEvents, Messages, NativeThreadBindings, Sessions,
    Sources, Threads, Topics,
};
use nexus_store::types::{AgentRow, AgentRuntimeRow, SessionRow};
use nexus_store::Store;

use crate::daemon::claude_resume_harvest::{
    default_home, harvest_claude_resume_id_for_runtime, revive_failure_truth,
    ClaudeResumeHarvestStatus,
};
use crate::daemon::hermes_native_forwarder::HermesRuntimeStateRepo;
use crate::daemon::opencode_native_forwarder::OpenCodeRuntimeStateRepo;
use crate::local_operator::{
    display_name as local_operator_display_name, LOCAL_OPERATOR_SESSION_ID,
};
use nexus_harness_claude::storage::ClaudeRuntimeStateRepo;
use nexus_harness_codex::storage::CodexRuntimeStateRepo;

use super::gateway_read_client::GatewayReadClient;

const DEFAULT_DAEMON_QUERY_TIMEOUT: Duration = Duration::from_secs(10);

/// Read client for CLI and MCP commands that do not need command-intent side effects.
#[derive(Clone)]
pub struct ReadClient {
    backend: ReadClientBackend,
    caller: ReadCaller,
    heartbeat_ttl_ms: i64,
}

#[derive(Clone)]
enum ReadClientBackend {
    Daemon(PathBuf),
    DirectStore(Arc<Store>),
}

#[derive(Debug, Clone)]
struct ReadCaller {
    name: String,
    project: String,
    session_id: Option<String>,
    client_key: Option<String>,
    kind: Kind,
    tier: Tier,
}

/// Daemon launch request needed to revive a dead headed runtime before terminal attach.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AttachRevivePlan {
    /// The compatibility session row being revived.
    pub session_id: SessionId,
    /// Addressable agent name to reuse for the revived runtime.
    pub name: String,
    /// Project that owns the revived runtime.
    pub project: String,
    /// Fully-resolved daemon launch request. Attach uses this request with `harness.launch`, then
    /// re-resolves the local terminal target and attaches to the newly materialized runtime.
    pub spawn: SpawnRequest,
}

/// Backend-neutral local terminal attach descriptor.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PtyAttachDescriptor {
    /// Nexus session id whose local terminal is being attached.
    pub session_id: SessionId,
    /// Backend that owns the local terminal runtime.
    pub backend: String,
    /// Command argv the CLI should exec to attach to the runtime.
    pub argv: Vec<String>,
    /// Optional command argv that probes whether the local attach target still exists.
    pub liveness_argv: Option<Vec<String>>,
}

impl ReadClient {
    /// Connect to the daemon-owned local endpoint using the ambient CLI identity.
    pub async fn from_config() -> Result<ReadClient, ContractError> {
        let config = Config::load();
        Ok(ReadClient::new_daemon(
            crate::daemon::lifecycle::nexus_home(),
            ReadCaller::from_env()?,
            config.heartbeat_ttl_ms,
        ))
    }

    /// Connect to the daemon-owned local endpoint with an explicit MCP/harness identity.
    pub async fn from_config_with_identity(
        identity: RegisterRequest,
    ) -> Result<ReadClient, ContractError> {
        let config = Config::load();
        Ok(ReadClient::new_daemon(
            crate::daemon::lifecycle::nexus_home(),
            ReadCaller::from_register(identity),
            config.heartbeat_ttl_ms,
        ))
    }

    /// Build a read client over an existing store. Integration tests use this to avoid any daemon
    /// transport while sharing rows with command-worker tests.
    pub fn from_store_with_caller_for_tests(
        store: Arc<Store>,
        name: impl Into<String>,
        project: impl Into<String>,
        session_id: Option<String>,
        client_key: Option<String>,
        tier: Tier,
    ) -> ReadClient {
        let kind = if tier == Tier::Admin {
            Kind::Human
        } else {
            Kind::Agent
        };
        ReadClient::new_store(
            store,
            ReadCaller {
                name: name.into(),
                project: project.into(),
                session_id,
                client_key,
                kind,
                tier,
            },
            Config::default().heartbeat_ttl_ms,
        )
    }

    /// Build a daemon-IPC read client rooted at an isolated test home.
    pub fn from_daemon_for_tests(home: PathBuf) -> ReadClient {
        ReadClient::new_daemon(
            home,
            ReadCaller::local_operator(),
            Config::default().heartbeat_ttl_ms,
        )
    }

    fn new_store(store: Arc<Store>, caller: ReadCaller, heartbeat_ttl_ms: i64) -> ReadClient {
        ReadClient {
            backend: ReadClientBackend::DirectStore(store),
            caller,
            heartbeat_ttl_ms,
        }
    }

    fn new_daemon(home: PathBuf, caller: ReadCaller, heartbeat_ttl_ms: i64) -> ReadClient {
        ReadClient {
            backend: ReadClientBackend::Daemon(home),
            caller,
            heartbeat_ttl_ms,
        }
    }

    fn store(&self) -> &Arc<Store> {
        match &self.backend {
            ReadClientBackend::DirectStore(store) => store,
            ReadClientBackend::Daemon(_) => {
                unreachable!("daemon-backed read methods must cross daemon_query")
            }
        }
    }

    async fn daemon_query<Req, Res>(&self, method: &str, params: &Req) -> Result<Res, ContractError>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        let ReadClientBackend::Daemon(home) = &self.backend else {
            unreachable!("daemon_query requires the daemon backend")
        };
        let request_id = format!("query-{}", uuid::Uuid::new_v4());
        let response = crate::daemon::daemon_ipc::call_daemon_ipc(
            home,
            DaemonIpcRequest {
                version: DAEMON_IPC_PROTOCOL_VERSION,
                token: String::new(),
                request_id,
                caller: Some(self.caller.to_ipc()),
                call: DaemonIpcCall::Query {
                    method: method.to_string(),
                    params: serde_json::to_value(params).map_err(json_err)?,
                },
            },
            DEFAULT_DAEMON_QUERY_TIMEOUT,
        )
        .await
        .map_err(ipc_err)?;
        if let Some(error) = response.error {
            return Err(ContractError {
                code: error.code,
                message: error.message,
            });
        }
        serde_json::from_value(response.result.unwrap_or(serde_json::Value::Null)).map_err(json_err)
    }

    /// Resolve the current caller's identity.
    pub async fn whoami(&self) -> Result<Whoami, ContractError> {
        if matches!(self.backend, ReadClientBackend::Daemon(_)) {
            return self.daemon_query("whoami", &serde_json::Value::Null).await;
        }
        match self.session_row().await? {
            Some(row) => Ok(Whoami {
                agent_id: if let Some(name) = row.name.as_deref() {
                    self.agent_id_for_name(name).await?
                } else {
                    row.agent_id.clone().map(AgentId)
                },
                name: row.name,
                session_id: row.session_id,
                role: row.role,
                tier: tier_from_store(&row.tier),
                project: row.project,
                presence: nexus_common::presence::presence_from_token(row.presence.as_deref()),
            }),
            None if self.caller.is_local_operator() => Ok(Whoami {
                agent_id: None,
                name: Some(self.caller.name.clone()),
                session_id: SessionId(
                    self.caller
                        .session_id
                        .clone()
                        .unwrap_or_else(|| "local-operator".to_string()),
                ),
                role: None,
                tier: Tier::Admin,
                project: self.caller.project.clone(),
                presence: Presence::Online,
            }),
            None => Err(ContractError {
                code: codes::NOT_FOUND,
                message: format!("caller not registered: {}", self.caller.name),
            }),
        }
    }

    /// List members in the caller's project using the same heartbeat-staleness rule as the daemon.
    pub async fn members(
        &self,
        req: MemberListRequest,
    ) -> Result<MemberListResponse, ContractError> {
        if matches!(self.backend, ReadClientBackend::Daemon(_)) {
            return self.daemon_query("members", &req).await;
        }
        let include_offline = req.include_offline.unwrap_or(false);
        let include_dead = req.include_dead.unwrap_or(false);
        let ts = now();
        let mut members = Vec::new();
        for row in Sessions::new(self.store())
            .list(&self.caller.project)
            .await
            .map_err(store_err)?
        {
            let stale = nexus_common::presence::is_stale(
                row.last_heartbeat.or(Some(row.created_at)),
                ts,
                self.heartbeat_ttl_ms,
            );
            let presence = if stale {
                Presence::Offline
            } else {
                nexus_common::presence::presence_from_token(row.presence.as_deref())
            };
            if presence == Presence::Offline && !include_offline {
                continue;
            }
            let agent_id = if let Some(name) = row.name.as_deref() {
                self.agent_id_for_name(name).await?
            } else {
                row.agent_id.clone().map(AgentId)
            };
            // Dead-marking is durable agent state,
            // separate from presence. Dead rows leave the default roster; audit views
            // pass include_dead.
            let (lifecycle_state, dead_reason) = match agent_id.as_ref() {
                Some(id) => Agents::new(self.store())
                    .lifecycle_for_id(&id.0)
                    .await
                    .map_err(store_err)?,
                None => (None, None),
            };
            if lifecycle_state.as_deref() == Some("dead") && !include_dead {
                continue;
            }
            members.push(MemberSummary {
                agent_id,
                name: row.name,
                session_id: row.session_id,
                agent: row.agent,
                role: row.role,
                presence,
                current_work: row.current_work,
                lifecycle_state,
                dead_reason,
            });
        }
        Ok(MemberListResponse { members })
    }

    /// List active threads. The stored project value is legacy metadata, not identity scope.
    pub async fn threads(&self) -> Result<ThreadListResponse, ContractError> {
        if matches!(self.backend, ReadClientBackend::Daemon(_)) {
            return self.daemon_query("threads", &serde_json::Value::Null).await;
        }
        let threads = Threads::new(self.store());
        let developer_events = DeveloperEvents::new(self.store());
        let mut out = Vec::new();
        for row in threads.list().await.map_err(store_err)? {
            let latest_seq = developer_events
                .latest_seq(&format!("sys.message.thread.{}", row.name))
                .await
                .map_err(store_err)?;
            out.push(ThreadSummary {
                name: row.name,
                topic: row.topic,
                description: row.description,
                members: threads.members(&row.thread_id).await.map_err(store_err)?,
                last_at: None,
                latest_seq: Some(latest_seq),
            });
        }
        Ok(ThreadListResponse { threads: out })
    }

    /// List members of one active named thread.
    pub async fn thread_members(
        &self,
        name: &str,
    ) -> Result<nexus_contracts::ThreadMembersResponse, ContractError> {
        if matches!(self.backend, ReadClientBackend::Daemon(_)) {
            return self
                .daemon_query(
                    "thread.members",
                    &ThreadMembersRequest {
                        name: name.to_string(),
                    },
                )
                .await;
        }
        let threads = Threads::new(self.store());
        let row = threads
            .find_active_any_by_name(name)
            .await
            .map_err(store_err)?
            .ok_or_else(|| ContractError {
                code: codes::NOT_FOUND,
                message: format!("thread:{name}"),
            })?;
        Ok(nexus_contracts::ThreadMembersResponse {
            name: name.to_string(),
            members: threads.members(&row.thread_id).await.map_err(store_err)?,
        })
    }

    /// List topics in the caller's project.
    pub async fn topics(&self) -> Result<TopicListResponse, ContractError> {
        if matches!(self.backend, ReadClientBackend::Daemon(_)) {
            return self.daemon_query("topics", &serde_json::Value::Null).await;
        }
        let topics = Topics::new(self.store());
        let mut out = Vec::new();
        for topic in topics.list(&self.caller.project).await.map_err(store_err)? {
            out.push(TopicSummary {
                subscribers: topics.subscribers(&topic).await.map_err(store_err)?.len() as u32,
                topic,
            });
        }
        Ok(TopicListResponse { topics: out })
    }

    /// List registered notification sources. Source tokens are intentionally not returned.
    pub async fn sources(&self) -> Result<SourceListResponse, ContractError> {
        if matches!(self.backend, ReadClientBackend::Daemon(_)) {
            return self
                .daemon_query("source.list", &serde_json::Value::Null)
                .await;
        }
        let rows = Sources::new(self.store()).list().await.map_err(store_err)?;
        Ok(SourceListResponse {
            sources: rows.into_iter().map(source_from_row).collect(),
        })
    }

    /// Show one notification source without exposing its token.
    pub async fn source(&self, name: &str) -> Result<Source, ContractError> {
        if matches!(self.backend, ReadClientBackend::Daemon(_)) {
            return self
                .daemon_query(
                    "source.show",
                    &SourceRef {
                        name: name.to_string(),
                    },
                )
                .await;
        }
        Sources::new(self.store())
            .find(name)
            .await
            .map_err(store_err)?
            .map(source_from_row)
            .ok_or_else(|| ContractError {
                code: codes::NOT_FOUND,
                message: format!("source not found: {name}"),
            })
    }

    /// Show one durable agent identity and runtime history visible to the caller.
    pub async fn agent_show(&self, name: &str) -> Result<AgentShowResponse, ContractError> {
        if matches!(self.backend, ReadClientBackend::Daemon(_)) {
            return self
                .daemon_query(
                    "agent.show",
                    &AgentShowRequest {
                        agent_id: None,
                        name: Some(name.to_string()),
                    },
                )
                .await;
        }
        let agent = self.resolve_agent(name).await?;
        let runtimes_repo = AgentRuntimes::new(self.store());
        let active = runtimes_repo
            .active_for_agent(&agent.agent_id)
            .await
            .map_err(store_err)?;
        let runtimes = runtimes_repo
            .list_for_agent(&agent.agent_id, true)
            .await
            .map_err(store_err)?
            .into_iter()
            .map(runtime_summary_from_row)
            .collect();
        Ok(AgentShowResponse {
            agent: agent_summary_from_row(agent, active),
            runtimes,
        })
    }

    /// List runtimes for one durable agent identity visible to the caller.
    pub async fn agent_runtimes(
        &self,
        name: &str,
        include_stopped: bool,
    ) -> Result<AgentRuntimeListResponse, ContractError> {
        if matches!(self.backend, ReadClientBackend::Daemon(_)) {
            return self
                .daemon_query(
                    "agent.runtime.list",
                    &AgentRuntimeListRequest {
                        agent_id: None,
                        name: Some(name.to_string()),
                        include_stopped: Some(include_stopped),
                    },
                )
                .await;
        }
        let agent = self.resolve_agent(name).await?;
        let runtimes = AgentRuntimes::new(self.store())
            .list_for_agent(&agent.agent_id, include_stopped)
            .await
            .map_err(store_err)?
            .into_iter()
            .map(runtime_summary_from_row)
            .collect();
        Ok(AgentRuntimeListResponse {
            agent_id: AgentId(agent.agent_id),
            runtimes,
        })
    }

    /// Return the local terminal attach descriptor for a headed runtime, resolved by stable
    /// `agent_id`, agent name, or session id.
    pub async fn pty_attach_descriptor(
        &self,
        target: &str,
    ) -> Result<PtyAttachDescriptor, ContractError> {
        if matches!(self.backend, ReadClientBackend::Daemon(_)) {
            return self
                .daemon_query(
                    "local.terminal.describe",
                    &serde_json::json!({ "target": target }),
                )
                .await;
        }
        let row = self.attach_row(target).await?;
        self.pty_attach_descriptor_for_row(&row).await
    }

    /// Return the daemon launch request needed to revive a dead headed runtime for `nexus attach`.
    ///
    /// The plan is deliberately built from durable store metadata: the compatibility row gives the
    /// name/project/cwd/transport, while harness-native sidecar state provides native resume ids
    /// when a harness stores them outside `sessions.harness_session_id`.
    pub async fn attach_revive_plan(
        &self,
        target: &str,
    ) -> Result<AttachRevivePlan, ContractError> {
        if matches!(self.backend, ReadClientBackend::Daemon(_)) {
            return self
                .daemon_query(
                    "local.terminal.revivePlan",
                    &serde_json::json!({ "target": target }),
                )
                .await;
        }
        let row = self.attach_row(target).await?;
        let row_name = row.name.clone().ok_or_else(|| ContractError {
            code: codes::INVALID_PARAMS,
            message: format!(
                "{} is unnamed; assign a name before using attach/revive",
                row.session_id.0
            ),
        })?;
        if row.kind != "agent" {
            return Err(ContractError {
                code: codes::INVALID_PARAMS,
                message: format!("{row_name} is not an agent runtime"),
            });
        }
        let kind = harness_from_session_row(&row)?;
        if !is_attach_revivable_transport(row.transport.as_deref(), kind) {
            return Err(ContractError {
                code: codes::INVALID_PARAMS,
                message: format!(
                    "{}:{} is not a headed runtime that `nexus attach` can revive",
                    row_name, row.session_id.0
                ),
            });
        }
        let cwd = row.cwd.clone().ok_or_else(|| ContractError {
            code: codes::INVALID_PARAMS,
            message: format!(
                "{}:{} has no stored launch cwd; cannot revive for attach",
                row_name, row.session_id.0
            ),
        })?;
        let resume_key = self.harness_resume_key(&row, kind).await?;
        let (resume, harness_args) = revive_tail(kind, resume_key.as_deref())?;
        let backend = Self::attach_revive_backend(kind);
        let spawn = SpawnRequest {
            kind,
            name: Some(row_name.clone()),
            identity_policy: None,
            cwd: Some(cwd),
            project: Some(row.project.clone()),
            role: row.role.clone(),
            initial_prompt: None,
            resume,
            harness_args,
            headless: false,
            backend,
        };
        Ok(AttachRevivePlan {
            session_id: row.session_id,
            name: row_name,
            project: row.project,
            spawn,
        })
    }

    fn attach_revive_backend(kind: Harness) -> Option<String> {
        match kind {
            Harness::Claude => Some("pty".to_string()),
            Harness::Hermes => Some("tmux".to_string()),
            _ => None,
        }
    }

    /// Return the local terminal attach descriptor for a headed runtime resolved by Nexus session id.
    pub async fn pty_attach_descriptor_for_session(
        &self,
        session: &SessionId,
    ) -> Result<PtyAttachDescriptor, ContractError> {
        if matches!(self.backend, ReadClientBackend::Daemon(_)) {
            return self
                .daemon_query(
                    "local.terminal.describe",
                    &serde_json::json!({ "target": session.0 }),
                )
                .await;
        }
        let row = Sessions::new(self.store())
            .find_by_session_id(session)
            .await
            .map_err(store_err)?
            .ok_or_else(|| ContractError {
                code: codes::NOT_FOUND,
                message: format!("session:{}", session.0),
            })?;
        self.pty_attach_descriptor_for_row(&row).await
    }

    /// Search scoped to the resolved caller.
    pub async fn search(&self, req: SearchRequest) -> Result<SearchResponse, ContractError> {
        if let ReadClientBackend::Daemon(home) = &self.backend {
            return GatewayReadClient::discover(home)?
                .search(&self.caller.name, req)
                .await;
        }
        let caller = self.caller().await?;
        nexus_search::Search::new(self.store().clone(), Arc::new(NoIdentity))
            .search(&caller, req)
            .await
    }

    /// Chronological history scoped to the resolved caller.
    pub async fn history(&self, req: HistoryRequest) -> Result<HistoryResponse, ContractError> {
        if let ReadClientBackend::Daemon(home) = &self.backend {
            return GatewayReadClient::discover(home)?
                .history(&self.caller.name, req)
                .await;
        }
        let caller = self.caller().await?;
        nexus_search::Search::new(self.store().clone(), Arc::new(NoIdentity))
            .history(&caller, req)
            .await
    }

    /// Fetch one committed message by id in the caller's project.
    ///
    /// This backs `nexus read <message-id>` and the MCP `read` tool used to expand drain-truncated
    /// messages. It intentionally mirrors the gateway REST `GET /api/v1/messages/:id` read view.
    pub async fn message(&self, id: &str) -> Result<Message, ContractError> {
        if let ReadClientBackend::Daemon(home) = &self.backend {
            return GatewayReadClient::discover(home)?
                .message(&self.caller.name, id)
                .await;
        }
        let caller = self.caller().await?;
        Messages::new(self.store())
            .get(&caller.project, &MessageId(id.to_string()))
            .await
            .map_err(store_err)?
            .ok_or_else(|| ContractError {
                code: codes::NOT_FOUND,
                message: format!("message:{id}"),
            })
    }

    async fn caller(&self) -> Result<Caller, ContractError> {
        if let Some(row) = self.session_row().await? {
            let agent_id = if let Some(name) = row.name.as_deref() {
                self.agent_id_for_name(name).await?
            } else {
                row.agent_id.clone().map(AgentId)
            };
            let mut session = row.session_id.clone();
            if let Some(agent_id) = &agent_id {
                if let Some(runtime) = AgentRuntimes::new(self.store())
                    .active_for_agent(&agent_id.0)
                    .await
                    .map_err(store_err)?
                {
                    session = SessionId(runtime.runtime_id);
                }
            }
            return Ok(Caller {
                agent_id,
                session,
                name: row.display_name(),
                project: row.project,
                tier: tier_from_store(&row.tier),
            });
        }
        if self.caller.is_local_operator() {
            return Ok(Caller {
                agent_id: None,
                session: SessionId(
                    self.caller
                        .session_id
                        .clone()
                        .unwrap_or_else(|| "local-operator".to_string()),
                ),
                name: self.caller.name.clone(),
                project: self.caller.project.clone(),
                tier: Tier::Admin,
            });
        }
        Err(ContractError {
            code: codes::NOT_FOUND,
            message: format!("caller not registered: {}", self.caller.name),
        })
    }

    async fn session_row(&self) -> Result<Option<nexus_store::types::SessionRow>, ContractError> {
        let sessions = Sessions::new(self.store());
        if let Some(client_key) = self.caller.client_key.as_deref() {
            if let Some(row) = sessions
                .find_by_client_key(&self.caller.project, client_key)
                .await
                .map_err(store_err)?
            {
                return Ok(Some(row));
            }
            return Ok(None);
        }
        if let Some(session_id) = self.caller.session_id.as_deref() {
            if let Some(row) = sessions
                .find_by_session_id(&SessionId(session_id.to_string()))
                .await
                .map_err(store_err)?
            {
                return Ok(Some(row));
            }
        }
        sessions
            .find_by_name(&self.caller.project, &self.caller.name)
            .await
            .map_err(store_err)
    }

    async fn attach_row(&self, target: &str) -> Result<SessionRow, ContractError> {
        let sessions = Sessions::new(self.store());
        if matches!(AgentRef::parse(target), AgentRef::Id(_)) {
            return sessions
                .active_runtime_session_for_agent(target)
                .await
                .map_err(store_err)?
                .ok_or_else(|| ContractError {
                    code: codes::NOT_FOUND,
                    message: format!("active runtime not found for agent id: {target}"),
                });
        }
        if target.starts_with("s_") {
            if let Some(row) = sessions
                .find_by_session_id(&SessionId(target.to_string()))
                .await
                .map_err(store_err)?
            {
                return Ok(row);
            }
        }
        // A public name addresses the durable agent, not whichever compatibility row happens to
        // carry that name. Runtime replacement can leave the named row behind while the current
        // headed process is represented by a newer (possibly unnamed) session row. Follow the
        // stable identity to its active runtime first so `nexus attach <name>` projects the
        // terminal that is actually running.
        if let Some(agent) = Agents::new(self.store())
            .find_by_name(target)
            .await
            .map_err(store_err)?
        {
            if self.caller.tier == Tier::Admin || agent.project == self.caller.project {
                if let Some(row) = sessions
                    .active_runtime_session_for_agent(&agent.agent_id)
                    .await
                    .map_err(store_err)?
                {
                    return Ok(row);
                }
            }
        }
        if let Some(row) = sessions
            .find_by_name(&self.caller.project, target)
            .await
            .map_err(store_err)?
        {
            return Ok(row);
        }
        sessions
            .find_by_session_id(&SessionId(target.to_string()))
            .await
            .map_err(store_err)?
            .ok_or_else(|| ContractError {
                code: codes::NOT_FOUND,
                message: format!("session or agent not found: {target}"),
            })
    }

    async fn harness_resume_key(
        &self,
        row: &SessionRow,
        kind: Harness,
    ) -> Result<Option<String>, ContractError> {
        let session_id = SessionId(row.session_id.0.clone());
        let session_key = row.harness_session_id.clone();
        if let Some(key) = self.native_thread_binding_resume_key(row, kind).await? {
            return Ok(Some(key));
        }
        match kind {
            Harness::Claude => {
                let repo = ClaudeRuntimeStateRepo::new(self.store());
                let mut key = if let Some(key) = session_key.filter(|key| !key.is_empty()) {
                    Some(key)
                } else {
                    repo.find_by_runtime_id(&session_id)
                        .await
                        .map_err(store_err)?
                        .and_then(|state| state.claude_session_id)
                        .filter(|key| !key.is_empty())
                };
                if key.is_none() {
                    let item = harvest_claude_resume_id_for_runtime(
                        self.store(),
                        &default_home(),
                        &session_id,
                    )
                    .await
                    .map_err(store_err)?;
                    match item.status {
                        ClaudeResumeHarvestStatus::Updated
                        | ClaudeResumeHarvestStatus::AlreadyPresent => {
                            key = item.native_session_id.filter(|key| !key.is_empty());
                        }
                        _ => {
                            return Err(ContractError {
                                code: codes::INVALID_PARAMS,
                                message: revive_failure_truth(&item),
                            });
                        }
                    }
                }
                // Claude Code owns this namespace. A duplicate native id across Nexus runtimes is
                // not proof of shared Nexus identity and must not block attach/revive. Forward the
                // exact value only as an opaque, best-effort provider resume hint.
                Ok(key)
            }
            Harness::OpenCode => {
                if let Some(key) = session_key {
                    return Ok(Some(key));
                }
                if let Some(state) = OpenCodeRuntimeStateRepo::new(self.store())
                    .find_by_runtime_id(&session_id)
                    .await
                    .map_err(store_err)?
                {
                    if state.opencode_session_id.is_some() {
                        return Ok(state.opencode_session_id);
                    }
                }
                Ok(opencode_ready_session_id(&session_id))
            }
            Harness::Hermes => {
                if let Some(key) = session_key {
                    return Ok(Some(key));
                }
                Ok(HermesRuntimeStateRepo::new(self.store())
                    .find_by_runtime_id(&session_id)
                    .await
                    .map_err(store_err)?
                    .and_then(|state| state.hermes_session_id))
            }
            Harness::Codex => {
                let sidecar_thread = CodexRuntimeStateRepo::new(self.store())
                    .find_by_runtime_id(&session_id)
                    .await
                    .map_err(store_err)?
                    .and_then(|state| state.codex_thread_id)
                    .filter(|key| !key.is_empty());
                if sidecar_thread.is_some() {
                    return Ok(sidecar_thread);
                }
                Ok(session_key
                    .filter(|key| !key.is_empty())
                    .filter(|key| !is_nexus_session_id_like(key)))
            }
            _ => Ok(session_key),
        }
    }

    async fn native_thread_binding_resume_key(
        &self,
        row: &SessionRow,
        kind: Harness,
    ) -> Result<Option<String>, ContractError> {
        let Some(harness) = native_thread_binding_harness(kind) else {
            return Ok(None);
        };
        let resolved_agent_id = match row.agent_id.as_deref() {
            Some(agent_id) => Some(agent_id.to_string()),
            None => match row.name.as_deref() {
                Some(name) => Agents::new(self.store())
                    .find_by_project_name(&row.project, name)
                    .await
                    .map_err(store_err)?
                    .map(|agent| agent.agent_id),
                None => None,
            },
        };
        let Some(agent_id) = resolved_agent_id.as_deref() else {
            return Ok(None);
        };
        let bindings = NativeThreadBindings::new(self.store());
        if let Some(binding) = bindings
            .find_for_runtime(harness, &row.session_id.0)
            .await
            .map_err(store_err)?
        {
            if binding.agent_id != agent_id {
                return Err(ContractError {
                    code: codes::INVALID_PARAMS,
                    message: format!(
                        "{}:{} native {harness} resume key {} belongs to {}",
                        row.display_name(),
                        row.session_id.0,
                        binding.native_thread_id,
                        binding.agent_id
                    ),
                });
            }
            return Ok(Some(binding.native_thread_id));
        }
        Ok(bindings
            .find_latest_for_agent(harness, agent_id)
            .await
            .map_err(store_err)?
            .map(|binding| binding.native_thread_id))
    }

    async fn agent_id_for_name(&self, name: &str) -> Result<Option<AgentId>, ContractError> {
        Ok(Agents::new(self.store())
            .find_by_name(name)
            .await
            .map_err(store_err)?
            .map(|row| AgentId(row.agent_id)))
    }

    async fn resolve_agent(&self, name: &str) -> Result<AgentRow, ContractError> {
        let agent = Agents::new(self.store())
            .find_by_name(name)
            .await
            .map_err(store_err)?
            .ok_or_else(|| ContractError {
                code: codes::NOT_FOUND,
                message: format!("agent:{name}"),
            })?;
        if self.caller.tier == Tier::Admin || agent.project == self.caller.project {
            Ok(agent)
        } else {
            Err(ContractError {
                code: codes::NOT_FOUND,
                message: format!("agent:{name}"),
            })
        }
    }

    async fn pty_attach_descriptor_for_row(
        &self,
        row: &SessionRow,
    ) -> Result<PtyAttachDescriptor, ContractError> {
        if !matches!(
            row.transport.as_deref(),
            Some("pty") | Some("codex-appserver") | Some("opencode-plugin")
        ) {
            return Err(ContractError {
                code: codes::INVALID_PARAMS,
                message: format!(
                    "{}:{} is not a local headed runtime with an attachable terminal",
                    row.display_name(),
                    row.session_id.0
                ),
            });
        }
        // A raw daemon-owned PTY runtime (de-tmux #22) publishes a terminal endpoint manifest at
        // bind time and has no tmux session. Route attach through the socket terminal client.
        if crate::daemon::terminal_socket::read_terminal_endpoint_manifest(&row.session_id)
            .is_some()
        {
            return Ok(raw_pty_attach_descriptor(&row.session_id));
        }
        if row.transport.as_deref() == Some("codex-appserver") {
            if let Some(descriptor) = self.codex_tmux_attach_descriptor(&row.session_id).await? {
                return Ok(descriptor);
            }
        }
        Ok(tmux_attach_descriptor(&row.session_id))
    }

    async fn codex_tmux_attach_descriptor(
        &self,
        session: &SessionId,
    ) -> Result<Option<PtyAttachDescriptor>, ContractError> {
        let state = CodexRuntimeStateRepo::new(self.store())
            .find_by_runtime_id(session)
            .await
            .map_err(store_err)?;
        let Some(state) = state else {
            return Ok(None);
        };
        match (state.tmux_socket, state.tmux_session) {
            (Some(socket), Some(session_name)) => Ok(Some(tmux_attach_descriptor_with_target(
                session,
                socket,
                session_name,
            ))),
            _ => Ok(None),
        }
    }
}

/// Attach descriptor for a raw daemon-owned PTY runtime. Resolve the stable `nexus` launcher in
/// the attaching operator's environment because package upgrades may unlink a live daemon's
/// executable. Liveness is carried by the manifest and socket.
fn raw_pty_attach_descriptor(session: &SessionId) -> PtyAttachDescriptor {
    PtyAttachDescriptor {
        session_id: session.clone(),
        backend: "raw-pty".to_string(),
        argv: vec![
            "nexus".to_string(),
            "terminal-client".to_string(),
            session.0.clone(),
        ],
        liveness_argv: None,
    }
}

/// Transports that `nexus attach` may repair by submitting a headed daemon launch.
///
/// Direct tmux attach remains stricter: `acp` rows have no local terminal. Claude is the one
/// exception here because a headless ACP row can be replaced with a headed `claude --resume` launch
/// using the stored native session id.
fn is_attach_revivable_transport(transport: Option<&str>, kind: Harness) -> bool {
    matches!(
        transport,
        Some("pty") | Some("codex-appserver") | Some("opencode-plugin")
    ) || matches!((transport, kind), (Some("acp"), Harness::Claude))
}

fn harness_from_session_row(row: &SessionRow) -> Result<Harness, ContractError> {
    let harness = match row.agent.as_deref() {
        Some("claude") => Harness::Claude,
        Some("codex") => Harness::Codex,
        Some("opencode") => Harness::OpenCode,
        Some("hermes") => Harness::Hermes,
        Some("pi") => Harness::Pi,
        Some("other") => Harness::Other,
        Some(other) => {
            return Err(ContractError {
                code: codes::INVALID_PARAMS,
                message: format!("unsupported harness for attach revive: {other}"),
            });
        }
        None => match row.transport.as_deref() {
            Some("codex-appserver") => Harness::Codex,
            Some("opencode-plugin") => Harness::OpenCode,
            _ => Harness::Claude,
        },
    };
    if matches!(harness, Harness::Pi | Harness::Other) {
        return Err(ContractError {
            code: codes::INVALID_PARAMS,
            message: format!(
                "{}:{} uses a harness that attach cannot revive",
                row.display_name(),
                row.session_id.0
            ),
        });
    }
    Ok(harness)
}

fn native_thread_binding_harness(kind: Harness) -> Option<&'static str> {
    match kind {
        Harness::Claude => Some("claude"),
        Harness::Codex => Some("codex"),
        Harness::OpenCode => Some("opencode"),
        Harness::Hermes => Some("hermes"),
        _ => None,
    }
}

fn revive_tail(
    kind: Harness,
    resume_key: Option<&str>,
) -> Result<(Option<String>, Vec<String>), ContractError> {
    let args = match kind {
        Harness::Claude => {
            let key = resume_key
                .filter(|key| !key.is_empty())
                .ok_or_else(|| ContractError {
                    code: codes::INVALID_PARAMS,
                    message: "cannot revive headed Claude runtime without a stored --resume session id; refusing unsafe --continue".into(),
                })?;
            vec!["--resume".to_string(), key.to_string()]
        }
        Harness::Codex => {
            let key = resume_key
                .filter(|key| !key.is_empty())
                .ok_or_else(|| ContractError {
                    code: codes::INVALID_PARAMS,
                    message: "cannot revive headed Codex runtime without a stored thread id".into(),
                })?;
            return Ok((Some(key.to_string()), Vec::new()));
        }
        Harness::OpenCode => {
            let key = resume_key
                .filter(|key| !key.is_empty())
                .ok_or_else(|| ContractError {
                    code: codes::INVALID_PARAMS,
                    message: "cannot revive headed OpenCode runtime without a stored native session id; refusing unsafe --continue".into(),
                })?;
            vec!["-s".to_string(), key.to_string()]
        }
        Harness::Hermes => {
            let key = resume_key
                .filter(|key| !key.is_empty())
                .ok_or_else(|| ContractError {
                    code: codes::INVALID_PARAMS,
                    message: "cannot revive headed Hermes runtime without a stored native session id; refusing unsafe --continue".into(),
                })?;
            vec!["--session".to_string(), key.to_string()]
        }
        Harness::Pi | Harness::Other => {
            return Err(ContractError {
                code: codes::INVALID_PARAMS,
                message: "unsupported harness for attach revive".into(),
            });
        }
    };
    Ok((None, args))
}

fn is_nexus_session_id_like(value: &str) -> bool {
    value.starts_with("s_")
}

fn opencode_ready_session_id(session: &SessionId) -> Option<String> {
    let home = std::env::var("HOME").ok()?;
    let path = std::path::PathBuf::from(home)
        .join(".nexus")
        .join("opencode-plugin-sessions")
        .join(&session.0)
        .join("ready.json");
    let json = std::fs::read_to_string(path).ok()?;
    let value: serde_json::Value = serde_json::from_str(&json).ok()?;
    value
        .get("sessionId")
        .and_then(|id| id.as_str())
        .filter(|id| !id.is_empty())
        .map(ToString::to_string)
}

fn tmux_attach_descriptor(session: &SessionId) -> PtyAttachDescriptor {
    let session_name = tmux_session_name(session);
    let socket = std::env::temp_dir().join(format!("nexus-tmux-{session_name}.sock"));
    let socket = socket.to_string_lossy().into_owned();
    tmux_attach_descriptor_with_target(session, socket, session_name)
}

fn tmux_attach_descriptor_with_target(
    session: &SessionId,
    socket: String,
    session_name: String,
) -> PtyAttachDescriptor {
    PtyAttachDescriptor {
        session_id: session.clone(),
        backend: "tmux".to_string(),
        argv: vec![
            "tmux".to_string(),
            "-S".to_string(),
            socket.clone(),
            "attach".to_string(),
            "-t".to_string(),
            session_name.clone(),
        ],
        liveness_argv: Some(vec![
            "tmux".to_string(),
            "-S".to_string(),
            socket,
            "has-session".to_string(),
            "-t".to_string(),
            session_name,
        ]),
    }
}

fn tmux_session_name(session: &SessionId) -> String {
    let safe: String = session
        .0
        .chars()
        .map(|c| if c == '.' || c == ':' { '-' } else { c })
        .collect();
    format!("nexus-{safe}")
}

impl ReadCaller {
    /// Resolve the ambient caller. An agent identity (`NEXUS_NAME`, issued at launch) always
    /// wins; an env-less local shell is the zero-auth **local operator** — that is the designed
    /// human default (`whoami` is always the user). The B21 invariant lives on the launch side:
    /// spawned agents always receive their own daemon-minted identity env and never the
    /// operator's, so this fallback is only ever reached by human shells.
    fn from_env() -> Result<ReadCaller, ContractError> {
        if let Some(identity) = crate::cli::ambient::identity_from_env_result()? {
            return Ok(ReadCaller::from_register(identity));
        }
        Ok(ReadCaller::local_operator())
    }

    /// The zero-auth local-operator Admin caller (env-less human shell).
    fn local_operator() -> ReadCaller {
        ReadCaller {
            name: local_operator_display_name(),
            project: std::env::var("NEXUS_PROJECT")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "default".to_string()),
            session_id: Some(LOCAL_OPERATOR_SESSION_ID.to_string()),
            client_key: None,
            kind: Kind::Human,
            tier: Tier::Admin,
        }
    }

    fn from_register(identity: RegisterRequest) -> ReadCaller {
        let kind = identity.kind.unwrap_or(Kind::Agent);
        let fallback = identity
            .agent_id
            .as_ref()
            .map(|id| id.0.clone())
            .unwrap_or_else(|| identity.client_key.clone());
        ReadCaller {
            name: identity.name.unwrap_or(fallback),
            project: identity.project,
            session_id: None,
            client_key: Some(identity.client_key),
            kind,
            tier: identity.tier,
        }
    }

    fn is_local_operator(&self) -> bool {
        self.session_id.as_deref() == Some(LOCAL_OPERATOR_SESSION_ID)
            && self.client_key.is_none()
            && self.tier == Tier::Admin
    }

    fn to_ipc(&self) -> DaemonIpcCaller {
        DaemonIpcCaller {
            name: Some(self.name.clone()),
            project: self.project.clone(),
            session_id: self.session_id.clone(),
            agent_id: None,
            runtime_id: self.session_id.clone(),
            client_key: self.client_key.clone(),
            kind: self.kind,
            tier: self.tier,
        }
    }
}

fn tier_from_store(value: &str) -> Tier {
    match value {
        "admin" => Tier::Admin,
        _ => Tier::Agent,
    }
}

fn source_from_row(row: nexus_store::repos::SourceRow) -> Source {
    Source {
        name: row.name,
        topic: row.topic,
        enabled: row.enabled,
        created_at: row.created_at,
        last_fired_at: row.last_fired_at,
    }
}

fn agent_summary_from_row(row: AgentRow, active_runtime: Option<AgentRuntimeRow>) -> AgentSummary {
    AgentSummary {
        agent_id: AgentId(row.agent_id),
        name: row.name,
        project: row.project,
        default_harness: row.default_harness.as_deref().map(harness_from_store),
        role: row.role,
        tier: Some(row.tier),
        disabled: row.disabled_at.is_some(),
        active_runtime: active_runtime.map(runtime_summary_from_row),
    }
}

fn runtime_summary_from_row(row: AgentRuntimeRow) -> AgentRuntimeSummary {
    AgentRuntimeSummary {
        runtime_id: SessionId(row.runtime_id),
        agent_id: AgentId(row.agent_id),
        harness: harness_from_store(&row.harness),
        cwd: row.cwd,
        transport: row.transport,
        presence: runtime_presence(row.presence.as_deref(), row.active),
        active: row.active,
        started_at: row.started_at,
        stopped_at: row.stopped_at,
        last_heartbeat: row.last_heartbeat,
    }
}

fn harness_from_store(value: &str) -> Harness {
    serde_json::from_value(serde_json::Value::String(value.to_string())).unwrap_or(Harness::Other)
}

fn runtime_presence(value: Option<&str>, active: bool) -> Presence {
    match value {
        Some(v) => nexus_common::presence::presence_from_token(Some(v)),
        None => {
            if active {
                Presence::Online
            } else {
                Presence::Offline
            }
        }
    }
}

fn store_err(error: nexus_common::NexusError) -> ContractError {
    error.to_contract_error()
}

fn json_err(error: serde_json::Error) -> ContractError {
    ContractError {
        code: codes::INVALID_PARAMS,
        message: format!("JSON serialization error: {error}"),
    }
}

fn ipc_err(error: std::io::Error) -> ContractError {
    ContractError {
        code: codes::INTERNAL_ERROR,
        message: format!("daemon IPC error: {error}"),
    }
}

struct NoIdentity;

#[async_trait]
impl IdentityPort for NoIdentity {
    async fn register(&self, _req: RegisterRequest) -> Result<RegisterResponse, ContractError> {
        unreachable!("read-client search does not register identities")
    }

    async fn whoami(&self, _caller: &Caller) -> Result<Whoami, ContractError> {
        unreachable!("read-client search does not call identity.whoami")
    }

    async fn resolve(&self, _project: &str, _name: &str) -> Result<Caller, ContractError> {
        unreachable!("read-client search does not resolve names through identity")
    }

    async fn members(
        &self,
        _caller: &Caller,
        _req: MemberListRequest,
    ) -> Result<MemberListResponse, ContractError> {
        unreachable!("read-client search does not call identity.members")
    }

    async fn status(
        &self,
        _caller: &Caller,
        _req: StatusRequest,
    ) -> Result<StatusResponse, ContractError> {
        unreachable!("read-client search does not call identity.status")
    }

    async fn heartbeat(&self, _caller: &Caller) -> Result<HeartbeatResponse, ContractError> {
        unreachable!("read-client search does not call identity.heartbeat")
    }

    async fn assign_project(
        &self,
        _name: &str,
        _to_project: &str,
    ) -> Result<nexus_contracts::AssignProjectResponse, ContractError> {
        unreachable!("read-client search does not call identity.assign_project")
    }
}

#[cfg(test)]
mod caller_gate_tests {
    use super::*;
    use crate::cli::ambient::with_scrubbed_identity_env;

    #[test]
    fn envless_shell_is_the_local_operator() {
        let caller = with_scrubbed_identity_env(&[], ReadCaller::from_env);
        let caller = caller.unwrap();
        assert!(caller.is_local_operator());
    }

    #[test]
    fn agent_identity_always_wins_and_is_never_operator() {
        let caller = with_scrubbed_identity_env(
            &[("NEXUS_NAME", "demoa"), ("NEXUS_CLIENT_KEY", "ck_demoa")],
            ReadCaller::from_env,
        )
        .unwrap();
        assert_eq!(caller.name, "demoa");
        assert_eq!(caller.tier, Tier::Agent);
        assert!(!caller.is_local_operator());
    }

    #[test]
    fn ambient_human_kind_survives_agent_tier_on_daemon_reads() {
        let caller = with_scrubbed_identity_env(
            &[
                ("NEXUS_NAME", "endurance-controller"),
                ("NEXUS_CLIENT_KEY", "ck_controller"),
                ("NEXUS_KIND", "human"),
            ],
            ReadCaller::from_env,
        )
        .unwrap();
        let ipc = caller.to_ipc();
        assert_eq!(ipc.kind, Kind::Human);
        assert_eq!(ipc.tier, Tier::Agent);
    }

    #[test]
    fn partial_agent_env_is_not_local_operator() {
        let error = with_scrubbed_identity_env(&[("NEXUS_NAME", "demoa")], ReadCaller::from_env)
            .unwrap_err();
        assert_eq!(error.code, codes::UNAUTHORIZED);
        assert!(error.message.contains("NEXUS_CLIENT_KEY"));
    }
}
