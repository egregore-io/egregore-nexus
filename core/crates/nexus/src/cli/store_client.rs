//! Daemon-IPC CLI command client.
//!
//! Production CLI and MCP calls cross the daemon-owned local socket/pipe. The daemon inserts the
//! durable command-intent row and holds the IPC response until its worker records a terminal
//! result. Direct-store construction remains only as an explicit integration-test seam while the
//! migration lands.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Serialize;

use nexus_common::now;
use nexus_contracts::{
    codes, validate_send_request, Ack, ContractError, DaemonIpcCall, DaemonIpcCaller,
    DaemonIpcRequest, Kind, NotifySendRequest, RegisterRequest, SendRequest, Tier,
    DAEMON_IPC_PROTOCOL_VERSION,
};
use nexus_store::command_kinds;
use nexus_store::repos::{CommandIntents, DaemonState, NewCommandIntent, Sessions};
use nexus_store::Store;

use crate::local_operator::{
    display_name as local_operator_display_name, LOCAL_OPERATOR_SESSION_ID,
};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_HARNESS_LAUNCH_TIMEOUT: Duration = Duration::from_secs(120);
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const RESTART_AWARE_KINDS: &[&str] = &[
    command_kinds::inbox::CONSUME,
    command_kinds::inbox::SUBSCRIPTION_NEXT,
];

/// Client-side handle for commands that now use the store-backed command-intent ingress.
#[derive(Clone)]
pub struct StoreClient {
    backend: StoreClientBackend,
    caller: StoreCaller,
    timeout: Duration,
    poll_interval: Duration,
}

#[derive(Clone)]
enum StoreClientBackend {
    Daemon(PathBuf),
    DirectStore(Arc<Store>),
}

#[derive(Debug, Clone)]
struct StoreCaller {
    name: String,
    project: String,
    session_id: Option<String>,
    agent_id: Option<String>,
    runtime_id: Option<String>,
    client_key: Option<String>,
    kind: Kind,
    tier: Tier,
}

impl StoreClient {
    /// Connect to the daemon-owned local endpoint and use the ambient CLI identity.
    pub async fn from_config() -> Result<StoreClient, ContractError> {
        let caller = StoreCaller::from_env()?;
        Ok(StoreClient::new_daemon(
            crate::daemon::lifecycle::nexus_home(),
            caller,
        ))
    }

    /// Open the daemon's configured store with an explicit caller identity. The MCP server uses this
    /// so tool calls carry the identity supplied by `nexus mcp --as/--project`.
    pub async fn from_config_with_identity(
        identity: RegisterRequest,
    ) -> Result<StoreClient, ContractError> {
        let caller = StoreCaller::from_register(identity);
        Ok(StoreClient::new_daemon(
            crate::daemon::lifecycle::nexus_home(),
            caller,
        ))
    }

    /// Build a client over an already-open store. Integration tests use this to share an in-memory
    /// store with the daemon worker or with direct command-intent assertions.
    pub fn from_store_for_tests(store: Arc<Store>) -> StoreClient {
        StoreClient::new_store(
            store,
            StoreCaller::from_env().expect("test ambient identity must be valid"),
        )
    }

    /// Build a test client over an existing store and canonicalize the ambient caller by
    /// registered client key before writing command intents.
    pub async fn from_store_for_tests_resolving_env(
        store: Arc<Store>,
    ) -> Result<StoreClient, ContractError> {
        let caller = StoreCaller::from_env()?
            .canonicalize_registered_client_key(&store)
            .await?;
        Ok(StoreClient::new_store(store, caller))
    }

    /// Build a test client with an explicit identity and canonicalize it by registered client key.
    pub async fn from_store_with_identity_for_tests_resolving(
        store: Arc<Store>,
        identity: RegisterRequest,
    ) -> Result<StoreClient, ContractError> {
        let caller = StoreCaller::from_register(identity)
            .canonicalize_registered_client_key(&store)
            .await?;
        Ok(StoreClient::new_store(store, caller))
    }

    /// Build a test client with explicit caller metadata, bypassing process environment.
    pub fn from_store_with_caller_for_tests(
        store: Arc<Store>,
        name: impl Into<String>,
        project: impl Into<String>,
        client_key: Option<String>,
        tier: Tier,
        kind: Kind,
    ) -> StoreClient {
        StoreClient::new_store(
            store,
            StoreCaller {
                name: name.into(),
                project: project.into(),
                session_id: None,
                agent_id: None,
                runtime_id: None,
                client_key,
                kind,
                tier,
            },
        )
    }

    /// Build a daemon-IPC client rooted at an isolated test home.
    pub fn from_daemon_for_tests(home: PathBuf) -> StoreClient {
        StoreClient::new_daemon(home, StoreCaller::local_operator())
    }

    /// Stable caller scope for producer idempotency keys.
    ///
    /// MCP request ids are per-connection and can restart at `1` for every harness process. Scope
    /// those keys to the registered runtime/session identity so retries dedupe inside one caller
    /// without colliding across different agents.
    pub fn idempotency_scope(&self) -> String {
        self.caller
            .session_id
            .as_deref()
            .or(self.caller.runtime_id.as_deref())
            .or(self.caller.client_key.as_deref())
            .unwrap_or(self.caller.name.as_str())
            .to_string()
    }

    /// Override the bounded command wait used by tests that complete rows manually.
    pub fn with_timeout(mut self, timeout: Duration) -> StoreClient {
        self.timeout = timeout;
        self
    }

    /// Override the command-row polling cadence.
    ///
    /// Production callers use the short default. Integration tests that run a daemon worker and a
    /// store client in the same process can widen this interval to avoid the test client constantly
    /// reading while the worker is trying to complete the same row.
    pub fn with_poll_interval(mut self, poll_interval: Duration) -> StoreClient {
        self.poll_interval = poll_interval;
        self
    }

    fn new_store(store: Arc<Store>, caller: StoreCaller) -> StoreClient {
        StoreClient {
            backend: StoreClientBackend::DirectStore(store),
            caller,
            timeout: DEFAULT_TIMEOUT,
            poll_interval: POLL_INTERVAL,
        }
    }

    fn new_daemon(home: PathBuf, caller: StoreCaller) -> StoreClient {
        StoreClient {
            backend: StoreClientBackend::Daemon(home),
            caller,
            timeout: DEFAULT_TIMEOUT,
            poll_interval: POLL_INTERVAL,
        }
    }

    /// Submit a Message Post [`SendRequest`] and wait for the daemon-written [`Ack`].
    pub async fn message_post_send(&self, req: &SendRequest) -> Result<Ack, ContractError> {
        validate_send_request(req)?;
        self.command(command_kinds::message_post::SEND, req).await
    }

    /// Submit a Message Post [`SendRequest`] with a caller-provided idempotency key.
    ///
    /// Retry-prone surfaces such as MCP use this so a repeated tool call polls the original
    /// command-intent row instead of enqueueing a second send.
    pub async fn message_post_send_idempotent(
        &self,
        req: &SendRequest,
        idempotency_key: Option<String>,
    ) -> Result<Ack, ContractError> {
        validate_send_request(req)?;
        self.command_with_idempotency(command_kinds::message_post::SEND, req, idempotency_key)
            .await
    }

    /// Submit one explicitly targeted notification through the daemon-owned message spine.
    pub async fn notification_send(&self, req: &NotifySendRequest) -> Result<Ack, ContractError> {
        self.command_with_idempotency(
            command_kinds::notification::SEND,
            req,
            req.idempotency_key.clone(),
        )
        .await
    }

    /// Submit any daemon-managed command intent and wait for the typed daemon result.
    ///
    /// The CLI/MCP choose the stable [`nexus_store::command_kinds`] value; the daemon worker maps
    /// that kind to the existing dispatch registry and applies the same auth, project, wake, and
    /// lifecycle rules as every daemon-managed write/control operation.
    pub async fn command<Req, Res>(&self, kind: &str, req: &Req) -> Result<Res, ContractError>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        self.command_with_idempotency(kind, req, None).await
    }

    /// Submit any daemon-managed command intent with an optional idempotency key and wait for the
    /// typed daemon result.
    pub async fn command_with_idempotency<Req, Res>(
        &self,
        kind: &str,
        req: &Req,
        idempotency_key: Option<String>,
    ) -> Result<Res, ContractError>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        if let StoreClientBackend::Daemon(home) = &self.backend {
            return self.daemon_command(home, kind, req, idempotency_key).await;
        }
        self.direct_store_command(kind, req, idempotency_key).await
    }

    async fn daemon_command<Req, Res>(
        &self,
        home: &std::path::Path,
        kind: &str,
        req: &Req,
        idempotency_key: Option<String>,
    ) -> Result<Res, ContractError>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        let params = serde_json::to_value(req).map_err(serde_err)?;
        let command_id = new_command_id(kind);
        let timeout = if kind == command_kinds::harness::LAUNCH && self.timeout == DEFAULT_TIMEOUT {
            DEFAULT_HARNESS_LAUNCH_TIMEOUT
        } else {
            self.timeout
        };
        let response = crate::daemon::daemon_ipc::call_daemon_ipc(
            home,
            DaemonIpcRequest {
                version: DAEMON_IPC_PROTOCOL_VERSION,
                token: String::new(),
                request_id: command_id.clone(),
                caller: Some(self.caller.to_ipc()),
                call: DaemonIpcCall::Command {
                    command_id,
                    kind: kind.to_string(),
                    params,
                    idempotency_key,
                },
            },
            timeout,
        )
        .await
        .map_err(ipc_err)?;
        if let Some(error) = response.error {
            return Err(ContractError {
                code: error.code,
                message: error.message,
            });
        }
        serde_json::from_value(response.result.unwrap_or(serde_json::Value::Null))
            .map_err(serde_err)
    }

    async fn direct_store_command<Req, Res>(
        &self,
        kind: &str,
        req: &Req,
        idempotency_key: Option<String>,
    ) -> Result<Res, ContractError>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        let store = self.direct_store()?;
        let request_json = serde_json::to_string(req).map_err(serde_err)?;
        let mut restart_retries = 0u8;
        loop {
            let boot_epoch_before = if RESTART_AWARE_KINDS.contains(&kind) {
                self.boot_epoch().await?
            } else {
                None
            };
            let command_id = new_command_id(kind);
            let command_id = if idempotency_key.is_some() {
                CommandIntents::new(store)
                    .insert_pending_idempotent(NewCommandIntent {
                        command_id: command_id.clone(),
                        kind: kind.to_string(),
                        project: self.caller.project.clone(),
                        caller_name: self.caller.name.clone(),
                        caller_session_id: self.caller.session_id.clone(),
                        caller_agent_id: self.caller.agent_id.clone(),
                        caller_runtime_id: self.caller.runtime_id.clone(),
                        caller_client_key: self.caller.client_key.clone(),
                        caller_kind: Some(kind_token(self.caller.kind).to_string()),
                        caller_tier: Some(tier_token(self.caller.tier).to_string()),
                        idempotency_key: idempotency_key.clone(),
                        request_json: request_json.clone(),
                        created_at: now(),
                    })
                    .await
                    .map_err(store_err)?
            } else {
                CommandIntents::new(store)
                    .insert_pending(NewCommandIntent {
                        command_id: command_id.clone(),
                        kind: kind.to_string(),
                        project: self.caller.project.clone(),
                        caller_name: self.caller.name.clone(),
                        caller_session_id: self.caller.session_id.clone(),
                        caller_agent_id: self.caller.agent_id.clone(),
                        caller_runtime_id: self.caller.runtime_id.clone(),
                        caller_client_key: self.caller.client_key.clone(),
                        caller_kind: Some(kind_token(self.caller.kind).to_string()),
                        caller_tier: Some(tier_token(self.caller.tier).to_string()),
                        idempotency_key: None,
                        request_json: request_json.clone(),
                        created_at: now(),
                    })
                    .await
                    .map_err(store_err)?;
                command_id
            };
            match self
                .poll_result(&command_id, kind, boot_epoch_before.as_deref())
                .await
            {
                Ok(result) => return Ok(result),
                Err(error) => {
                    if restart_retries < 3
                        && self
                            .should_retry_after_boot_epoch_change(kind, &error)
                            .await?
                    {
                        restart_retries += 1;
                        continue;
                    }
                    return Err(error);
                }
            }
        }
    }

    async fn boot_epoch(&self) -> Result<Option<String>, ContractError> {
        DaemonState::new(self.direct_store()?)
            .boot_epoch()
            .await
            .map_err(store_err)
    }

    async fn should_retry_after_boot_epoch_change(
        &self,
        kind: &str,
        error: &ContractError,
    ) -> Result<bool, ContractError> {
        if !RESTART_AWARE_KINDS.contains(&kind) {
            return Ok(false);
        }
        if error.code != codes::INTERNAL_ERROR
            || !error.message.contains("daemon boot epoch changed")
        {
            return Ok(false);
        }
        Ok(true)
    }

    async fn poll_result<Res: DeserializeOwned>(
        &self,
        command_id: &str,
        kind: &str,
        boot_epoch_before: Option<&str>,
    ) -> Result<Res, ContractError> {
        let timeout = if kind == command_kinds::harness::LAUNCH && self.timeout == DEFAULT_TIMEOUT {
            DEFAULT_HARNESS_LAUNCH_TIMEOUT
        } else {
            self.timeout
        };
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let Some(row) = CommandIntents::new(self.direct_store()?)
                .get(command_id)
                .await
                .map_err(store_err)?
            else {
                return Err(ContractError {
                    code: codes::INTERNAL_ERROR,
                    message: format!("command intent disappeared: {command_id}"),
                });
            };

            match row.status.as_str() {
                "done" => {
                    let json = row.result_json.ok_or_else(|| ContractError {
                        code: codes::INTERNAL_ERROR,
                        message: format!("command intent completed without result: {command_id}"),
                    })?;
                    return serde_json::from_str(&json).map_err(serde_err);
                }
                "error" => {
                    let json = row.error_json.ok_or_else(|| ContractError {
                        code: codes::INTERNAL_ERROR,
                        message: format!("command intent failed without error: {command_id}"),
                    })?;
                    return Err(error_json_to_contract_error(&json));
                }
                "claimed" if RESTART_AWARE_KINDS.contains(&kind) => {
                    let boot_epoch_now = self.boot_epoch().await?;
                    if boot_epoch_changed(boot_epoch_before, boot_epoch_now.as_deref()) {
                        return Err(ContractError {
                            code: codes::INTERNAL_ERROR,
                            message: format!(
                                "timed out waiting for command intent: {command_id}; daemon boot epoch changed"
                            ),
                        });
                    }
                }
                _ if tokio::time::Instant::now() >= deadline => {
                    return Err(ContractError {
                        code: codes::INTERNAL_ERROR,
                        message: format!("timed out waiting for command intent: {command_id}"),
                    });
                }
                _ => tokio::time::sleep(self.poll_interval).await,
            }
        }
    }

    fn direct_store(&self) -> Result<&Store, ContractError> {
        match &self.backend {
            StoreClientBackend::DirectStore(store) => Ok(store),
            StoreClientBackend::Daemon(_) => Err(ContractError {
                code: codes::INTERNAL_ERROR,
                message: "direct store access is unavailable for daemon IPC clients".into(),
            }),
        }
    }
}

fn boot_epoch_changed(before: Option<&str>, now: Option<&str>) -> bool {
    now.is_some() && before != now
}

impl StoreCaller {
    /// Resolve the ambient caller. An agent identity (`NEXUS_NAME`, issued at launch) always
    /// wins; an env-less local shell is the zero-auth **local operator** — that is the designed
    /// human default (`whoami` is always the user). The B21 invariant lives on the launch side:
    /// spawned agents always receive their own daemon-minted identity env and never the
    /// operator's, so this fallback is only ever reached by human shells.
    fn from_env() -> Result<StoreCaller, ContractError> {
        if let Some(identity) = crate::cli::ambient::identity_from_env_result()? {
            return Ok(StoreCaller::from_register(identity));
        }
        Ok(StoreCaller::local_operator())
    }

    /// The zero-auth local-operator Admin caller (env-less human shell).
    fn local_operator() -> StoreCaller {
        StoreCaller {
            name: local_operator_display_name(),
            project: std::env::var("NEXUS_PROJECT")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "default".to_string()),
            session_id: Some(LOCAL_OPERATOR_SESSION_ID.to_string()),
            agent_id: None,
            runtime_id: Some(LOCAL_OPERATOR_SESSION_ID.to_string()),
            client_key: None,
            kind: Kind::Human,
            tier: Tier::Admin,
        }
    }

    fn from_register(identity: RegisterRequest) -> StoreCaller {
        // Explicit identities are already authenticated by their request payload; do not borrow
        // ambient session/runtime ids from the current process.
        let fallback = identity
            .agent_id
            .as_ref()
            .map(|id| id.0.clone())
            .unwrap_or_else(|| identity.client_key.clone());
        StoreCaller {
            name: identity.name.unwrap_or(fallback),
            project: identity.project,
            session_id: None,
            agent_id: identity.agent_id.map(|id| id.0),
            runtime_id: None,
            client_key: Some(identity.client_key),
            kind: identity.kind.unwrap_or(Kind::Agent),
            tier: identity.tier,
        }
    }

    async fn canonicalize_registered_client_key(
        mut self,
        store: &Store,
    ) -> Result<StoreCaller, ContractError> {
        let Some(client_key) = self.client_key.as_deref() else {
            return Ok(self);
        };

        let Some(row) = Sessions::new(store)
            .find_by_client_key_any_project(client_key)
            .await
            .map_err(store_err)?
        else {
            return Ok(self);
        };

        self.name = row.display_name();
        self.project = row.project;
        self.agent_id = row.agent_id.or(self.agent_id);
        self.client_key = row.client_key.or(self.client_key);
        self.kind = kind_from_store(row.kind.as_str());
        self.tier = tier_from_store(row.tier.as_str());
        Ok(self)
    }

    fn to_ipc(&self) -> DaemonIpcCaller {
        DaemonIpcCaller {
            name: Some(self.name.clone()),
            project: self.project.clone(),
            session_id: self.session_id.clone(),
            agent_id: self.agent_id.clone(),
            runtime_id: self.runtime_id.clone(),
            client_key: self.client_key.clone(),
            kind: self.kind,
            tier: self.tier,
        }
    }
}

fn new_command_id(kind: &str) -> String {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    let sanitized = kind.replace('.', "_");
    let sequence = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!(
        "cmd_{sanitized}_{}_{}_{}",
        now(),
        std::process::id(),
        sequence
    )
}

fn error_json_to_contract_error(json: &str) -> ContractError {
    serde_json::from_str(json).unwrap_or_else(|e| ContractError {
        code: codes::INTERNAL_ERROR,
        message: format!("invalid command error_json: {e}"),
    })
}

fn serde_err(e: serde_json::Error) -> ContractError {
    ContractError {
        code: codes::INVALID_PARAMS,
        message: format!("json serialization error: {e}"),
    }
}

fn store_err(e: nexus_common::NexusError) -> ContractError {
    e.to_contract_error()
}

fn ipc_err(e: std::io::Error) -> ContractError {
    ContractError {
        code: codes::INTERNAL_ERROR,
        message: format!("daemon IPC error: {e}"),
    }
}

fn kind_token(kind: Kind) -> &'static str {
    match kind {
        Kind::Agent => "agent",
        Kind::Human => "human",
        Kind::Notification => "notification",
        Kind::App => "app",
    }
}

fn kind_from_store(kind: &str) -> Kind {
    match kind {
        "human" => Kind::Human,
        "notification" => Kind::Notification,
        "app" => Kind::App,
        _ => Kind::Agent,
    }
}

fn tier_token(tier: Tier) -> &'static str {
    match tier {
        Tier::Agent => "agent",
        Tier::Admin => "admin",
    }
}

fn tier_from_store(tier: &str) -> Tier {
    match tier {
        "admin" => Tier::Admin,
        _ => Tier::Agent,
    }
}

#[cfg(test)]
mod caller_gate_tests {
    use super::*;
    use crate::cli::ambient::with_scrubbed_identity_env;

    #[test]
    fn envless_shell_is_the_local_operator() {
        let caller = with_scrubbed_identity_env(&[], StoreCaller::from_env);
        let caller = caller.unwrap();
        assert_eq!(
            caller.session_id.as_deref(),
            Some(LOCAL_OPERATOR_SESSION_ID)
        );
        assert_eq!(caller.tier, Tier::Admin);
        assert!(caller.client_key.is_none());
    }

    #[test]
    fn agent_identity_always_wins_and_is_never_operator() {
        let caller = with_scrubbed_identity_env(
            &[("NEXUS_NAME", "demoa"), ("NEXUS_CLIENT_KEY", "ck_demoa")],
            StoreCaller::from_env,
        )
        .unwrap();
        assert_eq!(caller.name, "demoa");
        assert_eq!(caller.tier, Tier::Agent);
        assert_eq!(caller.client_key.as_deref(), Some("ck_demoa"));
        assert!(caller.session_id.is_none());
    }

    #[test]
    fn partial_agent_env_is_not_local_operator() {
        let error = with_scrubbed_identity_env(&[("NEXUS_NAME", "demoa")], StoreCaller::from_env)
            .unwrap_err();
        assert_eq!(error.code, codes::UNAUTHORIZED);
        assert!(error.message.contains("NEXUS_CLIENT_KEY"));
    }
}
