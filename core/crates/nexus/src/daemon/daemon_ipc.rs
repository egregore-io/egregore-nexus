//! Daemon-owned local request ingress.
//!
//! Producers no longer need a libSQL/Hrana client. Command calls are inserted into the durable
//! `command_intents` ledger by the daemon, then the response waits on an in-process completion
//! wake. Query calls will use the same local frame boundary without creating command rows.

use std::path::{Path, PathBuf};

use nexus_common::now;
use nexus_contracts::{
    codes, entity_kind, AgentId, Caller, CommandQueueMutationRequest, ContractError, DaemonIpcCall,
    DaemonIpcCaller, DaemonIpcRequest, DaemonIpcResponse, Kind, MessageId, Presence, Request,
    RpcError, SessionId, Tier, Whoami, DAEMON_IPC_PROTOCOL_VERSION, JSONRPC_VERSION,
};
use nexus_store::repos::{Agents, CommandIntents, CommandQueue, Inbox, NewCommandIntent, Sessions};
use nexus_store::types::SessionRow;
use serde_json::{json, Value};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::daemon::AppState;
use crate::local_operator::{
    display_name as local_operator_display_name, LOCAL_OPERATOR_SESSION_ID,
};

/// Maximum encoded local IPC frame. Large transcripts are paginated domain responses; one caller
/// cannot turn the daemon socket into an unbounded allocation.
pub const MAX_DAEMON_IPC_FRAME_LEN: usize = 16 * 1024 * 1024;
const ENDPOINT_MANIFEST: &str = "daemon-ipc-endpoint.json";

/// Boot-scoped local endpoint published for CLI, MCP, and gateway producers.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DaemonIpcEndpoint {
    pub version: u32,
    pub path: PathBuf,
    pub token: String,
    pub daemon_boot_id: String,
    pub created_at: i64,
}

/// Store-backed diagnostics returned to the local lifecycle CLI without opening a second DB
/// connection outside the daemon.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DaemonStoreStatusSnapshot {
    pub lane_depths: Vec<DaemonLaneDepth>,
    pub wedged_intents: i64,
    pub dead_letter_count: u64,
    pub dead_letter_oldest_created_at: Option<i64>,
    pub transport_pairs: Vec<DaemonTransportPair>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DaemonLaneDepth {
    pub kind: String,
    pub pending: i64,
    pub claimed: i64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DaemonTransportPair {
    pub session: String,
    pub name: String,
    pub transport: String,
    pub count: usize,
}

/// Owns the bound endpoint and removes its manifest/socket on daemon shutdown.
pub struct DaemonIpcHandle {
    endpoint: DaemonIpcEndpoint,
    manifest_path: PathBuf,
    task: JoinHandle<()>,
}

impl DaemonIpcHandle {
    pub fn endpoint(&self) -> &DaemonIpcEndpoint {
        &self.endpoint
    }
}

impl Drop for DaemonIpcHandle {
    fn drop(&mut self) {
        self.task.abort();
        cleanup_socket_path(&self.endpoint.path);
        let _ = std::fs::remove_file(&self.manifest_path);
    }
}

/// Bind the daemon's local request socket/pipe and atomically publish its boot manifest.
pub fn spawn_daemon_ipc(
    state: AppState,
    daemon_boot_id: String,
    nexus_home: &Path,
) -> std::io::Result<DaemonIpcHandle> {
    std::fs::create_dir_all(nexus_home)?;
    let endpoint = DaemonIpcEndpoint {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        path: daemon_ipc_socket_path(nexus_home),
        token: Uuid::new_v4().to_string(),
        daemon_boot_id,
        created_at: now(),
    };
    cleanup_socket_path(&endpoint.path);
    let task = spawn_socket_listener(endpoint.clone(), state)?;
    let manifest_path = daemon_ipc_endpoint_manifest_path(nexus_home);
    if let Err(error) = write_daemon_ipc_endpoint(&manifest_path, &endpoint) {
        task.abort();
        cleanup_socket_path(&endpoint.path);
        return Err(error);
    }
    Ok(DaemonIpcHandle {
        endpoint,
        manifest_path,
        task,
    })
}

pub fn daemon_ipc_endpoint_manifest_path(nexus_home: &Path) -> PathBuf {
    nexus_home.join(ENDPOINT_MANIFEST)
}

pub fn read_daemon_ipc_endpoint(nexus_home: &Path) -> Option<DaemonIpcEndpoint> {
    let raw = std::fs::read_to_string(daemon_ipc_endpoint_manifest_path(nexus_home)).ok()?;
    serde_json::from_str(&raw).ok()
}

/// Execute one producer call against the endpoint currently published by the daemon.
///
/// The boot token is always taken from the mode-0600 manifest, not from the caller's frame. A
/// daemon restart therefore makes a stale client reconnect through the new manifest instead of
/// continuing against an obsolete store owner.
pub async fn call_daemon_ipc(
    nexus_home: &Path,
    mut request: DaemonIpcRequest,
    timeout: std::time::Duration,
) -> std::io::Result<DaemonIpcResponse> {
    let endpoint = read_daemon_ipc_endpoint(nexus_home).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Nexus daemon IPC endpoint is unavailable; start the daemon",
        )
    })?;
    if endpoint.version != DAEMON_IPC_PROTOCOL_VERSION {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "unsupported daemon IPC endpoint version {}; expected {}",
                endpoint.version, DAEMON_IPC_PROTOCOL_VERSION
            ),
        ));
    }
    request.token = endpoint.token.clone();
    tokio::time::timeout(timeout, call_daemon_endpoint(&endpoint, &request))
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("daemon IPC request timed out: {}", request.request_id),
            )
        })?
}

#[cfg(unix)]
async fn call_daemon_endpoint(
    endpoint: &DaemonIpcEndpoint,
    request: &DaemonIpcRequest,
) -> std::io::Result<DaemonIpcResponse> {
    let mut stream = tokio::net::UnixStream::connect(&endpoint.path).await?;
    write_request_frame(&mut stream, request).await?;
    read_response_frame(&mut stream).await
}

#[cfg(windows)]
async fn call_daemon_endpoint(
    endpoint: &DaemonIpcEndpoint,
    request: &DaemonIpcRequest,
) -> std::io::Result<DaemonIpcResponse> {
    use tokio::net::windows::named_pipe::ClientOptions;
    let mut pipe = ClientOptions::new().open(&endpoint.path)?;
    write_request_frame(&mut pipe, request).await?;
    read_response_frame(&mut pipe).await
}

#[cfg(not(any(unix, windows)))]
async fn call_daemon_endpoint(
    _endpoint: &DaemonIpcEndpoint,
    _request: &DaemonIpcRequest,
) -> std::io::Result<DaemonIpcResponse> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "daemon IPC requires Unix sockets or Windows named pipes",
    ))
}

fn write_daemon_ipc_endpoint(
    manifest_path: &Path,
    endpoint: &DaemonIpcEndpoint,
) -> std::io::Result<()> {
    let tmp = manifest_path.with_extension(format!("json.tmp-{}", std::process::id()));
    let body = serde_json::to_string_pretty(endpoint).map_err(invalid_data)?;
    std::fs::write(&tmp, format!("{body}\n"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    let _ = std::fs::remove_file(manifest_path);
    std::fs::rename(tmp, manifest_path)
}

fn daemon_ipc_socket_path(nexus_home: &Path) -> PathBuf {
    endpoint_path_for_platform(nexus_home, cfg!(windows))
}

/// Pure platform contract kept executable on every CI host. macOS and Linux share the Unix
/// filesystem socket shape; Windows uses a stable named pipe derived from the Nexus home.
fn endpoint_path_for_platform(nexus_home: &Path, windows: bool) -> PathBuf {
    if windows {
        let key = fnv1a64(nexus_home.to_string_lossy().as_bytes());
        return PathBuf::from(format!(r"\\.\pipe\nexus-daemon-ipc-{key:016x}"));
    }
    nexus_home.join("daemon-ipc.sock")
}

#[allow(dead_code)]
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(unix)]
fn spawn_socket_listener(
    endpoint: DaemonIpcEndpoint,
    state: AppState,
) -> std::io::Result<JoinHandle<()>> {
    let listener = UnixListener::bind(&endpoint.path)?;
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(&endpoint.path, std::fs::Permissions::from_mode(0o600))?;
    Ok(tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let state = state.clone();
            let token = endpoint.token.clone();
            tokio::spawn(async move {
                if let Err(error) = serve_connection(stream, state, token).await {
                    tracing::debug!(%error, "daemon IPC connection closed with error");
                }
            });
        }
    }))
}

#[cfg(windows)]
fn spawn_socket_listener(
    endpoint: DaemonIpcEndpoint,
    state: AppState,
) -> std::io::Result<JoinHandle<()>> {
    use tokio::net::windows::named_pipe::{PipeMode, ServerOptions};
    let name = endpoint.path.to_string_lossy().into_owned();
    let first = ServerOptions::new()
        .pipe_mode(PipeMode::Byte)
        .first_pipe_instance(true)
        .create(&name)?;
    Ok(tokio::spawn(async move {
        let mut server = first;
        loop {
            if server.connect().await.is_err() {
                return;
            }
            let connected = server;
            let next = match ServerOptions::new().pipe_mode(PipeMode::Byte).create(&name) {
                Ok(next) => next,
                Err(_) => return,
            };
            let state = state.clone();
            let token = endpoint.token.clone();
            tokio::spawn(async move {
                if let Err(error) = serve_connection(connected, state, token).await {
                    tracing::debug!(%error, "daemon IPC pipe closed with error");
                }
            });
            server = next;
        }
    }))
}

#[cfg(not(any(unix, windows)))]
fn spawn_socket_listener(
    _endpoint: DaemonIpcEndpoint,
    _state: AppState,
) -> std::io::Result<JoinHandle<()>> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "daemon IPC requires Unix sockets or Windows named pipes",
    ))
}

#[cfg(unix)]
fn cleanup_socket_path(path: &Path) {
    let _ = std::fs::remove_file(path);
}

#[cfg(not(unix))]
fn cleanup_socket_path(_path: &Path) {}

#[path = "../../tests/unit/daemon_ipc.rs"]
mod daemon_ipc_contracts;

/// Serve exactly one request/response exchange on a connected local stream.
pub async fn serve_connection<S>(
    mut stream: S,
    state: AppState,
    expected_token: String,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request = read_request_frame(&mut stream).await?;
    let response = handle_request(&state, &expected_token, request).await;
    write_response_frame(&mut stream, &response).await
}

pub async fn write_request_frame<W>(
    writer: &mut W,
    request: &DaemonIpcRequest,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_json_frame(writer, request).await
}

pub async fn read_request_frame<R>(reader: &mut R) -> std::io::Result<DaemonIpcRequest>
where
    R: AsyncRead + Unpin,
{
    read_json_frame(reader).await
}

pub async fn write_response_frame<W>(
    writer: &mut W,
    response: &DaemonIpcResponse,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_json_frame(writer, response).await
}

pub async fn read_response_frame<R>(reader: &mut R) -> std::io::Result<DaemonIpcResponse>
where
    R: AsyncRead + Unpin,
{
    read_json_frame(reader).await
}

async fn write_json_frame<W, T>(writer: &mut W, value: &T) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
    T: serde::Serialize,
{
    let payload = serde_json::to_vec(value).map_err(invalid_data)?;
    if payload.len() > MAX_DAEMON_IPC_FRAME_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("daemon IPC frame too large: {}", payload.len()),
        ));
    }
    writer.write_u32(payload.len() as u32).await?;
    writer.write_all(&payload).await?;
    writer.flush().await
}

async fn read_json_frame<R, T>(reader: &mut R) -> std::io::Result<T>
where
    R: AsyncRead + Unpin,
    T: serde::de::DeserializeOwned,
{
    let len = reader.read_u32().await? as usize;
    if len > MAX_DAEMON_IPC_FRAME_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("daemon IPC frame too large: {len}"),
        ));
    }
    let mut payload = vec![0_u8; len];
    reader.read_exact(&mut payload).await?;
    serde_json::from_slice(&payload).map_err(invalid_data)
}

fn invalid_data(error: serde_json::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, error)
}

/// Validate and execute one already-decoded local IPC request.
///
/// The socket server calls this function after reading one bounded frame. Keeping durable ingress
/// here makes the command semantics directly testable without a filesystem socket or live daemon.
pub async fn handle_request(
    state: &AppState,
    expected_token: &str,
    request: DaemonIpcRequest,
) -> DaemonIpcResponse {
    let request_id = request.request_id.clone();
    if request.version != DAEMON_IPC_PROTOCOL_VERSION {
        return failure(
            request_id,
            codes::INVALID_REQUEST,
            format!(
                "unsupported daemon IPC version {}; expected {}",
                request.version, DAEMON_IPC_PROTOCOL_VERSION
            ),
        );
    }
    if request.token != expected_token {
        return failure(
            request_id,
            codes::UNAUTHORIZED,
            "stale or invalid daemon IPC token",
        );
    }

    match request.call {
        DaemonIpcCall::Command {
            command_id,
            kind,
            params,
            idempotency_key,
        } => {
            handle_command(
                state,
                request_id,
                request.caller,
                command_id,
                kind,
                params,
                idempotency_key,
            )
            .await
        }
        DaemonIpcCall::Enqueue {
            command_id,
            kind,
            params,
            idempotency_key,
        } => {
            handle_enqueue(
                state,
                request_id,
                request.caller,
                command_id,
                kind,
                params,
                idempotency_key,
            )
            .await
        }
        DaemonIpcCall::Query { method, params } => {
            handle_query(state, request_id, request.caller, method, params).await
        }
    }
}

async fn handle_enqueue(
    state: &AppState,
    request_id: String,
    caller: Option<DaemonIpcCaller>,
    command_id: String,
    kind: String,
    params: Value,
    idempotency_key: Option<String>,
) -> DaemonIpcResponse {
    let row = command_row(caller, command_id, kind, params, idempotency_key);
    let commands = CommandIntents::new(&state.store);
    let durable_command_id = match commands.insert_pending_or_resume(row).await {
        Ok(command_id) => command_id,
        Err(error) => return store_failure(request_id, error.to_contract_error()),
    };
    let receipt = match commands.receipt(&durable_command_id).await {
        Ok(Some(receipt)) => receipt,
        Ok(None) => {
            return failure(
                request_id,
                codes::INTERNAL_ERROR,
                format!("command intent disappeared: {durable_command_id}"),
            )
        }
        Err(error) => return store_failure(request_id, error.to_contract_error()),
    };
    DaemonIpcResponse::success(
        request_id,
        json!({
            "commandId": receipt.command_id,
            "status": receipt.status,
            "createdAt": receipt.created_at,
            "revision": receipt.revision,
            "sessionId": receipt.session_id,
            "seq": receipt.seq,
        }),
    )
}

async fn handle_query(
    state: &AppState,
    request_id: String,
    caller: Option<DaemonIpcCaller>,
    method: String,
    params: Value,
) -> DaemonIpcResponse {
    if method == "local.store.export" {
        return handle_local_store_export_query(state, request_id, caller, params).await;
    }
    if method == "local.store.read" {
        if state.store.has_split_authority() {
            return failure(
                request_id,
                codes::METHOD_NOT_FOUND,
                "daemon store reads are unavailable; use the Nexus Gateway REST API",
            );
        }
        return handle_local_store_read_query(state, request_id, caller, params).await;
    }
    if method == "local.sessionQueue.read" {
        return handle_local_session_queue_read(state, request_id, caller, params).await;
    }
    if method == "local.sessionQueue.mutate" {
        return handle_local_session_queue_mutation(state, request_id, caller, params).await;
    }
    if method == "local.humanRead.markDelivered" {
        return handle_local_human_read_settlement(state, request_id, caller, params).await;
    }
    if method == "local.daemon.storeStatus" {
        return handle_local_daemon_status_query(state, request_id, caller).await;
    }
    if matches!(
        method.as_str(),
        "local.gateway.deliveryMode.show" | "local.gateway.deliveryMode.set"
    ) {
        return handle_local_gateway_delivery_mode_query(
            state, request_id, caller, &method, params,
        );
    }
    if method == "local.mcp.resolveIdentity" {
        return handle_local_mcp_identity_query(state, request_id, caller, params).await;
    }
    if matches!(
        method.as_str(),
        "local.terminal.describe" | "local.terminal.revivePlan"
    ) {
        return handle_local_terminal_query(state, request_id, caller, &method, &params).await;
    }
    if method == "whoami" && caller.as_ref().is_some_and(is_local_operator) {
        let caller = caller.expect("checked above");
        return DaemonIpcResponse::success(
            request_id,
            serde_json::to_value(Whoami {
                agent_id: None,
                name: Some(local_operator_display_name()),
                session_id: SessionId(LOCAL_OPERATOR_SESSION_ID.into()),
                role: None,
                tier: Tier::Admin,
                project: caller.project,
                presence: Presence::Online,
            })
            .unwrap_or(Value::Null),
        );
    }
    let caller = match caller {
        None => None,
        Some(caller) if is_local_operator(&caller) => Some(Caller {
            agent_id: None,
            session: SessionId(LOCAL_OPERATOR_SESSION_ID.into()),
            name: local_operator_display_name(),
            project: caller.project,
            tier: Tier::Admin,
            locality: Default::default(),
            access: None,
            principal_id: None,
        }),
        Some(caller) => match resolve_registered_query_caller(state, &caller).await {
            Ok(caller) => Some(caller),
            Err(error) => return store_failure(request_id, error),
        },
    };
    let response = crate::daemon::routing::route_request(
        state,
        caller,
        Request {
            jsonrpc: JSONRPC_VERSION.into(),
            id: None,
            method,
            params: Some(params),
        },
    )
    .await;
    match (response.result, response.error) {
        (Some(result), None) => DaemonIpcResponse::success(request_id, result),
        (_, Some(error)) => DaemonIpcResponse::failure(request_id, error),
        _ => failure(
            request_id,
            codes::INTERNAL_ERROR,
            "daemon query returned neither a result nor an error",
        ),
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LocalGatewayDeliveryModeSetRequest {
    delivery_mode: String,
}

fn handle_local_gateway_delivery_mode_query(
    state: &AppState,
    request_id: String,
    caller: Option<DaemonIpcCaller>,
    method: &str,
    params: Value,
) -> DaemonIpcResponse {
    if !caller.as_ref().is_some_and(is_local_operator) {
        return failure(
            request_id,
            codes::UNAUTHORIZED,
            "Gateway delivery-mode control requires local operator authority",
        );
    }
    let Some(publisher) = state.ws.gateway_stream_publisher() else {
        return failure(
            request_id,
            codes::INTERNAL_ERROR,
            "daemon Gateway projection publisher is unavailable",
        );
    };
    if method == "local.gateway.deliveryMode.set" {
        let request: LocalGatewayDeliveryModeSetRequest = match serde_json::from_value(params) {
            Ok(request) => request,
            Err(error) => {
                return failure(
                    request_id,
                    codes::INVALID_PARAMS,
                    format!("invalid Gateway delivery-mode request: {error}"),
                )
            }
        };
        let mode = match request.delivery_mode.parse() {
            Ok(mode) => mode,
            Err(error) => return failure(request_id, codes::INVALID_PARAMS, error),
        };
        publisher.set_projection_delivery_mode(mode);
    }
    let stats = publisher.projection_stats();
    DaemonIpcResponse::success(
        request_id,
        json!({
            "configured": gateway_delivery_mode_token(stats.delivery_mode),
            "effective": gateway_delivery_mode_token(stats.delivery_mode),
            "daemonEpoch": stats.daemon_epoch,
            "events": stats.events,
            "bytes": stats.bytes,
            "gaps": stats.gaps,
            "dropped": stats.dropped,
            "coalesced": stats.coalesced,
            "ackedThrough": stats.acked_through,
            "nextSeq": stats.next_seq,
            "resyncRequired": stats.resync_required,
        }),
    )
}

fn gateway_delivery_mode_token(mode: nexus_common::GatewayProjectionDeliveryMode) -> &'static str {
    match mode {
        nexus_common::GatewayProjectionDeliveryMode::Buffered => "buffered",
        nexus_common::GatewayProjectionDeliveryMode::BestEffort => "best_effort",
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LocalStoreExportRequest {
    kind: String,
    cursor: Option<String>,
    #[serde(default = "default_legacy_export_limit")]
    limit: u32,
}

fn default_legacy_export_limit() -> u32 {
    200
}

async fn handle_local_store_export_query(
    state: &AppState,
    request_id: String,
    caller: Option<DaemonIpcCaller>,
    params: Value,
) -> DaemonIpcResponse {
    if !caller.as_ref().is_some_and(is_local_operator) {
        return failure(
            request_id,
            codes::UNAUTHORIZED,
            "legacy Gateway export requires local operator authority",
        );
    }
    let request: LocalStoreExportRequest = match serde_json::from_value(params) {
        Ok(request) => request,
        Err(error) => {
            return failure(
                request_id,
                codes::INVALID_PARAMS,
                format!("invalid legacy Gateway export request: {error}"),
            )
        }
    };
    let table = match request.kind.as_str() {
        "identities" => "agents",
        "runtimes" => "agent_runtimes",
        "threads" => "threads",
        "thread_members" => "thread_members",
        "topics" => "topics",
        "topic_subscriptions" => "subscriptions",
        "messages" => "messages",
        "notifications" => "notifications",
        "delivery_outcomes" => "in_flight",
        _ => {
            return failure(
                request_id,
                codes::INVALID_PARAMS,
                format!("unsupported legacy Gateway export kind: {}", request.kind),
            )
        }
    };
    if !(1..=500).contains(&request.limit) {
        return failure(
            request_id,
            codes::INVALID_PARAMS,
            "legacy Gateway export limit must be between 1 and 500",
        );
    }
    let after_rowid = match request.cursor.as_deref().unwrap_or("0").parse::<i64>() {
        Ok(cursor) if cursor >= 0 => cursor,
        _ => {
            return failure(
                request_id,
                codes::INVALID_PARAMS,
                "legacy Gateway export cursor must be a non-negative integer",
            )
        }
    };
    let fetch_limit = i64::from(request.limit) + 1;
    let sql = format!(
        "SELECT rowid AS _cursor, * FROM {table} WHERE rowid > ?1 ORDER BY rowid ASC LIMIT ?2"
    );
    let mut rows = match state
        .store
        .conn
        .query(&sql, libsql::params![after_rowid, fetch_limit])
        .await
    {
        Ok(rows) => rows,
        Err(error) => return failure(request_id, codes::INTERNAL_ERROR, error.to_string()),
    };
    let columns = (0..rows.column_count())
        .map(|index| rows.column_name(index).unwrap_or("").to_string())
        .collect::<Vec<_>>();
    let mut output = Vec::new();
    loop {
        let row = match rows.next().await {
            Ok(Some(row)) => row,
            Ok(None) => break,
            Err(error) => return failure(request_id, codes::INTERNAL_ERROR, error.to_string()),
        };
        let cursor = match row.get::<i64>(0) {
            Ok(cursor) => cursor,
            Err(error) => return failure(request_id, codes::INTERNAL_ERROR, error.to_string()),
        };
        let mut object = serde_json::Map::new();
        for (index, column) in columns.iter().enumerate().skip(1) {
            let value = match row.get_value(index as i32) {
                Ok(value) => sql_to_json_value(value),
                Err(error) => return failure(request_id, codes::INTERNAL_ERROR, error.to_string()),
            };
            object.insert(column.clone(), value);
        }
        output.push((cursor, Value::Object(object)));
    }
    let has_more = output.len() > request.limit as usize;
    output.truncate(request.limit as usize);
    let next_cursor = has_more
        .then(|| output.last().map(|(cursor, _)| cursor.to_string()))
        .flatten();
    DaemonIpcResponse::success(
        request_id,
        json!({
            "kind": request.kind,
            "rows": output.into_iter().map(|(_, row)| row).collect::<Vec<_>>(),
            "nextCursor": next_cursor,
        }),
    )
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LocalHumanReadSettlementRequest {
    session_id: String,
    message_ids: Vec<MessageId>,
    now: i64,
}

async fn handle_local_human_read_settlement(
    state: &AppState,
    request_id: String,
    caller: Option<DaemonIpcCaller>,
    params: Value,
) -> DaemonIpcResponse {
    if !caller.as_ref().is_some_and(is_local_operator) {
        return failure(
            request_id,
            codes::UNAUTHORIZED,
            "human read settlement requires local operator authority",
        );
    }
    let request: LocalHumanReadSettlementRequest = match serde_json::from_value(params) {
        Ok(request) => request,
        Err(error) => {
            return failure(
                request_id,
                codes::INVALID_PARAMS,
                format!("invalid human read settlement: {error}"),
            )
        }
    };
    match Inbox::new(&state.store)
        .mark_human_read_delivered(&request.session_id, &request.message_ids, request.now)
        .await
    {
        Ok(changed) => DaemonIpcResponse::success(request_id, json!({ "changed": changed })),
        Err(error) => store_failure(request_id, error.to_contract_error()),
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LocalSessionQueueMutationRequest {
    project: String,
    now: i64,
    request: CommandQueueMutationRequest,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LocalSessionQueueReadRequest {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    agent_id: Option<AgentId>,
    #[serde(default)]
    events_after: Option<i64>,
}

async fn handle_local_session_queue_read(
    state: &AppState,
    request_id: String,
    caller: Option<DaemonIpcCaller>,
    params: Value,
) -> DaemonIpcResponse {
    if !caller.as_ref().is_some_and(is_local_operator) {
        return failure(
            request_id,
            codes::UNAUTHORIZED,
            "session queue reads require local operator authority",
        );
    };
    let request: LocalSessionQueueReadRequest = match serde_json::from_value(params) {
        Ok(request) => request,
        Err(error) => {
            return failure(
                request_id,
                codes::INVALID_PARAMS,
                format!("invalid session queue read: {error}"),
            )
        }
    };
    let queue = CommandQueue::new(&state.store);
    let result: Result<Value, ContractError> = match request.events_after {
        Some(after_seq) if after_seq >= 0 => queue
            .events_after(after_seq)
            .await
            .map_err(|error| error.to_contract_error())
            .map(|page| {
                json!({
                    "events": page.events,
                    "nextSeq": page.next_seq,
                    "latestSeq": page.latest_seq,
                    "gap": page.gap,
                })
            }),
        Some(_) => {
            return failure(
                request_id,
                codes::INVALID_PARAMS,
                "eventsAfter must be a non-negative integer",
            )
        }
        None => {
            let name = request
                .name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty());
            if name.is_none() && request.agent_id.is_none() {
                return failure(
                    request_id,
                    codes::INVALID_PARAMS,
                    "name or agentId is required",
                );
            }
            let active_sessions = state.agent.active_turn_sessions();
            queue
                .snapshot_with_active_sessions(name, request.agent_id.as_ref(), &active_sessions)
                .await
                .map_err(|error| error.to_contract_error())
                .and_then(|snapshot| serde_json::to_value(snapshot).map_err(json_contract_error))
        }
    };
    match result {
        Ok(value) => DaemonIpcResponse::success(request_id, value),
        Err(error) => store_failure(request_id, error),
    }
}

async fn handle_local_session_queue_mutation(
    state: &AppState,
    request_id: String,
    caller: Option<DaemonIpcCaller>,
    params: Value,
) -> DaemonIpcResponse {
    if caller.filter(is_local_operator).is_none() {
        return failure(
            request_id,
            codes::UNAUTHORIZED,
            "session queue mutations require local operator authority",
        );
    }
    let request: LocalSessionQueueMutationRequest = match serde_json::from_value(params) {
        Ok(request) => request,
        Err(error) => {
            return failure(
                request_id,
                codes::INVALID_PARAMS,
                format!("invalid session queue mutation: {error}"),
            )
        }
    };
    let active_sessions = state.agent.active_turn_sessions();
    match CommandQueue::new(&state.store)
        .mutate_with_active_sessions(
            &request.project,
            &request.request,
            request.now,
            &active_sessions,
        )
        .await
    {
        Ok(outcome) => DaemonIpcResponse::success(
            request_id,
            json!({ "status": outcome.status, "body": outcome.body }),
        ),
        Err(error) => store_failure(request_id, error.to_contract_error()),
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LocalStoreReadRequest {
    sql: String,
    #[serde(default)]
    args: Vec<Value>,
}

async fn handle_local_store_read_query(
    state: &AppState,
    request_id: String,
    caller: Option<DaemonIpcCaller>,
    params: Value,
) -> DaemonIpcResponse {
    if !caller.as_ref().is_some_and(is_local_operator) {
        return failure(
            request_id,
            codes::UNAUTHORIZED,
            "raw gateway reads require local operator authority",
        );
    }
    let request: LocalStoreReadRequest = match serde_json::from_value(params) {
        Ok(request) => request,
        Err(error) => {
            return failure(
                request_id,
                codes::INVALID_PARAMS,
                format!("invalid local store read request: {error}"),
            )
        }
    };
    if !is_single_select(&request.sql) {
        return failure(
            request_id,
            codes::INVALID_PARAMS,
            "local store read accepts exactly one SELECT statement",
        );
    }
    let args = match request
        .args
        .into_iter()
        .map(json_to_sql_value)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(args) => args,
        Err(message) => return failure(request_id, codes::INVALID_PARAMS, message),
    };
    let mut rows = match state
        .store
        .conn
        .query(&request.sql, libsql::params_from_iter(args))
        .await
    {
        Ok(rows) => rows,
        Err(error) => return failure(request_id, codes::INTERNAL_ERROR, error.to_string()),
    };
    let columns = (0..rows.column_count())
        .map(|index| rows.column_name(index).unwrap_or("").to_string())
        .collect::<Vec<_>>();
    let mut output = Vec::new();
    loop {
        let row = match rows.next().await {
            Ok(Some(row)) => row,
            Ok(None) => break,
            Err(error) => return failure(request_id, codes::INTERNAL_ERROR, error.to_string()),
        };
        let mut values = Vec::with_capacity(columns.len());
        for index in 0..columns.len() {
            let value = match row.get_value(index as i32) {
                Ok(value) => sql_to_json_value(value),
                Err(error) => return failure(request_id, codes::INTERNAL_ERROR, error.to_string()),
            };
            values.push(value);
        }
        output.push(Value::Array(values));
    }
    DaemonIpcResponse::success(
        request_id,
        json!({
            "columns": columns,
            "rows": output,
            "rowsAffected": 0,
        }),
    )
}

fn is_single_select(sql: &str) -> bool {
    let trimmed = sql.trim();
    let without_trailing = trimmed.strip_suffix(';').unwrap_or(trimmed).trim_end();
    without_trailing
        .split_whitespace()
        .next()
        .is_some_and(|token| token.eq_ignore_ascii_case("SELECT"))
        && !without_trailing.contains(';')
}

fn json_to_sql_value(value: Value) -> Result<libsql::Value, String> {
    match value {
        Value::Null => Ok(libsql::Value::Null),
        Value::Bool(value) => Ok(libsql::Value::Integer(i64::from(value))),
        Value::Number(value) => value
            .as_i64()
            .map(libsql::Value::Integer)
            .or_else(|| value.as_f64().map(libsql::Value::Real))
            .ok_or_else(|| "daemon IPC SQL argument is outside the supported number range".into()),
        Value::String(value) => Ok(libsql::Value::Text(value)),
        Value::Object(mut value) => match (value.remove("$blob"), value.remove("$integer")) {
            (Some(Value::Array(bytes)), None) => bytes
                .into_iter()
                .map(|byte| {
                    byte.as_u64()
                        .filter(|byte| *byte <= u8::MAX as u64)
                        .map(|byte| byte as u8)
                        .ok_or_else(|| "daemon IPC blob argument contains a non-byte value".into())
                })
                .collect::<Result<Vec<_>, String>>()
                .map(libsql::Value::Blob),
            (None, Some(Value::String(integer))) => integer
                .parse::<i64>()
                .map(libsql::Value::Integer)
                .map_err(|_| "daemon IPC integer argument is outside the i64 range".into()),
            _ => Err(
                "daemon IPC SQL arguments must be scalar values, {$blob:[...]}, or {$integer:\"...\"}"
                    .into(),
            ),
        },
        Value::Array(_) => {
            Err("daemon IPC SQL arguments must be scalar values or {$blob:[...]}".into())
        }
    }
}

fn sql_to_json_value(value: libsql::Value) -> Value {
    match value {
        libsql::Value::Null => Value::Null,
        libsql::Value::Integer(value) => Value::Number(value.into()),
        libsql::Value::Real(value) => serde_json::Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        libsql::Value::Text(value) => Value::String(value),
        libsql::Value::Blob(value) => json!({ "$blob": value }),
    }
}

async fn handle_local_mcp_identity_query(
    state: &AppState,
    request_id: String,
    caller: Option<DaemonIpcCaller>,
    params: Value,
) -> DaemonIpcResponse {
    if !caller.as_ref().is_some_and(is_local_operator) {
        return failure(
            request_id,
            codes::UNAUTHORIZED,
            "MCP identity resolution requires local operator authority",
        );
    }
    let request = match serde_json::from_value(params) {
        Ok(request) => request,
        Err(error) => {
            return failure(
                request_id,
                codes::INVALID_PARAMS,
                format!("invalid MCP identity request: {error}"),
            )
        }
    };
    let identity =
        crate::cli::commands::mcp::resolve_legacy_mcp_identity_in_daemon(request, &state.store)
            .await;
    match serde_json::to_value(identity) {
        Ok(value) => DaemonIpcResponse::success(request_id, value),
        Err(error) => store_failure(request_id, json_contract_error(error)),
    }
}

async fn handle_local_daemon_status_query(
    state: &AppState,
    request_id: String,
    caller: Option<DaemonIpcCaller>,
) -> DaemonIpcResponse {
    if !caller.as_ref().is_some_and(is_local_operator) {
        return failure(
            request_id,
            codes::UNAUTHORIZED,
            "daemon status query requires local operator authority",
        );
    }
    let command_intents = CommandIntents::new(&state.store);
    let lane_depths = match command_intents.lane_depths().await {
        Ok(depths) => depths
            .into_iter()
            .map(|depth| DaemonLaneDepth {
                kind: depth.kind,
                pending: depth.pending,
                claimed: depth.claimed,
            })
            .collect(),
        Err(error) => return store_failure(request_id, error.to_contract_error()),
    };
    let wedged_intents = match command_intents.expired_claimed_count(now()).await {
        Ok(count) => count,
        Err(error) => return store_failure(request_id, error.to_contract_error()),
    };
    let dead_letters = match Inbox::new(&state.store).dead_letter_summary().await {
        Ok(summary) => summary,
        Err(error) => return store_failure(request_id, error.to_contract_error()),
    };
    let sessions = match Sessions::new(&state.store).list_all().await {
        Ok(sessions) => sessions,
        Err(error) => return store_failure(request_id, error.to_contract_error()),
    };
    let transport_pairs = sessions
        .into_iter()
        .filter(|row| {
            matches!(
                row.presence.as_deref(),
                Some("online") | Some("busy") | Some("paused")
            )
        })
        .filter_map(|row| {
            let name = row.display_name();
            let transport = row.transport?;
            Some(DaemonTransportPair {
                session: row.session_id.0,
                name,
                transport,
                count: 1,
            })
        })
        .collect();
    let snapshot = DaemonStoreStatusSnapshot {
        lane_depths,
        wedged_intents,
        dead_letter_count: dead_letters.count,
        dead_letter_oldest_created_at: dead_letters.oldest_created_at,
        transport_pairs,
    };
    match serde_json::to_value(snapshot) {
        Ok(value) => DaemonIpcResponse::success(request_id, value),
        Err(error) => store_failure(request_id, json_contract_error(error)),
    }
}

async fn handle_local_terminal_query(
    state: &AppState,
    request_id: String,
    caller: Option<DaemonIpcCaller>,
    method: &str,
    params: &Value,
) -> DaemonIpcResponse {
    let Some(caller) = caller.filter(is_local_operator) else {
        return failure(
            request_id,
            codes::UNAUTHORIZED,
            "local terminal queries require local operator authority",
        );
    };
    let Some(target) = params
        .get("target")
        .and_then(Value::as_str)
        .filter(|target| !target.is_empty())
    else {
        return failure(
            request_id,
            codes::INVALID_PARAMS,
            "local terminal query requires a non-empty target",
        );
    };
    let read = crate::cli::read_client::ReadClient::from_store_with_caller_for_tests(
        state.store.clone(),
        local_operator_display_name(),
        caller.project,
        Some(LOCAL_OPERATOR_SESSION_ID.into()),
        None,
        Tier::Admin,
    );
    let result = match method {
        "local.terminal.describe" => read
            .pty_attach_descriptor(target)
            .await
            .and_then(|value| serde_json::to_value(value).map_err(json_contract_error)),
        "local.terminal.revivePlan" => read
            .attach_revive_plan(target)
            .await
            .and_then(|value| serde_json::to_value(value).map_err(json_contract_error)),
        _ => unreachable!("terminal query method checked by caller"),
    };
    match result {
        Ok(value) => DaemonIpcResponse::success(request_id, value),
        Err(error) => store_failure(request_id, error),
    }
}

async fn resolve_registered_query_caller(
    state: &AppState,
    evidence: &DaemonIpcCaller,
) -> Result<Caller, ContractError> {
    let client_key = evidence
        .client_key
        .as_deref()
        .ok_or_else(|| unauthorized("daemon IPC caller requires a registered client key"))?;
    let session = Sessions::new(&state.store)
        .find_by_client_key_any_project(client_key)
        .await
        .map_err(|error| error.to_contract_error())?
        .ok_or_else(|| unauthorized("daemon IPC caller client key is not registered"))?;
    let caller = match session.agent_id.as_deref() {
        Some(agent_id) => {
            let agent = Agents::new(&state.store)
                .find_by_id(agent_id)
                .await
                .map_err(|error| error.to_contract_error())?
                .ok_or_else(|| {
                    unauthorized("daemon IPC caller stored agent id is not registered")
                })?;
            Caller {
                agent_id: Some(AgentId(agent.agent_id)),
                session: session.session_id.clone(),
                name: agent.name.unwrap_or_else(|| session.display_name()),
                project: agent.project,
                tier: match session.tier.as_str() {
                    "admin" => Tier::Admin,
                    _ => Tier::Agent,
                },
                locality: session
                    .entity_kind()
                    .map_err(|error| error.to_contract_error())?
                    .0,
                access: session
                    .access()
                    .map_err(|error| error.to_contract_error())?,
                principal_id: evidence.principal_id.clone(),
            }
        }
        None => match session.name.as_deref() {
            Some(name) => state.identity.resolve(&session.project, name).await?,
            None => {
                return Err(unauthorized(
                    "daemon IPC caller session has no durable agent identity",
                ));
            }
        },
    };
    validate_query_caller(evidence, &session, &caller)?;
    Ok(caller)
}

fn validate_query_caller(
    evidence: &DaemonIpcCaller,
    session: &SessionRow,
    caller: &Caller,
) -> Result<(), ContractError> {
    // A display name is mutable metadata. Running harnesses retain their launch-time environment,
    // so NEXUS_NAME can legitimately be stale after an operator rename. Authenticate the stable
    // client key above and, when present, the runtime/agent ids below; always project the current
    // canonical name from the registered session instead of treating the old label as evidence.
    for value in [
        evidence.session_id.as_deref(),
        evidence.runtime_id.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if !matches_session_id(value, session, caller) {
            return Err(unauthorized(
                "daemon IPC caller runtime does not match registered client key",
            ));
        }
    }
    if evidence
        .agent_id
        .as_deref()
        .is_some_and(|agent_id| caller.agent_id.as_ref().map(|id| id.0.as_str()) != Some(agent_id))
    {
        return Err(unauthorized(
            "daemon IPC caller agent does not match registered client key",
        ));
    }
    if session.kind != entity_kind::dotted(evidence.locality, evidence.kind)
        || session.tier != tier_token(evidence.tier)
    {
        return Err(unauthorized(
            "daemon IPC caller kind or tier does not match registered client key",
        ));
    }
    Ok(())
}

fn matches_session_id(value: &str, session: &SessionRow, caller: &Caller) -> bool {
    value == session.session_id.0
        || value == caller.session.0
        || session.harness_session_id.as_deref() == Some(value)
}

fn unauthorized(message: impl Into<String>) -> ContractError {
    ContractError {
        code: codes::UNAUTHORIZED,
        message: message.into(),
    }
}

fn json_contract_error(error: serde_json::Error) -> ContractError {
    ContractError {
        code: codes::INTERNAL_ERROR,
        message: format!("daemon IPC response serialization failed: {error}"),
    }
}

fn is_local_operator(caller: &DaemonIpcCaller) -> bool {
    caller.session_id.as_deref() == Some(LOCAL_OPERATOR_SESSION_ID)
        && matches!(
            caller.runtime_id.as_deref(),
            None | Some(LOCAL_OPERATOR_SESSION_ID)
        )
        && caller.client_key.is_none()
        && caller.kind == Kind::Human
        && caller.tier == Tier::Admin
}

async fn handle_command(
    state: &AppState,
    request_id: String,
    caller: Option<DaemonIpcCaller>,
    command_id: String,
    kind: String,
    params: Value,
    idempotency_key: Option<String>,
) -> DaemonIpcResponse {
    let row = command_row(
        caller,
        command_id.clone(),
        kind.clone(),
        params,
        idempotency_key,
    );
    let commands = CommandIntents::new(&state.store);
    let durable_command_id = match commands.insert_pending_or_resume(row).await {
        Ok(command_id) => command_id,
        Err(error) => return store_failure(request_id, error.to_contract_error()),
    };

    loop {
        // Snapshot before reading so a completion racing the SELECT cannot be lost.
        let epoch = state.store.command_completions_epoch();
        let row = match commands.get(&durable_command_id).await {
            Ok(Some(row)) => row,
            Ok(None) => {
                return failure(
                    request_id,
                    codes::INTERNAL_ERROR,
                    format!("command intent disappeared: {durable_command_id}"),
                )
            }
            Err(error) => return store_failure(request_id, error.to_contract_error()),
        };
        match row.status.as_str() {
            "done" => {
                let result = row
                    .result_json
                    .as_deref()
                    .and_then(|raw| serde_json::from_str(raw).ok())
                    .unwrap_or(Value::Null);
                return DaemonIpcResponse::success(request_id, result);
            }
            "error" => {
                let error = row
                    .error_json
                    .as_deref()
                    .and_then(|raw| serde_json::from_str::<ContractError>(raw).ok())
                    .unwrap_or_else(|| ContractError {
                        code: codes::INTERNAL_ERROR,
                        message: format!(
                            "command intent failed without a valid error: {durable_command_id}"
                        ),
                    });
                return store_failure(request_id, error);
            }
            "cancelled" => {
                return failure(
                    request_id,
                    codes::INTERNAL_ERROR,
                    format!("command intent was cancelled: {durable_command_id}"),
                )
            }
            _ => state.store.wait_for_command_completion_after(epoch).await,
        }
    }
}

fn command_row(
    caller: Option<DaemonIpcCaller>,
    command_id: String,
    kind: String,
    params: Value,
    idempotency_key: Option<String>,
) -> NewCommandIntent {
    let caller = caller.unwrap_or_else(anonymous_caller);
    let caller_name = caller
        .name
        .clone()
        .or_else(|| caller.agent_id.clone())
        .or_else(|| caller.session_id.clone())
        .or_else(|| caller.client_key.clone())
        .unwrap_or_else(|| "anonymous".into());
    NewCommandIntent {
        command_id,
        kind,
        project: caller.project,
        caller_name,
        caller_session_id: caller.session_id,
        caller_agent_id: caller.agent_id,
        caller_runtime_id: caller.runtime_id,
        caller_client_key: caller.client_key,
        caller_principal_id: caller.principal_id,
        caller_kind: Some(entity_kind::dotted(caller.locality, caller.kind)),
        caller_tier: Some(tier_token(caller.tier).into()),
        idempotency_key,
        request_json: serde_json::to_string(&params).unwrap_or_else(|_| "null".into()),
        created_at: now(),
    }
}

fn anonymous_caller() -> DaemonIpcCaller {
    DaemonIpcCaller {
        name: None,
        project: "default".into(),
        session_id: None,
        agent_id: None,
        runtime_id: None,
        client_key: None,
        kind: Kind::Agent,
        locality: Default::default(),
        access: None,
        principal_id: None,
        tier: Tier::Agent,
    }
}

fn tier_token(tier: Tier) -> &'static str {
    match tier {
        Tier::Agent => "agent",
        Tier::Admin => "admin",
    }
}

fn store_failure(request_id: String, error: ContractError) -> DaemonIpcResponse {
    DaemonIpcResponse::failure(
        request_id,
        RpcError {
            code: error.code,
            message: error.message,
            data: None,
        },
    )
}

fn failure(request_id: String, code: i32, message: impl Into<String>) -> DaemonIpcResponse {
    DaemonIpcResponse::failure(
        request_id,
        RpcError {
            code,
            message: message.into(),
            data: Some(json!({"transport": "daemon_ipc"})),
        },
    )
}
