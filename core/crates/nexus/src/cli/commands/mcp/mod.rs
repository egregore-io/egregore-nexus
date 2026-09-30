//! `nexus mcp` — MCP stdio server exposing the Nexus bus tools to agents.
//!
//! Speaks JSON-RPC 2.0 over stdin/stdout with **Content-Length LSP-style framing**:
//!
//! ```text
//! Content-Length: <N>\r\n
//! \r\n
//! <N bytes of JSON>
//! ```
//!
//! This matches the MCP stdio transport specification and is the framing used by clients
//! (e.g. Claude Code's MCP host). The server handles:
//!
//! - `initialize` → server info + capabilities(tools)
//! - `notifications/initialized` → ignored (notification, no response)
//! - `tools/list` → the bus tool registry (from `tools::registry()`)
//! - `tools/call {name, arguments}` → dispatches via `tools::dispatch()` to store-backed read views
//!   or daemon-managed command intents
//!
//! The MCP server uses [`crate::cli::read_client::ReadClient`] for read-only tools and
//! [`crate::cli::store_client::StoreClient`] for mutating tools.

pub mod tools;

use std::process::ExitCode;
use std::time::Duration;

use clap::Args;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

use nexus_common::now;
use nexus_contracts::{
    codes, ContractError, DaemonIpcCall, DaemonIpcCaller, DaemonIpcRequest, HarnessId, Kind,
    RegisterRequest, Tier, DAEMON_IPC_PROTOCOL_VERSION,
};
use nexus_store::Store;

use crate::cli::read_client::ReadClient;
use crate::cli::store_client::StoreClient;

/// `nexus mcp --as <name> --project <project>`.
#[derive(Args, Debug)]
pub struct McpArgs {
    /// The agent's name to register on the bus.
    #[arg(long = "as", value_name = "NAME")]
    pub name: String,
    /// The project to associate this agent with.
    #[arg(long)]
    pub project: String,
    /// Existing session key to resume. Omit for a standalone MCP helper (`mcp:<name>`).
    #[arg(long = "client-key", value_name = "KEY")]
    pub client_key: Option<String>,
    /// Harness label for a resumed agent identity (`claude`, `codex`, `opencode`, `hermes`, `pi`, `other`).
    #[arg(long, value_name = "AGENT")]
    pub agent: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LegacyMcpIdentityRequest {
    pub name: String,
    pub project: String,
    pub agent: Option<String>,
    /// Deprecated wire field retained for compatibility; never used as Nexus authentication.
    pub claude_session_id: Option<String>,
}

pub(crate) async fn resolve_legacy_mcp_identity_in_daemon(
    request: LegacyMcpIdentityRequest,
    store: &Store,
) -> RegisterRequest {
    let args = McpArgs {
        name: request.name,
        project: request.project,
        client_key: None,
        agent: request.agent,
    };
    mcp_identity_for_store_with_claude_session(&args, store, request.claude_session_id.as_deref())
        .await
}

/// Build the `RegisterRequest` for an MCP agent identity.
///
/// Default identity: `client_key = "mcp:<name>"`, `harness = Other`, `tier = Agent`.
/// Standalone MCP helpers never borrow provider-native session ids from their parent shell.
pub fn mcp_identity(
    name: &str,
    project: &str,
    client_key: Option<&str>,
    agent: Option<&str>,
) -> RegisterRequest {
    let client_key = client_key
        .map(str::to_string)
        .unwrap_or_else(|| format!("mcp:{name}"));
    RegisterRequest {
        agent_id: None,
        name: Some(name.to_string()),
        harness: mcp_harness(agent),
        harness_session_id: client_key.clone(),
        project: project.to_string(),
        client_key,
        runtime_credential: None,
        tier: Tier::Agent,
        kind: None,
        locality: Default::default(),
        access: None,
        role: None,
        cwd: None,
    }
}

/// Resolve the MCP server identity from Nexus-owned launch data.
///
/// Daemon-owned launches pass `--client-key` directly. A legacy MCP host without that key remains
/// a standalone `mcp:<name>` identity; Claude Code's provider session id is never authentication
/// proof and is not used to adopt a Nexus runtime.
async fn resolve_mcp_identity(args: &McpArgs) -> RegisterRequest {
    if args.client_key.is_some() {
        return mcp_identity(
            &args.name,
            &args.project,
            args.client_key.as_deref(),
            args.agent.as_deref(),
        );
    }

    let fallback = || mcp_identity(&args.name, &args.project, None, args.agent.as_deref());
    let response = crate::daemon::daemon_ipc::call_daemon_ipc(
        &crate::daemon::lifecycle::nexus_home(),
        DaemonIpcRequest {
            version: DAEMON_IPC_PROTOCOL_VERSION,
            token: String::new(),
            request_id: format!("mcp-identity-{}", uuid::Uuid::new_v4()),
            caller: Some(DaemonIpcCaller {
                name: Some(crate::local_operator::display_name()),
                project: args.project.clone(),
                session_id: Some(crate::local_operator::LOCAL_OPERATOR_SESSION_ID.into()),
                agent_id: None,
                runtime_id: Some(crate::local_operator::LOCAL_OPERATOR_SESSION_ID.into()),
                client_key: None,
                kind: Kind::Human,
                locality: Default::default(),
                access: None,
                principal_id: None,
                tier: Tier::Admin,
            }),
            call: DaemonIpcCall::Query {
                method: "local.mcp.resolveIdentity".into(),
                params: serde_json::to_value(LegacyMcpIdentityRequest {
                    name: args.name.clone(),
                    project: args.project.clone(),
                    agent: args.agent.clone(),
                    claude_session_id: None,
                })
                .unwrap_or(Value::Null),
            },
        },
        Duration::from_secs(10),
    )
    .await;
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(%error, "daemon IPC unavailable while resolving legacy MCP identity");
            return fallback();
        }
    };
    if let Some(error) = response.error {
        tracing::warn!(code = error.code, message = %error.message, "daemon rejected legacy MCP identity resolution");
        return fallback();
    }
    match serde_json::from_value(response.result.unwrap_or(Value::Null)) {
        Ok(identity) => identity,
        Err(error) => {
            tracing::warn!(%error, "daemon returned invalid legacy MCP identity");
            fallback()
        }
    }
}

#[doc(hidden)]
pub async fn mcp_identity_for_store(args: &McpArgs, store: &Store) -> RegisterRequest {
    if args.client_key.is_some() {
        return mcp_identity(
            &args.name,
            &args.project,
            args.client_key.as_deref(),
            args.agent.as_deref(),
        );
    }
    mcp_identity_for_store_with_claude_session(args, store, None).await
}

#[doc(hidden)]
pub async fn mcp_identity_for_store_with_claude_session(
    args: &McpArgs,
    _store: &Store,
    _claude_session_id: Option<&str>,
) -> RegisterRequest {
    mcp_identity(
        &args.name,
        &args.project,
        args.client_key.as_deref(),
        args.agent.as_deref(),
    )
}

/// Parse the optional MCP `--agent` label. Invalid labels fall back to `other` so manually-started
/// MCP helpers do not fail just because a malformed harness token reached an older CLI.
fn mcp_harness(agent: Option<&str>) -> HarnessId {
    agent
        .and_then(|s| HarnessId::new(s).ok())
        .unwrap_or_else(|| HarnessId::new("other").expect("builtin harness id is valid"))
}

/// Run the MCP stdio server.
///
/// Reads Content-Length framed JSON-RPC requests from stdin, dispatches them,
/// and writes Content-Length framed JSON-RPC responses to stdout.
pub async fn run_mcp(args: McpArgs) -> ExitCode {
    let identity = resolve_mcp_identity(&args).await;
    let store_client = match StoreClient::from_config_with_identity(identity.clone()).await {
        Ok(client) => Some(client),
        Err(err) => {
            tracing::warn!(error = %err, "failed to open store-backed MCP command client");
            None
        }
    };
    let read_client = match ReadClient::from_config_with_identity(identity).await {
        Ok(client) => Some(client),
        Err(err) => {
            tracing::warn!(error = %err, "failed to open store-backed MCP read client");
            None
        }
    };

    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin);
    let mcp_run_scope = new_mcp_run_scope();

    loop {
        // Read one JSON-RPC message, auto-detecting newline vs Content-Length framing.
        let (msg_bytes, framing) = match read_framed_message(&mut reader).await {
            Ok(Some(pair)) => pair,
            Ok(None) => break, // EOF — client disconnected
            Err(e) => {
                // I/O error on stdin — reply (defaulting to the spec's newline framing) and exit.
                let err_resp = json_rpc_error(
                    Value::Null,
                    codes::INTERNAL_ERROR,
                    &format!("framing error: {e}"),
                );
                let _ = write_framed_message(&mut stdout, &err_resp, Framing::Newline).await;
                return ExitCode::from(1);
            }
        };

        let request: Value = match serde_json::from_slice(&msg_bytes) {
            Ok(v) => v,
            Err(e) => {
                let err = json_rpc_error(
                    Value::Null,
                    codes::PARSE_ERROR,
                    &format!("invalid JSON: {e}"),
                );
                if write_framed_message(&mut stdout, &err, framing)
                    .await
                    .is_err()
                {
                    return ExitCode::from(1);
                }
                continue;
            }
        };

        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let method = request.get("method").and_then(|m| m.as_str()).unwrap_or("");

        // Notifications have no `id` (or explicit `id: null`) — handle them without sending a response.
        let is_notification = request.get("id").map_or(true, |v| v.is_null())
            || matches!(
                method,
                "notifications/initialized" | "notifications/cancelled"
            );

        let response = match method {
            "initialize" => handle_initialize(&id),
            "tools/list" => handle_tools_list(&id),
            "tools/call" => {
                let params = request.get("params").cloned().unwrap_or(Value::Null);
                handle_tools_call(
                    read_client.as_ref(),
                    store_client.as_ref(),
                    &mcp_run_scope,
                    &id,
                    &params,
                )
                .await
            }
            // Notifications: ignore, no response.
            _ if is_notification => continue,
            other => json_rpc_error(
                id,
                codes::METHOD_NOT_FOUND,
                &format!("method not found: '{other}'"),
            ),
        };

        if write_framed_message(&mut stdout, &response, framing)
            .await
            .is_err()
        {
            return ExitCode::from(1);
        }
    }

    ExitCode::SUCCESS
}

/// Handle `initialize` — return protocol version, server info, and tool capabilities.
fn handle_initialize(id: &Value) -> Value {
    json_rpc_result(
        id.clone(),
        json!({
            "protocolVersion": "2024-11-05",
            "serverInfo": {
                "name": "nexus",
                "version": env!("CARGO_PKG_VERSION")
            },
            "capabilities": {
                "tools": {}
            }
        }),
    )
}

/// Handle `tools/list` — return the full bus tool registry.
fn handle_tools_list(id: &Value) -> Value {
    let tools: Vec<Value> = tools::registry()
        .into_iter()
        .map(|t| {
            json!({
                "name": t.name,
                "description": t.description,
                "inputSchema": t.input_schema
            })
        })
        .collect();

    json_rpc_result(id.clone(), json!({ "tools": tools }))
}

/// Handle `tools/call` — dispatch to the daemon and return MCP tool result.
async fn handle_tools_call(
    read_client: Option<&ReadClient>,
    store_client: Option<&StoreClient>,
    mcp_run_scope: &str,
    id: &Value,
    params: &Value,
) -> Value {
    let name = match params.get("name").and_then(|n| n.as_str()) {
        Some(n) => n,
        None => {
            return json_rpc_error(
                id.clone(),
                codes::INVALID_PARAMS,
                "missing 'name' in tools/call params",
            );
        }
    };

    let arguments = params.get("arguments").cloned().unwrap_or(json!({}));

    let idempotency_key = store_client
        .map(|client| mcp_idempotency_key(&client.idempotency_scope(), mcp_run_scope, name, id))
        .unwrap_or_else(|| mcp_idempotency_key("no-store-client", mcp_run_scope, name, id));
    match tools::dispatch_with_idempotency(
        read_client,
        store_client,
        name,
        &arguments,
        idempotency_key.as_deref(),
    )
    .await
    {
        Ok(result) => {
            let text = serde_json::to_string_pretty(&result).unwrap_or_else(|_| result.to_string());
            json_rpc_result(
                id.clone(),
                json!({
                    "content": [{ "type": "text", "text": text }],
                    "isError": false
                }),
            )
        }
        Err(ContractError { code: _, message }) => json_rpc_result(
            id.clone(),
            json!({
                "content": [{ "type": "text", "text": message }],
                "isError": true
            }),
        ),
    }
}

fn new_mcp_run_scope() -> String {
    format!("{}-{}", std::process::id(), now())
}

fn mcp_idempotency_key(scope: &str, mcp_run_scope: &str, name: &str, id: &Value) -> Option<String> {
    if id.is_null() {
        return None;
    }
    serde_json::to_string(id)
        .ok()
        .map(|encoded| format!("mcp:{scope}:{mcp_run_scope}:{name}:{encoded}"))
}

/// Which stdio framing a peer is using. The MCP spec's stdio transport is
/// **newline-delimited JSON** (one JSON-RPC object per `\n`-terminated line),
/// which is what codex and the standard MCP SDKs send. Some LSP-derived clients
/// use **Content-Length** headers instead. We auto-detect per message and reply
/// in the SAME framing the request arrived in.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Framing {
    /// `{...}\n` — MCP stdio spec.
    Newline,
    /// `Content-Length: N\r\n\r\n{...}` — LSP style.
    ContentLength,
}

/// Parse a `Content-Length: <n>` header line (case-insensitive), returning the length.
fn parse_content_length(line: &str) -> Option<usize> {
    let rest = line
        .strip_prefix("Content-Length:")
        .or_else(|| line.strip_prefix("content-length:"))?;
    rest.trim().parse::<usize>().ok()
}

/// Read one JSON-RPC message from a buffered reader, auto-detecting the framing.
///
/// Returns `Ok(None)` on clean EOF (client disconnected). The detected [`Framing`]
/// is returned so the caller replies in kind.
async fn read_framed_message<R: AsyncBufReadExt + Unpin>(
    reader: &mut R,
) -> std::io::Result<Option<(Vec<u8>, Framing)>> {
    // Read the first non-blank line to detect the framing.
    let first = loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            return Ok(None); // EOF
        }
        let trimmed = line.trim_end_matches('\n').trim_end_matches('\r');
        if trimmed.is_empty() {
            continue; // tolerate blank lines between messages
        }
        break line;
    };
    let trimmed = first.trim_end_matches('\n').trim_end_matches('\r');

    // Content-Length framing: the first line is a header. Consume remaining
    // headers until the blank line, then read exactly `len` body bytes.
    if let Some(len0) = parse_content_length(trimmed) {
        let mut len = len0;
        loop {
            let mut line = String::new();
            let n = reader.read_line(&mut line).await?;
            if n == 0 {
                return Ok(None);
            }
            let t = line.trim_end_matches('\n').trim_end_matches('\r');
            if t.is_empty() {
                break; // end of headers
            }
            if let Some(l) = parse_content_length(t) {
                len = l;
            }
        }
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).await?;
        return Ok(Some((buf, Framing::ContentLength)));
    }

    // Otherwise: newline-delimited JSON — the line itself is the whole message.
    Ok(Some((trimmed.as_bytes().to_vec(), Framing::Newline)))
}

/// Write one JSON-RPC message in the given framing.
async fn write_framed_message<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    value: &Value,
    framing: Framing,
) -> std::io::Result<()> {
    let body =
        serde_json::to_vec(value).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    match framing {
        Framing::ContentLength => {
            let header = format!("Content-Length: {}\r\n\r\n", body.len());
            writer.write_all(header.as_bytes()).await?;
            writer.write_all(&body).await?;
        }
        Framing::Newline => {
            writer.write_all(&body).await?;
            writer.write_all(b"\n").await?;
        }
    }
    writer.flush().await
}

/// Build a JSON-RPC 2.0 success response.
fn json_rpc_result(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    })
}

/// Build a JSON-RPC 2.0 error response.
fn json_rpc_error(id: Value, code: i32, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::ambient::TestEnvGuard;
    use clap::Parser;

    /// Minimal CLI wrapper to test arg parsing.
    #[derive(Parser, Debug)]
    struct TestCli {
        #[command(flatten)]
        args: McpArgs,
    }

    fn parse_mcp(argv: &[&str]) -> McpArgs {
        TestCli::try_parse_from(std::iter::once("mcp").chain(argv.iter().copied()))
            .unwrap()
            .args
    }

    #[test]
    fn mcp_args_parse_required_flags() {
        let args = parse_mcp(&["--as", "ada", "--project", "lens"]);
        assert_eq!(args.name, "ada");
        assert_eq!(args.project, "lens");
        assert!(args.client_key.is_none());
        assert!(args.agent.is_none());
    }

    #[test]
    fn mcp_args_parse_resumed_agent_identity() {
        let args = parse_mcp(&[
            "--as",
            "ada",
            "--project",
            "lens",
            "--client-key",
            "s_123",
            "--agent",
            "codex",
        ]);
        assert_eq!(args.client_key.as_deref(), Some("s_123"));
        assert_eq!(args.agent.as_deref(), Some("codex"));
    }

    #[test]
    fn mcp_args_missing_name_fails() {
        let result = TestCli::try_parse_from(["mcp", "--project", "lens"]);
        assert!(result.is_err(), "should fail when --as is missing");
    }

    #[test]
    fn mcp_args_missing_project_fails() {
        let result = TestCli::try_parse_from(["mcp", "--as", "ada"]);
        assert!(result.is_err(), "should fail when --project is missing");
    }

    #[test]
    fn mcp_args_reject_gateway_bearer_flags() {
        for flag in ["--authorization", "--bearer", "--token"] {
            let result = TestCli::try_parse_from([
                "mcp",
                "--as",
                "ada",
                "--project",
                "lens",
                flag,
                "Bearer gateway-token",
            ]);
            assert!(
                result.is_err(),
                "local stdio MCP must not accept gateway auth flag {flag}"
            );
        }
    }

    #[test]
    fn mcp_identity_has_correct_fields() {
        let _env = TestEnvGuard::cleared(&["NEXUS_SESSION_ID", "CLAUDE_CODE_SESSION_ID"]);
        let id = mcp_identity("ada", "lens", None, None);
        assert_eq!(id.name.as_deref(), Some("ada"));
        assert_eq!(id.project, "lens");
        assert_eq!(id.client_key, "mcp:ada");
        assert_eq!(id.harness_session_id, "mcp:ada");
        assert_eq!(id.harness.as_str(), "other");
        assert!(matches!(id.tier, Tier::Agent));
    }

    #[test]
    fn mcp_identity_can_resume_daemon_owned_codex_agent() {
        let _env = TestEnvGuard::cleared(&["NEXUS_SESSION_ID", "CLAUDE_CODE_SESSION_ID"]);
        let id = mcp_identity("ada", "lens", Some("s_agent"), Some("codex"));
        assert_eq!(id.name.as_deref(), Some("ada"));
        assert_eq!(id.project, "lens");
        assert_eq!(id.client_key, "s_agent");
        assert_eq!(id.harness_session_id, "s_agent");
        assert_eq!(id.harness.as_str(), "codex");
    }

    #[test]
    fn mcp_idempotency_key_includes_caller_scope() {
        assert_eq!(
            mcp_idempotency_key("s_faye", "run_a", "reply", &json!(7)).as_deref(),
            Some("mcp:s_faye:run_a:reply:7")
        );
        assert_eq!(
            mcp_idempotency_key("s_pedro", "run_a", "reply", &json!(7)).as_deref(),
            Some("mcp:s_pedro:run_a:reply:7")
        );
    }

    #[test]
    fn mcp_idempotency_key_separates_mcp_server_runs() {
        let first = mcp_idempotency_key("s_morgan", "run_a", "post", &json!(8));
        let second = mcp_idempotency_key("s_morgan", "run_b", "post", &json!(8));

        assert_ne!(first, second);
    }

    #[test]
    fn mcp_idempotency_key_is_stable_within_one_mcp_server_run() {
        let first = mcp_idempotency_key("s_morgan", "run_a", "post", &json!(8));
        let second = mcp_idempotency_key("s_morgan", "run_a", "post", &json!(8));

        assert_eq!(first, second);
    }

    #[test]
    fn standalone_mcp_identity_does_not_borrow_ambient_session_id() {
        let _env = TestEnvGuard::new(&[
            ("NEXUS_SESSION_ID", Some("stable-mcp-session")),
            ("CLAUDE_CODE_SESSION_ID", Some("claude-session-ignored")),
        ]);

        let id = mcp_identity("ada", "lens", Some("s_agent"), Some("codex"));
        assert_eq!(id.client_key, "s_agent");
        assert_eq!(id.harness_session_id, "s_agent");
    }

    #[test]
    fn tools_list_response_has_all_tool_names() {
        let id = json!(1);
        let resp = handle_tools_list(&id);
        let tools = resp["result"]["tools"]
            .as_array()
            .expect("tools should be an array");
        let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
        for expected in &[
            "dm", "post", "reply", "publish", "members", "rename", "threads", "read", "history",
            "search", "inbox",
        ] {
            assert!(
                names.contains(expected),
                "missing tool '{expected}' in tools/list response"
            );
        }
    }

    #[test]
    fn tools_list_each_entry_has_input_schema() {
        let id = json!(1);
        let resp = handle_tools_list(&id);
        let tools = resp["result"]["tools"].as_array().unwrap();
        for tool in tools {
            let name = tool["name"].as_str().unwrap_or("?");
            let schema = &tool["inputSchema"];
            assert!(
                schema.is_object(),
                "tool '{name}' inputSchema should be an object"
            );
            assert_eq!(
                schema["type"].as_str(),
                Some("object"),
                "tool '{name}' inputSchema.type should be 'object'"
            );
        }
    }

    #[tokio::test]
    async fn tools_call_unknown_tool_returns_is_error_true() {
        let id = json!(42);
        let params = json!({ "name": "no_such_tool", "arguments": {} });
        let resp = handle_tools_call(None, None, "test-run", &id, &params).await;
        // Should be a result (not a JSON-RPC error) with isError=true.
        assert_eq!(
            resp["result"]["isError"],
            json!(true),
            "unknown tool should produce isError:true content, got: {resp}"
        );
    }

    #[test]
    fn read_framed_message_parses_content_length() {
        // Synchronous test of the framing logic using a byte buffer.
        let body = b"{\"jsonrpc\":\"2.0\"}";
        let frame = format!("Content-Length: {}\r\n\r\n", body.len());
        let mut input = Vec::new();
        input.extend_from_slice(frame.as_bytes());
        input.extend_from_slice(body);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(async {
            let mut reader = BufReader::new(input.as_slice());
            read_framed_message(&mut reader).await
        });

        let (bytes, framing) = result.unwrap().expect("should read a message");
        assert_eq!(bytes, body);
        assert_eq!(framing, Framing::ContentLength);
    }

    /// Regression guard: the MCP spec's stdio transport is newline-delimited JSON
    /// (what codex and the standard SDKs send). The server MUST read it — before this
    /// fix it only understood Content-Length and hung 30 s waiting for a header that
    /// never came, so codex reported "MCP client for `nexus-bus` timed out".
    #[test]
    fn read_framed_message_parses_newline_delimited_json() {
        let line = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}\n";
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(async {
            let mut reader = BufReader::new(&line[..]);
            read_framed_message(&mut reader).await
        });
        let (bytes, framing) = result
            .unwrap()
            .expect("should read a newline-delimited message");
        assert_eq!(
            bytes,
            b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}"
        );
        assert_eq!(framing, Framing::Newline);
    }

    #[test]
    fn write_framed_message_content_length_emits_header() {
        let value = json!({"jsonrpc": "2.0", "id": 1, "result": {}});
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let output = runtime.block_on(async {
            let mut buf = Vec::new();
            write_framed_message(&mut buf, &value, Framing::ContentLength)
                .await
                .unwrap();
            buf
        });

        let raw = String::from_utf8(output.clone()).unwrap();
        assert!(
            raw.starts_with("Content-Length: "),
            "should start with Content-Length header"
        );
        assert!(raw.contains("\r\n\r\n"), "should have CRLF CRLF separator");
    }

    /// Newline framing: the response is the JSON object followed by a single `\n`,
    /// with no header — what an MCP stdio client expects.
    #[test]
    fn write_framed_message_newline_emits_json_line() {
        let value = json!({"jsonrpc": "2.0", "id": 1, "result": {}});
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let output = runtime.block_on(async {
            let mut buf = Vec::new();
            write_framed_message(&mut buf, &value, Framing::Newline)
                .await
                .unwrap();
            buf
        });
        let raw = String::from_utf8(output).unwrap();
        assert!(
            !raw.contains("Content-Length"),
            "newline framing must not emit a header"
        );
        assert!(
            raw.ends_with('\n'),
            "newline framing must end with a newline"
        );
        assert!(
            raw.trim_end().ends_with('}'),
            "body should be the JSON object"
        );
    }
}
