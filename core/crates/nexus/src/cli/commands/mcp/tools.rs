//! Shared MCP tool surface for the Nexus bus.
//!
//! This module owns:
//! - The static tool registry: each tool's name, description, and JSON Schema for its arguments.
//! - The dispatch function: given optional store read/command clients, a tool name, and parsed
//!   arguments, it builds the matching `nexus_contracts` request and uses the store-backed path for
//!   that tool.
//!
//! Both the stdio MCP server (`super`) and a future gateway auth-proxy import this surface —
//! the tool definitions and dispatch logic are NOT buried in the stdio loop.
//!
//! ## Tool → daemon method map
//!
//! | MCP tool       | execution path                 | request type           |
//! |----------------|---------------|------------------------|
//! | `dm`           | `message.post.send` intent     | `SendRequest` (Dm)     |
//! | `post`         | `message.post.send` intent     | `SendRequest` (Post)   |
//! | `reply`        | `message.post.send` intent     | `SendRequest` (Reply)  |
//! | `publish`      | `message.post.send` intent     | `SendRequest` (Publish)|
//! | `members`      | store read view                | `MemberListRequest`    |
//! | `threads`      | store read view                | `()`                   |
//! | `read`         | store read view                | `MessageId`            |
//! | `history`      | store read view                | `HistoryRequest`       |
//! | `search`       | store read view                | `SearchRequest`        |
//! | `inbox`        | `inbox.consume` intent         | `ConsumeRequest`       |

use serde_json::{json, Value};

use nexus_contracts::{
    Ack, ConsumeRequest, ContractError, HistoryRequest, HistoryResponse, MemberListRequest,
    MemberListResponse, Message, NexusBatch, RenameRequest, RenameResponse, SearchMode,
    SearchRequest, SearchResponse, SendRequest, SendTarget, ThreadListResponse,
};

use crate::cli::read_client::ReadClient;
use crate::cli::store_client::StoreClient;

/// One entry in the MCP tool registry.
#[derive(Debug, Clone)]
pub struct ToolDef {
    pub name: &'static str,
    pub description: &'static str,
    /// JSON Schema object for the tool's `arguments`.
    pub input_schema: Value,
}

/// The full static tool registry. Returned by `tools/list`.
pub fn registry() -> Vec<ToolDef> {
    vec![
        ToolDef {
            name: "dm",
            description: "Send a private direct message to another agent or human by name.",
            input_schema: json!({
                "type": "object",
                "required": ["to", "message"],
                "properties": {
                    "to": {
                        "type": "string",
                        "description": "The name of the recipient."
                    },
                    "message": {
                        "type": "string",
                        "description": "The message body to send."
                    },
                    "summary": {
                        "type": "string",
                        "description": "Optional short summary line for indexing."
                    }
                }
            }),
        },
        ToolDef {
            name: "post",
            description: "Post a message to a named thread, fanning out to all thread members.",
            input_schema: json!({
                "type": "object",
                "required": ["thread", "message"],
                "properties": {
                    "thread": {
                        "type": "string",
                        "description": "The thread name to post to."
                    },
                    "message": {
                        "type": "string",
                        "description": "The message body."
                    },
                    "summary": {
                        "type": "string",
                        "description": "Optional short summary line for indexing."
                    },
                    "mention": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Names to soft-highlight in the thread."
                    }
                }
            }),
        },
        ToolDef {
            name: "reply",
            description: "Reply into the current conversation context (context-aware; no target required).",
            input_schema: json!({
                "type": "object",
                "required": ["message"],
                "properties": {
                    "message": {
                        "type": "string",
                        "description": "The reply body."
                    },
                    "summary": {
                        "type": "string",
                        "description": "Optional short summary line for indexing."
                    },
                    "mention": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Names to soft-highlight."
                    }
                }
            }),
        },
        ToolDef {
            name: "publish",
            description: "Publish a message to a topic (pub/sub fan-out to all subscribers).",
            input_schema: json!({
                "type": "object",
                "required": ["topic", "message"],
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": "The topic name to publish to."
                    },
                    "message": {
                        "type": "string",
                        "description": "The message body."
                    },
                    "summary": {
                        "type": "string",
                        "description": "Optional short summary line."
                    }
                }
            }),
        },
        ToolDef {
            name: "members",
            description: "List the current members (agents and humans) on the bus.",
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolDef {
            name: "rename",
            description: "Change your OWN name on the bus (rename yourself). Others address you by the new name afterward. The new name must be free in your project.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "Your new name" }
                },
                "required": ["name"]
            }),
        },
        ToolDef {
            name: "threads",
            description: "List all threads visible to the caller.",
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolDef {
            name: "history",
            description: "Fetch chronological message history. Scope to a thread, DM partner, or topic.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "thread": {
                        "type": "string",
                        "description": "Restrict to a named thread."
                    },
                    "with": {
                        "type": "string",
                        "description": "Restrict to a DM conversation with this name."
                    },
                    "topic": {
                        "type": "string",
                        "description": "Restrict to a topic feed."
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of entries to return."
                    }
                }
            }),
        },
        ToolDef {
            name: "read",
            description: "Fetch one full committed message by id, including bodies truncated by the drain view.",
            input_schema: json!({
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "The Nexus message id to read."
                    }
                }
            }),
        },
        ToolDef {
            name: "search",
            description: "Search messages in the caller's scope (DMs, threads, subscriptions).",
            input_schema: json!({
                "type": "object",
                "required": ["query"],
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query string."
                    },
                    "mode": {
                        "type": "string",
                        "enum": ["fts", "semantic", "hybrid"],
                        "description": "Search engine: fts, semantic, or hybrid (default: hybrid)."
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of hits to return."
                    },
                    "thread": {
                        "type": "string",
                        "description": "Restrict to a named thread."
                    },
                    "with": {
                        "type": "string",
                        "description": "Restrict to a DM conversation with this name."
                    }
                }
            }),
        },
        ToolDef {
            name: "inbox",
            description: "Drain the caller's inbox (held-receive window). Returns queued messages.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "timeout_ms": {
                        "type": "integer",
                        "description": "Held-receive window in milliseconds (default: daemon default)."
                    },
                    "max": {
                        "type": "integer",
                        "description": "Maximum number of messages to drain in one call."
                    }
                }
            }),
        },
    ]
}

/// Dispatch a `tools/call` to the daemon.
///
/// `name` is the MCP tool name; `args` is the `arguments` object from the MCP request (may be
/// `Value::Null` or an empty object for tools with no required args).
///
/// Returns the daemon's response serialized as JSON, or a `ContractError` on failure.
pub async fn dispatch(
    read_client: Option<&ReadClient>,
    store_client: Option<&StoreClient>,
    name: &str,
    args: &Value,
) -> Result<Value, ContractError> {
    dispatch_with_idempotency(read_client, store_client, name, args, None).await
}

/// Dispatch a `tools/call` with a stable idempotency key for retry-prone write tools.
pub async fn dispatch_with_idempotency(
    read_client: Option<&ReadClient>,
    store_client: Option<&StoreClient>,
    name: &str,
    args: &Value,
    idempotency_key: Option<&str>,
) -> Result<Value, ContractError> {
    let obj = args.as_object();
    let get_str = |key: &str| -> Option<String> {
        obj.and_then(|o| o.get(key))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    };
    let get_u32 = |key: &str| -> Option<u32> {
        obj.and_then(|o| o.get(key))
            .and_then(|v| v.as_u64())
            .map(|n| n as u32)
    };
    let get_strvec = |key: &str| -> Vec<String> {
        obj.and_then(|o| o.get(key))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|e| e.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default()
    };

    match name {
        "dm" => {
            let req = SendRequest {
                to: SendTarget::dm_name(get_str("to").ok_or_else(|| missing("to"))?),
                body: get_str("message").ok_or_else(|| missing("message"))?,
                summary: get_str("summary"),
                mention: vec![],
                idempotency_key: idempotency_key.map(str::to_string),
            };
            let ack: Ack = send_message_post(store_client, &req, idempotency_key).await?;
            Ok(serde_json::to_value(ack).map_err(serde_err)?)
        }
        "post" => {
            let req = SendRequest {
                to: SendTarget::Post {
                    thread: get_str("thread").ok_or_else(|| missing("thread"))?,
                },
                body: get_str("message").ok_or_else(|| missing("message"))?,
                summary: get_str("summary"),
                mention: get_strvec("mention"),
                idempotency_key: idempotency_key.map(str::to_string),
            };
            let ack: Ack = send_message_post(store_client, &req, idempotency_key).await?;
            Ok(serde_json::to_value(ack).map_err(serde_err)?)
        }
        "reply" => {
            let req = SendRequest {
                to: SendTarget::Reply,
                body: get_str("message").ok_or_else(|| missing("message"))?,
                summary: get_str("summary"),
                mention: get_strvec("mention"),
                idempotency_key: idempotency_key.map(str::to_string),
            };
            let ack: Ack = send_message_post(store_client, &req, idempotency_key).await?;
            Ok(serde_json::to_value(ack).map_err(serde_err)?)
        }
        "publish" => {
            let req = SendRequest {
                to: SendTarget::Publish {
                    topic: get_str("topic").ok_or_else(|| missing("topic"))?,
                },
                body: get_str("message").ok_or_else(|| missing("message"))?,
                summary: get_str("summary"),
                mention: vec![],
                idempotency_key: idempotency_key.map(str::to_string),
            };
            let ack: Ack = send_message_post(store_client, &req, idempotency_key).await?;
            Ok(serde_json::to_value(ack).map_err(serde_err)?)
        }
        "members" => {
            let req = MemberListRequest {
                include_offline: Some(true),
                include_dead: None,
            };
            let res: MemberListResponse = read_client_for(read_client)?.members(req).await?;
            Ok(serde_json::to_value(res).map_err(serde_err)?)
        }
        "rename" => {
            let req = RenameRequest {
                name: get_str("name").ok_or_else(|| missing("name"))?,
            };
            let res: RenameResponse = message_post_client(store_client)?
                .command(nexus_store::command_kinds::identity::RENAME, &req)
                .await?;
            Ok(serde_json::to_value(res).map_err(serde_err)?)
        }
        "threads" => {
            let res: ThreadListResponse = read_client_for(read_client)?.threads().await?;
            Ok(serde_json::to_value(res).map_err(serde_err)?)
        }
        "history" => {
            let req = HistoryRequest {
                thread: get_str("thread"),
                with: get_str("with"),
                topic: get_str("topic"),
                limit: get_u32("limit"),
                before: None,
            };
            let res: HistoryResponse = read_client_for(read_client)?.history(req).await?;
            Ok(serde_json::to_value(res).map_err(serde_err)?)
        }
        "read" => {
            let id = get_str("id").ok_or_else(|| missing("id"))?;
            let res: Message = read_client_for(read_client)?.message(&id).await?;
            Ok(serde_json::to_value(res).map_err(serde_err)?)
        }
        "search" => {
            let mode = match get_str("mode").as_deref() {
                Some("fts") => SearchMode::Fts,
                Some("semantic") => SearchMode::Semantic,
                _ => SearchMode::Hybrid,
            };
            let req = SearchRequest {
                query: get_str("query").ok_or_else(|| missing("query"))?,
                mode,
                limit: get_u32("limit"),
                thread: get_str("thread"),
                with: get_str("with"),
                since: None,
            };
            let res: SearchResponse = read_client_for(read_client)?.search(req).await?;
            Ok(serde_json::to_value(res).map_err(serde_err)?)
        }
        "inbox" => {
            let req = ConsumeRequest {
                timeout_ms: get_u32("timeout_ms"),
                max: get_u32("max"),
            };
            let res: NexusBatch = message_post_client(store_client)?
                .command(nexus_store::command_kinds::inbox::CONSUME, &req)
                .await?;
            Ok(serde_json::to_value(res).map_err(serde_err)?)
        }
        other => Err(ContractError {
            code: nexus_contracts::codes::NOT_FOUND,
            message: format!("unknown tool '{other}'"),
        }),
    }
}

async fn send_message_post(
    client: Option<&StoreClient>,
    req: &SendRequest,
    idempotency_key: Option<&str>,
) -> Result<Ack, ContractError> {
    match idempotency_key {
        Some(key) => {
            message_post_client(client)?
                .message_post_send_idempotent(req, Some(key.to_string()))
                .await
        }
        None => message_post_client(client)?.message_post_send(req).await,
    }
}

fn missing(field: &str) -> ContractError {
    ContractError {
        code: nexus_contracts::codes::INVALID_PARAMS,
        message: format!("missing required argument '{field}'"),
    }
}

fn serde_err(e: serde_json::Error) -> ContractError {
    ContractError {
        code: nexus_contracts::codes::INVALID_PARAMS,
        message: format!("json serialization error: {e}"),
    }
}

fn message_post_client(client: Option<&StoreClient>) -> Result<&StoreClient, ContractError> {
    client.ok_or_else(|| ContractError {
        code: nexus_contracts::codes::INTERNAL_ERROR,
        message: "store-backed message post client is unavailable".into(),
    })
}

fn read_client_for(client: Option<&ReadClient>) -> Result<&ReadClient, ContractError> {
    client.ok_or_else(|| ContractError {
        code: nexus_contracts::codes::INTERNAL_ERROR,
        message: "store-backed read client is unavailable".into(),
    })
}
