use std::sync::Arc;
use std::time::Duration;

use nexus::cli::ambient::with_test_env_vars;
use nexus::cli::read_client::ReadClient;
use nexus::cli::store_client::StoreClient;
use nexus::daemon::gateway_stream_socket::GatewayStreamPublisher;
use nexus::daemon::{command_worker, daemon_ipc, AppState};
use nexus_common::config::GatewayProjectionDeliveryMode;
use nexus_common::Config;
use nexus_contracts::{
    DaemonIpcCall, DaemonIpcCaller, DaemonIpcRequest, HistoryRequest, Kind, MemberListRequest,
    RegisterRequest, RegisterResponse, SearchMode, SearchRequest, SessionId, ThreadId, Tier,
    Whoami, DAEMON_IPC_PROTOCOL_VERSION,
};
use nexus_store::repos::{
    Agents, CommandIntents, NewAgent, NewSession, Sessions, Sources, Threads,
};
use nexus_store::Store;

async fn state() -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    AppState::wire(store, &Config::default())
}

fn register_request() -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some("ipc-agent".into()),
        harness: hid("other"),
        harness_session_id: "harness-ipc-agent".into(),
        project: "metadata-only".into(),
        client_key: "client-ipc-agent".into(),
        runtime_credential: None,
        tier: Tier::Agent,
        kind: Some(Kind::Agent),
        role: None,
        cwd: None,
    }
}

#[tokio::test]
async fn command_call_is_durable_and_held_until_worker_completion_without_polling() {
    let state = state().await;
    let worker = command_worker::spawn(state.clone());
    let request = DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: "rpc-register".into(),
        caller: None,
        call: DaemonIpcCall::Command {
            command_id: "cmd-register".into(),
            kind: nexus_store::command_kinds::identity::REGISTER.into(),
            params: serde_json::to_value(register_request()).unwrap(),
            idempotency_key: None,
        },
    };

    let response = tokio::time::timeout(
        Duration::from_secs(2),
        daemon_ipc::handle_request(&state, "boot-token", request),
    )
    .await
    .expect("held daemon IPC response timed out");

    assert!(response.error.is_none(), "{:?}", response.error);
    let registered: RegisterResponse =
        serde_json::from_value(response.result.expect("register result")).unwrap();
    assert!(!registered.session_id.0.is_empty());
    let row = CommandIntents::new(&state.store)
        .get("cmd-register")
        .await
        .unwrap()
        .expect("durable command row");
    assert_eq!(row.status, "done");
    worker.abort();
}

#[tokio::test]
async fn reconnecting_command_with_the_same_id_resumes_the_durable_result() {
    let state = state().await;
    let worker = command_worker::spawn(state.clone());
    let request = DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: "rpc-register-first".into(),
        caller: None,
        call: DaemonIpcCall::Command {
            command_id: "cmd-register-reconnect".into(),
            kind: nexus_store::command_kinds::identity::REGISTER.into(),
            params: serde_json::to_value(register_request()).unwrap(),
            idempotency_key: None,
        },
    };

    let first = tokio::time::timeout(
        Duration::from_secs(2),
        daemon_ipc::handle_request(&state, "boot-token", request.clone()),
    )
    .await
    .expect("first daemon IPC command timed out");
    assert!(first.error.is_none(), "{:?}", first.error);

    let mut retried = request;
    retried.request_id = "rpc-register-reconnected".into();
    let second = tokio::time::timeout(
        Duration::from_secs(2),
        daemon_ipc::handle_request(&state, "boot-token", retried),
    )
    .await
    .expect("reconnected daemon IPC command timed out");
    assert!(second.error.is_none(), "{:?}", second.error);
    assert_eq!(first.result, second.result);

    let mut rows = state
        .store
        .conn
        .query(
            "SELECT COUNT(*) FROM command_intents WHERE command_id = ?1",
            libsql::params!["cmd-register-reconnect"],
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        1
    );
    worker.abort();
}

#[tokio::test]
async fn reconnecting_command_cannot_reuse_an_id_for_a_different_payload() {
    let state = state().await;
    let worker = command_worker::spawn(state.clone());
    let request = DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: "rpc-register-original".into(),
        caller: None,
        call: DaemonIpcCall::Command {
            command_id: "cmd-register-conflict".into(),
            kind: nexus_store::command_kinds::identity::REGISTER.into(),
            params: serde_json::to_value(register_request()).unwrap(),
            idempotency_key: None,
        },
    };
    let first = tokio::time::timeout(
        Duration::from_secs(2),
        daemon_ipc::handle_request(&state, "boot-token", request.clone()),
    )
    .await
    .expect("first daemon IPC command timed out");
    assert!(first.error.is_none(), "{:?}", first.error);

    let mut changed = request;
    changed.request_id = "rpc-register-conflict".into();
    if let DaemonIpcCall::Command { params, .. } = &mut changed.call {
        params["name"] = serde_json::json!("different-agent");
    }
    let response = daemon_ipc::handle_request(&state, "boot-token", changed).await;
    assert_eq!(
        response
            .error
            .expect("conflicting command id must fail")
            .code,
        nexus_contracts::codes::INVALID_PARAMS
    );
    worker.abort();
}

#[tokio::test]
async fn enqueue_call_returns_a_durable_receipt_without_waiting_for_execution() {
    let state = state().await;
    let request = DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: "rpc-prompt-enqueue".into(),
        caller: None,
        call: DaemonIpcCall::Enqueue {
            command_id: "cmd-prompt-enqueue".into(),
            kind: nexus_store::command_kinds::harness::PROMPT.into(),
            params: serde_json::json!({
                "name": "ipc-agent",
                "text": "queued while active",
                "clientMessageId": "client-prompt-1"
            }),
            idempotency_key: Some("client-prompt-1".into()),
        },
    };

    let response = tokio::time::timeout(
        Duration::from_millis(100),
        daemon_ipc::handle_request(&state, "boot-token", request),
    )
    .await
    .expect("durable enqueue receipt must not wait for command execution");
    assert!(response.error.is_none(), "{:?}", response.error);
    assert_eq!(
        response.result.as_ref().unwrap()["commandId"],
        "cmd-prompt-enqueue"
    );
    assert_eq!(response.result.as_ref().unwrap()["status"], "pending");
    assert_eq!(response.result.as_ref().unwrap()["revision"], 1);
    assert_eq!(response.result.as_ref().unwrap()["seq"], 1);
    assert_eq!(
        response.result.as_ref().unwrap()["sessionId"],
        serde_json::Value::Null
    );
    let row = CommandIntents::new(&state.store)
        .get("cmd-prompt-enqueue")
        .await
        .unwrap()
        .expect("durable queued row");
    assert_eq!(row.status, "pending");
}

#[tokio::test]
async fn wrong_boot_token_is_rejected_before_command_insert() {
    let state = state().await;
    let request = DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "stale-token".into(),
        request_id: "rpc-stale".into(),
        caller: None,
        call: DaemonIpcCall::Command {
            command_id: "cmd-must-not-exist".into(),
            kind: nexus_store::command_kinds::identity::REGISTER.into(),
            params: serde_json::to_value(register_request()).unwrap(),
            idempotency_key: None,
        },
    };

    let response = daemon_ipc::handle_request(&state, "current-token", request).await;
    assert_eq!(
        response.error.unwrap().code,
        nexus_contracts::codes::UNAUTHORIZED
    );
    assert!(CommandIntents::new(&state.store)
        .get("cmd-must-not-exist")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn query_call_routes_inside_daemon_without_creating_command_row() {
    let state = state().await;
    let request = DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: "rpc-whoami".into(),
        caller: Some(DaemonIpcCaller {
            name: Some("Local Operator".into()),
            project: "metadata-only".into(),
            session_id: Some("local-operator".into()),
            agent_id: None,
            runtime_id: Some("local-operator".into()),
            client_key: None,
            kind: Kind::Human,
            tier: Tier::Admin,
        }),
        call: DaemonIpcCall::Query {
            method: "whoami".into(),
            params: serde_json::Value::Null,
        },
    };

    let response = daemon_ipc::handle_request(&state, "boot-token", request).await;
    assert!(response.error.is_none(), "{:?}", response.error);
    let whoami: Whoami = serde_json::from_value(response.result.unwrap()).unwrap();
    assert_eq!(whoami.session_id.0, "local-operator");
    assert_eq!(whoami.tier, Tier::Admin);

    let mut rows = state
        .store
        .conn
        .query("SELECT COUNT(*) FROM command_intents", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        0
    );
}

#[tokio::test]
async fn local_daemon_status_query_reads_store_inside_owner_process() {
    let state = state().await;
    let request = DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: "rpc-daemon-status".into(),
        caller: Some(DaemonIpcCaller {
            name: Some("Local Operator".into()),
            project: "default".into(),
            session_id: Some("local-operator".into()),
            agent_id: None,
            runtime_id: Some("local-operator".into()),
            client_key: None,
            kind: Kind::Human,
            tier: Tier::Admin,
        }),
        call: DaemonIpcCall::Query {
            method: "local.daemon.storeStatus".into(),
            params: serde_json::Value::Null,
        },
    };

    let response = daemon_ipc::handle_request(&state, "boot-token", request).await;
    assert!(response.error.is_none(), "{:?}", response.error);
    let result = response.result.unwrap();
    assert_eq!(result["wedgedIntents"], 0);
    assert_eq!(result["deadLetterCount"], 0);
    assert_eq!(result["laneDepths"], serde_json::json!([]));
    assert_eq!(result["transportPairs"], serde_json::json!([]));
}

#[tokio::test]
async fn local_operator_can_show_and_hot_apply_gateway_delivery_mode() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let mut config = Config::default();
    config.gateway_projection.delivery_mode = GatewayProjectionDeliveryMode::BestEffort;
    let publisher = GatewayStreamPublisher::new_with_projection_config(
        16,
        config.gateway_projection,
        "epoch-before",
    );
    let state = AppState::wire_pty_with_gateway_stream(store, &config, Some(publisher));
    let caller = DaemonIpcCaller {
        name: Some("Local Operator".into()),
        project: "default".into(),
        session_id: Some("local-operator".into()),
        agent_id: None,
        runtime_id: Some("local-operator".into()),
        client_key: None,
        kind: Kind::Human,
        tier: Tier::Admin,
    };
    let query = |request_id: &str, method: &str, params: serde_json::Value| DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: request_id.into(),
        caller: Some(caller.clone()),
        call: DaemonIpcCall::Query {
            method: method.into(),
            params,
        },
    };

    let show = daemon_ipc::handle_request(
        &state,
        "boot-token",
        query(
            "gateway-mode-show",
            "local.gateway.deliveryMode.show",
            serde_json::Value::Null,
        ),
    )
    .await;
    assert!(show.error.is_none(), "{:?}", show.error);
    let show = show.result.unwrap();
    assert_eq!(show["effective"], "best_effort");
    assert_eq!(show["daemonEpoch"], "epoch-before");

    let set = daemon_ipc::handle_request(
        &state,
        "boot-token",
        query(
            "gateway-mode-set",
            "local.gateway.deliveryMode.set",
            serde_json::json!({ "deliveryMode": "buffered" }),
        ),
    )
    .await;
    assert!(set.error.is_none(), "{:?}", set.error);
    let set = set.result.unwrap();
    assert_eq!(set["effective"], "buffered");
    assert_ne!(set["daemonEpoch"], "epoch-before");
    assert_eq!(set["resyncRequired"], true);

    let invalid = daemon_ipc::handle_request(
        &state,
        "boot-token",
        query(
            "gateway-mode-invalid",
            "local.gateway.deliveryMode.set",
            serde_json::json!({ "deliveryMode": "lossy-maybe" }),
        ),
    )
    .await;
    assert_eq!(
        invalid.error.expect("unsupported mode must fail").code,
        nexus_contracts::codes::INVALID_PARAMS
    );
}

#[tokio::test]
async fn local_gateway_read_executes_select_inside_the_daemon_and_rejects_writes() {
    let state = state().await;
    let caller = DaemonIpcCaller {
        name: Some("Local Operator".into()),
        project: "default".into(),
        session_id: Some("local-operator".into()),
        agent_id: None,
        runtime_id: Some("local-operator".into()),
        client_key: None,
        kind: Kind::Human,
        tier: Tier::Admin,
    };
    let query = DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: "rpc-local-read".into(),
        caller: Some(caller.clone()),
        call: DaemonIpcCall::Query {
            method: "local.store.read".into(),
            params: serde_json::json!({
                "sql": "SELECT 41 + ?1 AS answer, ?2 AS label",
                "args": [1, "daemon-owned"]
            }),
        },
    };
    let response = daemon_ipc::handle_request(&state, "boot-token", query).await;
    assert!(response.error.is_none(), "{:?}", response.error);
    assert_eq!(
        response.result.unwrap(),
        serde_json::json!({
            "columns": ["answer", "label"],
            "rows": [[42, "daemon-owned"]],
            "rowsAffected": 0
        })
    );

    let write = DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: "rpc-local-write-rejected".into(),
        caller: Some(caller),
        call: DaemonIpcCall::Query {
            method: "local.store.read".into(),
            params: serde_json::json!({
                "sql": "UPDATE sessions SET presence = 'offline'",
                "args": []
            }),
        },
    };
    let response = daemon_ipc::handle_request(&state, "boot-token", write).await;
    assert_eq!(
        response.error.expect("write-shaped read must fail").code,
        nexus_contracts::codes::INVALID_PARAMS
    );
}

#[tokio::test]
async fn legacy_gateway_export_is_bounded_typed_and_cursor_paginated() {
    let state = state().await;
    state
        .store
        .conn
        .execute(
            "INSERT INTO messages (message_id, from_name, kind, body, provenance, project, created_at) \
             VALUES ('m-export-1', 'ada', 'dm', 'one', '{}', 'metadata-only', 1), \
                    ('m-export-2', 'ada', 'dm', 'two', '{}', 'metadata-only', 2)",
            (),
        )
        .await
        .unwrap();
    let request = |request_id: &str, cursor: Option<&str>| DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: request_id.into(),
        caller: Some(DaemonIpcCaller {
            name: Some("Local Operator".into()),
            project: "metadata-only".into(),
            session_id: Some("local-operator".into()),
            agent_id: None,
            runtime_id: Some("local-operator".into()),
            client_key: None,
            kind: Kind::Human,
            tier: Tier::Admin,
        }),
        call: DaemonIpcCall::Query {
            method: "local.store.export".into(),
            params: serde_json::json!({
                "kind": "messages",
                "cursor": cursor,
                "limit": 1,
            }),
        },
    };

    let first = daemon_ipc::handle_request(&state, "boot-token", request("export-1", None)).await;
    assert!(first.error.is_none(), "{:?}", first.error);
    let first = first.result.unwrap();
    assert_eq!(first["kind"], "messages");
    assert_eq!(first["rows"].as_array().unwrap().len(), 1);
    assert_eq!(first["rows"][0]["message_id"], "m-export-1");
    assert!(first["rows"][0].get("_cursor").is_none());
    let cursor = first["nextCursor"].as_str().expect("next cursor");

    let second =
        daemon_ipc::handle_request(&state, "boot-token", request("export-2", Some(cursor))).await;
    assert!(second.error.is_none(), "{:?}", second.error);
    assert_eq!(
        second.result.unwrap()["rows"][0]["message_id"],
        "m-export-2"
    );

    let invalid = DaemonIpcRequest {
        call: DaemonIpcCall::Query {
            method: "local.store.export".into(),
            params: serde_json::json!({ "kind": "command_intents", "limit": 10 }),
        },
        ..request("export-invalid", None)
    };
    let invalid = daemon_ipc::handle_request(&state, "boot-token", invalid).await;
    assert_eq!(
        invalid.error.unwrap().code,
        nexus_contracts::codes::INVALID_PARAMS
    );
}

#[tokio::test]
async fn local_queue_mutation_executes_atomically_inside_the_daemon_owner() {
    let state = state().await;
    state
        .store
        .conn
        .execute(
            "INSERT INTO sessions (session_id, agent_id, name, agent, transport, project, created_at) \
             VALUES ('s_queue', 'a_queue', 'queue-agent', 'codex', 'codex-appserver', 'default', 1)",
            (),
        )
        .await
        .unwrap();
    state
        .store
        .conn
        .execute(
            "INSERT INTO agent_session_turns VALUES \
             ('turn_queue', 's_queue', 'streaming', 1, 1, 1, 1, NULL)",
            (),
        )
        .await
        .unwrap();
    CommandIntents::new(&state.store)
        .insert_pending(nexus_store::repos::NewCommandIntent {
            command_id: "cmd_queue_redirect".into(),
            kind: nexus_store::command_kinds::harness::PROMPT.into(),
            project: "default".into(),
            caller_name: "Alex".into(),
            caller_session_id: Some("local-operator".into()),
            caller_agent_id: None,
            caller_runtime_id: Some("local-operator".into()),
            caller_client_key: None,
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: Some("cm_queue_redirect".into()),
            request_json: serde_json::json!({
                "name": "queue-agent",
                "agentId": "a_queue",
                "text": "redirect me",
                "clientMessageId": "cm_queue_redirect"
            })
            .to_string(),
            created_at: 1,
        })
        .await
        .unwrap();
    let request = DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: "rpc-queue-mutate".into(),
        caller: Some(DaemonIpcCaller {
            name: Some("Nexus Gateway".into()),
            project: "default".into(),
            session_id: Some("local-operator".into()),
            agent_id: None,
            runtime_id: Some("local-operator".into()),
            client_key: None,
            kind: Kind::Human,
            tier: Tier::Admin,
        }),
        call: DaemonIpcCall::Query {
            method: "local.sessionQueue.mutate".into(),
            params: serde_json::json!({
                "project": "default",
                "now": 9_000,
                "request": {
                    "name": "queue-agent",
                    "agentId": "a_queue",
                    "action": "redirect_now",
                    "clientMutationId": "mut_queue_redirect",
                    "commandId": "cmd_queue_redirect",
                    "expectedRevision": 1,
                    "commandIds": [],
                    "expectedRevisions": []
                }
            }),
        },
    };

    let response = daemon_ipc::handle_request(&state, "boot-token", request).await;
    assert!(response.error.is_none(), "{:?}", response.error);
    assert_eq!(response.result.as_ref().unwrap()["status"], 200);
    assert_eq!(response.result.as_ref().unwrap()["body"]["state"], "queued");
    assert_eq!(
        response.result.as_ref().unwrap()["body"]["steerCapability"],
        "native_steer"
    );
    let row = CommandIntents::new(&state.store)
        .get("cmd_queue_redirect")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.kind, nexus_store::command_kinds::harness::STEER);
    assert_eq!(row.revision, 2);
}

#[tokio::test]
async fn local_human_read_settlement_updates_only_the_authenticated_session_rows() {
    let state = state().await;
    state
        .store
        .conn
        .execute_batch(
            "INSERT INTO in_flight (in_flight_id, message_id, recipient_session, state) VALUES
             ('f_human_1', 'm_human_1', 'human-session', 'pending'),
             ('f_human_2', 'm_human_2', 'human-session', 'notified'),
             ('f_other', 'm_human_1', 'other-session', 'pending');",
        )
        .await
        .unwrap();
    let request = DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: "rpc-human-read".into(),
        caller: Some(DaemonIpcCaller {
            name: Some("Nexus Gateway".into()),
            project: "default".into(),
            session_id: Some("local-operator".into()),
            agent_id: None,
            runtime_id: Some("local-operator".into()),
            client_key: None,
            kind: Kind::Human,
            tier: Tier::Admin,
        }),
        call: DaemonIpcCall::Query {
            method: "local.humanRead.markDelivered".into(),
            params: serde_json::json!({
                "sessionId": "human-session",
                "messageIds": ["m_human_1", "m_human_2"],
                "now": 9_000
            }),
        },
    };

    let response = daemon_ipc::handle_request(&state, "boot-token", request).await;
    assert!(response.error.is_none(), "{:?}", response.error);
    assert_eq!(response.result.unwrap()["changed"], 2);
    let mut rows = state
        .store
        .conn
        .query(
            "SELECT state FROM in_flight WHERE in_flight_id = 'f_other'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next()
            .await
            .unwrap()
            .unwrap()
            .get::<String>(0)
            .unwrap(),
        "pending"
    );
}

#[tokio::test]
async fn legacy_mcp_identity_resolution_runs_inside_daemon_store_owner() {
    let state = state().await;
    let request = DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: "rpc-mcp-identity".into(),
        caller: Some(DaemonIpcCaller {
            name: Some("Local Operator".into()),
            project: "default".into(),
            session_id: Some("local-operator".into()),
            agent_id: None,
            runtime_id: Some("local-operator".into()),
            client_key: None,
            kind: Kind::Human,
            tier: Tier::Admin,
        }),
        call: DaemonIpcCall::Query {
            method: "local.mcp.resolveIdentity".into(),
            params: serde_json::json!({
                "name": "legacy-mcp",
                "project": "metadata-only",
                "agent": "claude",
                "claudeSessionId": null
            }),
        },
    };

    let response = daemon_ipc::handle_request(&state, "boot-token", request).await;
    assert!(response.error.is_none(), "{:?}", response.error);
    let identity: RegisterRequest = serde_json::from_value(response.result.unwrap()).unwrap();
    assert_eq!(identity.name.as_deref(), Some("legacy-mcp"));
    assert_eq!(identity.client_key, "mcp:legacy-mcp");
    assert_eq!(identity.harness, hid("claude"));
}

#[tokio::test]
async fn registered_query_caller_is_canonicalized_by_client_key_after_rename() {
    let state = state().await;
    let registered = state.identity.register(register_request()).await.unwrap();
    let request = DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: "rpc-agent-whoami".into(),
        caller: Some(DaemonIpcCaller {
            // A running harness keeps its launch-time environment after an operator renames it.
            // The stable client key must resolve the current name instead of rejecting this stale
            // display label as authentication evidence.
            name: Some("stale-before-rename".into()),
            project: "metadata-only".into(),
            session_id: Some(registered.session_id.0.clone()),
            agent_id: registered.agent_id.as_ref().map(|id| id.0.clone()),
            runtime_id: Some(registered.session_id.0.clone()),
            client_key: Some("client-ipc-agent".into()),
            kind: Kind::Agent,
            tier: Tier::Agent,
        }),
        call: DaemonIpcCall::Query {
            method: "whoami".into(),
            params: serde_json::Value::Null,
        },
    };

    let response = daemon_ipc::handle_request(&state, "boot-token", request).await;
    assert!(response.error.is_none(), "{:?}", response.error);
    let whoami: Whoami = serde_json::from_value(response.result.unwrap()).unwrap();
    assert_eq!(whoami.name.as_deref(), Some("ipc-agent"));
    assert_eq!(whoami.session_id, registered.session_id);
}

#[tokio::test]
async fn wsl_ambient_whoami_resolves_renamed_agent_by_stable_identity() {
    let state = state().await;
    let session_id = SessionId("s_wsl-agent".into());
    let agent_id = "a_s_wsl-agent";
    let client_key = "client-wsl-agent";
    Agents::new(&state.store)
        .create(NewAgent {
            agent_id: agent_id.into(),
            project: "default".into(),
            name: Some("after-rename".into()),
            default_harness: Some("opencode".into()),
            role: None,
            tier: Some("agent".into()),
            owner: None,
        })
        .await
        .unwrap();
    Sessions::new(&state.store)
        .create(NewSession {
            session_id: session_id.clone(),
            name: Some("after-rename".into()),
            agent: Some("opencode".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some(client_key.into()),
            cwd: Some("/mnt/c/Users/tester/project".into()),
            project: "default".into(),
            transport: Some("pty".into()),
        })
        .await
        .unwrap();
    Sessions::new(&state.store)
        .set_agent_id(&session_id, agent_id)
        .await
        .unwrap();

    let ambient = with_test_env_vars(
        &[
            ("WSL_DISTRO_NAME", Some("Ubuntu")),
            ("WSL_INTEROP", Some("/run/WSL/1_interop")),
            ("NEXUS_NAME", Some("before-rename")),
            ("NEXUS_AGENT_ID", Some(agent_id)),
            ("NEXUS_CLIENT_KEY", Some(client_key)),
            ("NEXUS_SESSION_ID", Some("s_wsl-agent")),
            ("NEXUS_PROJECT", Some("default")),
            ("NEXUS_AGENT", Some("opencode")),
        ],
        || {
            nexus::cli::ambient::identity_from_env_result()
                .unwrap()
                .unwrap()
        },
    );
    let fallback_name = ambient
        .name
        .clone()
        .or_else(|| ambient.agent_id.as_ref().map(|id| id.0.clone()))
        .unwrap_or_else(|| ambient.client_key.clone());
    let response = daemon_ipc::handle_request(
        &state,
        "boot-token",
        DaemonIpcRequest {
            version: DAEMON_IPC_PROTOCOL_VERSION,
            token: "boot-token".into(),
            request_id: "rpc-wsl-whoami".into(),
            caller: Some(DaemonIpcCaller {
                name: Some(fallback_name),
                project: ambient.project,
                session_id: None,
                agent_id: None,
                runtime_id: None,
                client_key: Some(ambient.client_key),
                kind: Kind::Agent,
                tier: Tier::Agent,
            }),
            call: DaemonIpcCall::Query {
                method: "whoami".into(),
                params: serde_json::Value::Null,
            },
        },
    )
    .await;

    assert!(response.error.is_none(), "{:?}", response.error);
    let whoami: Whoami = serde_json::from_value(response.result.unwrap()).unwrap();
    assert_eq!(whoami.name.as_deref(), Some("after-rename"));
    assert_eq!(whoami.session_id, session_id);
    assert_eq!(
        whoami.agent_id.as_ref().map(|id| id.0.as_str()),
        Some(agent_id)
    );
}

#[tokio::test]
async fn unnamed_registered_query_caller_resolves_whoami_by_stable_agent_id() {
    let state = state().await;
    let session_id = SessionId("s_unnamed-ipc-agent".into());
    let agent_id = "a_s_unnamed-ipc-agent";
    let client_key = "client-unnamed-ipc-agent";
    Agents::new(&state.store)
        .create(NewAgent {
            agent_id: agent_id.into(),
            project: "metadata-only".into(),
            name: None,
            default_harness: Some("opencode".into()),
            role: None,
            tier: Some("agent".into()),
            owner: None,
        })
        .await
        .unwrap();
    Sessions::new(&state.store)
        .create(NewSession {
            session_id: session_id.clone(),
            name: None,
            agent: Some("opencode".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some(client_key.into()),
            cwd: None,
            project: "metadata-only".into(),
            transport: Some("acp".into()),
        })
        .await
        .unwrap();
    Sessions::new(&state.store)
        .set_agent_id(&session_id, agent_id)
        .await
        .unwrap();

    let response = daemon_ipc::handle_request(
        &state,
        "boot-token",
        DaemonIpcRequest {
            version: DAEMON_IPC_PROTOCOL_VERSION,
            token: "boot-token".into(),
            request_id: "rpc-unnamed-agent-whoami".into(),
            caller: Some(DaemonIpcCaller {
                // This is the fallback emitted by an unnamed launch's ambient CLI identity.
                name: Some(agent_id.into()),
                project: "metadata-only".into(),
                session_id: None,
                agent_id: None,
                runtime_id: None,
                client_key: Some(client_key.into()),
                kind: Kind::Agent,
                tier: Tier::Agent,
            }),
            call: DaemonIpcCall::Query {
                method: "whoami".into(),
                params: serde_json::Value::Null,
            },
        },
    )
    .await;

    assert!(response.error.is_none(), "{:?}", response.error);
    let whoami: Whoami = serde_json::from_value(response.result.unwrap()).unwrap();
    assert_eq!(
        whoami.agent_id.as_ref().map(|id| id.0.as_str()),
        Some(agent_id)
    );
    assert_eq!(whoami.name, None);
    assert_eq!(whoami.session_id, session_id);
}

#[tokio::test]
async fn bounded_binary_frame_roundtrips_over_one_local_connection() {
    let state = state().await;
    let (mut client, server) = tokio::io::duplex(16 * 1024);
    let task = tokio::spawn(async move {
        daemon_ipc::serve_connection(server, state, "boot-token".into()).await
    });
    let request = DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: "rpc-framed".into(),
        caller: Some(DaemonIpcCaller {
            name: Some("Local Operator".into()),
            project: "metadata-only".into(),
            session_id: Some("local-operator".into()),
            agent_id: None,
            runtime_id: Some("local-operator".into()),
            client_key: None,
            kind: Kind::Human,
            tier: Tier::Admin,
        }),
        call: DaemonIpcCall::Query {
            method: "whoami".into(),
            params: serde_json::Value::Null,
        },
    };

    daemon_ipc::write_request_frame(&mut client, &request)
        .await
        .unwrap();
    let response = daemon_ipc::read_response_frame(&mut client).await.unwrap();
    assert_eq!(response.request_id, "rpc-framed");
    assert!(response.error.is_none());
    task.await.unwrap().unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn unix_listener_publishes_boot_manifest_and_serves_real_client() {
    let state = state().await;
    let home = std::env::temp_dir().join(format!(
        "nexus-daemon-ipc-test-{}-{}",
        std::process::id(),
        nexus_common::now()
    ));
    std::fs::create_dir_all(&home).unwrap();
    let handle = daemon_ipc::spawn_daemon_ipc(state, "boot-test".into(), &home).unwrap();
    let endpoint = daemon_ipc::read_daemon_ipc_endpoint(&home).expect("endpoint manifest");
    assert_eq!(endpoint.daemon_boot_id, "boot-test");
    assert_eq!(endpoint.path, handle.endpoint().path);

    let mut stream = tokio::net::UnixStream::connect(&endpoint.path)
        .await
        .unwrap();
    let request = DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: endpoint.token,
        request_id: "rpc-real-socket".into(),
        caller: Some(DaemonIpcCaller {
            name: Some("Local Operator".into()),
            project: "metadata-only".into(),
            session_id: Some("local-operator".into()),
            agent_id: None,
            runtime_id: Some("local-operator".into()),
            client_key: None,
            kind: Kind::Human,
            tier: Tier::Admin,
        }),
        call: DaemonIpcCall::Query {
            method: "whoami".into(),
            params: serde_json::Value::Null,
        },
    };
    daemon_ipc::write_request_frame(&mut stream, &request)
        .await
        .unwrap();
    let response = daemon_ipc::read_response_frame(&mut stream).await.unwrap();
    assert!(response.error.is_none(), "{:?}", response.error);

    let response = daemon_ipc::call_daemon_ipc(
        &home,
        DaemonIpcRequest {
            version: DAEMON_IPC_PROTOCOL_VERSION,
            token: String::new(),
            request_id: "rpc-client-helper".into(),
            caller: Some(DaemonIpcCaller {
                name: Some("Local Operator".into()),
                project: "metadata-only".into(),
                session_id: Some("local-operator".into()),
                agent_id: None,
                runtime_id: Some("local-operator".into()),
                client_key: None,
                kind: Kind::Human,
                tier: Tier::Admin,
            }),
            call: DaemonIpcCall::Query {
                method: "whoami".into(),
                params: serde_json::Value::Null,
            },
        },
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert_eq!(response.request_id, "rpc-client-helper");
    assert!(response.error.is_none());

    let socket_path = handle.endpoint().path.clone();
    drop(handle);
    assert!(!socket_path.exists());
    assert!(daemon_ipc::read_daemon_ipc_endpoint(&home).is_none());
    std::fs::remove_dir_all(home).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn store_client_command_uses_daemon_ipc_without_opening_store() {
    let state = state().await;
    let worker = command_worker::spawn(state.clone());
    let home = std::env::temp_dir().join(format!(
        "nexus-store-client-ipc-test-{}-{}",
        std::process::id(),
        nexus_common::now()
    ));
    std::fs::create_dir_all(&home).unwrap();
    let handle = daemon_ipc::spawn_daemon_ipc(state.clone(), "boot-client".into(), &home).unwrap();
    let client = StoreClient::from_daemon_for_tests(home.clone());

    let response: RegisterResponse = client
        .command(
            nexus_store::command_kinds::identity::REGISTER,
            &register_request(),
        )
        .await
        .unwrap();
    assert!(!response.session_id.0.is_empty());
    let mut rows = state
        .store
        .conn
        .query(
            "SELECT COUNT(*) FROM command_intents WHERE kind = ?1",
            libsql::params![nexus_store::command_kinds::identity::REGISTER],
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        1
    );

    drop(handle);
    worker.abort();
    std::fs::remove_dir_all(home).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn read_client_query_uses_daemon_ipc_without_opening_store() {
    let state = state().await;
    let home = std::env::temp_dir().join(format!(
        "nexus-read-client-ipc-test-{}-{}",
        std::process::id(),
        nexus_common::now()
    ));
    std::fs::create_dir_all(&home).unwrap();
    let handle =
        daemon_ipc::spawn_daemon_ipc(state.clone(), "boot-read-client".into(), &home).unwrap();
    let client = ReadClient::from_daemon_for_tests(home.clone());

    let whoami = client.whoami().await.unwrap();
    assert_eq!(whoami.session_id.0, "local-operator");
    assert_eq!(whoami.tier, Tier::Admin);

    let mut rows = state
        .store
        .conn
        .query("SELECT COUNT(*) FROM command_intents", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        0
    );

    drop(handle);
    std::fs::remove_dir_all(home).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn read_client_members_query_uses_daemon_ipc() {
    let state = state().await;
    let mut registration = register_request();
    registration.project = "default".into();
    let registered = state.identity.register(registration).await.unwrap();
    let home = std::env::temp_dir().join(format!(
        "nexus-read-members-ipc-test-{}-{}",
        std::process::id(),
        nexus_common::now()
    ));
    std::fs::create_dir_all(&home).unwrap();
    let handle = daemon_ipc::spawn_daemon_ipc(state, "boot-read-members".into(), &home).unwrap();
    let client = ReadClient::from_daemon_for_tests(home.clone());

    let response = client
        .members(MemberListRequest {
            include_offline: Some(true),
            include_dead: Some(true),
        })
        .await
        .unwrap();
    assert!(response
        .members
        .iter()
        .any(|member| member.session_id == registered.session_id));

    drop(handle);
    std::fs::remove_dir_all(home).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn read_client_collection_queries_use_daemon_ipc() {
    let state = state().await;
    let home = std::env::temp_dir().join(format!(
        "nexus-read-collections-ipc-test-{}-{}",
        std::process::id(),
        nexus_common::now()
    ));
    std::fs::create_dir_all(&home).unwrap();
    let handle =
        daemon_ipc::spawn_daemon_ipc(state, "boot-read-collections".into(), &home).unwrap();
    let client = ReadClient::from_daemon_for_tests(home.clone());

    assert!(client.threads().await.unwrap().threads.is_empty());
    assert!(client.topics().await.unwrap().topics.is_empty());
    assert!(client.sources().await.unwrap().sources.is_empty());

    drop(handle);
    std::fs::remove_dir_all(home).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn read_client_target_queries_use_daemon_while_recall_requires_gateway() {
    let state = state().await;
    let mut registration = register_request();
    registration.project = "default".into();
    state.identity.register(registration).await.unwrap();
    let thread_id = ThreadId("thread-ipc-read".into());
    Threads::new(&state.store)
        .create(&thread_id, "ipc-read-thread", "metadata-only", "ipc-agent")
        .await
        .unwrap();
    Threads::new(&state.store)
        .add_member(&thread_id, "ipc-agent")
        .await
        .unwrap();
    Sources::new(&state.store)
        .create("ipc-source", "test-token", "ipc-topic", nexus_common::now())
        .await
        .unwrap();
    let home = std::env::temp_dir().join(format!(
        "nexus-read-targets-ipc-test-{}-{}",
        std::process::id(),
        nexus_common::now()
    ));
    std::fs::create_dir_all(&home).unwrap();
    let handle = daemon_ipc::spawn_daemon_ipc(state, "boot-read-targets".into(), &home).unwrap();
    let client = ReadClient::from_daemon_for_tests(home.clone());

    assert_eq!(
        client
            .thread_members("ipc-read-thread")
            .await
            .unwrap()
            .members,
        vec!["ipc-agent"]
    );
    assert_eq!(
        client.source("ipc-source").await.unwrap().name,
        "ipc-source"
    );
    assert_eq!(
        client
            .agent_show("ipc-agent")
            .await
            .unwrap()
            .agent
            .name
            .as_deref(),
        Some("ipc-agent")
    );
    assert_eq!(
        client
            .agent_runtimes("ipc-agent", true)
            .await
            .unwrap()
            .runtimes
            .len(),
        1
    );
    let search_error = client
        .search(SearchRequest {
            query: "nothing".into(),
            mode: SearchMode::Fts,
            limit: Some(5),
            thread: None,
            with: None,
            since: None,
        })
        .await
        .unwrap_err();
    assert!(search_error.message.contains("Gateway unavailable"));
    let history_error = client
        .history(HistoryRequest {
            thread: None,
            with: None,
            topic: None,
            limit: Some(5),
            before: None,
        })
        .await
        .unwrap_err();
    assert!(history_error.message.contains("Gateway unavailable"));
    let read_error = client.message("missing-message").await.unwrap_err();
    assert!(read_error.message.contains("Gateway unavailable"));

    drop(handle);
    std::fs::remove_dir_all(home).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn read_client_terminal_queries_execute_inside_daemon() {
    let state = state().await;
    let mut registration = register_request();
    registration.project = "default".into();
    registration.harness = hid("claude");
    registration.harness_session_id = "claude-native-ipc".into();
    let registered = state.identity.register(registration).await.unwrap();
    state
        .store
        .conn
        .execute(
            "UPDATE sessions SET transport = 'pty', cwd = '/tmp' WHERE session_id = ?1",
            libsql::params![registered.session_id.0.clone()],
        )
        .await
        .unwrap();
    let home = std::env::temp_dir().join(format!(
        "nexus-read-terminal-ipc-test-{}-{}",
        std::process::id(),
        nexus_common::now()
    ));
    std::fs::create_dir_all(&home).unwrap();
    let handle = daemon_ipc::spawn_daemon_ipc(state, "boot-read-terminal".into(), &home).unwrap();
    let client = ReadClient::from_daemon_for_tests(home.clone());

    let by_target = client
        .pty_attach_descriptor(&registered.session_id.0)
        .await
        .unwrap();
    assert_eq!(by_target.session_id, registered.session_id);
    let by_session = client
        .pty_attach_descriptor_for_session(&registered.session_id)
        .await
        .unwrap();
    assert_eq!(by_session, by_target);
    let revive = client
        .attach_revive_plan(&registered.session_id.0)
        .await
        .unwrap();
    assert_eq!(revive.session_id, registered.session_id);
    assert_eq!(revive.spawn.kind, hid("claude"));

    drop(handle);
    std::fs::remove_dir_all(home).unwrap();
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
