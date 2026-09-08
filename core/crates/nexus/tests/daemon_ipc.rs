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
    AgentId, DaemonIpcCall, DaemonIpcCaller, DaemonIpcRequest, HistoryRequest, Kind, Locality,
    MemberListRequest, RegisterRequest, RegisterResponse, SearchMode, SearchRequest, SessionId,
    ThreadId, Tier, Whoami, DAEMON_IPC_PROTOCOL_VERSION,
};
use nexus_store::repos::{
    Agents, CommandIntents, DaemonState, NewAgent, NewSession, Sessions, Sources, Threads,
};
use nexus_store::Store;

async fn state() -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    AppState::wire(store, &Config::default())
}

fn queue_operator() -> DaemonIpcCaller {
    DaemonIpcCaller {
        name: Some("transport".into()),
        project: "default".into(),
        session_id: Some("local-operator".into()),
        runtime_id: Some("local-operator".into()),
        agent_id: None,
        client_key: None,
        kind: Kind::Human,
        locality: Locality::Local,
        access: None,
        principal_id: None,
        tier: Tier::Admin,
    }
}

#[tokio::test]
async fn exact_retained_read_follows_real_worker_rejection_after_rebind() {
    let state = state().await;
    state.store.identity_conn().execute_batch("INSERT INTO agents (agent_id,project,name,tier,created_at) VALUES ('a_retained','default','retained','agent',1); INSERT INTO agent_runtimes (runtime_id,agent_id,harness,active,started_at) VALUES ('s_old','a_retained','other',1,1);").await.unwrap();
    state.store.conn.execute_batch("INSERT INTO sessions (session_id,agent_id,name,agent,kind,tier,project,created_at) VALUES ('s_old','a_retained','retained','other','agent','agent','default',1);").await.unwrap();
    let enqueue = daemon_ipc::handle_request(&state, "boot-token", DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION, token: "boot-token".into(), request_id: "enqueue-old".into(), caller: Some(queue_operator()),
        call: DaemonIpcCall::Enqueue { command_id:"cmd_retained".into(), kind:"harness.prompt".into(), params:serde_json::json!({"name":"retained","agentId":"a_retained","expectedSessionId":"s_old","text":"original private command","clientMessageId":"cm_retained"}), idempotency_key:Some("cm_retained".into()) },
    }).await;
    assert!(enqueue.error.is_none(), "{:?}", enqueue.error);
    state.store.identity_conn().execute_batch("UPDATE agent_runtimes SET active=0,stopped_at=2; INSERT INTO agent_runtimes (runtime_id,agent_id,harness,active,started_at) VALUES ('s_new','a_retained','other',1,2);").await.unwrap();
    state.store.conn.execute_batch("INSERT INTO sessions (session_id,agent_id,name,agent,kind,tier,project,created_at) VALUES ('s_new','a_retained','retained-new','other','agent','agent','default',2);").await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(3), command_worker::process_next(&state))
            .await
            .expect("real worker must settle the old exact row")
            .unwrap()
    );
    let terminal = CommandIntents::new(&state.store)
        .get("cmd_retained")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.status, "error");
    let error: serde_json::Value =
        serde_json::from_str(terminal.error_json.as_deref().unwrap()).unwrap();
    assert!(
        error["message"].as_str().unwrap().contains("session"),
        "{error}"
    );
    let snapshot = daemon_ipc::handle_request(&state, "boot-token", DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION, token:"boot-token".into(), request_id:"read-old".into(), caller:Some(queue_operator()),
        call:DaemonIpcCall::Query { method:"local.sessionQueue.read".into(), params:serde_json::json!({"project":"default","agentId":"a_retained","expectedSessionId":"s_old","requester":queue_operator()}) },
    }).await;
    assert!(snapshot.error.is_none(), "{:?}", snapshot.error);
    let snapshot = snapshot.result.unwrap();
    assert_eq!(snapshot["sessionId"], "s_old");
    assert_eq!(snapshot["commands"][0]["state"], "failed");
    assert_eq!(snapshot["commands"][0]["errorCode"], error["code"]);
    assert_eq!(snapshot["commands"][0]["correlationOwned"], true);
    assert_eq!(snapshot["commands"][0]["text"], "original private command");
}

#[tokio::test]
async fn queue_read_resolves_cookie_requester_without_boot_rebind_or_writes() {
    let state = state().await;
    state.store.conn.execute_batch("INSERT INTO sessions (session_id,agent_id,name,agent,kind,tier,project,created_at,client_key) VALUES ('s_target','a_target','target','other','agent','agent','default',1,NULL), ('s_human_new',NULL,'human-renamed','other','human','admin','default',2,'ck_human');").await.unwrap();
    CommandIntents::new(&state.store).insert_pending(nexus_store::repos::NewCommandIntent {
        command_id:"cmd_human".into(),kind:"harness.prompt".into(),project:"default".into(),caller_name:"old-human-name".into(),caller_session_id:Some("s_human_old".into()),caller_agent_id:None,caller_runtime_id:Some("s_human_old".into()),caller_client_key:Some("ck_human".into()),caller_principal_id:Some("h_owner".into()),caller_kind:Some("human".into()),caller_tier:Some("admin".into()),idempotency_key:Some("same-client-id".into()),request_json:serde_json::json!({"agentId":"a_target","text":"owned text","clientMessageId":"same-client-id"}).to_string(),created_at:1,
    }).await.unwrap();
    for (key, principal, project, owned) in [
        ("ck_human", "h_owner", "default", true),
        ("ck_human", "h_other", "default", false),
        ("unregistered", "h_owner", "default", false),
        ("ck_human", "h_owner", "other", false),
    ] {
        let requester = DaemonIpcCaller {
            client_key: Some(key.into()),
            principal_id: Some(principal.into()),
            project: project.into(),
            session_id: None,
            runtime_id: None,
            ..queue_operator()
        };
        let before = state
            .store
            .conn
            .query("SELECT total_changes()", ())
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap()
            .get::<i64>(0)
            .unwrap();
        let response = daemon_ipc::handle_request(&state,"boot-token",DaemonIpcRequest {
            version:DAEMON_IPC_PROTOCOL_VERSION,token:"boot-token".into(),request_id:"read-human".into(),caller:Some(queue_operator()),call:DaemonIpcCall::Query {method:"local.sessionQueue.read".into(),params:serde_json::json!({"project":"default","agentId":"a_target","requester":requester,"correlationOwned":true})},
        }).await;
        assert!(response.error.is_none(), "{:?}", response.error);
        assert_eq!(
            response.result.unwrap()["commands"][0]["correlationOwned"],
            owned,
            "{key}/{principal}/{project}"
        );
        let after = state
            .store
            .conn
            .query("SELECT total_changes()", ())
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap()
            .get::<i64>(0)
            .unwrap();
        assert_eq!(before, after, "read must not register or rebind a human");
    }
}

#[tokio::test]
async fn unsupported_prompt_options_are_rejected_before_durable_enqueue() {
    let state = state().await;
    for (index, options) in [
        serde_json::json!({"delivery": "auto"}),
        serde_json::json!({"modelSelection": {"modelId": "target-model"}}),
    ]
    .into_iter()
    .enumerate()
    {
        let mut params =
            serde_json::json!({"agentId": "a_target", "name": "target", "text": "preserve intent"});
        params
            .as_object_mut()
            .unwrap()
            .extend(options.as_object().unwrap().clone());
        let command_id = format!("cmd-unsupported-options-{index}");
        let response = daemon_ipc::handle_request(
            &state,
            "boot-token",
            DaemonIpcRequest {
                version: DAEMON_IPC_PROTOCOL_VERSION,
                token: "boot-token".into(),
                request_id: format!("rpc-options-{index}"),
                caller: None,
                call: DaemonIpcCall::Enqueue {
                    command_id: command_id.clone(),
                    kind: nexus_store::command_kinds::harness::PROMPT.into(),
                    params,
                    idempotency_key: None,
                },
            },
        )
        .await;
        assert!(
            response.error.is_some(),
            "unsupported options must not silently become ordinary delivery"
        );
        assert_eq!(
            response.error.unwrap().code,
            nexus_contracts::codes::INVALID_PARAMS
        );
        assert!(CommandIntents::new(&state.store)
            .get(&command_id)
            .await
            .unwrap()
            .is_none());
    }
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
        locality: Default::default(),
        access: None,
        role: None,
        cwd: None,
    }
}

#[tokio::test]
async fn malformed_exact_selector_is_rejected_before_any_durable_enqueue() {
    let state = state().await;
    for kind in [
        nexus_store::command_kinds::harness::PROMPT,
        nexus_store::command_kinds::harness::STEER,
        nexus_store::command_kinds::harness::INTERRUPT,
        nexus_store::command_kinds::harness::COMPACT,
    ] {
        for (index, options) in [
            serde_json::json!({"agentId":"a_target", "expectedSessionId":null}),
            serde_json::json!({"agentId":"a_target", "expectedSessionId":""}),
            serde_json::json!({"agentId":"a_target", "expectedSessionId":"  "}),
            serde_json::json!({"agentId":"a_target", "expectedSessionId":{}}),
            serde_json::json!({"expectedSessionId":"s_target"}),
            serde_json::json!({"agentId":null, "expectedSessionId":"s_target"}),
            serde_json::json!({"agentId":" ", "expectedSessionId":"s_target"}),
        ]
        .into_iter()
        .enumerate()
        {
            let mut params = serde_json::json!({"name":"target", "text":"no side effects"});
            params
                .as_object_mut()
                .unwrap()
                .extend(options.as_object().unwrap().clone());
            let command_id = format!("malformed-{kind}-{index}");
            let response = daemon_ipc::handle_request(
                &state,
                "test-token",
                DaemonIpcRequest {
                    version: DAEMON_IPC_PROTOCOL_VERSION,
                    token: "test-token".into(),
                    request_id: command_id.clone(),
                    caller: None,
                    call: DaemonIpcCall::Enqueue {
                        command_id: command_id.clone(),
                        kind: kind.into(),
                        params,
                        idempotency_key: None,
                    },
                },
            )
            .await;
            assert_eq!(
                response.error.as_ref().map(|error| error.code),
                Some(nexus_contracts::codes::INVALID_PARAMS),
                "{kind} / {options}: {response:?}"
            );
            assert!(CommandIntents::new(&state.store)
                .get(&command_id)
                .await
                .unwrap()
                .is_none());
        }
    }
}

#[tokio::test]
async fn malformed_exact_queue_selector_is_rejected_before_mutation_journal() {
    let state = state().await;
    for options in [
        serde_json::json!({"agentId":"a_target", "expectedSessionId":null}),
        serde_json::json!({"agentId":"a_target", "expectedSessionId":""}),
        serde_json::json!({"expectedSessionId":"s_target"}),
    ] {
        let mut request = serde_json::json!({"name":"target", "action":"cancel", "clientMutationId":"malformed-mutation", "commandId":"never-created", "expectedRevision":1});
        request
            .as_object_mut()
            .unwrap()
            .extend(options.as_object().unwrap().clone());
        let response = daemon_ipc::handle_request(
            &state,
            "test-token",
            DaemonIpcRequest {
                version: DAEMON_IPC_PROTOCOL_VERSION,
                token: "test-token".into(),
                request_id: "malformed-query".into(),
                caller: Some(DaemonIpcCaller {
                    name: Some("operator".into()),
                    project: "metadata".into(),
                    session_id: Some("local-operator".into()),
                    agent_id: None,
                    runtime_id: Some("local-operator".into()),
                    client_key: None,
                    kind: Kind::Human,
                    locality: Default::default(),
                    access: None,
                    principal_id: None,
                    tier: Tier::Admin,
                }),
                call: DaemonIpcCall::Query {
                    method: "local.sessionQueue.mutate".into(),
                    params: serde_json::json!({"project":"metadata", "now":1, "request":request}),
                },
            },
        )
        .await;
        assert_eq!(
            response.error.as_ref().map(|error| error.code),
            Some(nexus_contracts::codes::INVALID_PARAMS),
            "{response:?}"
        );
    }
    let mut rows = state
        .store
        .identity_conn()
        .query("SELECT COUNT(*) FROM command_queue_mutations", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        0
    );
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
async fn gateway_transport_enqueue_returns_a_principal_bound_durable_receipt() {
    let state = state().await;
    let request = DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: "rpc-external-enqueue".into(),
        caller: Some(DaemonIpcCaller {
            name: Some("outside".into()),
            project: "default".into(),
            session_id: Some("transport:telegram".into()),
            agent_id: None,
            runtime_id: Some("transport:telegram".into()),
            client_key: None,
            kind: Kind::Human,
            locality: Locality::External,
            access: Some("guest".into()),
            principal_id: Some("x_external_abc".into()),
            tier: Tier::Agent,
        }),
        call: DaemonIpcCall::Enqueue {
            command_id: "cmd-external-enqueue".into(),
            kind: nexus_store::command_kinds::message_post::SEND.into(),
            params: serde_json::json!({
                "to": {"verb": "dm", "name": "ipc-agent"},
                "body": "queued while active"
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
        "cmd-external-enqueue"
    );
    assert_eq!(response.result.as_ref().unwrap()["status"], "pending");
    assert_eq!(response.result.as_ref().unwrap()["revision"], 1);
    assert_eq!(response.result.as_ref().unwrap()["seq"], 0);
    assert_eq!(
        response.result.as_ref().unwrap()["sessionId"],
        serde_json::Value::Null
    );
    let row = CommandIntents::new(&state.store)
        .get("cmd-external-enqueue")
        .await
        .unwrap()
        .expect("durable queued row");
    assert_eq!(row.status, "pending");
    assert_eq!(row.caller_principal_id.as_deref(), Some("x_external_abc"));
    assert_eq!(row.caller_kind.as_deref(), Some("external.human"));
}

#[tokio::test]
async fn gateway_transport_principal_bypass_rejects_every_near_miss() {
    let state = state().await;
    let caller = DaemonIpcCaller {
        name: Some("outside".into()),
        project: "default".into(),
        session_id: Some("transport:telegram".into()),
        agent_id: None,
        runtime_id: Some("transport:telegram".into()),
        client_key: None,
        kind: Kind::Human,
        locality: Locality::External,
        access: Some("guest".into()),
        principal_id: Some("x_external_abc".into()),
        tier: Tier::Agent,
    };
    let mut cases = Vec::new();
    let mut changed = caller.clone();
    changed.access = Some("admin".into());
    cases.push((
        "wrong access",
        changed,
        nexus_store::command_kinds::message_post::SEND,
    ));
    let mut changed = caller.clone();
    changed.principal_id = Some("h_local".into());
    cases.push((
        "wrong principal",
        changed,
        nexus_store::command_kinds::message_post::SEND,
    ));
    let mut changed = caller.clone();
    changed.runtime_id = Some("transport:other".into());
    cases.push((
        "wrong runtime",
        changed,
        nexus_store::command_kinds::message_post::SEND,
    ));
    let mut changed = caller.clone();
    changed.agent_id = Some("a_intruder".into());
    cases.push((
        "agent shaped",
        changed,
        nexus_store::command_kinds::message_post::SEND,
    ));
    let mut changed = caller.clone();
    changed.client_key = Some("unregistered".into());
    cases.push((
        "client keyed",
        changed,
        nexus_store::command_kinds::message_post::SEND,
    ));
    cases.push((
        "wrong command",
        caller,
        nexus_store::command_kinds::thread::CREATE,
    ));

    for (index, (label, caller, kind)) in cases.into_iter().enumerate() {
        let command_id = format!("cmd-external-rejected-{index}");
        let response = daemon_ipc::handle_request(
            &state,
            "boot-token",
            DaemonIpcRequest {
                version: DAEMON_IPC_PROTOCOL_VERSION,
                token: "boot-token".into(),
                request_id: format!("rpc-external-rejected-{index}"),
                caller: Some(caller),
                call: DaemonIpcCall::Enqueue {
                    command_id: command_id.clone(),
                    kind: kind.into(),
                    params: serde_json::json!({
                        "to": {"verb": "dm", "name": "ipc-agent"},
                        "body": "must not enqueue"
                    }),
                    idempotency_key: None,
                },
            },
        )
        .await;
        assert_eq!(
            response.error.expect(label).code,
            nexus_contracts::codes::UNAUTHORIZED,
            "{label}"
        );
        assert!(
            CommandIntents::new(&state.store)
                .get(&command_id)
                .await
                .unwrap()
                .is_none(),
            "{label}"
        );
    }
}

#[tokio::test]
async fn session_prompt_enqueue_is_atomically_bounded_and_retry_safe() {
    let state = state().await;
    CommandIntents::new(&state.store)
        .insert_pending(nexus_store::repos::NewCommandIntent {
            command_id: "cmd-capacity-foreign-metadata".into(),
            kind: nexus_store::command_kinds::harness::PROMPT.into(),
            project: "foreign-metadata".into(),
            caller_name: "foreign-caller".into(),
            caller_session_id: None,
            caller_agent_id: None,
            caller_runtime_id: None,
            caller_client_key: None,
            caller_principal_id: None,
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: Some("cm-capacity-foreign-metadata".into()),
            request_json: serde_json::json!({
                "name": "bounded",
                "agentId": "a_bounded",
                "text": "project metadata cannot partition capacity",
                "clientMessageId": "cm-capacity-foreign-metadata"
            })
            .to_string(),
            created_at: 1,
        })
        .await
        .unwrap();
    let request = |index: usize, target: &str| DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: format!("rpc-capacity-{target}-{index}"),
        caller: None,
        call: DaemonIpcCall::Enqueue {
            command_id: format!("cmd-capacity-{target}-{index}"),
            kind: nexus_store::command_kinds::harness::PROMPT.into(),
            params: serde_json::json!({
                "name": target,
                "agentId": format!("a_{target}"),
                "text": format!("queued input {index}"),
                "clientMessageId": format!("cm-capacity-{target}-{index}")
            }),
            idempotency_key: Some(format!("cm-capacity-{target}-{index}")),
        },
    };

    let responses =
        futures::future::join_all((0..128).map(|index| {
            daemon_ipc::handle_request(&state, "boot-token", request(index, "bounded"))
        }))
        .await;
    let accepted = responses
        .iter()
        .filter(|response| response.error.is_none())
        .count();
    let full = responses
        .iter()
        .filter(|response| {
            response.error.as_ref().map(|error| error.code)
                == Some(nexus_contracts::codes::COMMAND_QUEUE_FULL)
        })
        .count();
    assert_eq!(accepted, 99);
    assert_eq!(full, 29);

    let accepted_index = responses
        .iter()
        .position(|response| response.error.is_none())
        .expect("one capacity probe must be accepted");
    let mut retry = request(accepted_index, "bounded");
    retry.request_id = "rpc-capacity-retry".into();
    let retry = daemon_ipc::handle_request(&state, "boot-token", retry).await;
    assert!(
        retry.error.is_none(),
        "an accepted command retry must remain accepted"
    );
    assert_eq!(
        retry.result.unwrap()["commandId"],
        format!("cmd-capacity-bounded-{accepted_index}")
    );

    let other = daemon_ipc::handle_request(&state, "boot-token", request(0, "independent")).await;
    assert!(
        other.error.is_none(),
        "capacity must be isolated by stable target"
    );

    let mut rows = state
        .store
        .conn
        .query(
            "SELECT COUNT(*) FROM command_intents WHERE kind = 'harness.prompt' \
             AND status = 'pending' AND json_extract(request_json, '$.agentId') = 'a_bounded'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        100
    );
}

#[tokio::test]
async fn shutdown_fence_rejects_new_enqueue_before_durable_acceptance() {
    let state = state().await;
    command_worker::begin_shutdown(&state).await;
    let request = DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: "rpc-after-shutdown-fence".into(),
        caller: None,
        call: DaemonIpcCall::Enqueue {
            command_id: "cmd-after-shutdown-fence".into(),
            kind: nexus_store::command_kinds::harness::PROMPT.into(),
            params: serde_json::json!({
                "name": "ipc-agent",
                "text": "must retry against the next daemon",
                "clientMessageId": "client-after-shutdown-fence"
            }),
            idempotency_key: Some("client-after-shutdown-fence".into()),
        },
    };

    let response = daemon_ipc::handle_request(&state, "boot-token", request).await;
    let error = response
        .error
        .expect("post-fence ingress must fail before durable acceptance");
    assert_eq!(error.code, nexus_contracts::codes::INTERNAL_ERROR);
    assert!(error.message.contains("shutting down"));
    assert!(
        CommandIntents::new(&state.store)
            .get("cmd-after-shutdown-fence")
            .await
            .unwrap()
            .is_none(),
        "a rejected post-fence command must leave no durable row"
    );
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
            locality: Default::default(),
            access: None,
            principal_id: None,
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
            locality: Default::default(),
            access: None,
            principal_id: None,
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
        locality: Default::default(),
        access: None,
        principal_id: None,
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
        locality: Default::default(),
        access: None,
        principal_id: None,
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
            locality: Default::default(),
            access: None,
            principal_id: None,
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
            "INSERT INTO sessions (session_id, agent_id, name, agent, kind, transport, project, created_at) \
             VALUES ('s_queue', 'a_queue', 'queue-agent', 'codex', 'agent', 'codex-appserver', 'runtime-label', 1)",
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
            project: "accepted-label".into(),
            caller_name: "Alex".into(),
            caller_session_id: Some("local-operator".into()),
            caller_agent_id: None,
            caller_runtime_id: Some("local-operator".into()),
            caller_client_key: None,
            caller_principal_id: None,
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
            project: "caller-label".into(),
            session_id: Some("local-operator".into()),
            agent_id: None,
            runtime_id: Some("local-operator".into()),
            client_key: None,
            kind: Kind::Human,
            locality: Default::default(),
            access: None,
            principal_id: None,
            tier: Tier::Admin,
        }),
        call: DaemonIpcCall::Query {
            method: "local.sessionQueue.mutate".into(),
            params: serde_json::json!({
                "project": "target-label",
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
async fn authenticated_daemon_query_treats_the_registered_session_agent_id_as_exclusive() {
    let state = state().await;
    Agents::new(&state.store)
        .create(NewAgent {
            agent_id: "a_alias_owner".into(),
            project: "alias-project".into(),
            name: Some("a_missing_registered_owner".into()),
            default_harness: Some("other".into()),
            role: None,
            tier: Some("agent".into()),
            owner: None,
        })
        .await
        .unwrap();
    for (session_id, name, project, client_key, agent_id) in [
        (
            "s_alias_owner",
            "a_missing_registered_owner",
            "alias-project",
            "ck_alias_owner",
            "a_alias_owner",
        ),
        (
            "s_registered_missing_owner",
            "registered-caller",
            "actual-project",
            "ck_registered_missing_owner",
            "a_missing_registered_owner",
        ),
    ] {
        Sessions::new(&state.store)
            .create(NewSession {
                session_id: SessionId(session_id.into()),
                name: Some(name.into()),
                agent: Some("other".into()),
                kind: "agent".into(),
                role: None,
                tier: "agent".into(),
                harness_session_id: Some(format!("hs_{session_id}")),
                client_key: Some(client_key.into()),
                cwd: None,
                project: project.into(),
                transport: Some("pty".into()),
            })
            .await
            .unwrap();
        Sessions::new(&state.store)
            .set_agent_id(&SessionId(session_id.into()), agent_id)
            .await
            .unwrap();
    }

    let response = daemon_ipc::handle_request(
        &state,
        "boot-token",
        DaemonIpcRequest {
            version: DAEMON_IPC_PROTOCOL_VERSION,
            token: "boot-token".into(),
            request_id: "rpc-exclusive-stored-agent".into(),
            caller: Some(DaemonIpcCaller {
                name: Some("stale-display".into()),
                project: "stale-project-metadata".into(),
                session_id: None,
                agent_id: None,
                runtime_id: None,
                client_key: Some("ck_registered_missing_owner".into()),
                kind: Kind::Agent,
                locality: Default::default(),
                access: None,
                principal_id: None,
                tier: Tier::Agent,
            }),
            call: DaemonIpcCall::Query {
                method: "whoami".into(),
                params: serde_json::json!({}),
            },
        },
    )
    .await;

    let error = response
        .error
        .expect("missing stored agent id must fail closed");
    assert!(matches!(
        error.code,
        nexus_contracts::codes::NOT_FOUND | nexus_contracts::codes::UNAUTHORIZED
    ));
    assert_ne!(
        response.result,
        Some(serde_json::json!({ "agentId": "a_alias_owner" }))
    );
}

#[tokio::test]
async fn local_queue_read_returns_typed_snapshot_and_transitions_without_raw_store_access() {
    let state = state().await;
    state
        .store
        .conn
        .execute(
            "INSERT INTO sessions (session_id, agent_id, name, agent, kind, transport, project, created_at) \
             VALUES ('s_queue_read', 'a_queue_read', 'queue-reader', 'codex', 'agent', \
             'codex-appserver', 'default', 1)",
            (),
        )
        .await
        .unwrap();
    CommandIntents::new(&state.store)
        .insert_pending(nexus_store::repos::NewCommandIntent {
            command_id: "cmd_queue_read".into(),
            kind: nexus_store::command_kinds::harness::PROMPT.into(),
            project: "default".into(),
            caller_name: "Alex".into(),
            caller_session_id: Some("local-operator".into()),
            caller_agent_id: None,
            caller_runtime_id: Some("local-operator".into()),
            caller_client_key: None,
            caller_principal_id: None,
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: Some("cm_queue_read".into()),
            request_json: serde_json::json!({
                "name": "queue-reader",
                "agentId": "a_queue_read",
                "text": "read me",
                "clientMessageId": "cm_queue_read"
            })
            .to_string(),
            created_at: 1,
        })
        .await
        .unwrap();
    let caller = DaemonIpcCaller {
        name: Some("Nexus Gateway".into()),
        project: "default".into(),
        session_id: Some("local-operator".into()),
        agent_id: None,
        runtime_id: Some("local-operator".into()),
        client_key: None,
        kind: Kind::Human,
        locality: Default::default(),
        access: None,
        principal_id: None,
        tier: Tier::Admin,
    };
    let read = |request_id: &str, params: serde_json::Value| DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: request_id.into(),
        caller: Some(caller.clone()),
        call: DaemonIpcCall::Query {
            method: "local.sessionQueue.read".into(),
            params,
        },
    };

    let snapshot = daemon_ipc::handle_request(
        &state,
        "boot-token",
        read(
            "rpc-queue-snapshot",
            serde_json::json!({
                "project": "other-metadata",
                "agentId": "a_queue_read"
            }),
        ),
    )
    .await;
    assert!(snapshot.error.is_none(), "{:?}", snapshot.error);
    let snapshot = snapshot.result.unwrap();
    assert_eq!(snapshot["target"], "queue-reader");
    assert_eq!(snapshot["sessionId"], "s_queue_read");
    assert_eq!(snapshot["commands"][0]["commandId"], "cmd_queue_read");
    assert_eq!(snapshot["commands"][0]["state"], "queued");
    assert_eq!(
        snapshot["commands"][0]["correlationOwned"], false,
        "the Gateway transport marker is not the requesting human"
    );
    for selector in [
        serde_json::json!(""),
        serde_json::json!(null),
        serde_json::json!(123),
    ] {
        let malformed = daemon_ipc::handle_request(
            &state,
            "boot-token",
            read(
                "bad-exact",
                serde_json::json!({"agentId":"a_queue_read", "expectedSessionId": selector}),
            ),
        )
        .await;
        assert_eq!(
            malformed.error.unwrap().code,
            nexus_contracts::codes::INVALID_PARAMS
        );
    }

    let stale_name = daemon_ipc::handle_request(
        &state,
        "boot-token",
        read(
            "rpc-queue-stale-name",
            serde_json::json!({
                "project": "stale-project-metadata",
                "name": "stale-display-name",
                "agentId": "a_queue_read"
            }),
        ),
    )
    .await;
    assert!(stale_name.error.is_none(), "{:?}", stale_name.error);
    let stale_name = stale_name.result.unwrap();
    assert_eq!(stale_name["target"], "queue-reader");
    assert_eq!(stale_name["sessionId"], "s_queue_read");
    assert_eq!(stale_name["commands"][0]["commandId"], "cmd_queue_read");

    let events = daemon_ipc::handle_request(
        &state,
        "boot-token",
        read(
            "rpc-queue-events",
            serde_json::json!({ "project": "other-metadata", "eventsAfter": 0 }),
        ),
    )
    .await;
    assert!(events.error.is_none(), "{:?}", events.error);
    assert_eq!(
        events.result.unwrap()["events"][0]["commandId"],
        "cmd_queue_read"
    );

    let name_fallback = daemon_ipc::handle_request(
        &state,
        "boot-token",
        read(
            "rpc-queue-name-fallback",
            serde_json::json!({
                "project": "other-metadata",
                "name": "queue-reader"
            }),
        ),
    )
    .await;
    assert!(name_fallback.error.is_none(), "{:?}", name_fallback.error);
    assert_eq!(name_fallback.result.unwrap()["sessionId"], "s_queue_read");

    let unauthorized = daemon_ipc::handle_request(
        &state,
        "boot-token",
        DaemonIpcRequest {
            version: DAEMON_IPC_PROTOCOL_VERSION,
            token: "boot-token".into(),
            request_id: "rpc-queue-unauthorized".into(),
            caller: None,
            call: DaemonIpcCall::Query {
                method: "local.sessionQueue.read".into(),
                params: serde_json::json!({
                    "project": "default",
                    "name": "queue-reader"
                }),
            },
        },
    )
    .await;
    assert_eq!(
        unauthorized.error.unwrap().code,
        nexus_contracts::codes::UNAUTHORIZED
    );
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
            locality: Default::default(),
            access: None,
            principal_id: None,
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
            locality: Default::default(),
            access: None,
            principal_id: None,
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
            locality: Default::default(),
            access: None,
            principal_id: None,
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
async fn registered_human_command_caller_does_not_resolve_through_same_name_agent() {
    let state = state().await;
    DaemonState::new(&state.store)
        .set_boot_epoch("boot_same_name_human", 1)
        .await
        .unwrap();
    Agents::new(&state.store)
        .create(NewAgent {
            agent_id: "a_same_name_agent".into(),
            project: "default".into(),
            name: Some("Earl".into()),
            default_harness: Some("other".into()),
            role: None,
            tier: Some("admin".into()),
            owner: None,
        })
        .await
        .unwrap();
    let registered = state
        .identity
        .register(RegisterRequest {
            agent_id: None,
            name: Some("Earl".into()),
            harness: hid("other"),
            harness_session_id: "hs_gateway_human".into(),
            project: "default".into(),
            client_key: "gateway-human-client".into(),
            runtime_credential: None,
            tier: Tier::Admin,
            kind: Some(Kind::Human),
            locality: Locality::Local,
            access: Some("admin".into()),
            role: None,
            cwd: None,
        })
        .await
        .unwrap();

    let response = daemon_ipc::handle_request(
        &state,
        "boot-token",
        DaemonIpcRequest {
            version: DAEMON_IPC_PROTOCOL_VERSION,
            token: "boot-token".into(),
            request_id: "rpc-same-name-human".into(),
            caller: Some(DaemonIpcCaller {
                name: Some("Earl".into()),
                project: "default".into(),
                session_id: Some(registered.session_id.0.clone()),
                agent_id: None,
                runtime_id: Some(registered.session_id.0.clone()),
                client_key: Some("gateway-human-client".into()),
                kind: Kind::Human,
                locality: Locality::Local,
                access: Some("admin".into()),
                principal_id: Some("h_gateway_human".into()),
                tier: Tier::Admin,
            }),
            call: DaemonIpcCall::Enqueue {
                command_id: "cmd-same-name-human".into(),
                kind: nexus_store::command_kinds::message_post::SEND.into(),
                params: serde_json::json!({
                    "to": {"verb": "dm", "name": "ipc-agent"},
                    "body": "human caller remains bound to its exact client-key session"
                }),
                idempotency_key: None,
            },
        },
    )
    .await;

    assert!(response.error.is_none(), "{:?}", response.error);
    let row = CommandIntents::new(&state.store)
        .get("cmd-same-name-human")
        .await
        .unwrap()
        .expect("durable human command row");
    assert_eq!(row.caller_name, "Earl");
    assert_eq!(
        row.caller_session_id.as_deref(),
        Some(registered.session_id.0.as_str())
    );
    assert_eq!(row.caller_agent_id, None);
    assert_eq!(
        row.caller_client_key.as_deref(),
        Some("gateway-human-client")
    );
    assert_eq!(row.caller_principal_id.as_deref(), Some("h_gateway_human"));
    assert_eq!(row.caller_kind.as_deref(), Some("local.human"));
}

#[tokio::test]
async fn registered_query_caller_prefers_the_authenticated_session_agent_id_over_a_reused_name() {
    let state = state().await;
    state
        .store
        .conn
        .execute("DROP INDEX idx_sessions_name", ())
        .await
        .unwrap();
    for (agent_id, name) in [
        ("a_stable_ipc", "current-name"),
        ("a_wrong_ipc", "reused-name"),
    ] {
        Agents::new(&state.store)
            .create(NewAgent {
                agent_id: agent_id.into(),
                project: "identity-metadata".into(),
                name: Some(name.into()),
                default_harness: Some("opencode".into()),
                role: None,
                tier: Some("agent".into()),
                owner: None,
            })
            .await
            .unwrap();
    }
    for (session_id, agent_id, client_key) in [
        ("s_stable_ipc", "a_stable_ipc", "ck_stable_ipc"),
        ("s_wrong_ipc", "a_wrong_ipc", "ck_wrong_ipc"),
    ] {
        Sessions::new(&state.store)
            .create(NewSession {
                session_id: SessionId(session_id.into()),
                name: Some("reused-name".into()),
                agent: Some("opencode".into()),
                kind: "agent".into(),
                role: None,
                tier: "agent".into(),
                harness_session_id: None,
                client_key: Some(client_key.into()),
                cwd: None,
                project: "transport-metadata".into(),
                transport: Some("pty".into()),
            })
            .await
            .unwrap();
        Sessions::new(&state.store)
            .set_agent_id(&SessionId(session_id.into()), agent_id)
            .await
            .unwrap();
    }

    let response = daemon_ipc::handle_request(
        &state,
        "boot-token",
        DaemonIpcRequest {
            version: DAEMON_IPC_PROTOCOL_VERSION,
            token: "boot-token".into(),
            request_id: "rpc-stable-ipc-whoami".into(),
            caller: Some(DaemonIpcCaller {
                name: Some("reused-name".into()),
                project: "stale-caller-metadata".into(),
                session_id: Some("s_stable_ipc".into()),
                agent_id: None,
                runtime_id: Some("s_stable_ipc".into()),
                client_key: Some("ck_stable_ipc".into()),
                kind: Kind::Agent,
                locality: Default::default(),
                access: None,
                principal_id: None,
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
    assert_eq!(whoami.agent_id, Some(AgentId("a_stable_ipc".into())));
    assert_eq!(whoami.name.as_deref(), Some("current-name"));
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
                locality: Default::default(),
                access: None,
                principal_id: None,
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
                locality: Default::default(),
                access: None,
                principal_id: None,
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
            locality: Default::default(),
            access: None,
            principal_id: None,
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
            locality: Default::default(),
            access: None,
            principal_id: None,
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
                locality: Default::default(),
                access: None,
                principal_id: None,
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
            project: None,
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
