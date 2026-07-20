use nexus_contracts::{
    DaemonIpcCall, DaemonIpcCaller, DaemonIpcRequest, DaemonIpcResponse, Kind, Locality, RpcError,
    Tier, DAEMON_IPC_PROTOCOL_VERSION,
};
use serde_json::json;

#[test]
fn daemon_ipc_command_roundtrip_keeps_caller_and_ledger_identity() {
    let frame = DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: "rpc-17".into(),
        caller: Some(DaemonIpcCaller {
            name: Some("casey".into()),
            project: "metadata-only".into(),
            session_id: Some("s_casey".into()),
            agent_id: Some("a_casey".into()),
            runtime_id: Some("r_casey".into()),
            client_key: Some("client-casey".into()),
            kind: Kind::Agent,
            locality: Locality::External,
            access: Some("guest".into()),
            principal_id: Some("x_casey".into()),
            tier: Tier::Agent,
        }),
        call: DaemonIpcCall::Command {
            command_id: "cmd-17".into(),
            kind: "message.post.send".into(),
            params: json!({"to": "a_target", "body": "hello"}),
            idempotency_key: Some("send-17".into()),
        },
    };

    let encoded = serde_json::to_value(&frame).unwrap();
    assert_eq!(encoded["call"]["mode"], "command");
    assert_eq!(encoded["call"]["commandId"], "cmd-17");
    assert_eq!(encoded["caller"]["project"], "metadata-only");
    assert_eq!(encoded["caller"]["locality"], "external");
    assert_eq!(encoded["caller"]["access"], "guest");
    assert_eq!(encoded["caller"]["principalId"], "x_casey");
    assert_eq!(
        serde_json::from_value::<DaemonIpcRequest>(encoded).unwrap(),
        frame
    );
}

#[test]
fn daemon_ipc_query_roundtrip_has_no_command_ledger_fields() {
    let frame = DaemonIpcRequest {
        version: DAEMON_IPC_PROTOCOL_VERSION,
        token: "boot-token".into(),
        request_id: "rpc-18".into(),
        caller: None,
        call: DaemonIpcCall::Query {
            method: "members".into(),
            params: json!({"includeOffline": true}),
        },
    };

    let encoded = serde_json::to_value(&frame).unwrap();
    assert_eq!(encoded["call"]["mode"], "query");
    assert!(encoded["call"].get("commandId").is_none());
    assert!(encoded["call"].get("idempotencyKey").is_none());
    assert_eq!(
        serde_json::from_value::<DaemonIpcRequest>(encoded).unwrap(),
        frame
    );
}

#[test]
fn daemon_ipc_response_carries_one_result_or_error() {
    let success = DaemonIpcResponse::success("rpc-19", json!({"ok": true}));
    assert_eq!(success.version, DAEMON_IPC_PROTOCOL_VERSION);
    assert_eq!(success.result, Some(json!({"ok": true})));
    assert_eq!(success.error, None);

    let failure = DaemonIpcResponse::failure(
        "rpc-20",
        RpcError {
            code: -32003,
            message: "target not found".into(),
            data: None,
        },
    );
    assert_eq!(failure.result, None);
    assert_eq!(failure.error.as_ref().unwrap().code, -32003);
}
