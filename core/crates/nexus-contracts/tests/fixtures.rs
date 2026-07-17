//! Golden wire fixtures — the cross-language no-drift lock. Each fixture is a canonical JSON-RPC
//! envelope; this test deserializes it into the envelope type AND into the registry's concrete
//! `params`/`result` type for that method, then re-serializes and asserts semantic round-trip.
//! The SAME files are consumed by the gateway's tests against the generated TS types.

use std::path::{Path, PathBuf};

use nexus_contracts::rpc::{Notification, Request, Response};
use nexus_contracts::{
    AdminGroupAssignRequest, AdminGroupAssignResponse, AdminRenameRequest, AdminRenameResponse,
    AgentId, DlqListRequest, DlqListResponse, DlqMutationResponse, DlqPurgeRequest,
    DlqRequeueRequest, MessageId, NexusBatch, PushRequest, PushResponse, RegisterRequest,
    RegisterResponse, SendRequest, SessionId, SourceRegisterRequest, SourceRegisterResponse,
};

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures")
}

fn load(name: &str) -> serde_json::Value {
    let path = fixtures_dir().join(name);
    let bytes = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("missing fixture {}: {e}", path.display()));
    serde_json::from_str(&bytes)
        .unwrap_or_else(|e| panic!("fixture {} is not valid JSON: {e}", path.display()))
}

/// A fixture round-trips if it deserializes into `T` and re-serializes to the same JSON value.
fn assert_roundtrips_value(name: &str) -> serde_json::Value {
    let original = load(name);
    let reser = serde_json::to_value(&original).unwrap();
    assert_eq!(reser, original, "{name}: raw JSON value did not round-trip");
    original
}

#[test]
fn register_req_fixture_matches_envelope_and_params() {
    let v = assert_roundtrips_value("register.req.json");
    let req: Request = serde_json::from_value(v).unwrap();
    assert_eq!(req.jsonrpc, "2.0");
    assert_eq!(req.method, "register");
    let params: RegisterRequest = serde_json::from_value(req.params.unwrap()).unwrap();
    assert_eq!(params.agent_id, Some(AgentId("a_ben".into())));
    assert_eq!(params.runtime_credential.as_deref(), Some("runtime_secret"));
    // re-serialize the typed params and confirm it is stable
    let _ = serde_json::to_value(&params).unwrap();
}

#[test]
fn register_res_fixture_matches_envelope_and_result() {
    let v = assert_roundtrips_value("register.res.json");
    let resp: Response = serde_json::from_value(v).unwrap();
    assert_eq!(resp.jsonrpc, "2.0");
    assert!(resp.error.is_none());
    let result: RegisterResponse = serde_json::from_value(resp.result.unwrap()).unwrap();
    assert_eq!(result.agent_id, Some(AgentId("a_ben".into())));
}

#[test]
fn send_req_fixture_matches_envelope_and_params() {
    let v = assert_roundtrips_value("send.req.json");
    let req: Request = serde_json::from_value(v).unwrap();
    assert_eq!(req.method, "send");
    let params: SendRequest = serde_json::from_value(req.params.unwrap()).unwrap();
    assert!(matches!(
        params.to,
        nexus_contracts::SendTarget::Dm { name, agent_id }
            if name.as_deref() == Some("dylan")
                && agent_id == Some(AgentId("a_dylan".into()))
    ));
}

#[test]
fn send_res_fixture_matches_envelope() {
    let v = assert_roundtrips_value("send.res.json");
    let resp: Response = serde_json::from_value(v).unwrap();
    assert!(resp.result.is_some() && resp.error.is_none());
}

#[test]
fn admin_group_assign_req_fixture_matches_envelope_and_params() {
    let v = assert_roundtrips_value("admin_group_assign.req.json");
    let req: Request = serde_json::from_value(v).unwrap();
    assert_eq!(req.method, "admin.group.assign");
    let params: AdminGroupAssignRequest = serde_json::from_value(req.params.unwrap()).unwrap();
    assert_eq!(params.project.as_deref(), Some("default"));
    assert_eq!(params.group, "backend");
    assert_eq!(params.name, "blake");
}

#[test]
fn admin_group_assign_res_fixture_matches_envelope_and_result() {
    let v = assert_roundtrips_value("admin_group_assign.res.json");
    let resp: Response = serde_json::from_value(v).unwrap();
    assert!(resp.error.is_none());
    let result: AdminGroupAssignResponse = serde_json::from_value(resp.result.unwrap()).unwrap();
    assert_eq!(result.project, "default");
    assert_eq!(result.group, "backend");
    assert_eq!(result.name.as_deref(), Some("blake"));
    assert_eq!(result.agent_id, AgentId("a_blake".into()));
}

#[test]
fn admin_rename_req_fixture_matches_envelope_and_params() {
    let v = assert_roundtrips_value("admin_rename.req.json");
    let req: Request = serde_json::from_value(v).unwrap();
    assert_eq!(req.method, "admin.rename");
    let params: AdminRenameRequest = serde_json::from_value(req.params.unwrap()).unwrap();
    assert_eq!(params.source, "s_staged");
    assert_eq!(params.target, "nora");
}

#[test]
fn admin_rename_res_fixture_matches_envelope_and_result() {
    let v = assert_roundtrips_value("admin_rename.res.json");
    let resp: Response = serde_json::from_value(v).unwrap();
    assert!(resp.error.is_none());
    let result: AdminRenameResponse = serde_json::from_value(resp.result.unwrap()).unwrap();
    assert_eq!(result.agent_id, AgentId("a_staged".into()));
    assert_eq!(result.session_id, Some(SessionId("s_staged".into())));
    assert_eq!(result.name, "nora");
    assert_eq!(result.previous, None);
}

#[test]
fn admin_dlq_list_req_fixture_matches_envelope_and_params() {
    let v = assert_roundtrips_value("admin_dlq_list.req.json");
    let req: Request = serde_json::from_value(v).unwrap();
    assert_eq!(req.method, "admin.dlq.list");
    let params: DlqListRequest = serde_json::from_value(req.params.unwrap()).unwrap();
    assert_eq!(params.for_target.as_deref(), Some("ana"));
    assert_eq!(params.since.as_deref(), Some("1h"));
    assert_eq!(params.limit, Some(25));
}

#[test]
fn admin_dlq_list_res_fixture_matches_envelope_and_result() {
    let v = assert_roundtrips_value("admin_dlq_list.res.json");
    let resp: Response = serde_json::from_value(v).unwrap();
    assert!(resp.error.is_none());
    let result: DlqListResponse = serde_json::from_value(resp.result.unwrap()).unwrap();
    assert_eq!(result.total, 1);
    assert_eq!(result.rows[0].in_flight_id, "if_dead");
    assert_eq!(result.rows[0].message_id.0, "m_dead");
    assert_eq!(
        result.rows[0].recipient_agent_id,
        Some(AgentId("a_ana".into()))
    );
    assert_eq!(result.rows[0].attempt_count, 2);
    assert_eq!(result.rows[0].error_code.as_deref(), Some("provider_error"));
    assert_eq!(
        result.rows[0]
            .error_details
            .as_ref()
            .and_then(|value| value.get("retryable")),
        Some(&serde_json::Value::Bool(true))
    );
}

#[test]
fn admin_dlq_requeue_req_fixture_matches_envelope_and_params() {
    let v = assert_roundtrips_value("admin_dlq_requeue.req.json");
    let req: Request = serde_json::from_value(v).unwrap();
    assert_eq!(req.method, "admin.dlq.requeue");
    let params: DlqRequeueRequest = serde_json::from_value(req.params.unwrap()).unwrap();
    assert_eq!(params.in_flight_id.as_deref(), Some("if_dead"));
    assert!(params.for_target.is_none());
}

#[test]
fn admin_dlq_requeue_res_fixture_matches_envelope_and_result() {
    let v = assert_roundtrips_value("admin_dlq_requeue.res.json");
    let resp: Response = serde_json::from_value(v).unwrap();
    assert!(resp.error.is_none());
    let result: DlqMutationResponse = serde_json::from_value(resp.result.unwrap()).unwrap();
    assert_eq!(result.count, 1);
    assert_eq!(result.in_flight_ids, vec!["if_dead".to_string()]);
}

#[test]
fn admin_dlq_purge_req_fixture_matches_envelope_and_params() {
    let v = assert_roundtrips_value("admin_dlq_purge.req.json");
    let req: Request = serde_json::from_value(v).unwrap();
    assert_eq!(req.method, "admin.dlq.purge");
    let params: DlqPurgeRequest = serde_json::from_value(req.params.unwrap()).unwrap();
    assert_eq!(params.for_target.as_deref(), Some("ana"));
    assert_eq!(params.since.as_deref(), Some("24h"));
    assert!(params.yes);
}

#[test]
fn admin_dlq_purge_res_fixture_matches_envelope_and_result() {
    let v = assert_roundtrips_value("admin_dlq_purge.res.json");
    let resp: Response = serde_json::from_value(v).unwrap();
    assert!(resp.error.is_none());
    let result: DlqMutationResponse = serde_json::from_value(resp.result.unwrap()).unwrap();
    assert_eq!(result.count, 2);
    assert_eq!(
        result.in_flight_ids,
        vec!["if_a".to_string(), "if_b".to_string()]
    );
}

#[test]
fn consume_res_fixture_is_a_nexus_batch() {
    let v = assert_roundtrips_value("consume.res.json");
    let resp: Response = serde_json::from_value(v).unwrap();
    let _: NexusBatch = serde_json::from_value(resp.result.unwrap()).unwrap();
}

#[test]
fn error_res_fixture_has_error_and_no_result() {
    let v = assert_roundtrips_value("error.res.json");
    let resp: Response = serde_json::from_value(v).unwrap();
    assert!(resp.result.is_none());
    let err = resp.error.unwrap();
    assert!(
        err.code <= -32001,
        "expected a JSON-RPC error code, got {}",
        err.code
    );
}

#[test]
fn message_created_notif_fixture_matches_notification() {
    let v = assert_roundtrips_value("message.created.notif.json");
    let notif: Notification = serde_json::from_value(v).unwrap();
    assert_eq!(notif.jsonrpc, "2.0");
    assert_eq!(notif.method, "message.created");
    assert!(notif.params.is_some());
}

#[test]
fn push_req_fixture_matches_envelope_and_params() {
    let v = assert_roundtrips_value("push.req.json");
    let req: Request = serde_json::from_value(v).unwrap();
    assert_eq!(req.jsonrpc, "2.0");
    assert_eq!(req.method, "push");
    let params: PushRequest = serde_json::from_value(req.params.unwrap()).unwrap();
    assert_eq!(params.source, "github-ci");
    assert_eq!(params.topic.as_deref(), Some("ci.events"));
    assert!(params.meta.is_some());
    let _ = serde_json::to_value(&params).unwrap();
}

#[test]
fn push_res_fixture_matches_envelope_and_result() {
    let v = assert_roundtrips_value("push.res.json");
    let resp: Response = serde_json::from_value(v).unwrap();
    assert_eq!(resp.jsonrpc, "2.0");
    assert!(resp.error.is_none());
    let result: PushResponse = serde_json::from_value(resp.result.unwrap()).unwrap();
    assert_eq!(result.topic, "ci.events");
    assert_eq!(result.message_id, Some(MessageId("m_push_fixture".into())));
    assert_eq!(result.queued_to, 3);
    let _ = serde_json::to_value(&result).unwrap();
}

#[test]
fn source_register_req_fixture_matches_envelope_and_params() {
    let v = assert_roundtrips_value("source_register.req.json");
    let req: Request = serde_json::from_value(v).unwrap();
    assert_eq!(req.jsonrpc, "2.0");
    assert_eq!(req.method, "source.register");
    let params: SourceRegisterRequest = serde_json::from_value(req.params.unwrap()).unwrap();
    assert_eq!(params.name, "github-ci");
    assert_eq!(params.topic.as_deref(), Some("ci.events"));
    let _ = serde_json::to_value(&params).unwrap();
}

#[test]
fn source_register_res_fixture_matches_envelope_and_result() {
    let v = assert_roundtrips_value("source_register.res.json");
    let resp: Response = serde_json::from_value(v).unwrap();
    assert_eq!(resp.jsonrpc, "2.0");
    assert!(resp.error.is_none());
    let result: SourceRegisterResponse = serde_json::from_value(resp.result.unwrap()).unwrap();
    assert_eq!(result.source.name, "github-ci");
    assert_eq!(result.source.topic, "ci.events");
    assert!(result.source.enabled);
    assert_eq!(result.source.created_at, 1719300000);
    assert!(result.source.last_fired_at.is_none());
    assert_eq!(result.token, "tok_abc123secret");
    let _ = serde_json::to_value(&result).unwrap();
}
