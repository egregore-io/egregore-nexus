//! Wire-size and compatibility gate for optional contract fields.

use std::collections::VecDeque;
use std::fmt::Debug;
use std::fs;
use std::path::Path;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};

fn assert_absent_round_trip<T>(wire: Value)
where
    T: DeserializeOwned + Serialize + PartialEq + Debug,
{
    let decoded: T = serde_json::from_value(wire.clone()).expect("decode omitted optional fields");
    let encoded = serde_json::to_value(&decoded).expect("encode omitted optional fields");
    assert_eq!(
        encoded, wire,
        "None fields must remain absent, not become null"
    );
    let decoded_again: T = serde_json::from_value(encoded).expect("decode compact wire again");
    assert_eq!(decoded_again, decoded);
}

#[test]
fn queue_evidence_fields_are_strict_optional_wire_values() {
    use nexus_contracts::CommandQueueEntry;
    let wire = json!({"commandId":"cmd", "text":"retained", "state":"failed", "mode":"queue", "revision":2, "seq":3, "createdAt":1, "errorCode":-32602, "correlationOwned":true});
    assert_absent_round_trip::<CommandQueueEntry>(wire.clone());
    for invalid in [
        json!("-32602"),
        json!(true),
        json!(2147483648_i64),
        json!(-2147483649_i64),
    ] {
        let mut bad = wire.clone();
        bad["errorCode"] = invalid;
        assert!(serde_json::from_value::<CommandQueueEntry>(bad).is_err());
    }
    for invalid in [json!("true"), json!(1)] {
        let mut bad = wire.clone();
        bad["correlationOwned"] = invalid;
        assert!(serde_json::from_value::<CommandQueueEntry>(bad).is_err());
    }
}

#[test]
fn representative_contract_modules_round_trip_with_absent_optionals() {
    use nexus_contracts::admin::SpawnRequest;
    use nexus_contracts::agents::AgentListRequest;
    use nexus_contracts::batch::BatchMessage;
    use nexus_contracts::events::ToolCallData;
    use nexus_contracts::message::Message;
    use nexus_contracts::notify::NotifyRequest;
    use nexus_contracts::ports::OperatorAction;
    use nexus_contracts::project::Project;
    use nexus_contracts::prompt::CommandQueueEntry;
    use nexus_contracts::register::RegisterRequest;
    use nexus_contracts::search::HistoryRequest;
    use nexus_contracts::send::SendRequest;
    use nexus_contracts::source::SourceRegisterRequest;
    use nexus_contracts::threads::ThreadSummary;
    use nexus_contracts::topics::SubscribeRequest;

    assert_absent_round_trip::<SpawnRequest>(json!({
        "kind": "codex", "harnessArgs": [], "headless": false
    }));
    assert_absent_round_trip::<AgentListRequest>(json!({}));
    assert_absent_round_trip::<BatchMessage>(json!({
        "id": "m_1", "from": "ana", "kind": "agent", "scope": "dm", "body": "hello",
        "truncated": false
    }));
    assert_absent_round_trip::<ToolCallData>(json!({"id": "tool_1", "tool": "read"}));
    assert_absent_round_trip::<Message>(json!({
        "id": "m_1",
        "project": "default",
        "from": "ana",
        "scope": "dm",
        "body": "hello",
        "provenance": {"from": "ana", "kind": "agent", "locality": "local"},
        "createdAt": 1
    }));
    assert_absent_round_trip::<NotifyRequest>(json!({"source": "ci", "payload": {"ok": true}}));
    assert_absent_round_trip::<OperatorAction>(json!({
        "harness": "codex", "session": "s_1", "reason": "login", "source": "adapter"
    }));
    assert_absent_round_trip::<Project>(json!({
        "projectId": "p_1", "name": "default", "createdBy": "operator", "createdAt": 1
    }));
    assert_absent_round_trip::<CommandQueueEntry>(json!({
        "commandId": "cmd_1",
        "text": "continue",
        "state": "queued",
        "mode": "queue",
        "revision": 1,
        "seq": 1,
        "createdAt": 1
    }));
    assert_absent_round_trip::<RegisterRequest>(json!({
        "harness": "codex",
        "harnessSessionId": "native_1",
        "project": "default",
        "clientKey": "client_1",
        "tier": "agent",
        "locality": "local"
    }));
    assert_absent_round_trip::<HistoryRequest>(json!({}));
    assert_absent_round_trip::<SendRequest>(json!({
        "to": {"verb": "reply"}, "body": "hello", "mention": []
    }));
    assert_absent_round_trip::<SourceRegisterRequest>(json!({"name": "ci"}));
    assert_absent_round_trip::<ThreadSummary>(json!({"name": "release", "members": []}));
    assert_absent_round_trip::<SubscribeRequest>(json!({"topic": "builds"}));
}

#[test]
fn every_serializable_struct_option_omits_none() {
    let source_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut violations = Vec::new();
    for entry in fs::read_dir(source_dir).expect("read contract sources") {
        let path = entry.expect("source entry").path();
        if path.extension().and_then(|value| value.to_str()) != Some("rs") {
            continue;
        }
        let source = fs::read_to_string(&path).expect("read contract source");
        let mut next_serializable = false;
        let mut in_serializable_struct = false;
        let mut recent = VecDeque::<String>::with_capacity(6);
        for (index, line) in source.lines().enumerate() {
            if line.starts_with("#[derive(") {
                next_serializable = line.contains("Serialize");
            }
            if line.starts_with("pub struct ") {
                in_serializable_struct = next_serializable;
                next_serializable = false;
                recent.clear();
            } else if line.starts_with("pub enum ") || line.starts_with("pub type ") {
                next_serializable = false;
            }
            if in_serializable_struct
                && line.trim_start().starts_with("pub ")
                && line.contains(": Option<")
                && !recent
                    .iter()
                    .any(|prior| prior.contains("skip_serializing_if = \"Option::is_none\""))
            {
                violations.push(format!("{}:{}: {}", path.display(), index + 1, line.trim()));
            }
            if recent.len() == 6 {
                recent.pop_front();
            }
            recent.push_back(line.to_string());
            if line == "}" {
                in_serializable_struct = false;
                recent.clear();
            }
        }
    }
    assert!(
        violations.is_empty(),
        "serializable Option fields must omit None:\n{}",
        violations.join("\n")
    );
}

#[test]
fn representative_dm_and_thread_frames_are_smaller_and_accept_legacy_nulls() {
    use nexus_contracts::enums::{Kind, Scope};
    use nexus_contracts::ids::{MessageId, ProjectId, ThreadId};
    use nexus_contracts::message::{Message, Provenance};

    let message = |scope, thread: Option<ThreadId>, provenance_thread: Option<String>| Message {
        id: MessageId::from("m_size"),
        project: ProjectId::from("default"),
        from: "ana".into(),
        scope,
        thread,
        topic: None,
        body: "hello".into(),
        summary: None,
        provenance: Provenance {
            from: "ana".into(),
            kind: Kind::Agent,
            locality: Default::default(),
            access: None,
            thread: provenance_thread,
            topic: None,
            stamp: None,
        },
        created_at: 1,
    };

    for (label, compact) in [
        (
            "dm",
            serde_json::to_value(message(Scope::Dm, None, None)).unwrap(),
        ),
        (
            "thread",
            serde_json::to_value(message(
                Scope::Thread,
                Some(ThreadId::from("t_release")),
                Some("release".into()),
            ))
            .unwrap(),
        ),
    ] {
        let mut legacy = compact.clone();
        let object = legacy.as_object_mut().unwrap();
        object.entry("thread").or_insert(Value::Null);
        object.entry("topic").or_insert(Value::Null);
        object.entry("summary").or_insert(Value::Null);
        let provenance = object
            .get_mut("provenance")
            .and_then(Value::as_object_mut)
            .unwrap();
        provenance.entry("thread").or_insert(Value::Null);
        provenance.entry("topic").or_insert(Value::Null);
        provenance.entry("stamp").or_insert(Value::Null);

        let compact_bytes = serde_json::to_vec(&compact).unwrap().len();
        let legacy_bytes = serde_json::to_vec(&legacy).unwrap().len();
        println!("{label}: explicit-null={legacy_bytes} compact={compact_bytes}");
        assert!(compact_bytes < legacy_bytes);
        let compact_decoded: Message = serde_json::from_value(compact).unwrap();
        let legacy_decoded: Message = serde_json::from_value(legacy).unwrap();
        assert_eq!(
            compact_decoded, legacy_decoded,
            "absent must decode like null"
        );
    }
}
