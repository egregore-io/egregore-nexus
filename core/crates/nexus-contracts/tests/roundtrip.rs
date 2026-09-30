//! Cross-module round-trip: a full delivery flow exercises field-name consistency between modules.

use nexus_contracts::{
    Ack, AckThreadsRequest, BatchCounts, BatchMessage, Harness, Kind, Message, MessageId,
    NexusBatch, ProjectId, Provenance, RegisterRequest, RegisterResponse, Scope, SendRequest,
    SendTarget, SessionId, Tier,
};

#[test]
fn full_flow_uses_consistent_top_level_names() {
    // register
    let reg = RegisterRequest {
        agent_id: None,
        name: Some("ben".into()),
        harness: Harness::Claude,
        harness_session_id: "hs".into(),
        project: "egregore".into(),
        client_key: "ck".into(),
        runtime_credential: None,
        tier: Tier::Agent,
        kind: None,
        role: None,
        cwd: None,
    };
    let reg_json = serde_json::to_string(&reg).unwrap();
    let _: RegisterRequest = serde_json::from_str(&reg_json).unwrap();

    let reg_resp = RegisterResponse {
        agent_id: None,
        session_id: SessionId("s_01".into()),
        directive: "<nexus> = bus".into(),
    };
    assert_eq!(
        serde_json::to_value(&reg_resp).unwrap()["sessionId"],
        "s_01"
    );

    // send
    let send = SendRequest {
        to: SendTarget::Dm {
            name: Some("dylan".into()),
            agent_id: None,
        },
        summary: None,
        body: "hi".into(),
        mention: vec![],
        idempotency_key: None,
    };
    let _: SendRequest = serde_json::from_str(&serde_json::to_string(&send).unwrap()).unwrap();
    let ack = Ack {
        message_id: MessageId("m_01".into()),
        fanout: None,
    };
    assert_eq!(serde_json::to_value(&ack).unwrap()["messageId"], "m_01");

    // delivery batch
    let batch = NexusBatch {
        counts: BatchCounts {
            dms: 1,
            thread: 0,
            total: 1,
        },
        dms: vec![BatchMessage {
            id: MessageId("m_01".into()),
            from: "dylan".into(),
            kind: Kind::Agent,
            scope: Scope::Dm,
            thread: None,
            topic: None,
            body: "hi".into(),
            truncated: false,
        }],
        threads: vec![],
        dm_message_ids: vec![MessageId("m_01".into())],
        thread_message_ids: vec![],
        message_ids: vec![MessageId("m_01".into())],
    };
    let _: NexusBatch = serde_json::from_str(&serde_json::to_string(&batch).unwrap()).unwrap();

    // ack
    let ackt = AckThreadsRequest {
        message_ids: vec![MessageId("m_01".into())],
    };
    assert_eq!(
        serde_json::to_value(&ackt).unwrap()["messageIds"][0],
        "m_01"
    );

    // a stored Message round-trips
    let msg = Message {
        id: MessageId("m_01".into()),
        project: ProjectId("p_01".into()),
        from: "dylan".into(),
        scope: Scope::Dm,
        thread: None,
        topic: None,
        body: "hi".into(),
        summary: None,
        provenance: Provenance {
            from: "dylan".into(),
            kind: Kind::Agent,
            thread: None,
            topic: None,
            stamp: None,
        },
        created_at: 1,
    };
    let _: Message = serde_json::from_str(&serde_json::to_string(&msg).unwrap()).unwrap();
}
