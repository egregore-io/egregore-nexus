//! `agent.spawned` carries the durable identity (identity-by-id slice 1).

use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::SessionId;

#[test]
fn agent_spawned_serializes_agent_id() {
    let event = WsEvent::AgentSpawned {
        session_id: SessionId("s_1".into()),
        name: Some("ada".into()),
        agent_id: Some("a_1".into()),
    };
    let json = serde_json::to_value(&event).unwrap();
    assert_eq!(json["type"], "agent.spawned");
    assert_eq!(json["agentId"], "a_1");
    assert_eq!(json["name"], "ada");

    let back: WsEvent = serde_json::from_value(json).unwrap();
    assert!(matches!(
        back,
        WsEvent::AgentSpawned { agent_id: Some(id), .. } if id == "a_1"
    ));
}
