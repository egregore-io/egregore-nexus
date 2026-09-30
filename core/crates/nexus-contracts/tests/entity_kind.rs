use nexus_contracts::{entity_kind, Caller, Kind, Locality, MemberSummary, Tier};
use serde_json::json;

#[test]
fn dotted_entity_kind_round_trips_every_supported_locality_and_nature() {
    for locality in [Locality::Local, Locality::External, Locality::Trusted] {
        for kind in [Kind::Human, Kind::Agent] {
            let dotted = entity_kind::dotted(locality, kind);
            assert_eq!(entity_kind::parse(&dotted), Some((locality, kind)));
        }
    }

    assert_eq!(
        entity_kind::parse("human"),
        Some((Locality::Local, Kind::Human))
    );
    assert_eq!(
        entity_kind::parse("agent"),
        Some((Locality::Local, Kind::Agent))
    );
}

#[test]
fn dotted_entity_kind_refuses_unknown_or_empty_tokens() {
    for invalid in ["remote.human", "external.bot", ""] {
        assert_eq!(entity_kind::parse(invalid), None, "accepted {invalid:?}");
    }
}

#[test]
fn legacy_member_summary_defaults_to_local_without_rewriting_kind() {
    let member: MemberSummary = serde_json::from_value(json!({
        "agentId": "a_legacy",
        "name": "legacy",
        "sessionId": "s_legacy",
        "agent": "codex",
        "presence": "online"
    }))
    .expect("legacy member summary remains readable");

    assert_eq!(member.locality, Locality::Local);
    assert_eq!(member.access, None);
}

#[test]
fn legacy_caller_defaults_new_identity_facets() {
    let caller: Caller = serde_json::from_value(json!({
        "agentId": "a_legacy",
        "session": "s_legacy",
        "name": "legacy",
        "project": "metadata-only",
        "tier": "agent"
    }))
    .expect("legacy caller remains readable");

    assert_eq!(caller.locality, Locality::Local);
    assert_eq!(caller.access, None);
    assert_eq!(caller.principal_id, None);
    assert_eq!(caller.tier, Tier::Agent);
}
