use nexus::harness_registry::harness_registry_by_id;
use nexus_contracts::HarnessId;

#[test]
fn every_builtin_token_resolves_through_the_id_map() {
    for token in ["claude", "codex", "opencode", "hermes", "pi", "other"] {
        let id = HarnessId::new(token).expect("builtin token is a valid harness id");
        let resolved = harness_registry_by_id(&id);
        assert_eq!(
            resolved.agent_token(),
            token,
            "id-based lookup returns wrong contract"
        );
    }
}

#[test]
fn unknown_id_falls_back_to_generic_non_headed_contract() {
    let id = HarnessId::new("acme-agent").expect("valid token");
    let contract = harness_registry_by_id(&id);
    assert_eq!(contract.agent_token(), "other");
    assert!(
        contract.program().is_empty(),
        "fallback must stay non-headed"
    );
}
