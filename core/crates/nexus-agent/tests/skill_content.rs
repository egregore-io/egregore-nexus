use nexus_agent::adapter::skill::SKILL_MD;

#[test]
fn skill_documents_hook_bootstrap_and_manual_self_registration() {
    assert!(SKILL_MD.contains("SessionStart"));
    assert!(SKILL_MD.contains("nexus register"));
    assert!(SKILL_MD.contains("NEXUS_NAME"));
    assert!(SKILL_MD.contains("NEXUS_CLIENT_KEY"));
    assert!(SKILL_MD.contains("idempotent"));
}
