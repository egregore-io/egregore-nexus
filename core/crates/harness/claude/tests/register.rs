use nexus_contracts::HarnessId;

#[test]
fn register_installs_claude_factory() {
    let mut registry = nexus_agent::AdapterRegistry::new();
    assert!(!registry.has(&HarnessId::new("claude").unwrap()));

    nexus_harness_claude::register(&mut registry);

    assert!(registry.has(&HarnessId::new("claude").unwrap()));
}
