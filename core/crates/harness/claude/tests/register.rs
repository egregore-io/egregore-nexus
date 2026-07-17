use nexus_contracts::Harness;

#[test]
fn register_installs_claude_factory() {
    let mut registry = nexus_agent::AdapterRegistry::new();
    assert!(!registry.has(Harness::Claude));

    nexus_harness_claude::register(&mut registry);

    assert!(registry.has(Harness::Claude));
}
