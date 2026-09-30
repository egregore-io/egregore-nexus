//! Characterization of durable agent identities without a default harness.

use nexus_store::repos::{Agents, NewAgent};
use nexus_store::Store;

#[tokio::test]
async fn accepts_agent_identity_without_a_default_harness() {
    let store = Store::open(":memory:").await.expect("open store");
    store.migrate().await.expect("migrate store");

    let agents = Agents::new(&store);
    let agent_id = agents
        .create(NewAgent {
            agent_id: "a_harnessless".into(),
            project: "default".into(),
            name: Some("harnessless".into()),
            default_harness: None,
            role: None,
            tier: Some("agent".into()),
            owner: None,
        })
        .await
        .expect("create harness-less identity");

    assert_eq!(agent_id, "a_harnessless");
    let stored = agents
        .find_by_id(&agent_id)
        .await
        .expect("read identity")
        .expect("stored identity");
    assert_eq!(stored.default_harness, None);
}
