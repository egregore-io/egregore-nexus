//! Dead-marking is durable agent lifecycle state, separate from presence.

use nexus_store::repos::Agents;
use nexus_store::Store;

async fn store_with_agent(agent_id: &str) -> Store {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    store
        .conn
        .execute(
            "INSERT INTO agents (agent_id, name, project, created_at) VALUES (?1, 'zed', 'default', 1)",
            libsql::params![agent_id],
        )
        .await
        .unwrap();
    store
}

#[tokio::test]
async fn mark_dead_round_trips_and_unknown_ids_read_active() {
    let store = store_with_agent("a_s_dead1").await;
    let agents = Agents::new(&store);

    assert_eq!(
        agents.lifecycle_for_id("a_s_dead1").await.unwrap(),
        (None, None)
    );
    assert!(agents
        .mark_dead("a_s_dead1", "revive_exhausted")
        .await
        .unwrap());
    assert_eq!(
        agents.lifecycle_for_id("a_s_dead1").await.unwrap(),
        (Some("dead".into()), Some("revive_exhausted".into()))
    );
    // Unknown id: active/unknown, not an error.
    assert_eq!(
        agents.lifecycle_for_id("a_missing").await.unwrap(),
        (None, None)
    );
    // Marking a missing row reports false.
    assert!(!agents.mark_dead("a_missing", "no_resume_id").await.unwrap());
}
