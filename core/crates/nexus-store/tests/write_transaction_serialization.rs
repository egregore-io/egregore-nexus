//! Regression coverage for one-store write-transaction serialization.

use std::sync::Arc;
use std::time::Duration;

use nexus_store::repos::{AgentRuntimes, Agents, NewAgent, NewAgentRuntime};
use nexus_store::Store;

#[tokio::test]
async fn runtime_create_waits_for_an_existing_store_write_transaction() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    Agents::new(&store)
        .create(NewAgent {
            agent_id: "a_serialized_runtime".into(),
            project: "default".into(),
            name: Some("serialized-runtime".into()),
            default_harness: Some("codex".into()),
            role: None,
            tier: None,
            owner: None,
        })
        .await
        .unwrap();

    let holder = store
        .begin_write_txn("write_transaction_serialization_holder")
        .await
        .unwrap();
    let runtime_store = store.clone();
    let create = tokio::spawn(async move {
        AgentRuntimes::new(&runtime_store)
            .create(NewAgentRuntime {
                runtime_id: "s_serialized_runtime".into(),
                agent_id: "a_serialized_runtime".into(),
                harness: "codex".into(),
                cwd: None,
                transport: Some("acp".into()),
                presence: Some("online".into()),
                active: true,
            })
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !create.is_finished(),
        "runtime creation must wait behind the store's active write transaction"
    );

    holder.commit().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), create)
        .await
        .expect("runtime creation should resume after the holder commits")
        .expect("runtime creation task should not panic")
        .expect("runtime creation should succeed");
}
