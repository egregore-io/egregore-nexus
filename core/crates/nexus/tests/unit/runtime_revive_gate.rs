use super::RuntimeReviveGate;
use nexus_contracts::SessionId;
use std::time::Duration;

#[tokio::test]
async fn same_runtime_revives_are_serialized() {
    let gate = RuntimeReviveGate::default();
    let session = SessionId("s_same_runtime".into());
    let first = gate.acquire(&session).await;

    let waiting_gate = gate.clone();
    let waiting_session = session.clone();
    let waiter = tokio::spawn(async move {
        let _guard = waiting_gate.acquire(&waiting_session).await;
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(50), waiter)
            .await
            .is_err(),
        "a second resurrection for the same runtime must wait"
    );
    drop(first);

    tokio::time::timeout(Duration::from_secs(1), gate.acquire(&session))
        .await
        .expect("the next resurrection should proceed after the owner exits");
}

#[tokio::test]
async fn different_runtime_revives_do_not_block_each_other() {
    let gate = RuntimeReviveGate::default();
    let _first = gate.acquire(&SessionId("s_runtime_a".into())).await;

    tokio::time::timeout(
        Duration::from_millis(50),
        gate.acquire(&SessionId("s_runtime_b".into())),
    )
    .await
    .expect("independent runtimes must revive concurrently");
}
