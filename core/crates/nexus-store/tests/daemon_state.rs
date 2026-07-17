use nexus_store::repos::DaemonState;
use nexus_store::Store;

async fn migrated() -> Store {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    store
}

#[tokio::test]
async fn boot_epoch_round_trips_and_replaces_previous_epoch() {
    let store = migrated().await;
    let repo = DaemonState::new(&store);

    assert_eq!(repo.boot_epoch().await.unwrap(), None);

    repo.set_boot_epoch("boot_first", 100).await.unwrap();
    assert_eq!(
        repo.boot_epoch().await.unwrap().as_deref(),
        Some("boot_first")
    );

    repo.set_boot_epoch("boot_second", 200).await.unwrap();
    assert_eq!(
        repo.boot_epoch().await.unwrap().as_deref(),
        Some("boot_second")
    );
}
