use nexus_contracts::AgentId;
use nexus_store::repos::AgentGroups;
use nexus_store::Store;

#[tokio::test]
async fn agent_groups_assign_and_share_membership() {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    let groups = AgentGroups::new(&store);

    let blake = AgentId("a_blake".into());
    let bianca = AgentId("a_bianca".into());
    let roman = AgentId("a_roman".into());

    groups
        .assign("default", "backend", &blake, "blake")
        .await
        .unwrap();
    groups
        .assign("default", "backend", &bianca, "bianca")
        .await
        .unwrap();
    groups
        .assign("default", "docs", &roman, "roman")
        .await
        .unwrap();

    assert!(groups
        .is_member("default", "backend", &blake)
        .await
        .unwrap());
    assert_eq!(
        groups.groups_for_agent("default", &blake).await.unwrap(),
        vec!["backend".to_string()]
    );
    assert!(groups
        .share_group("default", &blake, &bianca)
        .await
        .unwrap());
    assert!(!groups.share_group("default", &blake, &roman).await.unwrap());
}
