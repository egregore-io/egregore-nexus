use nexus_store::repos::{Metadata, MetadataEntity};
use nexus_store::Store;

async fn migrated() -> Store {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    store
}

#[tokio::test]
async fn metadata_roundtrips_for_core_entities() {
    let store = migrated().await;
    seed_entities(&store, "nexus").await;
    let repo = Metadata::new(&store);

    for (entity, id) in [
        (MetadataEntity::Message, "m_1"),
        (MetadataEntity::Session, "s_1"),
        (MetadataEntity::Thread, "design"),
        (MetadataEntity::Agent, "bianca"),
    ] {
        assert_eq!(
            repo.get("nexus", entity, id).await.unwrap().metadata,
            serde_json::json!({})
        );
        let updated = repo
            .set(
                "nexus",
                entity,
                id,
                &serde_json::json!({ "owner": "qa", "rank": 1 }),
            )
            .await
            .unwrap();
        assert_eq!(updated.entity, entity);
        assert_eq!(updated.id, id);
        assert_eq!(
            repo.get("nexus", entity, id).await.unwrap().metadata,
            serde_json::json!({ "owner": "qa", "rank": 1 })
        );
    }
}

#[tokio::test]
async fn metadata_is_project_scoped() {
    let store = migrated().await;
    seed_entities(&store, "nexus").await;
    let repo = Metadata::new(&store);

    let err = repo
        .set(
            "other",
            MetadataEntity::Message,
            "m_1",
            &serde_json::json!({ "wrong": true }),
        )
        .await
        .unwrap_err();

    assert!(err.to_string().contains("message:m_1"));
    assert_eq!(
        repo.get("nexus", MetadataEntity::Message, "m_1")
            .await
            .unwrap()
            .metadata,
        serde_json::json!({})
    );
}

#[tokio::test]
async fn hook_metadata_merge_is_recursive_global_and_provenance_idempotent() {
    let store = migrated().await;
    seed_entities(&store, "nexus").await;
    let repo = Metadata::new(&store);
    repo.set(
        "nexus",
        MetadataEntity::Message,
        "m_1",
        &serde_json::json!({
            "nested": {"before": true},
            "_nexus": {"hooks": {"executedBy": [
                {"invocationId": "hi_before", "hookId": "before"}
            ]}}
        }),
    )
    .await
    .unwrap();

    let patch = serde_json::json!({
        "nested": {"after": true},
        "indexed": true,
        "_nexus": {"hooks": {"executedBy": [
            {"invocationId": "hi_after", "hookId": "after"}
        ]}}
    });
    let first = repo
        .merge_message_hook_metadata("m_1", "hr_m_1", &patch)
        .await
        .unwrap();
    let replay = repo
        .merge_message_hook_metadata("m_1", "hr_m_1", &patch)
        .await
        .unwrap();

    assert_eq!(first, replay);
    assert_eq!(
        replay["nested"],
        serde_json::json!({"before": true, "after": true})
    );
    assert_eq!(replay["indexed"], true);
    assert_eq!(
        replay["_nexus"]["hooks"]["executedBy"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

async fn seed_entities(store: &Store, project: &str) {
    store
        .conn
        .execute(
            "INSERT INTO messages (message_id, from_name, kind, project, created_at) \
             VALUES ('m_1', 'bianca', 'dm', ?1, 1)",
            libsql::params![project],
        )
        .await
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO sessions (session_id, name, project, created_at) \
             VALUES ('s_1', 'bianca', ?1, 1)",
            libsql::params![project],
        )
        .await
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO threads (thread_id, name, project, created_at) \
             VALUES ('t_1', 'design', ?1, 1)",
            libsql::params![project],
        )
        .await
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO agents (agent_id, name, project, tier, created_at) \
             VALUES ('a_1', 'bianca', ?1, 'agent', 1)",
            libsql::params![project],
        )
        .await
        .unwrap();
}
