use nexus_common::NexusError;
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
async fn project_labels_do_not_gate_stable_entity_metadata() {
    let store = migrated().await;
    seed_entities(&store, "nexus").await;
    let repo = Metadata::new(&store);

    repo.set(
        "other",
        MetadataEntity::Message,
        "m_1",
        &serde_json::json!({ "wrong": true }),
    )
    .await
    .unwrap();

    assert_eq!(
        repo.get("nexus", MetadataEntity::Message, "m_1")
            .await
            .unwrap()
            .metadata,
        serde_json::json!({ "wrong": true })
    );
}

#[tokio::test]
async fn agent_metadata_get_prefers_an_exact_id_over_a_colliding_alias() {
    let store = migrated().await;
    seed_agent_collision(&store).await;

    let row = Metadata::new(&store)
        .get("ignored-project", MetadataEntity::Agent, "a_collision")
        .await
        .unwrap();

    assert_eq!(row.metadata, serde_json::json!({"identity": "exact-id"}));
}

#[tokio::test]
async fn agent_metadata_set_updates_only_the_exact_id_over_a_colliding_alias() {
    let store = migrated().await;
    seed_agent_collision(&store).await;

    Metadata::new(&store)
        .set(
            "ignored-project",
            MetadataEntity::Agent,
            "a_collision",
            &serde_json::json!({"updated": "exact-id"}),
        )
        .await
        .unwrap();

    assert_eq!(
        agent_metadata_by_id(&store, "a_collision").await,
        serde_json::json!({"updated": "exact-id"})
    );
    assert_eq!(
        agent_metadata_by_id(&store, "a_alias_owner").await,
        serde_json::json!({"identity": "alias"}),
        "the mutable alias owner must not be updated"
    );
}

#[tokio::test]
async fn agent_metadata_get_rejects_duplicate_global_names() {
    let store = migrated().await;
    seed_duplicate_agent_names(&store).await;

    let error = Metadata::new(&store)
        .get("ignored-project", MetadataEntity::Agent, "duplicate")
        .await
        .unwrap_err();

    assert!(matches!(error, NexusError::Ambiguous(_)));
}

#[tokio::test]
async fn agent_metadata_set_rejects_duplicate_global_names_without_mutation() {
    let store = migrated().await;
    seed_duplicate_agent_names(&store).await;

    let error = Metadata::new(&store)
        .set(
            "ignored-project",
            MetadataEntity::Agent,
            "duplicate",
            &serde_json::json!({"updated": true}),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, NexusError::Ambiguous(_)));
    assert_eq!(
        agent_metadata_by_id(&store, "a_duplicate_one").await,
        serde_json::json!({"identity": "one"})
    );
    assert_eq!(
        agent_metadata_by_id(&store, "a_duplicate_two").await,
        serde_json::json!({"identity": "two"})
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

async fn seed_agent_collision(store: &Store) {
    store
        .identity_conn()
        .execute(
            "INSERT INTO agents (agent_id, name, project, tier, metadata_json, created_at) VALUES \
             ('a_alias_owner', 'a_collision', 'two', 'agent', '{\"identity\":\"alias\"}', 1), \
             ('a_collision', 'id-owner', 'one', 'agent', '{\"identity\":\"exact-id\"}', 2)",
            (),
        )
        .await
        .unwrap();
}

async fn seed_duplicate_agent_names(store: &Store) {
    store
        .identity_conn()
        .execute("DROP INDEX idx_agents_name_unique", ())
        .await
        .unwrap();
    store
        .identity_conn()
        .execute(
            "INSERT INTO agents (agent_id, name, project, tier, metadata_json, created_at) VALUES \
             ('a_duplicate_one', 'duplicate', 'one', 'agent', '{\"identity\":\"one\"}', 1), \
             ('a_duplicate_two', 'duplicate', 'two', 'agent', '{\"identity\":\"two\"}', 2)",
            (),
        )
        .await
        .unwrap();
}

async fn agent_metadata_by_id(store: &Store, agent_id: &str) -> serde_json::Value {
    let mut rows = store
        .identity_conn()
        .query(
            "SELECT metadata_json FROM agents WHERE agent_id = ?1",
            libsql::params![agent_id],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    serde_json::from_str(&row.get::<String>(0).unwrap()).unwrap()
}
