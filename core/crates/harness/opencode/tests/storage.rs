use nexus_harness_opencode::storage::OpenCodeRuntimeStateRepo;
use nexus_store::Store;

#[tokio::test]
async fn opencode_schema_repair_ignores_duplicate_parser_state_column() {
    let store = Store::open(":memory:").await.unwrap();
    let repo = OpenCodeRuntimeStateRepo::new(&store);

    repo.ensure_schema().await.unwrap();
    repo.ensure_schema().await.unwrap();
}

#[tokio::test]
async fn opencode_schema_repair_fails_loud_on_non_duplicate_alter_error() {
    let store = Store::open(":memory:").await.unwrap();
    store
        .conn
        .execute(
            "CREATE TABLE opencode_runtime_state (
                runtime_id TEXT PRIMARY KEY,
                parser_state_json TEXT NOT NULL DEFAULT '{}'
            )",
            (),
        )
        .await
        .unwrap();
    store
        .conn
        .execute("CREATE VIEW blocker AS SELECT 1", ())
        .await
        .unwrap();
    store
        .conn
        .execute("DROP TABLE opencode_runtime_state", ())
        .await
        .unwrap();
    store
        .conn
        .execute(
            "CREATE VIEW opencode_runtime_state AS SELECT 1 AS runtime_id",
            (),
        )
        .await
        .unwrap();

    let err = OpenCodeRuntimeStateRepo::new(&store)
        .ensure_schema()
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("view") || err.to_string().contains("not a table"),
        "unexpected error: {err}"
    );
}
