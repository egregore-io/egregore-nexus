use nexus_store::repos::{
    InitialPromptDeliveries, InitialPromptInsert, InitialPromptInsertOutcome,
};
use nexus_store::Store;

async fn migrated() -> Store {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    store
}

fn insert(runtime_id: &str, rendered_prompt: &str) -> InitialPromptInsert {
    InitialPromptInsert {
        runtime_id: runtime_id.to_string(),
        agent_id: "a_s_ada".to_string(),
        session_id: "s_ada".to_string(),
        harness: "codex".to_string(),
        template: "Hello <var.name>".to_string(),
        rendered_prompt: rendered_prompt.to_string(),
        client_message_id: format!("initial-prompt:{runtime_id}"),
        created_at_ms: 1234,
    }
}

#[tokio::test]
async fn insert_pending_once_is_idempotent_for_same_prompt() {
    let store = migrated().await;
    let repo = InitialPromptDeliveries::new(&store);

    let first = repo
        .insert_pending_once(insert("r_1", "Hello ada"))
        .await
        .unwrap();
    let second = repo
        .insert_pending_once(insert("r_1", "Hello ada"))
        .await
        .unwrap();

    assert_eq!(first, InitialPromptInsertOutcome::Pending);
    assert_eq!(second, InitialPromptInsertOutcome::AlreadyPending);
}

#[tokio::test]
async fn conflicting_prompt_for_same_runtime_fails_closed() {
    let store = migrated().await;
    let repo = InitialPromptDeliveries::new(&store);

    repo.insert_pending_once(insert("r_1", "Hello ada"))
        .await
        .unwrap();
    let err = repo
        .insert_pending_once(insert("r_1", "Different prompt"))
        .await
        .unwrap_err();

    assert!(err.to_string().contains("conflicting initial prompt"));
}

#[tokio::test]
async fn failed_row_is_not_retried_for_same_runtime() {
    let store = migrated().await;
    let repo = InitialPromptDeliveries::new(&store);

    repo.insert_pending_once(insert("r_1", "Hello ada"))
        .await
        .unwrap();
    repo.mark_failed("r_1", "transport rejected", 1300)
        .await
        .unwrap();
    let outcome = repo
        .insert_pending_once(insert("r_1", "Hello ada"))
        .await
        .unwrap();

    assert_eq!(outcome, InitialPromptInsertOutcome::AlreadyFailed);
}

#[tokio::test]
async fn pending_sessions_returns_only_pending_rows() {
    let store = migrated().await;
    let repo = InitialPromptDeliveries::new(&store);

    repo.insert_pending_once(insert("r_pending", "Hello ada"))
        .await
        .unwrap();
    repo.insert_pending_once(insert("r_accepted", "Hello ada"))
        .await
        .unwrap();
    repo.mark_accepted("r_accepted", 1300).await.unwrap();
    repo.insert_pending_once(insert("r_failed", "Hello ada"))
        .await
        .unwrap();
    repo.mark_failed("r_failed", "transport rejected", 1300)
        .await
        .unwrap();

    let pending = repo
        .pending_sessions(&["s_ada".to_string(), "s_missing".to_string()])
        .await
        .unwrap();

    assert_eq!(pending, vec!["s_ada".to_string()]);
}
