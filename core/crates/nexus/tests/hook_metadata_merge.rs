use std::sync::Arc;

use nexus::daemon::{dispatch, AppState};
use nexus_common::Config;
use nexus_contracts::{Caller, Request, SessionId, Tier, JSONRPC_VERSION};
use nexus_store::Store;

async fn state() -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    store
        .conn
        .execute(
            "INSERT INTO messages
             (message_id, from_name, kind, body, provenance, project, created_at, metadata_json)
             VALUES ('m_hook', 'fixture-sender', 'dm', 'hello', '{}', 'metadata-only', 1, '{}')",
            (),
        )
        .await
        .unwrap();
    AppState::wire(store, &Config::default())
}

fn admin() -> Caller {
    Caller {
        agent_id: None,
        session: SessionId("local-operator".into()),
        name: "Nexus Gateway".into(),
        project: "default".into(),
        tier: Tier::Admin,
        locality: Default::default(),
        access: None,
        principal_id: None,
    }
}

fn request() -> Request {
    Request {
        jsonrpc: JSONRPC_VERSION.into(),
        id: None,
        method: "hook.metadata.merge".into(),
        params: Some(serde_json::json!({
            "messageId": "m_hook",
            "invocationId": "hr_m_hook",
            "metadata": {
                "indexed": true,
                "_nexus": {"hooks": {"executedBy": [
                    {"invocationId": "hi_after", "hookId": "after"}
                ]}}
            }
        })),
    }
}

#[tokio::test]
async fn gateway_hook_metadata_merge_is_global_and_idempotent() {
    let state = state().await;
    for _ in 0..2 {
        let response = dispatch(&state, Some(admin()), request()).await;
        assert!(
            response.error.is_none(),
            "unexpected error: {:?}",
            response.error
        );
    }

    let mut rows = state
        .store
        .conn
        .query(
            "SELECT metadata_json FROM messages WHERE message_id = 'm_hook'",
            (),
        )
        .await
        .unwrap();
    let raw: String = rows.next().await.unwrap().unwrap().get(0).unwrap();
    let metadata: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(metadata["indexed"], true);
    assert_eq!(
        metadata["_nexus"]["hooks"]["executedBy"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn hook_metadata_merge_requires_gateway_admin_authority() {
    let state = state().await;
    let mut caller = admin();
    caller.tier = Tier::Agent;
    let response = dispatch(&state, Some(caller), request()).await;
    assert_eq!(
        response.error.unwrap().code,
        nexus_contracts::codes::UNAUTHORIZED
    );
}
