use std::path::PathBuf;

use nexus_contracts::ids::{MessageId, ProjectId, SessionId};
use nexus_contracts::message::{Message, Provenance};
use nexus_contracts::{DeliveryTiming, Kind, Scope};
use nexus_store::repos::{Inbox, Messages};
use nexus_store::Store;

fn message(id: &str) -> Message {
    Message {
        id: MessageId(id.into()),
        project: ProjectId("default".into()),
        from: "sender".into(),
        scope: Scope::Dm,
        thread: None,
        topic: None,
        body: "timed delivery".into(),
        summary: None,
        provenance: Provenance {
            from: "sender".into(),
            kind: Kind::Agent,
            thread: None,
            topic: None,
            stamp: None,
        },
        created_at: 1,
    }
}

async fn seed_live_session(store: &Store, session: &SessionId) {
    store
        .conn
        .execute(
            "INSERT INTO sessions (session_id, name, kind, project, presence, tier, last_heartbeat) \
             VALUES (?1, 'recipient', 'agent', 'default', 'online', 'agent', 1)",
            libsql::params![session.0.clone()],
        )
        .await
        .unwrap();
}

fn temp_store_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "nexus-delivery-timing-{}-{}.db",
        std::process::id(),
        nexus_common::now()
    ))
}

#[tokio::test]
async fn unsettled_rows_retain_each_explicit_timing() {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    let session = SessionId("s_timed".into());
    seed_live_session(&store, &session).await;

    for (index, timing) in [
        DeliveryTiming::Interrupt,
        DeliveryTiming::YieldTurn,
        DeliveryTiming::AfterToolLoop,
    ]
    .into_iter()
    .enumerate()
    {
        let id = MessageId(format!("m_timed_{index}"));
        Messages::new(&store).insert(&message(&id.0)).await.unwrap();
        Inbox::new(&store)
            .enqueue_with_timing(&id, &session, timing)
            .await
            .unwrap();
    }

    let rows = Inbox::new(&store)
        .pending_timed_for(&session, "default", 10)
        .await
        .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.timing).collect::<Vec<_>>(),
        vec![
            DeliveryTiming::Interrupt,
            DeliveryTiming::YieldTurn,
            DeliveryTiming::AfterToolLoop,
        ]
    );
}

#[tokio::test]
async fn delivery_timing_survives_store_reopen_without_a_second_row() {
    let path = temp_store_path();
    let path_string = path.to_string_lossy().into_owned();
    let session = SessionId("s_restart".into());
    let id = MessageId("m_restart_timing".into());

    {
        let store = Store::open(&path_string).await.unwrap();
        store.migrate().await.unwrap();
        seed_live_session(&store, &session).await;
        Messages::new(&store).insert(&message(&id.0)).await.unwrap();
        Inbox::new(&store)
            .enqueue_with_timing(&id, &session, DeliveryTiming::AfterToolLoop)
            .await
            .unwrap();
    }

    {
        let store = Store::open(&path_string).await.unwrap();
        store.migrate().await.unwrap();
        let inbox = Inbox::new(&store);
        inbox
            .enqueue_with_timing(&id, &session, DeliveryTiming::Interrupt)
            .await
            .unwrap();
        let rows = inbox
            .pending_timed_for(&session, "default", 10)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "idempotent re-enqueue must not duplicate");
        assert_eq!(rows[0].timing, DeliveryTiming::AfterToolLoop);
    }

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{path_string}-wal"));
    let _ = std::fs::remove_file(format!("{path_string}-shm"));
}
