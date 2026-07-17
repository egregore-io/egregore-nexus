use nexus_contracts::MessageId;
use nexus_store::repos::Inbox;
use nexus_store::Store;

#[tokio::test]
async fn human_read_marks_only_visible_pending_rows_in_one_daemon_owned_update() {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    store
        .conn
        .execute_batch(
            "INSERT INTO in_flight (in_flight_id, message_id, recipient_session, state) VALUES
             ('f_1', 'm_1', 'human-1', 'pending'),
             ('f_2', 'm_2', 'human-1', 'notified'),
             ('f_3', 'm_3', 'human-1', 'injecting'),
             ('f_4', 'm_1', 'human-2', 'pending');",
        )
        .await
        .unwrap();

    let changed = Inbox::new(&store)
        .mark_human_read_delivered(
            "human-1",
            &[
                MessageId("m_1".into()),
                MessageId("m_2".into()),
                MessageId("m_1".into()),
            ],
            9_000,
        )
        .await
        .unwrap();
    assert_eq!(changed, 2);

    let mut rows = store
        .conn
        .query(
            "SELECT in_flight_id, state, delivered_at FROM in_flight ORDER BY in_flight_id",
            (),
        )
        .await
        .unwrap();
    let mut states = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        states.push((
            row.get::<String>(0).unwrap(),
            row.get::<String>(1).unwrap(),
            row.get::<Option<i64>>(2).unwrap(),
        ));
    }
    assert_eq!(
        states,
        vec![
            ("f_1".into(), "delivered".into(), Some(9_000)),
            ("f_2".into(), "delivered".into(), Some(9_000)),
            ("f_3".into(), "injecting".into(), None),
            ("f_4".into(), "pending".into(), None),
        ]
    );
}
