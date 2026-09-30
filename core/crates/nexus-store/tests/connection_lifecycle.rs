use nexus_store::Store;

#[tokio::test]
async fn repeated_store_lifecycles_leave_the_embedded_runtime_usable() {
    for cycle in 0..32 {
        let store = Store::open(":memory:")
            .await
            .unwrap_or_else(|error| panic!("store open failed on cycle {cycle}: {error}"));
        store
            .migrate()
            .await
            .unwrap_or_else(|error| panic!("store migration failed on cycle {cycle}: {error}"));

        let mut rows = store
            .conn
            .query("SELECT 1", ())
            .await
            .unwrap_or_else(|error| panic!("store query failed on cycle {cycle}: {error}"));
        assert!(
            rows.next()
                .await
                .unwrap_or_else(|error| panic!("row read failed on cycle {cycle}: {error}"))
                .is_some(),
            "store query returned no row on cycle {cycle}"
        );

        drop(rows);
        drop(store);
    }
}
