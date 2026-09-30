use std::sync::Arc;

use nexus_store::Store;
use tokio::sync::Barrier;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transactional_batch_rollback_cannot_erase_concurrent_raw_write() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.conn.execute_batch("CREATE TABLE batch_owner (id INTEGER PRIMARY KEY); INSERT INTO batch_owner VALUES (1); CREATE TABLE raw_owner (id INTEGER);").await.unwrap();
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer_done = done.clone();
    let raw = store.conn.raw();
    let (ready, started) = tokio::sync::oneshot::channel();
    let writer = tokio::spawn(async move {
        let mut insert = raw
            .prepare("INSERT INTO raw_owner VALUES (7)")
            .await
            .unwrap();
        ready.send(()).unwrap();
        let saw_uncommitted_marker =
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    let mut rows = raw
                        .query("SELECT COUNT(*) FROM batch_owner WHERE id = 2", ())
                        .await
                        .unwrap();
                    let visible = rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap() > 0;
                    if visible {
                        break true;
                    }
                    if writer_done.load(std::sync::atomic::Ordering::Acquire) {
                        break false;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("native batch must complete");
        insert.run(()).await.unwrap();
        saw_uncommitted_marker
    });
    started.await.unwrap();
    let sql = format!(
        "INSERT INTO batch_owner VALUES (2);{}INSERT INTO batch_owner VALUES (1)",
        "SELECT 1;".repeat(20_000)
    );
    let error = store
        .conn
        .execute_transactional_batch(&sql)
        .await
        .unwrap_err();
    done.store(true, std::sync::atomic::Ordering::Release);
    assert!(error.to_string().contains("UNIQUE constraint failed"));
    let saw_uncommitted_marker = writer.await.unwrap();
    eprintln!("raw clone observed uncommitted batch marker: {saw_uncommitted_marker}");
    let mut rows = store
        .conn
        .query("SELECT COUNT(*) FROM raw_owner WHERE id = 7", ())
        .await
        .unwrap();
    assert_eq!(rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(), 1,
        "a successful unrelated raw write must not join the batch owner's rollback; observed marker={saw_uncommitted_marker}");
    assert!(
        !saw_uncommitted_marker,
        "raw clones must not enter another batch's transaction"
    );
}

#[tokio::test]
async fn transactional_batch_commit_failure_rolls_back_and_preserves_foreign_owner() {
    let store = Store::open(":memory:").await.unwrap();
    store.conn.execute_batch("PRAGMA foreign_keys = ON; CREATE TABLE parent (id INTEGER PRIMARY KEY); CREATE TABLE child (id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);").await.unwrap();
    let error = store
        .conn
        .execute_transactional_batch("INSERT INTO child VALUES (7)")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("FOREIGN KEY constraint failed"));
    assert!(
        store.conn.raw().is_autocommit(),
        "failed COMMIT must clean up its transaction"
    );
    let mut rows = store
        .conn
        .query("SELECT COUNT(*) FROM child", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        0
    );
    drop(rows);
    store
        .conn
        .execute_transactional_batch("INSERT INTO parent VALUES (7); INSERT INTO child VALUES (7)")
        .await
        .unwrap();
    store
        .conn
        .execute_batch("BEGIN; INSERT INTO parent VALUES (8)")
        .await
        .unwrap();
    assert!(store
        .conn
        .execute_transactional_batch("DELETE FROM parent WHERE id = 8")
        .await
        .is_err());
    assert!(
        !store.conn.raw().is_autocommit(),
        "failed BEGIN must not roll back a foreign transaction"
    );
    let mut rows = store
        .conn
        .query("SELECT COUNT(*) FROM parent WHERE id = 8", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        1
    );
    drop(rows);
    store.conn.execute("ROLLBACK", ()).await.unwrap();
}

/// Each lane uses the same SQLite handle, just as concurrent daemon repositories do.
/// Errors must retain the code and text of their own failing operation, even while
/// another lane prepares, steps, or drops a successful statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_connection_errors_keep_their_original_diagnostics() {
    let store = Store::open(":memory:").await.unwrap();
    store
        .conn
        .execute("CREATE TABLE probe (id INTEGER PRIMARY KEY)", ())
        .await
        .unwrap();
    store
        .conn
        .execute("INSERT INTO probe VALUES (1)", ())
        .await
        .unwrap();
    let barrier = Arc::new(Barrier::new(8));
    let mut tasks = Vec::new();
    for lane in 0..8 {
        let conn = store.conn.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            for iteration in 0..2_000 {
                tokio::task::yield_now().await;
                let error = match lane {
                    0 => {
                        let mut rows = conn.query("SELECT 42", ()).await.unwrap();
                        assert_eq!(rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(), 42);
                        assert!(rows.next().await.unwrap().is_none());
                        continue;
                    }
                    1 => conn.execute("ALTER TABLE probe ADD COLUMN id INTEGER", ()).await.unwrap_err(),
                    2 => conn.execute("INSERT INTO probe VALUES (1)", ()).await.unwrap_err(),
                    3 => {
                        let mut rows = conn.query("SELECT 1 UNION ALL SELECT abs(-9223372036854775808)", ()).await.unwrap();
                        assert_eq!(rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(), 1);
                        rows.next().await.unwrap_err()
                    }
                    4 => {
                        let raw = conn.raw();
                        assert_eq!(raw.execute("UPDATE probe SET id = id", ()).await.unwrap(), 1);
                        continue;
                    }
                    5 => {
                        let raw = conn.raw();
                        let mut stmt = raw.prepare("INSERT INTO probe VALUES (1)").await.unwrap();
                        stmt.run(()).await.unwrap_err()
                    }
                    6 => conn.raw().execute_batch("INSERT INTO probe VALUES (1)").await.unwrap_err(),
                    _ => {
                        let mut rows = conn.raw().query("SELECT abs(-9223372036854775808)", ()).await.unwrap();
                        rows.next().await.unwrap_err()
                    }
                };
                let (expected_code, expected_text) = match lane {
                    1 => (1, "duplicate column name: id"),
                    2 | 5 | 6 => (1555, "UNIQUE constraint failed: probe.id"),
                    _ => (1, "integer overflow"),
                };
                match error {
                    libsql::Error::SqliteFailure(code, message)
                        if code == expected_code && message == expected_text => {}
                    error => panic!("lane {lane}, iteration {iteration}: expected ({expected_code}, {expected_text:?}), got {error:?}"),
                }
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_write_releases_connection_for_subsequent_transactions() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store
        .conn
        .execute("CREATE TABLE cancelled (id INTEGER)", ())
        .await
        .unwrap();
    let (ready, started) = tokio::sync::oneshot::channel();
    let writer_store = store.clone();
    let writer = tokio::spawn(async move {
        let txn = writer_store
            .begin_write_txn("cancelled-concurrency-control")
            .await
            .unwrap();
        txn.execute("INSERT INTO cancelled VALUES (1)", ())
            .await
            .unwrap();
        ready.send(()).unwrap();
        std::future::pending::<()>().await;
        txn.commit().await.unwrap();
    });
    started.await.unwrap();
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let txn = store.begin_write_txn("after-cancellation").await.unwrap();
        let mut rows = txn
            .query("SELECT count(*) FROM cancelled", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            0
        );
        drop(rows);
        txn.execute("INSERT INTO cancelled VALUES (2)", ())
            .await
            .unwrap();
        txn.commit().await.unwrap();
    })
    .await
    .expect("cancellation must release transaction and connection locks");
}
