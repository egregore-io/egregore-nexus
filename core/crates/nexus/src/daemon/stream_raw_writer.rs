//! Store-backed raw terminal-output writer for the `/agent` live raw lane.
//!
//! This task deliberately does not add a daemon socket or endpoint. The daemon only copies headed
//! PTY/tmux output bytes into `mem.stream_raw` in the tmpfs stream store; gateway consumers tail the
//! store file independently.

use std::sync::Arc;

use nexus_contracts::SessionId;
use nexus_store::repos::StreamRaw;
use nexus_store::Store;
use tokio::sync::broadcast;

/// Spawn a best-effort task that appends every raw terminal chunk to `mem.stream_raw`.
///
/// Store failures are logged and skipped so terminal delivery, AG-UI materialization, and harness
/// wakeups never block on the raw lane.
pub fn spawn_stream_raw_writer(
    store: Arc<Store>,
    session: SessionId,
    mut output: broadcast::Receiver<Vec<u8>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match output.recv().await {
                Ok(chunk) => {
                    if chunk.is_empty() {
                        continue;
                    }
                    if let Err(error) = StreamRaw::new(&store).append(&session, &chunk).await {
                        tracing::warn!(
                            target: "nexus::stream_raw",
                            session = %session,
                            error = %error,
                            "failed to buffer raw terminal chunk"
                        );
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(
                        target: "nexus::stream_raw",
                        session = %session,
                        skipped,
                        "raw terminal stream lagged; skipped chunks"
                    );
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn raw_writer_appends_chunks_and_exits_on_close() {
        let store = Arc::new(Store::open(":memory:").await.unwrap());
        store.migrate().await.unwrap();
        let session = SessionId("s_raw_writer".to_string());
        let (tx, rx) = broadcast::channel(8);

        let handle = spawn_stream_raw_writer(store.clone(), session.clone(), rx);
        tx.send(b"alpha".to_vec()).unwrap();
        tx.send(b"beta".to_vec()).unwrap();
        drop(tx);
        handle.await.unwrap();

        let rows = StreamRaw::new(&store).since(&session, 0).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].chunk, b"alpha");
        assert_eq!(rows[1].chunk, b"beta");
    }
}
