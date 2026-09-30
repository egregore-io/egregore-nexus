#![cfg(test)]

use super::daemon_error_message;
use std::io;

use nexus_common::NexusError;
use std::sync::{Arc, Mutex};
use tracing::instrument::WithSubscriber;

struct LogEvents(Arc<Mutex<Vec<String>>>);

impl tracing::Subscriber for LogEvents {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Message(String);
        impl tracing::field::Visit for Message {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }
        let mut message = Message(String::new());
        event.record(&mut message);
        self.0.lock().unwrap().push(message.0);
    }
}

struct StreamDrop(Arc<Mutex<Vec<&'static str>>>);

impl Drop for StreamDrop {
    fn drop(&mut self) {
        self.0.lock().unwrap().push("release");
    }
}

#[tokio::test]
async fn shutdown_finalizer_awaits_reaper_then_releases_then_awaits_checkpoint() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let stream = StreamDrop(events.clone());
    let (reap_tx, reap_rx) = tokio::sync::oneshot::channel();
    let (checkpoint_tx, checkpoint_rx) = tokio::sync::oneshot::channel();
    let mut finalize = Box::pin(super::finalize_shutdown(
        Ok(()),
        async {
            events.lock().unwrap().push("reap-start");
            reap_rx.await.unwrap();
            events.lock().unwrap().push("reap-end");
            Ok(17)
        },
        move || drop(stream),
        async {
            events.lock().unwrap().push("checkpoint-start");
            checkpoint_rx.await.unwrap();
            events.lock().unwrap().push("checkpoint-end");
            Ok(())
        },
    ));
    assert!(futures::poll!(&mut finalize).is_pending());
    assert_eq!(*events.lock().unwrap(), ["reap-start"]);
    reap_tx.send(()).unwrap();
    assert!(futures::poll!(&mut finalize).is_pending());
    assert_eq!(
        *events.lock().unwrap(),
        ["reap-start", "reap-end", "release", "checkpoint-start"]
    );
    checkpoint_tx.send(()).unwrap();
    assert_eq!(finalize.await.unwrap(), 17);
    assert_eq!(
        *events.lock().unwrap(),
        [
            "reap-start",
            "reap-end",
            "release",
            "checkpoint-start",
            "checkpoint-end"
        ]
    );
}

#[tokio::test]
async fn shutdown_finalizer_preserves_causes_and_always_attempts_result_cleanup() {
    for model_fails in [false, true] {
        for reap_fails in [false, true] {
            for checkpoint_fails in [false, true] {
                let events = Arc::new(Mutex::new(Vec::new()));
                let logs = Arc::new(Mutex::new(Vec::new()));
                let stream = StreamDrop(events.clone());
                let result = super::finalize_shutdown(
                    if model_fails {
                        Err(NexusError::Internal("model-marker".into()))
                    } else {
                        Ok(())
                    },
                    async {
                        events.lock().unwrap().push("reap");
                        if reap_fails {
                            Err(NexusError::Internal("reaper-marker".into()))
                        } else {
                            Ok(23)
                        }
                    },
                    move || drop(stream),
                    async {
                        events.lock().unwrap().push("checkpoint");
                        if checkpoint_fails {
                            Err(NexusError::Internal("wal-marker".into()))
                        } else {
                            Ok(())
                        }
                    },
                )
                .with_subscriber(LogEvents(logs.clone()))
                .await;
                assert_eq!(*events.lock().unwrap(), ["reap", "release", "checkpoint"]);
                let logs = logs.lock().unwrap();
                assert_eq!(
                    logs.iter()
                        .any(|line| line.contains("model reporting drained for shutdown")),
                    !model_fails
                );
                assert_eq!(
                    logs.iter()
                        .any(|line| line.contains("final WAL checkpoint failed")),
                    checkpoint_fails
                );
                if model_fails || reap_fails {
                    let error = result.unwrap_err().to_string();
                    assert_eq!(error.contains("model-marker"), model_fails, "{error}");
                    assert_eq!(error.contains("reaper-marker"), reap_fails, "{error}");
                    assert!(
                        !error.contains("wal-marker"),
                        "checkpoint remains warning-only"
                    );
                } else {
                    assert_eq!(result.unwrap(), 23, "checkpoint must not lose reaped count");
                }
            }
        }
    }
}

#[test]
fn daemon_already_running_error_is_rendered_without_generic_prefix() {
    let err = io::Error::new(
        io::ErrorKind::AlreadyExists,
        "nexus daemon already running (pid 123)",
    );

    assert_eq!(
        daemon_error_message(&err),
        "nexus daemon already running (pid 123)"
    );
}

#[test]
fn daemon_other_errors_keep_generic_prefix() {
    let err = io::Error::new(io::ErrorKind::Other, "db failed");

    assert_eq!(daemon_error_message(&err), "error: db failed");
}
