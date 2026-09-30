// Fixed daemon boot checkpoints, not a general startup scheduler. Watch snapshots retain terminal
// outcomes for both current and later waiters; NexusError is not Clone, so retain its diagnostic.
use std::future::Future;

use nexus_common::NexusError;
use tokio::sync::watch;

type Outcome = Result<(), String>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InitialPresenceOutcome {
    // These are best-effort attempts. Either error still permits the later periodic cadence.
    pub(crate) presume_dead: Outcome,
    pub(crate) stale: Outcome,
}

impl Default for InitialPresenceOutcome {
    fn default() -> Self {
        Self {
            presume_dead: Ok(()),
            stale: Ok(()),
        }
    }
}

#[derive(Clone, Default)]
struct Outcomes {
    model: Option<Outcome>,
    directory: Option<Outcome>,
    initial_presence: Option<Result<InitialPresenceOutcome, String>>,
}

#[derive(Clone)]
pub(crate) struct BootReadiness {
    unmanaged: bool,
    outcomes: watch::Sender<Outcomes>,
}

impl BootReadiness {
    pub(crate) fn pending() -> Self {
        Self {
            unmanaged: false,
            outcomes: watch::channel(Outcomes::default()).0,
        }
    }

    /// Low-level mock-port construction has no boot task and no initialized model coordinator.
    pub(crate) fn unmanaged() -> Self {
        Self {
            unmanaged: true,
            ..Self::pending()
        }
    }

    /// Create BEFORE spawning: even destruction of a never-polled task must settle waiters.
    pub(crate) fn guard(&self, close_admission: impl FnOnce() + Send + 'static) -> BootGuard {
        BootGuard {
            readiness: self.clone(),
            close_admission: Some(Box::new(close_admission)),
        }
    }

    pub(crate) async fn wait_ingress(&self) -> Result<(), NexusError> {
        if self.unmanaged {
            return Ok(());
        }
        let mut receiver = self.outcomes.subscribe();
        loop {
            {
                let outcomes = receiver.borrow_and_update();
                if let Some(model) = &outcomes.model {
                    model.clone().map_err(internal)?;
                    if let Some(directory) = &outcomes.directory {
                        return directory.clone().map_err(internal);
                    }
                }
            }
            receiver
                .changed()
                .await
                .map_err(|_| internal("boot outcome channel closed"))?;
        }
    }

    pub(crate) async fn wait_initial_presence(&self) -> Result<InitialPresenceOutcome, NexusError> {
        let mut receiver = self.outcomes.subscribe();
        loop {
            if let Some(outcome) = receiver.borrow_and_update().initial_presence.clone() {
                return outcome.map_err(internal);
            }
            receiver
                .changed()
                .await
                .map_err(|_| internal("boot outcome channel closed"))?;
        }
    }
}

pub(crate) struct BootGuard {
    readiness: BootReadiness,
    close_admission: Option<Box<dyn FnOnce() + Send>>,
}

impl BootGuard {
    fn fail(&mut self, reason: String) {
        // Closing is local admission only, not a claim of durable revocation or async drain.
        if let Some(close) = self.close_admission.take() {
            close();
            self.readiness.outcomes.send_modify(|outcomes| {
                outcomes.model.get_or_insert_with(|| Err(reason.clone()));
                outcomes
                    .directory
                    .get_or_insert_with(|| Err(reason.clone()));
                outcomes.initial_presence.get_or_insert_with(|| Err(reason));
            });
        }
    }
}

impl Drop for BootGuard {
    fn drop(&mut self) {
        self.fail("daemon boot task cancelled or panicked before completion".into());
    }
}

/// The same orchestration is used by real boot and private source-inclusion tests. Ingress needs
/// model invalidation and identity restoration only; periodic presence needs adoption and both
/// initial reconcile attempts, but neither waits for pending-inbox backlog recovery.
pub(crate) async fn run_boot(
    mut guard: BootGuard,
    model: impl Future<Output = Result<(), NexusError>>,
    directory: impl Future<Output = Result<(), NexusError>>,
    initial_presence: impl Future<Output = Result<InitialPresenceOutcome, NexusError>>,
    backlog: impl Future<Output = ()>,
) -> Result<(), NexusError> {
    let result: Result<(), NexusError> = async {
        let result = model.await;
        guard.readiness.outcomes.send_modify(|outcomes| {
            outcomes.model = Some(result.as_ref().map(|_| ()).map_err(ToString::to_string));
        });
        result?;
        let result = directory.await;
        guard.readiness.outcomes.send_modify(|outcomes| {
            outcomes.directory = Some(result.as_ref().map(|_| ()).map_err(ToString::to_string));
        });
        result?;
        let result = initial_presence.await;
        guard.readiness.outcomes.send_modify(|outcomes| {
            outcomes.initial_presence = Some(result.as_ref().cloned().map_err(ToString::to_string));
        });
        result?;
        backlog.await;
        Ok(())
    }
    .await;
    match &result {
        Ok(()) => {
            guard.close_admission.take();
        }
        Err(error) => guard.fail(error.to_string()),
    }
    result
}

fn internal(message: impl Into<String>) -> NexusError {
    NexusError::Internal(message.into())
}
