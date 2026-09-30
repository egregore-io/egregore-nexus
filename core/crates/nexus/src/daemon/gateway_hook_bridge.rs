//! Correlated, bounded daemon-to-Gateway message-hook evaluations.
//!
//! This is deliberately separate from the broadcast projection publisher: every request has one
//! provider generation, one correlation ID, and one terminal response or typed failure.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nexus_contracts::{
    GatewayHookCapabilities, GatewayHookEvaluation, HookEvaluationFailure, HookEvaluationRequest,
    HookEvaluationResponse,
};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum GatewayHookBridgeError {
    #[error("no Gateway hook provider is connected")]
    Unavailable,
    #[error("Gateway hook provider does not support event {0}")]
    UnsupportedEvent(String),
    #[error("Gateway hook request queue is full")]
    Backpressure,
    #[error("Gateway hook provider disconnected")]
    Disconnected,
    #[error("Gateway hook evaluation timed out")]
    TimedOut,
    #[error("Gateway hook evaluation failed: {0:?}")]
    Remote(HookEvaluationFailure),
}

#[derive(Clone)]
pub struct GatewayHookBridge {
    inner: Arc<Inner>,
}

struct Inner {
    capacity: usize,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    next_provider_id: u64,
    provider: Option<Provider>,
    pending: HashMap<String, Pending>,
}

struct Provider {
    id: u64,
    capabilities: GatewayHookCapabilities,
    requests: mpsc::Sender<GatewayHookEvaluation>,
}

struct Pending {
    provider_id: u64,
    result: oneshot::Sender<Result<HookEvaluationResponse, GatewayHookBridgeError>>,
}

pub struct GatewayHookProvider {
    bridge: GatewayHookBridge,
    provider_id: u64,
}

impl GatewayHookBridge {
    pub fn new(capacity: usize) -> Self {
        assert!(
            capacity > 0,
            "Gateway hook bridge capacity must be positive"
        );
        Self {
            inner: Arc::new(Inner {
                capacity,
                state: Mutex::new(State::default()),
            }),
        }
    }

    pub fn connect(
        &self,
        capabilities: GatewayHookCapabilities,
    ) -> (GatewayHookProvider, mpsc::Receiver<GatewayHookEvaluation>) {
        let (requests, receiver) = mpsc::channel(self.inner.capacity);
        let mut state = self
            .inner
            .state
            .lock()
            .expect("Gateway hook bridge poisoned");
        let displaced = state.provider.take().map(|provider| provider.id);
        if let Some(provider_id) = displaced {
            fail_provider_pending(&mut state, provider_id);
        }
        state.next_provider_id += 1;
        let provider_id = state.next_provider_id;
        state.provider = Some(Provider {
            id: provider_id,
            capabilities,
            requests,
        });
        drop(state);
        (
            GatewayHookProvider {
                bridge: self.clone(),
                provider_id,
            },
            receiver,
        )
    }

    pub async fn evaluate(
        &self,
        request: HookEvaluationRequest,
        timeout: Duration,
    ) -> Result<HookEvaluationResponse, GatewayHookBridgeError> {
        let event = request.event_name();
        let correlation_id = format!("hc_{}", Uuid::new_v4().simple());
        let (result_tx, result_rx) = oneshot::channel();
        let (provider_id, requests) = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("Gateway hook bridge poisoned");
            let provider = state
                .provider
                .as_ref()
                .ok_or(GatewayHookBridgeError::Unavailable)?;
            if provider.capabilities.protocol_version != 1
                || !provider
                    .capabilities
                    .events
                    .iter()
                    .any(|value| value == event)
            {
                return Err(GatewayHookBridgeError::UnsupportedEvent(event.to_string()));
            }
            let provider_id = provider.id;
            let requests = provider.requests.clone();
            state.pending.insert(
                correlation_id.clone(),
                Pending {
                    provider_id,
                    result: result_tx,
                },
            );
            (provider_id, requests)
        };

        let evaluation = GatewayHookEvaluation {
            correlation_id: correlation_id.clone(),
            request,
        };
        if let Err(error) = requests.try_send(evaluation) {
            self.remove_pending(&correlation_id, provider_id);
            return Err(match error {
                mpsc::error::TrySendError::Full(_) => GatewayHookBridgeError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => GatewayHookBridgeError::Disconnected,
            });
        }

        match tokio::time::timeout(timeout, result_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                self.remove_pending(&correlation_id, provider_id);
                Err(GatewayHookBridgeError::Disconnected)
            }
            Err(_) => {
                self.remove_pending(&correlation_id, provider_id);
                Err(GatewayHookBridgeError::TimedOut)
            }
        }
    }

    pub fn pending_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("Gateway hook bridge poisoned")
            .pending
            .len()
    }

    fn remove_pending(&self, correlation_id: &str, provider_id: u64) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("Gateway hook bridge poisoned");
        if state
            .pending
            .get(correlation_id)
            .is_some_and(|pending| pending.provider_id == provider_id)
        {
            state.pending.remove(correlation_id);
        }
    }

    fn disconnect(&self, provider_id: u64) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("Gateway hook bridge poisoned");
        if state
            .provider
            .as_ref()
            .is_some_and(|provider| provider.id == provider_id)
        {
            state.provider = None;
            fail_provider_pending(&mut state, provider_id);
        }
    }
}

impl GatewayHookProvider {
    pub fn complete(
        &self,
        correlation_id: String,
        result: Result<HookEvaluationResponse, HookEvaluationFailure>,
    ) -> bool {
        let pending = {
            let mut state = self
                .bridge
                .inner
                .state
                .lock()
                .expect("Gateway hook bridge poisoned");
            if !state
                .pending
                .get(&correlation_id)
                .is_some_and(|pending| pending.provider_id == self.provider_id)
            {
                return false;
            }
            state.pending.remove(&correlation_id)
        };
        pending.is_some_and(|pending| {
            pending
                .result
                .send(result.map_err(GatewayHookBridgeError::Remote))
                .is_ok()
        })
    }
}

impl Drop for GatewayHookProvider {
    fn drop(&mut self) {
        self.bridge.disconnect(self.provider_id);
    }
}

fn fail_provider_pending(state: &mut State, provider_id: u64) {
    let ids = state
        .pending
        .iter()
        .filter_map(|(id, pending)| (pending.provider_id == provider_id).then(|| id.clone()))
        .collect::<Vec<_>>();
    for id in ids {
        if let Some(pending) = state.pending.remove(&id) {
            let _ = pending
                .result
                .send(Err(GatewayHookBridgeError::Disconnected));
        }
    }
}

impl Default for GatewayHookBridge {
    fn default() -> Self {
        Self::new(64)
    }
}
