//! Harness-neutral message-hook port.
//!
//! The daemon owns this boundary but never executes developer code. A concrete adapter delegates
//! the request to a hook-capable Gateway; the bus only knows that a new logical send must be
//! evaluated before its canonical transaction commits.

use async_trait::async_trait;

use crate::hooks::{HookBeforeSendRequest, HookBeforeSendResult};
use crate::ports::PortResult;

#[async_trait]
pub trait MessageHookPort: Send + Sync {
    /// Evaluate one new logical message before canonical acceptance.
    async fn before_send(&self, request: HookBeforeSendRequest)
        -> PortResult<HookBeforeSendResult>;
}
