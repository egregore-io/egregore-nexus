//! Prefixed, uuid-backed id generators for the typed `nexus_contracts` id newtypes.

use nexus_contracts::{MessageId, ProjectId, SessionId, ThreadId};
use uuid::Uuid;

fn gen(prefix: &str) -> String {
    format!("{prefix}{}", Uuid::new_v4().simple())
}

/// A fresh opaque runtime binding incarnation, never a session identity or reset counter.
pub fn new_binding_id() -> String {
    gen("b_")
}

/// A fresh `m_…` message id.
pub fn new_message_id() -> MessageId {
    MessageId(gen("m_"))
}
/// A fresh `s_…` session id.
pub fn new_session_id() -> SessionId {
    SessionId(gen("s_"))
}
/// A fresh `t_…` thread id.
pub fn new_thread_id() -> ThreadId {
    ThreadId(gen("t_"))
}
/// A fresh `p_…` project id.
pub fn new_project_id() -> ProjectId {
    ProjectId(gen("p_"))
}
/// A fresh `src_…` notification-source token (plaintext; stored by the daemon, verified by the
/// gateway over HMAC). Prefix + 32 hex chars from a random UUID v4 → ample entropy.
pub fn new_source_token() -> String {
    gen("src_")
}
