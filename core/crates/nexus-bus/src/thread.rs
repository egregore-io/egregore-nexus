//! Thread scope (backend §3): a named, shared conversation. One `messages` row, fan-out to all
//! members (N `in_flight` rows). Threads are named with ids hidden from agents; `--mention` inside
//! a thread is a soft highlight, never a routing change.

use nexus_contracts::enums::Scope;
use nexus_contracts::ids::ThreadId;

use crate::broadcast_messages::WriteSpec;
use crate::router::Recipient;

/// Build the durable write spec for a thread post fanning out to `members`.
pub(crate) fn spec<'a>(
    thread_id: &ThreadId,
    thread_name: String,
    thread_project: String,
    members: &'a [Recipient],
) -> WriteSpec<'a> {
    WriteSpec {
        scope: Scope::Thread,
        thread: Some(thread_id.clone()),
        topic: None,
        dm_name: None,
        thread_name: Some(thread_name),
        topic_name: None,
        project: Some(thread_project),
        recipients: members,
    }
}
