//! DM scope (backend §3): a strictly-private 2-party message. Runtime DMs resolve to exactly one
//! recipient session; local-operator DMs resolve to a durable read-view row with no runtime
//! recipient. In both cases the message never enters any third agent's context
//! (context-hygiene invariant, req #4).

use crate::broadcast_messages::WriteSpec;
use crate::router::Recipient;
use nexus_contracts::enums::Scope;

/// Build the durable write spec for a DM to a single recipient session.
pub(crate) fn spec(recipient: &[Recipient]) -> WriteSpec<'_> {
    WriteSpec {
        scope: Scope::Dm,
        thread: None,
        topic: None,
        dm_name: recipient.first().and_then(|r| r.name.clone()),
        thread_name: None,
        topic_name: None,
        project: None,
        recipients: recipient,
    }
}

/// Build the durable write spec for a DM addressed to the local web-console operator. The operator
/// is a human read-view sink, not a runtime session, so this deliberately carries no recipients.
pub(crate) fn local_operator_spec<'a>(
    operator_name: String,
    recipients: &'a [Recipient],
) -> WriteSpec<'a> {
    WriteSpec {
        scope: Scope::Dm,
        thread: None,
        topic: None,
        dm_name: Some(operator_name),
        thread_name: None,
        topic_name: None,
        project: None,
        recipients,
    }
}

/// Build a notification-kind DM scope carrying one canonical body to a resolved group. The group
/// label is display metadata; each member receives only an `in_flight` state row.
pub(crate) fn group_spec(group: String, recipients: &[Recipient]) -> WriteSpec<'_> {
    WriteSpec {
        scope: Scope::Dm,
        thread: None,
        topic: None,
        dm_name: Some(group),
        thread_name: None,
        topic_name: None,
        project: None,
        recipients,
    }
}
