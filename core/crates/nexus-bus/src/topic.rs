//! Topic scope (backend §3): pub/sub. One `messages` row, fan-out to the topic's current
//! subscribers. Used for the Pub feed and external notifications (§8).

use nexus_contracts::enums::Scope;
use nexus_contracts::ids::TopicId;

use crate::broadcast_messages::WriteSpec;
use crate::router::Recipient;

/// Build the durable write spec for a topic publish fanning out to `subscribers`.
pub(crate) fn spec<'a>(topic_id: &TopicId, subscribers: &'a [Recipient]) -> WriteSpec<'a> {
    WriteSpec {
        scope: Scope::Topic,
        thread: None,
        topic: Some(topic_id.clone()),
        dm_name: None,
        thread_name: None,
        topic_name: Some(topic_id.0.clone()),
        project: None,
        recipients: subscribers,
    }
}
