//! Versioned, acknowledged daemon-to-Gateway projection records.
//!
//! These records carry canonical product facts to the persistent Gateway backend. They are
//! deliberately separate from [`crate::WsEvent`]: agent-session updates and terminal bytes are
//! ephemeral stream frames, not durable history projections.

use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use typeshare::typeshare;

/// Initial Gateway projection payload version.
pub const GATEWAY_PROJECTION_VERSION: u32 = 1;

/// A committed canonical fact before the daemon's volatile Gateway publisher assigns a boot
/// epoch and sequence number.
///
/// Domain services create this value only after their store transaction commits. Keeping the
/// transport position out of the effect makes the `event_id` reusable across reclaim/replay while
/// preserving the daemon's same-boot, RAM-only buffering boundary.
#[derive(Debug, Clone, PartialEq)]
pub struct GatewayProjectionEffect {
    pub event_id: String,
    pub occurred_at: i64,
    pub kind: GatewayProjectionKind,
    pub payload: serde_json::Value,
}

/// Closed set of canonical facts the daemon may project into Gateway persistence.
#[typeshare]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GatewayProjectionKind {
    #[serde(rename = "identity.upserted")]
    IdentityUpserted,
    #[serde(rename = "identity.removed")]
    IdentityRemoved,
    #[serde(rename = "runtime.upserted")]
    RuntimeUpserted,
    #[serde(rename = "runtime.stopped")]
    RuntimeStopped,
    #[serde(rename = "thread.declared")]
    ThreadDeclared,
    #[serde(rename = "thread.membership.changed")]
    ThreadMembershipChanged,
    #[serde(rename = "topic.declared")]
    TopicDeclared,
    #[serde(rename = "topic.subscription.changed")]
    TopicSubscriptionChanged,
    #[serde(rename = "message.accepted")]
    MessageAccepted,
    #[serde(rename = "delivery.settled")]
    DeliverySettled,
    #[serde(rename = "notification.emitted")]
    NotificationEmitted,
    #[serde(rename = "presence.changed")]
    PresenceChanged,
}

/// One immutable canonical fact ordered within a volatile daemon boot epoch.
///
/// `event_id` is the idempotency key and must remain stable if an effect is reclaimed under a new
/// epoch. `daemon_epoch` plus `seq` is only the transport position used for replay and ACKs.
#[typeshare]
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayProjectionEvent {
    pub event_id: String,
    pub daemon_epoch: String,
    #[typeshare(serialized_as = "number")]
    pub seq: i64,
    #[typeshare(serialized_as = "number")]
    pub occurred_at: i64,
    pub kind: GatewayProjectionKind,
    pub version: u32,
    pub payload: serde_json::Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GatewayProjectionEventWire {
    event_id: String,
    daemon_epoch: String,
    seq: i64,
    occurred_at: i64,
    kind: GatewayProjectionKind,
    version: u32,
    payload: serde_json::Value,
}

impl<'de> Deserialize<'de> for GatewayProjectionEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = GatewayProjectionEventWire::deserialize(deserializer)?;
        if wire.version != GATEWAY_PROJECTION_VERSION {
            return Err(D::Error::custom(format!(
                "unsupported Gateway projection version {}",
                wire.version
            )));
        }
        if wire.event_id.trim().is_empty() {
            return Err(D::Error::custom(
                "Gateway projection eventId must not be empty",
            ));
        }
        if wire.daemon_epoch.trim().is_empty() {
            return Err(D::Error::custom(
                "Gateway projection daemonEpoch must not be empty",
            ));
        }
        if wire.seq < 0 {
            return Err(D::Error::custom(
                "Gateway projection seq must be non-negative",
            ));
        }
        Ok(Self {
            event_id: wire.event_id,
            daemon_epoch: wire.daemon_epoch,
            seq: wire.seq,
            occurred_at: wire.occurred_at,
            kind: wire.kind,
            version: wire.version,
            payload: wire.payload,
        })
    }
}

/// Gateway's durable projection watermark for one daemon boot epoch.
#[typeshare]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayProjectionAck {
    pub daemon_epoch: String,
    #[typeshare(serialized_as = "number")]
    pub through_seq: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GatewayProjectionAckWire {
    daemon_epoch: String,
    through_seq: i64,
}

impl<'de> Deserialize<'de> for GatewayProjectionAck {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = GatewayProjectionAckWire::deserialize(deserializer)?;
        if wire.daemon_epoch.trim().is_empty() {
            return Err(D::Error::custom(
                "Gateway projection ACK daemonEpoch must not be empty",
            ));
        }
        if wire.through_seq < 0 {
            return Err(D::Error::custom(
                "Gateway projection ACK throughSeq must be non-negative",
            ));
        }
        Ok(Self {
            daemon_epoch: wire.daemon_epoch,
            through_seq: wire.through_seq,
        })
    }
}
