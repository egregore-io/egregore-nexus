//! Daemon storage ownership split.
//!
//! The daemon keeps one small file-backed identity/continuity store and one boot-scoped in-memory
//! transport store. Gateway presentation/search/history tables are authoritative in Gateway and
//! are deliberately absent from both daemon databases.

use std::collections::HashSet;
use std::sync::Arc;

use nexus_common::NexusError;

use crate::migrate::{IDENTITY_SCHEMA_NAME, TRANSPORT_SCHEMA_NAME};
use crate::Store;

pub const PERSISTENT_CONTINUITY_TABLES: &[&str] = &[
    "agent_acl_grants",
    "agent_credentials",
    "agent_runtimes",
    "agents",
    "command_intent_events",
    "command_intents",
    "command_queue_mutations",
    "daemon_state",
    "delivery_obligations",
    "identity_sessions",
    "initial_prompt_deliveries",
    "native_thread_bindings",
    "producer_identities",
    "routing_thread_members",
    "routing_threads",
    "sources",
];

pub const VOLATILE_TRANSPORT_TABLES: &[&str] = &[
    "agent_group_members",
    "agent_groups",
    "developer_event_topics",
    "developer_events",
    "in_flight",
    "inbox_subscription_batches",
    "inbox_subscriptions",
    "live_sessions",
    "messages",
    "reply_contexts",
    "sessions",
    "stream_events",
    "stream_raw",
    "subscriptions",
    "thread_members",
    "threads",
    "topics",
];

pub const GATEWAY_ONLY_PRODUCT_TABLES: &[&str] = &[
    "agent_session_messages",
    "agent_session_stream_cursors",
    "agent_session_turns",
    "message_vectors",
    "messages_fts",
    "notifications",
    "projects",
    "transcript_archive",
];

const IDENTITY_SCHEMA: &str = include_str!("../../../migrations/identity/0001_identity.sql");
const TRANSPORT_SCHEMA: &str = include_str!("../../../migrations/transport/0001_transport.sql");

/// The daemon's two explicit storage authorities.
pub struct DaemonStore {
    identity: Arc<Store>,
    transport: Arc<Store>,
}

impl DaemonStore {
    /// Open the embedded identity file and a fresh anonymous transport database.
    pub async fn open(identity_path: &str) -> Result<Self, NexusError> {
        let identity = Arc::new(Store::open(identity_path).await?);
        identity.migrate().await?;
        prune_to_authority(&identity, PERSISTENT_CONTINUITY_TABLES).await?;
        identity
            .conn
            .execute_batch(IDENTITY_SCHEMA)
            .await
            .map_err(store_error)?;
        identity.mark_schema_variant(IDENTITY_SCHEMA_NAME).await?;

        let transport = Arc::new(Store::open(":memory:").await?);
        transport.migrate().await?;
        prune_to_authority(&transport, VOLATILE_TRANSPORT_TABLES).await?;
        transport
            .conn
            .execute_batch(TRANSPORT_SCHEMA)
            .await
            .map_err(store_error)?;
        transport.mark_schema_variant(TRANSPORT_SCHEMA_NAME).await?;

        Ok(Self {
            identity,
            transport,
        })
    }

    /// File-backed identity, resurrection and unsettled-delivery continuity.
    pub fn identity(&self) -> &Store {
        &self.identity
    }

    /// Boot-scoped message routing, presence, queue and stream state.
    pub fn transport(&self) -> &Store {
        &self.transport
    }

    pub fn identity_arc(&self) -> Arc<Store> {
        self.identity.clone()
    }

    pub fn transport_arc(&self) -> Arc<Store> {
        self.transport.clone()
    }

    /// Transitional application handle: existing service constructors keep one `Store`, while
    /// migrated repositories select `identity_conn()` and transport repositories keep `conn`.
    pub fn compatibility_store(&self) -> Store {
        Store::with_identity_authority(&self.transport, self.identity.clone())
    }
}

async fn prune_to_authority(store: &Store, authoritative: &[&str]) -> Result<(), NexusError> {
    // Drop cross-table projection triggers before pruning either side of their dependency graph.
    // Dropping the virtual table first removes all of its SQLite-owned shadow tables.
    store
        .conn
        .execute_batch(
            "DROP TRIGGER IF EXISTS trg_command_intent_queue_insert;
             DROP TRIGGER IF EXISTS trg_command_intent_queue_update;
             DROP TRIGGER IF EXISTS nexus_broadcast_ingress_insert;
             DROP TRIGGER IF EXISTS in_flight_reply_context_on_injecting;
             DROP VIEW IF EXISTS nexus_broadcast_ingress;
             DROP TABLE IF EXISTS messages_fts;",
        )
        .await
        .map_err(store_error)?;

    let keep = authoritative.iter().copied().collect::<HashSet<_>>();
    let mut rows = store
        .conn
        .query(
            "SELECT name FROM sqlite_master
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
            (),
        )
        .await
        .map_err(store_error)?;
    let mut drop_tables = Vec::new();
    while let Some(row) = rows.next().await.map_err(store_error)? {
        let table: String = row.get(0).map_err(store_error)?;
        if table != "schema_migrations" && !keep.contains(table.as_str()) {
            drop_tables.push(table);
        }
    }
    drop(rows);

    for table in drop_tables {
        let escaped = table.replace('"', "\"\"");
        store
            .conn
            .execute_batch(&format!("DROP TABLE IF EXISTS \"{escaped}\""))
            .await
            .map_err(store_error)?;
    }
    Ok(())
}

fn store_error(error: libsql::Error) -> NexusError {
    NexusError::Store(error.to_string())
}
