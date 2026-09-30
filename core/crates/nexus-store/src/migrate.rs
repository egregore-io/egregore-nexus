//! Public Nexus schema bootstrap and forward-only upgrades.
//!
//! v0.1.0 established the first supported database format. Every later public schema change is
//! additive and recorded here; unknown pre-release ladders still fail closed without mutation.

use std::path::{Path, PathBuf};

use nexus_common::{now, NexusError};

use crate::error::store_err;
use crate::state::Store;

pub const CURRENT_SCHEMA_VERSION: i64 = 5;
pub const CURRENT_SCHEMA_NAME: &str = "v0.1.6_caller_authority";
const BASELINE_SCHEMA_VERSION: i64 = 1;
const BASELINE_SCHEMA_NAME: &str = "v0.1.0_baseline";
const MESSAGE_HOOKS_SCHEMA_VERSION: i64 = 2;
const MESSAGE_HOOKS_SCHEMA_NAME: &str = "v0.1.5_message_hooks";
const DELIVERY_TIMING_SCHEMA_VERSION: i64 = 3;
const DELIVERY_TIMING_SCHEMA_NAME: &str = "v0.1.5_delivery_timing";
const CALLER_PRINCIPAL_SCHEMA_VERSION: i64 = 4;
const CALLER_PRINCIPAL_SCHEMA_NAME: &str = "v0.1.6_caller_principal";
const CALLER_VALIDATION_SCHEMA_NAME: &str = "v0.1.5_caller_validation";
pub(crate) const IDENTITY_SCHEMA_NAME: &str = "v0.1.0_identity";
pub(crate) const IDENTITY_PROVIDER_SCHEMA_NAME: &str = "v0.1.6_identity_provider";
pub(crate) const IDENTITY_CALLER_PRINCIPAL_SCHEMA_NAME: &str = "v0.1.6_identity_caller_principal";
pub(crate) const IDENTITY_MODEL_REPORT_SCHEMA_NAME: &str = "v0.1.6_identity_model_report";
pub(crate) const TRANSPORT_SCHEMA_NAME: &str = "v0.1.0_transport";

/// Reachable test seam for proving identity-provider and model-report upgrades are atomic.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationFault {
    None,
    AfterFirstStatement,
}

const BASELINE_SCHEMA: &str = include_str!("../../../migrations/0001_init.sql");
const MESSAGE_HOOKS_SCHEMA: &str = include_str!("../../../migrations/0002_message_hooks.sql");
const DELIVERY_TIMING_SCHEMA: &str = include_str!("../../../migrations/0003_delivery_timing.sql");
const CALLER_PRINCIPAL_SCHEMA: &str = include_str!("../../../migrations/0004_caller_principal.sql");
const CALLER_VALIDATION_SCHEMA: &str =
    include_str!("../../../migrations/0004_caller_validation.sql");
const EPHEMERAL_STREAM_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS mem.stream_events (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  kind       TEXT NOT NULL,
  data       TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS mem.idx_stream_events_session_id
  ON stream_events(session_id, id);

CREATE TABLE IF NOT EXISTS mem.stream_raw (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  chunk      BLOB NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS mem.idx_stream_raw_session_id
  ON stream_raw(session_id, id);
"#;

const CORE_REQUIRED_OBJECTS: &[(&str, &str)] = &[
    ("table", "agent_credentials"),
    ("table", "agent_runtimes"),
    ("table", "agent_groups"),
    ("table", "agent_group_members"),
    ("table", "messages"),
    ("table", "in_flight"),
    ("table", "threads"),
    ("table", "thread_members"),
    ("table", "topics"),
    ("table", "subscriptions"),
    ("table", "notifications"),
    ("table", "developer_event_topics"),
    ("table", "developer_events"),
    ("table", "command_intents"),
    ("table", "command_intent_events"),
    ("table", "command_queue_mutations"),
    ("table", "daemon_state"),
    ("table", "sources"),
    ("table", "agent_session_turns"),
    ("table", "agent_session_messages"),
    ("table", "agent_session_stream_cursors"),
    ("table", "messages_fts"),
    ("table", "agent_acl_grants"),
    ("table", "initial_prompt_deliveries"),
    ("table", "inbox_subscriptions"),
    ("table", "inbox_subscription_batches"),
    ("table", "producer_identities"),
    ("table", "transcript_archive"),
    ("table", "native_thread_bindings"),
    ("table", "sessions"),
    ("table", "agents"),
    ("table", "reply_contexts"),
    ("view", "nexus_broadcast_ingress"),
    ("index", "idx_agent_credentials_agent_active"),
    ("index", "idx_agent_runtimes_agent_active"),
    ("index", "idx_agent_runtimes_one_active"),
    ("index", "idx_agent_group_members_agent"),
    ("index", "idx_messages_to_created"),
    ("index", "idx_messages_thread"),
    ("index", "idx_messages_from"),
    ("index", "idx_messages_from_agent_created"),
    ("index", "idx_messages_to_agent_created"),
    ("index", "idx_messages_sender_idempotency"),
    ("index", "idx_in_flight_recipient_state"),
    ("index", "idx_in_flight_msg_recipient"),
    ("index", "idx_in_flight_agent_state"),
    ("index", "idx_in_flight_msg_agent"),
    ("index", "idx_in_flight_undrained"),
    ("index", "idx_in_flight_injecting"),
    ("index", "idx_in_flight_error"),
    ("index", "idx_thread_members_agent"),
    ("index", "idx_thread_members_thread_agent"),
    ("index", "idx_subscriptions_agent"),
    ("index", "idx_subscriptions_topic_agent"),
    ("index", "idx_developer_events_message"),
    ("index", "idx_command_intents_pending"),
    ("index", "idx_command_intents_lease"),
    ("index", "idx_command_intents_harness_claim_agent"),
    ("index", "idx_command_intents_harness_claim_name"),
    ("index", "idx_command_intents_idempotency_scope"),
    ("index", "idx_command_intent_events_session_seq"),
    ("index", "idx_command_queue_mutations_completed"),
    ("index", "idx_agent_session_turns_session_event"),
    ("index", "idx_agent_session_turns_streaming"),
    ("index", "idx_agent_session_messages_session_created"),
    ("index", "idx_agent_session_messages_turn"),
    ("index", "idx_agent_acl_grants_principal"),
    ("index", "idx_agent_acl_grants_principal_agent"),
    ("index", "idx_agent_acl_grants_grantor_agent"),
    ("index", "idx_initial_prompt_deliveries_session_status"),
    ("index", "idx_inbox_subscriptions_active"),
    ("index", "idx_inbox_subscription_batches_pending"),
    ("index", "idx_inbox_subscription_batches_pending_signature"),
    ("index", "idx_producer_identities_runtime_created"),
    ("index", "idx_transcript_archive_agent"),
    ("index", "idx_native_thread_bindings_agent"),
    ("index", "idx_native_thread_bindings_runtime"),
    ("index", "idx_sessions_name"),
    ("index", "idx_sessions_client_key"),
    ("index", "idx_sessions_harness"),
    ("index", "idx_sessions_project"),
    ("index", "idx_sessions_agent_id"),
    ("index", "idx_agents_name_unique"),
    ("index", "idx_agents_project_name"),
    ("index", "idx_agents_owner"),
    ("index", "idx_agents_owner_agent"),
    ("trigger", "trg_command_intent_queue_insert"),
    ("trigger", "trg_command_intent_queue_update"),
    ("trigger", "nexus_broadcast_ingress_insert"),
    ("trigger", "in_flight_reply_context_on_injecting"),
];

const IDENTITY_REQUIRED_TABLES: &[&str] = &[
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

impl Store {
    /// Clear the boot-scoped stream lane, then validate/bootstrap the durable baseline.
    ///
    /// Only the daemon may call this owner variant. Other processes call [`Store::migrate`] so a
    /// live shared stream file is never unlinked under its writer.
    pub async fn migrate_as_stream_owner(&self) -> Result<(), NexusError> {
        if let Some(path) = self.stream_db_path() {
            let stream_conn = self.stream_conn();
            if !attached_database_exists_on(&stream_conn, "mem").await? {
                clear_stale_stream_db_files(path)?;
            }
        }
        self.migrate().await
    }

    /// Bootstrap, upgrade, or validate a supported public schema.
    ///
    /// Pre-release and unknown databases are intentionally unsupported. Known public upgrades run
    /// in one write transaction and never discard existing rows.
    pub async fn migrate(&self) -> Result<(), NexusError> {
        match self.schema_marker().await? {
            Some(marker) if marker == CURRENT_SCHEMA_NAME => {
                self.validate_objects(CORE_REQUIRED_OBJECTS).await?;
                self.validate_message_hook_schema().await?;
                self.validate_delivery_timing_schema().await?;
                self.validate_caller_authority_schema().await?;
            }
            Some(marker) if marker == CALLER_PRINCIPAL_SCHEMA_NAME => {
                self.upgrade_caller_principal_to_authority().await?;
                self.validate_objects(CORE_REQUIRED_OBJECTS).await?;
                self.validate_message_hook_schema().await?;
                self.validate_delivery_timing_schema().await?;
                self.validate_caller_authority_schema().await?;
            }
            Some(marker) if marker == CALLER_VALIDATION_SCHEMA_NAME => {
                self.upgrade_caller_validation_to_authority().await?;
                self.validate_objects(CORE_REQUIRED_OBJECTS).await?;
                self.validate_message_hook_schema().await?;
                self.validate_delivery_timing_schema().await?;
                self.validate_caller_authority_schema().await?;
            }
            Some(marker) if marker == DELIVERY_TIMING_SCHEMA_NAME => {
                self.upgrade_delivery_timing_to_caller_authority().await?;
                self.validate_objects(CORE_REQUIRED_OBJECTS).await?;
                self.validate_message_hook_schema().await?;
                self.validate_delivery_timing_schema().await?;
                self.validate_caller_authority_schema().await?;
            }
            Some(marker) if marker == MESSAGE_HOOKS_SCHEMA_NAME => {
                self.upgrade_message_hooks_to_delivery_timing().await?;
                self.upgrade_delivery_timing_to_caller_authority().await?;
                self.validate_objects(CORE_REQUIRED_OBJECTS).await?;
                self.validate_message_hook_schema().await?;
                self.validate_delivery_timing_schema().await?;
                self.validate_caller_authority_schema().await?;
            }
            Some(marker) if marker == BASELINE_SCHEMA_NAME => {
                self.upgrade_v010_to_message_hooks().await?;
                self.upgrade_message_hooks_to_delivery_timing().await?;
                self.upgrade_delivery_timing_to_caller_authority().await?;
                self.validate_objects(CORE_REQUIRED_OBJECTS).await?;
                self.validate_message_hook_schema().await?;
                self.validate_delivery_timing_schema().await?;
                self.validate_caller_authority_schema().await?;
            }
            Some(marker) if marker == IDENTITY_SCHEMA_NAME => {
                self.validate_legacy_identity_schema().await?;
                self.upgrade_identity_provider(MigrationFault::None).await?;
                self.validate_identity_provider_schema().await?;
                self.upgrade_identity_caller_principal().await?;
                self.upgrade_identity_caller_validation().await?;
                self.validate_caller_authority_schema().await?;
            }
            Some(marker) if marker == IDENTITY_PROVIDER_SCHEMA_NAME => {
                self.validate_identity_schema().await?;
                self.validate_identity_provider_schema().await?;
                self.upgrade_identity_caller_principal().await?;
                self.upgrade_identity_caller_validation().await?;
                self.validate_caller_authority_schema().await?;
            }
            Some(marker) if marker == IDENTITY_CALLER_PRINCIPAL_SCHEMA_NAME => {
                self.validate_identity_schema().await?;
                self.validate_identity_provider_schema().await?;
                self.upgrade_identity_caller_validation().await?;
                self.validate_caller_authority_schema().await?;
            }
            Some(marker) if marker == IDENTITY_MODEL_REPORT_SCHEMA_NAME => {
                self.validate_identity_schema().await?;
                self.validate_identity_provider_schema().await?;
                self.validate_caller_authority_schema().await?;
                self.validate_model_report_schema().await?;
            }
            Some(marker) if marker == TRANSPORT_SCHEMA_NAME => {}
            Some(marker) => return Err(unsupported_schema(&marker)),
            None if self.has_schema_migrations_table().await? => {
                return Err(unsupported_schema("pre-release migration ladder"));
            }
            None if self.user_schema_is_empty().await? => {
                self.install_baseline().await?;
                self.validate_objects(CORE_REQUIRED_OBJECTS).await?;
                self.validate_message_hook_schema().await?;
                self.validate_delivery_timing_schema().await?;
                self.validate_caller_authority_schema().await?;
            }
            None => {
                return Err(NexusError::Store(
                    "unrecognized non-empty Nexus database; refusing to modify it. Back it up and start with a fresh v0.1.0 store".into(),
                ));
            }
        }

        if self.object_exists("table", "agent_runtimes").await? {
            self.upgrade_model_report(MigrationFault::None).await?;
        }
        self.ensure_ephemeral_stream_schema().await
    }

    async fn validate_model_report_schema(&self) -> Result<(), NexusError> {
        validate_model_report_schema_rows(
            self.conn
                .query("PRAGMA table_info(agent_runtimes)", ())
                .await
                .map_err(store_err)?,
            self.conn
                .query("PRAGMA table_info(retired_model_runtime_ids)", ())
                .await
                .map_err(store_err)?,
        )
        .await
    }

    /// Add model authority without rewriting any published baseline SQL. Compatibility stores
    /// keep their existing ledger; split identity stores receive the new named marker atomically.
    async fn upgrade_model_report(&self, fault: MigrationFault) -> Result<(), NexusError> {
        let identity = self
            .schema_marker()
            .await?
            .is_some_and(|name| name == IDENTITY_CALLER_PRINCIPAL_SCHEMA_NAME);
        let mut present = 0;
        for column in [
            "model_observer_token",
            "model_observer_sequence",
            "model_report_revision",
            "model_report_json",
        ] {
            present += usize::from(self.column_exists("agent_runtimes", column).await?);
        }
        if present != 0 && present != 4 {
            return Err(NexusError::Store(
                "incomplete model report authority schema".into(),
            ));
        }
        if present == 4 && !identity {
            return self.validate_model_report_schema().await;
        }
        let tx = self
            .begin_write_txn("v016_identity_model_report_migration")
            .await?;
        let result = async {
            if present == 0 {
                for (index, sql) in [
                    "ALTER TABLE agent_runtimes ADD COLUMN model_observer_token TEXT",
                    "ALTER TABLE agent_runtimes ADD COLUMN model_observer_sequence INTEGER NOT NULL DEFAULT 0",
                    "ALTER TABLE agent_runtimes ADD COLUMN model_report_revision INTEGER NOT NULL DEFAULT 0",
                    "ALTER TABLE agent_runtimes ADD COLUMN model_report_json TEXT",
                ].into_iter().enumerate() {
                    tx.execute(sql, ()).await?;
                    if index == 0 && fault == MigrationFault::AfterFirstStatement {
                        return Err(NexusError::Store(
                            "injected identity migration fault after first statement".into()
                        ));
                    }
                }
            }
            tx.execute(
                "CREATE TABLE IF NOT EXISTS retired_model_runtime_ids \
                 (runtime_id TEXT PRIMARY KEY NOT NULL)",
                (),
            ).await?;
            // CREATE IF NOT EXISTS does not establish the shape of a preexisting table.
            // Validate through this pinned transaction before accepting the new marker.
            validate_model_report_schema_rows(
                tx.query("PRAGMA table_info(agent_runtimes)", ()).await?,
                tx.query("PRAGMA table_info(retired_model_runtime_ids)", ()).await?,
            ).await?;
            if identity {
                tx.execute("DELETE FROM schema_migrations", ()).await?;
                tx.execute(
                    "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?1, ?2, ?3)",
                    libsql::params![BASELINE_SCHEMA_VERSION, IDENTITY_MODEL_REPORT_SCHEMA_NAME, now()],
                ).await?;
            }
            Ok(())
        }.await;
        match result {
            Ok(()) => tx.commit().await,
            Err(error) => rollback_error(tx, error).await,
        }
    }

    pub(crate) async fn mark_schema_variant(&self, name: &str) -> Result<(), NexusError> {
        let tx = self.begin_write_txn("schema_variant_marker").await?;
        if let Err(error) = tx.execute("DELETE FROM schema_migrations", ()).await {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx
            .execute(
                "INSERT INTO schema_migrations(version, name, applied_at)
                 VALUES (?1, ?2, ?3)",
                libsql::params![BASELINE_SCHEMA_VERSION, name, now()],
            )
            .await
        {
            return rollback_error(tx, error).await;
        }
        tx.commit().await
    }

    async fn install_baseline(&self) -> Result<(), NexusError> {
        let tx = self.begin_write_txn("v010_schema_baseline").await?;
        if let Err(error) = tx
            .execute_batch(
                "CREATE TABLE schema_migrations (
                   version INTEGER PRIMARY KEY,
                   name TEXT NOT NULL,
                   applied_at INTEGER NOT NULL
                 );",
            )
            .await
        {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx.execute_batch(BASELINE_SCHEMA).await {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx.execute_batch(MESSAGE_HOOKS_SCHEMA).await {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx.execute_batch(DELIVERY_TIMING_SCHEMA).await {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx
            .execute(
                "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?1, ?2, ?3)",
                libsql::params![BASELINE_SCHEMA_VERSION, BASELINE_SCHEMA_NAME, now()],
            )
            .await
        {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx
            .execute(
                "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?1, ?2, ?3)",
                libsql::params![
                    MESSAGE_HOOKS_SCHEMA_VERSION,
                    MESSAGE_HOOKS_SCHEMA_NAME,
                    now()
                ],
            )
            .await
        {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx
            .execute(
                "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?1, ?2, ?3)",
                libsql::params![
                    DELIVERY_TIMING_SCHEMA_VERSION,
                    DELIVERY_TIMING_SCHEMA_NAME,
                    now()
                ],
            )
            .await
        {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx
            .execute(
                "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?1, ?2, ?3)",
                libsql::params![
                    CALLER_PRINCIPAL_SCHEMA_VERSION,
                    CALLER_PRINCIPAL_SCHEMA_NAME,
                    now()
                ],
            )
            .await
        {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx
            .execute(
                "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?1, ?2, ?3)",
                libsql::params![CURRENT_SCHEMA_VERSION, CURRENT_SCHEMA_NAME, now()],
            )
            .await
        {
            return rollback_error(tx, error).await;
        }
        tx.commit().await
    }

    async fn upgrade_v010_to_message_hooks(&self) -> Result<(), NexusError> {
        let tx = self.begin_write_txn("v015_message_hooks_schema").await?;
        if let Err(error) = tx.execute_batch(MESSAGE_HOOKS_SCHEMA).await {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx
            .execute(
                "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?1, ?2, ?3)",
                libsql::params![
                    MESSAGE_HOOKS_SCHEMA_VERSION,
                    MESSAGE_HOOKS_SCHEMA_NAME,
                    now()
                ],
            )
            .await
        {
            return rollback_error(tx, error).await;
        }
        tx.commit().await
    }

    async fn upgrade_message_hooks_to_delivery_timing(&self) -> Result<(), NexusError> {
        let tx = self.begin_write_txn("v015_delivery_timing_schema").await?;
        if let Err(error) = tx.execute_batch(DELIVERY_TIMING_SCHEMA).await {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx
            .execute(
                "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?1, ?2, ?3)",
                libsql::params![
                    DELIVERY_TIMING_SCHEMA_VERSION,
                    DELIVERY_TIMING_SCHEMA_NAME,
                    now()
                ],
            )
            .await
        {
            return rollback_error(tx, error).await;
        }
        tx.commit().await
    }

    async fn upgrade_delivery_timing_to_caller_authority(&self) -> Result<(), NexusError> {
        let tx = self.begin_write_txn("v016_caller_authority_schema").await?;
        if let Err(error) = tx.execute_batch(CALLER_PRINCIPAL_SCHEMA).await {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx.execute_batch(CALLER_VALIDATION_SCHEMA).await {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx
            .execute(
                "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?1, ?2, ?3)",
                libsql::params![
                    CALLER_PRINCIPAL_SCHEMA_VERSION,
                    CALLER_PRINCIPAL_SCHEMA_NAME,
                    now()
                ],
            )
            .await
        {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx
            .execute(
                "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?1, ?2, ?3)",
                libsql::params![CURRENT_SCHEMA_VERSION, CURRENT_SCHEMA_NAME, now()],
            )
            .await
        {
            return rollback_error(tx, error).await;
        }
        tx.commit().await
    }

    async fn upgrade_caller_principal_to_authority(&self) -> Result<(), NexusError> {
        let tx = self
            .begin_write_txn("v016_caller_principal_to_authority")
            .await?;
        if let Err(error) = tx.execute_batch(CALLER_VALIDATION_SCHEMA).await {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx
            .execute(
                "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?1, ?2, ?3)",
                libsql::params![CURRENT_SCHEMA_VERSION, CURRENT_SCHEMA_NAME, now()],
            )
            .await
        {
            return rollback_error(tx, error).await;
        }
        tx.commit().await
    }

    async fn upgrade_caller_validation_to_authority(&self) -> Result<(), NexusError> {
        let tx = self
            .begin_write_txn("v015_caller_validation_to_v016_authority")
            .await?;
        if let Err(error) = tx.execute_batch(CALLER_PRINCIPAL_SCHEMA).await {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx
            .execute(
                "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?1, ?2, ?3)",
                libsql::params![CURRENT_SCHEMA_VERSION, CURRENT_SCHEMA_NAME, now()],
            )
            .await
        {
            return rollback_error(tx, error).await;
        }
        tx.commit().await
    }

    async fn upgrade_identity_caller_validation(&self) -> Result<(), NexusError> {
        if self
            .column_exists("command_intents", "caller_validated_boot_epoch")
            .await?
        {
            return Ok(());
        }
        let tx = self
            .begin_write_txn("v015_identity_caller_validation_schema")
            .await?;
        if let Err(error) = tx.execute_batch(CALLER_VALIDATION_SCHEMA).await {
            return rollback_error(tx, error).await;
        }
        tx.commit().await
    }

    async fn schema_marker(&self) -> Result<Option<String>, NexusError> {
        if !self.has_schema_migrations_table().await? {
            return Ok(None);
        }
        if !self.schema_migrations_has_name().await? {
            return Ok(None);
        }
        let mut rows = self
            .conn
            .query(
                "SELECT version, name FROM schema_migrations ORDER BY version",
                (),
            )
            .await
            .map_err(store_err)?;
        let mut markers = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            markers.push((
                row.get::<i64>(0).map_err(store_err)?,
                row.get::<String>(1).map_err(store_err)?,
            ));
        }
        let Some((first_version, _)) = markers.first() else {
            return Ok(Some("empty migration ledger".into()));
        };
        if markers.iter().all(|(_, name)| name == IDENTITY_SCHEMA_NAME) {
            return Ok(Some(IDENTITY_SCHEMA_NAME.into()));
        }
        if markers
            .iter()
            .all(|(_, name)| name == IDENTITY_PROVIDER_SCHEMA_NAME)
        {
            return Ok(Some(IDENTITY_PROVIDER_SCHEMA_NAME.into()));
        }
        if markers
            .iter()
            .all(|(_, name)| name == IDENTITY_CALLER_PRINCIPAL_SCHEMA_NAME)
        {
            return Ok(Some(IDENTITY_CALLER_PRINCIPAL_SCHEMA_NAME.into()));
        }
        if markers
            .iter()
            .all(|(_, name)| name == TRANSPORT_SCHEMA_NAME)
        {
            return Ok(Some(TRANSPORT_SCHEMA_NAME.into()));
        }
        let marker = match markers.as_slice() {
            [(BASELINE_SCHEMA_VERSION, name)] => name.clone(),
            [(BASELINE_SCHEMA_VERSION, _), (MESSAGE_HOOKS_SCHEMA_VERSION, name)] => name.clone(),
            [(BASELINE_SCHEMA_VERSION, _), (MESSAGE_HOOKS_SCHEMA_VERSION, _), (DELIVERY_TIMING_SCHEMA_VERSION, name)] => {
                name.clone()
            }
            [(BASELINE_SCHEMA_VERSION, _), (MESSAGE_HOOKS_SCHEMA_VERSION, _), (DELIVERY_TIMING_SCHEMA_VERSION, _), (CALLER_PRINCIPAL_SCHEMA_VERSION, name)] => {
                name.clone()
            }
            [(BASELINE_SCHEMA_VERSION, _), (MESSAGE_HOOKS_SCHEMA_VERSION, _), (DELIVERY_TIMING_SCHEMA_VERSION, _), (CALLER_PRINCIPAL_SCHEMA_VERSION, _), (CURRENT_SCHEMA_VERSION, name)] => {
                name.clone()
            }
            _ => {
                return Ok(Some(format!(
                    "pre-release migration ladder ending at {first_version}"
                )));
            }
        };
        Ok(Some(marker))
    }

    async fn has_schema_migrations_table(&self) -> Result<bool, NexusError> {
        self.object_exists("table", "schema_migrations").await
    }

    async fn schema_migrations_has_name(&self) -> Result<bool, NexusError> {
        let mut rows = self
            .conn
            .query("PRAGMA table_info(schema_migrations)", ())
            .await
            .map_err(store_err)?;
        while let Some(row) = rows.next().await.map_err(store_err)? {
            let name: String = row.get(1).map_err(store_err)?;
            if name == "name" {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn user_schema_is_empty(&self) -> Result<bool, NexusError> {
        let mut rows = self
            .conn
            .query(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type IN ('table', 'view', 'trigger', 'index')
                   AND name NOT LIKE 'sqlite_%'",
                (),
            )
            .await
            .map_err(store_err)?;
        let count = rows
            .next()
            .await
            .map_err(store_err)?
            .map(|row| row.get::<i64>(0).map_err(store_err))
            .transpose()?
            .unwrap_or(0);
        Ok(count == 0)
    }

    async fn validate_objects(&self, objects: &[(&str, &str)]) -> Result<(), NexusError> {
        for (kind, name) in objects {
            if !self.object_exists(kind, name).await? {
                return Err(NexusError::Store(format!(
                    "incomplete v0.1.0 Nexus schema: missing {kind} {name}"
                )));
            }
        }
        Ok(())
    }

    async fn validate_identity_schema(&self) -> Result<(), NexusError> {
        for table in IDENTITY_REQUIRED_TABLES {
            if !self.object_exists("table", table).await? {
                return Err(NexusError::Store(format!(
                    "incomplete v0.1.0 identity schema: missing table {table}"
                )));
            }
        }
        Ok(())
    }

    async fn validate_legacy_identity_schema(&self) -> Result<(), NexusError> {
        self.validate_identity_schema().await?;
        if !self
            .column_exists("native_thread_bindings", "harness")
            .await?
            || self
                .column_exists("native_thread_bindings", "provider")
                .await?
            || self.column_exists("native_thread_bindings", "kind").await?
        {
            return Err(NexusError::Store(
                "incomplete v0.1.0 identity schema: native_thread_bindings must have harness and no provider/kind columns"
                    .into(),
            ));
        }
        Ok(())
    }

    async fn validate_identity_provider_schema(&self) -> Result<(), NexusError> {
        for column in ["provider", "kind"] {
            if !self.column_exists("native_thread_bindings", column).await? {
                return Err(NexusError::Store(format!(
                    "incomplete {IDENTITY_PROVIDER_SCHEMA_NAME} identity schema: missing native_thread_bindings.{column}"
                )));
            }
        }
        if self
            .column_exists("native_thread_bindings", "harness")
            .await?
        {
            return Err(NexusError::Store(format!(
                "incomplete {IDENTITY_PROVIDER_SCHEMA_NAME} identity schema: legacy native_thread_bindings.harness remains"
            )));
        }
        Ok(())
    }

    async fn upgrade_identity_provider(&self, fault: MigrationFault) -> Result<(), NexusError> {
        let tx = self
            .begin_write_txn("v016_identity_provider_migration")
            .await?;
        if let Err(error) = tx
            .execute(
                "ALTER TABLE native_thread_bindings RENAME COLUMN harness TO provider",
                (),
            )
            .await
        {
            return rollback_error(tx, error).await;
        }
        if fault == MigrationFault::AfterFirstStatement {
            return rollback_error(
                tx,
                NexusError::Store("injected identity migration fault after first statement".into()),
            )
            .await;
        }
        if let Err(error) = tx
            .execute(
                "ALTER TABLE native_thread_bindings ADD COLUMN kind TEXT NOT NULL DEFAULT 'harness'",
                (),
            )
            .await
        {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx.execute("DELETE FROM schema_migrations", ()).await {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx
            .execute(
                "INSERT INTO schema_migrations(version, name, applied_at)
                 VALUES (?1, ?2, ?3)",
                libsql::params![
                    BASELINE_SCHEMA_VERSION,
                    IDENTITY_PROVIDER_SCHEMA_NAME,
                    now()
                ],
            )
            .await
        {
            return rollback_error(tx, error).await;
        }
        tx.commit().await
    }

    async fn upgrade_identity_caller_principal(&self) -> Result<(), NexusError> {
        let tx = self
            .begin_write_txn("v016_identity_caller_principal_migration")
            .await?;
        if let Err(error) = tx.execute_batch(CALLER_PRINCIPAL_SCHEMA).await {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx.execute("DELETE FROM schema_migrations", ()).await {
            return rollback_error(tx, error).await;
        }
        if let Err(error) = tx
            .execute(
                "INSERT INTO schema_migrations(version, name, applied_at)
                 VALUES (?1, ?2, ?3)",
                libsql::params![
                    BASELINE_SCHEMA_VERSION,
                    IDENTITY_CALLER_PRINCIPAL_SCHEMA_NAME,
                    now()
                ],
            )
            .await
        {
            return rollback_error(tx, error).await;
        }
        tx.commit().await
    }

    async fn validate_message_hook_schema(&self) -> Result<(), NexusError> {
        for (object, column) in [
            ("messages", "mention_json"),
            ("nexus_broadcast_ingress", "metadata_json"),
            ("nexus_broadcast_ingress", "mention_json"),
        ] {
            if !self.column_exists(object, column).await? {
                return Err(NexusError::Store(format!(
                    "incomplete {CURRENT_SCHEMA_NAME} Nexus schema: missing {object}.{column}"
                )));
            }
        }
        Ok(())
    }

    async fn validate_delivery_timing_schema(&self) -> Result<(), NexusError> {
        for (object, column) in [
            ("in_flight", "delivery_timing"),
            ("nexus_broadcast_ingress", "delivery_timing"),
        ] {
            if !self.column_exists(object, column).await? {
                return Err(NexusError::Store(format!(
                    "incomplete {CURRENT_SCHEMA_NAME} Nexus schema: missing {object}.{column}"
                )));
            }
        }
        Ok(())
    }

    async fn validate_caller_principal_schema(&self) -> Result<(), NexusError> {
        if !self
            .column_exists("command_intents", "caller_principal_id")
            .await?
        {
            return Err(NexusError::Store(format!(
                "incomplete {CURRENT_SCHEMA_NAME} Nexus schema: missing command_intents.caller_principal_id"
            )));
        }
        Ok(())
    }

    async fn validate_caller_validation_schema(&self) -> Result<(), NexusError> {
        if !self
            .column_exists("command_intents", "caller_validated_boot_epoch")
            .await?
        {
            return Err(NexusError::Store(format!(
                "incomplete {CURRENT_SCHEMA_NAME} Nexus schema: missing command_intents.caller_validated_boot_epoch"
            )));
        }
        Ok(())
    }

    async fn validate_caller_authority_schema(&self) -> Result<(), NexusError> {
        self.validate_caller_principal_schema().await?;
        self.validate_caller_validation_schema().await
    }

    async fn column_exists(&self, object: &str, column: &str) -> Result<bool, NexusError> {
        let escaped = object.replace('"', "\"\"");
        let mut rows = self
            .conn
            .query(&format!("PRAGMA table_info(\"{escaped}\")"), ())
            .await
            .map_err(store_err)?;
        while let Some(row) = rows.next().await.map_err(store_err)? {
            if row.get::<String>(1).map_err(store_err)? == column {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn object_exists(&self, kind: &str, name: &str) -> Result<bool, NexusError> {
        let mut rows = self
            .conn
            .query(
                "SELECT 1 FROM sqlite_master WHERE type = ?1 AND name = ?2 LIMIT 1",
                libsql::params![kind, name],
            )
            .await
            .map_err(store_err)?;
        Ok(rows.next().await.map_err(store_err)?.is_some())
    }

    async fn ensure_ephemeral_stream_schema(&self) -> Result<(), NexusError> {
        let stream_conn = self.stream_conn();
        if !attached_database_exists_on(&stream_conn, "mem").await? {
            if let Some(path) = self.stream_db_path() {
                prepare_stream_db_file(path)?;
                let sql = format!("ATTACH DATABASE {} AS mem", sqlite_quote(path));
                stream_conn.execute(&sql, ()).await.map_err(store_err)?;
                drain_pragma(&stream_conn, "PRAGMA mem.journal_mode=WAL").await?;
                drain_pragma(&stream_conn, "PRAGMA mem.busy_timeout=5000").await?;
                drain_pragma(&stream_conn, "PRAGMA mem.synchronous=NORMAL").await?;
            } else {
                stream_conn
                    .execute("ATTACH DATABASE ':memory:' AS mem", ())
                    .await
                    .map_err(store_err)?;
            }
        }
        stream_conn
            .execute_batch(EPHEMERAL_STREAM_SCHEMA)
            .await
            .map_err(store_err)?;
        Ok(())
    }
}

/// Migrate one legacy identity database through the externally reachable fault seam.
#[doc(hidden)]
pub async fn migrate_identity_with_fault(
    identity_db_path: &Path,
    fault: MigrationFault,
) -> Result<(), NexusError> {
    let location = identity_db_path
        .to_str()
        .ok_or_else(|| NexusError::Invalid("identity database path is not valid UTF-8".into()))?;
    let store = Store::open(location).await?;
    match store.schema_marker().await? {
        Some(marker) if marker == IDENTITY_SCHEMA_NAME => {
            store.validate_legacy_identity_schema().await?;
            store.upgrade_identity_provider(fault).await?;
            store.validate_identity_provider_schema().await?;
            store.upgrade_identity_caller_principal().await?;
            store.upgrade_identity_caller_validation().await?;
            store.validate_caller_authority_schema().await
        }
        Some(marker) if marker == IDENTITY_PROVIDER_SCHEMA_NAME => {
            if fault != MigrationFault::None {
                return Err(NexusError::Invalid(
                    "identity migration fault requires a legacy identity schema".into(),
                ));
            }
            store.validate_identity_schema().await?;
            store.validate_identity_provider_schema().await?;
            store.upgrade_identity_caller_principal().await?;
            store.upgrade_identity_caller_validation().await?;
            store.validate_caller_authority_schema().await
        }
        Some(marker) if marker == IDENTITY_CALLER_PRINCIPAL_SCHEMA_NAME => {
            store.validate_identity_schema().await?;
            store.validate_identity_provider_schema().await?;
            store.upgrade_identity_caller_validation().await?;
            store.validate_caller_authority_schema().await?;
            store.upgrade_model_report(fault).await
        }
        Some(marker) if marker == IDENTITY_MODEL_REPORT_SCHEMA_NAME => {
            store.validate_model_report_schema().await
        }
        Some(marker) => Err(unsupported_schema(&marker)),
        None => Err(unsupported_schema("missing identity schema marker")),
    }
}

/// One validator for ordinary reopen and transactional upgrade; consumes both schema cursors
/// before the caller can commit or perform further migration writes.
async fn validate_model_report_schema_rows(
    mut runtime_columns: libsql::Rows,
    mut retired_columns: libsql::Rows,
) -> Result<(), NexusError> {
    let mut names = Vec::new();
    while let Some(row) = runtime_columns.next().await.map_err(store_err)? {
        names.push(row.get::<String>(1).map_err(store_err)?);
    }
    for column in [
        "model_observer_token",
        "model_observer_sequence",
        "model_report_revision",
        "model_report_json",
    ] {
        if !names.iter().any(|name| name == column) {
            return Err(NexusError::Store(format!(
                "incomplete {IDENTITY_MODEL_REPORT_SCHEMA_NAME} schema: \
                 missing agent_runtimes.{column}"
            )));
        }
    }
    let row = retired_columns
        .next()
        .await
        .map_err(store_err)?
        .ok_or_else(|| {
            NexusError::Store("missing retired model runtime identity authority".into())
        })?;
    if row.get::<String>(1).map_err(store_err)? != "runtime_id"
        || row.get::<String>(2).map_err(store_err)? != "TEXT"
        || row.get::<i64>(3).map_err(store_err)? != 1
        || row.get::<i64>(5).map_err(store_err)? != 1
        || retired_columns.next().await.map_err(store_err)?.is_some()
    {
        return Err(NexusError::Store(
            "invalid retired model runtime identity authority".into(),
        ));
    }
    Ok(())
}

async fn rollback_error(tx: crate::state::WriteTxn, error: NexusError) -> Result<(), NexusError> {
    match tx.rollback(&error).await {
        Ok(()) => Err(error),
        Err(rollback_error) => Err(rollback_error),
    }
}

fn unsupported_schema(marker: &str) -> NexusError {
    NexusError::Store(format!(
        "unsupported Nexus database schema ({marker}); only published forward migrations are applied automatically. Back up the store before replacing it"
    ))
}

async fn attached_database_exists_on(
    conn: &libsql::Connection,
    schema: &str,
) -> Result<bool, NexusError> {
    let mut rows = conn
        .query("PRAGMA database_list", ())
        .await
        .map_err(store_err)?;
    while let Some(row) = rows.next().await.map_err(store_err)? {
        let name: String = row.get(1).map_err(store_err)?;
        if name == schema {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Resolve an explicitly configured cross-process stream DB path.
///
/// The daemon's normal transport authority is anonymous memory. A named stream file remains an
/// opt-in seam for tests and nonstandard deployments that deliberately provide one.
pub fn resolve_stream_db_path() -> Option<String> {
    std::env::var("NEXUS_STREAM_DB_PATH")
        .ok()
        .map(|path| path.trim().to_string())
        .filter(|path| !path.is_empty())
}

fn prepare_stream_db_file(path: &str) -> Result<(), NexusError> {
    let path = Path::new(path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            NexusError::Store(format!("create stream db dir {parent:?}: {error}"))
        })?;
    }
    if !path.exists() {
        match create_0600(path) {
            Ok(()) => {}
            Err(_) if path.exists() => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn clear_stale_stream_db_files(path: &str) -> Result<(), NexusError> {
    for suffix in ["", "-wal", "-shm"] {
        let stale = PathBuf::from(format!("{path}{suffix}"));
        match std::fs::remove_file(&stale) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(NexusError::Store(format!(
                    "remove stale stream db file {stale:?}: {error}"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
fn create_0600(path: &Path) -> Result<(), NexusError> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| NexusError::Store(format!("create stream db file {path:?}: {error}")))?;
    Ok(())
}

#[cfg(not(unix))]
fn create_0600(path: &Path) -> Result<(), NexusError> {
    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| NexusError::Store(format!("create stream db file {path:?}: {error}")))?;
    Ok(())
}

fn sqlite_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

async fn drain_pragma(conn: &libsql::Connection, pragma: &str) -> Result<(), NexusError> {
    let mut rows = conn.query(pragma, ()).await.map_err(store_err)?;
    while rows.next().await.map_err(store_err)?.is_some() {}
    Ok(())
}
