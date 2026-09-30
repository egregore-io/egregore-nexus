/// Single parameterized entrypoint for the schema-owned atomic broadcast trigger.
pub(crate) const BROADCAST_INGRESS_SQL: &str = "INSERT INTO nexus_broadcast_ingress ( \
       message_id, from_name, kind, to_name, thread_id, topic, summary, body, provenance, \
       project, created_at, from_agent_id, to_agent_id, sender_session_id, idempotency_key, \
       recipients_json, events_json \
     ) VALUES ( \
       ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17 \
     )";

pub(crate) fn push_stmt(batch: &mut String, stmt: String) {
    batch.push_str(&stmt);
    batch.push_str(";\n");
}

pub(crate) fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(crate) fn opt_literal(value: Option<&str>) -> String {
    value.map(literal).unwrap_or_else(|| "NULL".to_string())
}

#[path = "../tests/unit/sql_batch.rs"]
mod sql_batch_contracts;
