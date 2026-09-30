#![cfg(test)]

use super::*;

#[test]
fn broadcast_ingress_is_one_parameterized_statement() {
    assert!(BROADCAST_INGRESS_SQL.starts_with("INSERT INTO nexus_broadcast_ingress"));
    assert_eq!(BROADCAST_INGRESS_SQL.matches('?').count(), 17);
    assert!(!BROADCAST_INGRESS_SQL.contains("BEGIN"));
    assert!(!BROADCAST_INGRESS_SQL.contains("COMMIT"));
}

#[test]
fn literal_escapes_single_quotes() {
    assert_eq!(literal("a'b"), "'a''b'");
}

#[test]
fn opt_literal_maps_none_to_null() {
    assert_eq!(opt_literal(None), "NULL");
    assert_eq!(opt_literal(Some("ada")), "'ada'");
}
