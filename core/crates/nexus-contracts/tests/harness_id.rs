//! Contract tests for the open-set `HarnessId` newtype.

use nexus_contracts::{HarnessId, HARNESS_ID_MAX_LEN};

#[test]
fn accepts_known_and_unknown_lowercase_tokens() {
    for tok in [
        "claude",
        "codex",
        "opencode",
        "hermes",
        "pi",
        "other",
        "cursor-agent",
        "my_custom2",
        "a",
    ] {
        let id = HarnessId::new(tok).expect(tok);
        assert_eq!(id.as_str(), tok);
        assert_eq!(id.to_string(), tok);
    }
}

#[test]
fn rejects_bad_tokens_fail_closed() {
    for tok in [
        "",
        "Claude",
        "CLAUDE",
        "1claude",
        "-claude",
        "_x",
        "cla ude",
        "cla.ude",
        "claude!",
        "clÅude",
        &"x".repeat(HARNESS_ID_MAX_LEN + 1),
    ] {
        assert!(HarnessId::new(tok).is_err(), "should reject {tok:?}");
    }
    // Exactly at the ceiling is fine.
    assert!(HarnessId::new("x".repeat(HARNESS_ID_MAX_LEN)).is_ok());
}

#[test]
fn serializes_as_bare_string_and_validates_on_deserialize() {
    let id = HarnessId::new("claude").unwrap();
    assert_eq!(serde_json::to_string(&id).unwrap(), "\"claude\"");

    let round: HarnessId = serde_json::from_str("\"cursor-agent\"").unwrap();
    assert_eq!(round.as_str(), "cursor-agent");

    // Invalid wire input is rejected at the boundary.
    assert!(serde_json::from_str::<HarnessId>("\"Not A Token\"").is_err());
    assert!(serde_json::from_str::<HarnessId>("\"\"").is_err());
    assert!(serde_json::from_str::<HarnessId>("42").is_err());
}

#[test]
fn historical_wire_tokens_round_trip() {
    // The tokens that used to be closed-enum variants must keep working as
    // plain open-set ids with identical wire encoding.
    for tok in ["claude", "codex", "opencode", "hermes", "pi", "other"] {
        let id = HarnessId::new(tok).expect(tok);
        assert_eq!(serde_json::to_string(&id).unwrap(), format!("\"{tok}\""));
        let round: HarnessId = serde_json::from_str(&format!("\"{tok}\"")).unwrap();
        assert_eq!(round, id);
    }
}

#[test]
fn from_str_parses() {
    let id: HarnessId = "hermes".parse().unwrap();
    assert_eq!(id.as_str(), "hermes");
    assert!("Hermes".parse::<HarnessId>().is_err());
}
