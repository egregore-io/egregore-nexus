use nexus::cli::ambient::with_test_env_vars;
use nexus_contracts::{Harness, Kind, Tier};

#[test]
fn ambient_identity_reads_human_kind_and_admin_tier() {
    with_test_env_vars(
        &[
            ("NEXUS_NAME", Some("Alex Morgan")),
            ("NEXUS_CLIENT_KEY", Some("ck_human")),
            ("NEXUS_PROJECT", Some("default")),
            ("NEXUS_AGENT", Some("other")),
            ("NEXUS_TIER", Some("admin")),
            ("NEXUS_KIND", Some("human")),
        ],
        || {
            let identity = nexus::cli::ambient::identity_from_env().expect("ambient identity");
            assert_eq!(identity.name.as_deref(), Some("Alex Morgan"));
            assert_eq!(identity.client_key, "ck_human");
            assert_eq!(identity.project, "default");
            assert_eq!(identity.harness, Harness::Other);
            assert_eq!(identity.tier, Tier::Admin);
            assert_eq!(identity.kind, Some(Kind::Human));
        },
    );
}

#[test]
fn ambient_identity_prefers_stable_session_id_over_client_key() {
    with_test_env_vars(
        &[
            ("NEXUS_NAME", Some("Alex Morgan")),
            ("NEXUS_CLIENT_KEY", Some("ck_human")),
            ("NEXUS_SESSION_ID", Some("stable-runtime-session")),
            ("CLAUDE_CODE_SESSION_ID", Some("claude-session-ignored")),
            ("NEXUS_PROJECT", Some("default")),
            ("NEXUS_AGENT", Some("other")),
            ("NEXUS_TIER", Some("admin")),
            ("NEXUS_KIND", Some("human")),
        ],
        || {
            let identity = nexus::cli::ambient::identity_from_env().expect("ambient identity");
            assert_eq!(identity.name.as_deref(), Some("Alex Morgan"));
            assert_eq!(identity.client_key, "ck_human");
            assert_eq!(identity.harness_session_id, "stable-runtime-session");
            assert_eq!(identity.project, "default");
            assert_eq!(identity.harness, Harness::Other);
            assert_eq!(identity.tier, Tier::Admin);
            assert_eq!(identity.kind, Some(Kind::Human));
        },
    );
}

#[test]
fn ambient_identity_does_not_use_claude_code_identity_as_nexus_identity() {
    with_test_env_vars(
        &[
            ("NEXUS_NAME", Some("clara")),
            ("NEXUS_CLIENT_KEY", Some("ck_clara")),
            ("NEXUS_SESSION_ID", None),
            ("CLAUDE_CODE_SESSION_ID", Some("claude-provider-session")),
            ("NEXUS_PROJECT", Some("default")),
            ("NEXUS_AGENT", Some("claude")),
        ],
        || {
            let identity = nexus::cli::ambient::identity_from_env().expect("ambient identity");
            assert_eq!(identity.client_key, "ck_clara");
            assert_eq!(identity.harness_session_id, "hs_ck_clara");
        },
    );
}

#[test]
fn ambient_identity_accepts_staged_agent_id_without_name() {
    with_test_env_vars(
        &[
            ("NEXUS_NAME", None),
            ("NEXUS_AGENT_ID", Some("a_s_staged")),
            ("NEXUS_CLIENT_KEY", Some("ck_staged")),
            ("NEXUS_SESSION_ID", Some("s_staged")),
            ("NEXUS_PROJECT", Some("default")),
            ("NEXUS_AGENT", Some("codex")),
            ("NEXUS_TIER", Some("")),
            ("NEXUS_KIND", None),
        ],
        || {
            let identity = nexus::cli::ambient::identity_from_env().expect("ambient identity");
            assert_eq!(identity.name, None);
            assert_eq!(
                identity.agent_id.as_ref().map(|id| id.0.as_str()),
                Some("a_s_staged")
            );
            assert_eq!(identity.client_key, "ck_staged");
            assert_eq!(identity.harness_session_id, "s_staged");
            assert_eq!(identity.project, "default");
            assert_eq!(identity.harness, Harness::Codex);
            assert_eq!(identity.tier, Tier::Agent);
            assert_eq!(identity.kind, Some(Kind::Agent));
        },
    );
}

#[test]
fn ambient_identity_rejects_name_without_client_key() {
    with_test_env_vars(
        &[
            ("NEXUS_NAME", Some("remy")),
            ("NEXUS_CLIENT_KEY", None),
            ("NEXUS_AGENT_ID", None),
            ("NEXUS_SESSION_ID", Some("s_remy")),
            ("NEXUS_PROJECT", Some("default")),
            ("NEXUS_AGENT", Some("claude")),
        ],
        || {
            let error =
                nexus::cli::ambient::identity_from_env_result().expect_err("must fail closed");
            assert_eq!(error.code, nexus_contracts::codes::UNAUTHORIZED);
            assert!(error.message.contains("NEXUS_CLIENT_KEY"));
        },
    );
}

#[test]
fn ambient_identity_rejects_agent_id_without_client_key() {
    with_test_env_vars(
        &[
            ("NEXUS_NAME", None),
            ("NEXUS_AGENT_ID", Some("a_s_staged")),
            ("NEXUS_CLIENT_KEY", None),
            ("NEXUS_SESSION_ID", Some("s_staged")),
        ],
        || {
            let error =
                nexus::cli::ambient::identity_from_env_result().expect_err("must fail closed");
            assert_eq!(error.code, nexus_contracts::codes::UNAUTHORIZED);
            assert!(error.message.contains("NEXUS_CLIENT_KEY"));
        },
    );
}
