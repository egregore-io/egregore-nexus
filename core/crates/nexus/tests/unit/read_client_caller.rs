use super::*;
use crate::cli::ambient::with_scrubbed_identity_env;

#[test]
fn envless_shell_is_the_local_operator() {
    let caller = with_scrubbed_identity_env(&[], ReadCaller::from_env);
    let caller = caller.unwrap();
    assert!(caller.is_local_operator());
}

#[test]
fn agent_identity_always_wins_and_is_never_operator() {
    let caller = with_scrubbed_identity_env(
        &[("NEXUS_NAME", "demoa"), ("NEXUS_CLIENT_KEY", "ck_demoa")],
        ReadCaller::from_env,
    )
    .unwrap();
    assert_eq!(caller.name, "demoa");
    assert_eq!(caller.tier, Tier::Agent);
    assert!(!caller.is_local_operator());
}

#[test]
fn ambient_human_kind_survives_agent_tier_on_daemon_reads() {
    let caller = with_scrubbed_identity_env(
        &[
            ("NEXUS_NAME", "endurance-controller"),
            ("NEXUS_CLIENT_KEY", "ck_controller"),
            ("NEXUS_KIND", "human"),
        ],
        ReadCaller::from_env,
    )
    .unwrap();
    let ipc = caller.to_ipc();
    assert_eq!(ipc.kind, Kind::Human);
    assert_eq!(ipc.tier, Tier::Agent);
}

#[test]
fn ambient_stable_agent_id_survives_daemon_ipc_evidence() {
    let caller = with_scrubbed_identity_env(
        &[
            ("NEXUS_NAME", "stale-display"),
            ("NEXUS_AGENT_ID", "a_stable"),
            ("NEXUS_CLIENT_KEY", "ck_stable"),
        ],
        ReadCaller::from_env,
    )
    .unwrap();

    assert_eq!(caller.to_ipc().agent_id.as_deref(), Some("a_stable"));
}

#[test]
fn partial_agent_env_is_not_local_operator() {
    let error =
        with_scrubbed_identity_env(&[("NEXUS_NAME", "demoa")], ReadCaller::from_env).unwrap_err();
    assert_eq!(error.code, codes::UNAUTHORIZED);
    assert!(error.message.contains("NEXUS_CLIENT_KEY"));
}
