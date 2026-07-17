use std::fs;
use std::path::PathBuf;

fn source(path: &str) -> String {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    fs::read_to_string(root.join(path)).unwrap()
}

#[test]
fn gateway_ownership_is_not_in_daemon_boot_or_shutdown() {
    let main = source("src/main.rs");

    assert!(
        !main.contains("GatewaySupervisorConfig::from_env"),
        "daemon boot must not discover or spawn the separately installed gateway"
    );
    assert!(
        !main.contains("tombstone_gateway(&lifecycle_paths)"),
        "daemon shutdown must not remove independently owned gateway discovery"
    );
}

#[test]
fn daemon_service_units_do_not_capture_gateway_source_paths() {
    let lifecycle = source("src/daemon/lifecycle.rs");

    assert!(
        !lifecycle.contains("command.env(\"NEXUS_GATEWAY_DIR\"")
            && !lifecycle.contains("Environment=NEXUS_GATEWAY_DIR="),
        "daemon lifecycle must not couple services to gateway source worktrees"
    );
}
