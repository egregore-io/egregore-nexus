//! Architecture gate for the daemon composition root.

use std::fs;
use std::path::PathBuf;

fn app_source() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/daemon/app.rs");
    fs::read_to_string(path).expect("read daemon app.rs")
}

#[test]
fn app_state_stays_a_thin_composition_root() {
    let source = app_source();
    let lines = source.lines().count();

    assert!(
        lines <= 1_500,
        "daemon/app.rs must stay at or below 1,500 lines; found {lines}"
    );
    assert!(
        !source.contains("#[cfg(test)]"),
        "daemon/app.rs must not contain embedded test configuration"
    );
    assert!(
        !source.contains("mod tests"),
        "daemon/app.rs tests belong in external test targets"
    );
}

#[test]
fn extracted_services_do_not_retain_app_state() {
    let services = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/daemon/services");
    for entry in fs::read_dir(services).expect("read daemon services") {
        let path = entry.expect("service entry").path();
        if path.extension().and_then(|value| value.to_str()) != Some("rs") {
            continue;
        }
        let source = fs::read_to_string(&path).expect("read daemon service");
        assert!(
            !source.contains("app: AppState"),
            "{} must own explicit dependencies, not retain AppState",
            path.display()
        );
    }
}

#[test]
fn app_state_shards_remain_bounded_and_test_free() {
    let shards = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/daemon/app");
    for entry in fs::read_dir(shards).expect("read AppState shards") {
        let path = entry.expect("AppState shard entry").path();
        if path.extension().and_then(|value| value.to_str()) != Some("rs") {
            continue;
        }
        let source = fs::read_to_string(&path).expect("read AppState shard");
        let lines = source.lines().count();
        assert!(
            lines <= 1_000,
            "{} must stay at or below 1,000 lines; found {lines}",
            path.display()
        );
        assert!(
            !source.contains("#[cfg(test)]") && !source.contains("mod tests"),
            "{} tests belong in external test targets",
            path.display()
        );
    }
}
