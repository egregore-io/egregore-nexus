//! Static boundary contract for the production updater.

#[test]
fn updater_uses_the_shared_bounded_runner_without_reader_joins() {
    let source = include_str!("../src/update/system.rs");

    assert!(source.contains("lifecycle_process::run_bounded"));
    assert!(source.contains("Duration::from_secs(15 * 60)"));
    assert!(!source.contains("stdout_reader.join()"));
    assert!(!source.contains("stderr_reader.join()"));
}
