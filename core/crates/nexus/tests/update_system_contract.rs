//! Static boundary contract for the production updater.

#[test]
fn updater_uses_the_shared_bounded_runner_without_reader_joins() {
    let source = include_str!("../src/update/system.rs");

    assert!(source.contains("lifecycle_process::run_bounded"));
    assert!(source.contains("Duration::from_secs(15 * 60)"));
    assert!(!source.contains("stdout_reader.join()"));
    assert!(!source.contains("stderr_reader.join()"));
}

#[test]
fn updater_keeps_registered_webconsole_restoration_on_its_supervisor() {
    let source = include_str!("../src/update/system.rs");
    let registered = source
        .find("ServiceFacet::Webconsole if crate::webconsole_service::installed()")
        .expect("registered Webconsole requires a supervisor branch");
    let detached = source
        .find("ServiceFacet::Webconsole => crate::webconsole_lifecycle::restart_webconsole(")
        .unwrap();
    assert!(registered < detached);
    let branch = &source[registered..detached];
    assert!(branch.contains("crate::webconsole_service::stop()?"));
    assert!(branch.contains("crate::webconsole_service::start().await"));
}
