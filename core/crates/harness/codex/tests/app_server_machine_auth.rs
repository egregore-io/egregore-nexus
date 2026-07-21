use std::path::{Path, PathBuf};

use nexus_harness_codex::app_server::bridge::resolve_resume_codex_home;

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nexus-codex-machine-auth-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
        tag,
    ));
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}

fn write_thread_rollout(home: &Path, thread_id: &str) -> PathBuf {
    let dir = home.join("sessions/2026/07/22");
    std::fs::create_dir_all(&dir).expect("create rollout dir");
    let path = dir.join(format!("rollout-{thread_id}.jsonl"));
    std::fs::write(
        &path,
        format!("{{\"payload\":{{\"id\":\"{thread_id}\"}}}}\n"),
    )
    .expect("write rollout");
    path
}

#[test]
fn canonical_machine_home_wins_known_thread_lookup() {
    let root = tempdir("canonical-resume");
    let session_dir = root.join(".nexus/codex-sessions/s_current");
    let canonical = root.join("machine/.codex");
    let external = root.join("external");
    write_thread_rollout(&canonical, "thread-canonical");
    write_thread_rollout(&external, "thread-canonical");

    let resolved =
        resolve_resume_codex_home(&session_dir, &canonical, &[external], "thread-canonical")
            .expect("resolve known thread")
            .expect("matching home");

    assert_eq!(resolved, canonical);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn legacy_nexus_rollout_migrates_without_auth_or_config() {
    let root = tempdir("legacy-migration");
    let sessions_root = root.join(".nexus/codex-sessions");
    let session_dir = sessions_root.join("s_current");
    let legacy = sessions_root.join("s_legacy/codex-home");
    let canonical = root.join("machine/.codex");
    let source = write_thread_rollout(&legacy, "thread-legacy");
    std::fs::write(legacy.join("auth.json"), b"must-not-migrate").expect("write auth marker");
    std::fs::write(legacy.join("config.toml"), b"must-not-migrate").expect("write config marker");

    let resolved = resolve_resume_codex_home(&session_dir, &canonical, &[], "thread-legacy")
        .expect("migrate legacy rollout")
        .expect("matching home");

    assert_eq!(resolved, canonical);
    let relative = source
        .strip_prefix(legacy.join("sessions"))
        .expect("rollout relative path");
    assert_eq!(
        std::fs::read(canonical.join("sessions").join(relative)).expect("migrated rollout"),
        std::fs::read(&source).expect("source rollout")
    );
    assert!(!canonical.join("auth.json").exists());
    assert!(!canonical.join("config.toml").exists());
    let destination_parent = canonical
        .join("sessions")
        .join(relative)
        .parent()
        .expect("migrated rollout parent")
        .to_path_buf();
    assert!(
        std::fs::read_dir(destination_parent)
            .expect("read migrated rollout parent")
            .all(|entry| !entry
                .expect("read migrated rollout entry")
                .file_name()
                .to_string_lossy()
                .starts_with(".nexus-rollout-")),
        "atomic migration must not leave a staging file"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn legacy_rollout_migration_refuses_conflicting_machine_bytes() {
    let root = tempdir("legacy-conflict");
    let sessions_root = root.join(".nexus/codex-sessions");
    let session_dir = sessions_root.join("s_current");
    let legacy = sessions_root.join("s_legacy/codex-home");
    let canonical = root.join("machine/.codex");
    let source = write_thread_rollout(&legacy, "thread-conflict");
    let relative = source
        .strip_prefix(legacy.join("sessions"))
        .expect("rollout relative path");
    let destination = canonical.join("sessions").join(relative);
    std::fs::create_dir_all(destination.parent().expect("destination parent"))
        .expect("create destination parent");
    std::fs::write(&destination, b"different machine rollout\n")
        .expect("write conflicting destination");

    let error = resolve_resume_codex_home(&session_dir, &canonical, &[], "thread-conflict")
        .expect_err("conflicting machine rollout must reject");

    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(
        std::fs::read(&destination).expect("read unchanged destination"),
        b"different machine rollout\n"
    );
    assert!(source.exists(), "failed migration must preserve its source");
    let _ = std::fs::remove_dir_all(&root);
}

#[cfg(unix)]
#[test]
fn legacy_rollout_lookup_never_follows_symlinked_sources() {
    use std::os::unix::fs::symlink;

    let root = tempdir("legacy-symlink");
    let sessions_root = root.join(".nexus/codex-sessions");
    let session_dir = sessions_root.join("s_current");
    let legacy = sessions_root.join("s_legacy/codex-home");
    let canonical = root.join("machine/.codex");
    let outside = root.join("outside-rollout.jsonl");
    std::fs::write(&outside, "{\"payload\":{\"id\":\"thread-symlink\"}}\n")
        .expect("write outside rollout");
    let link_dir = legacy.join("sessions/2026/07/22");
    std::fs::create_dir_all(&link_dir).expect("create legacy rollout dir");
    symlink(&outside, link_dir.join("rollout-thread-symlink.jsonl"))
        .expect("create rollout symlink");

    let resolved = resolve_resume_codex_home(&session_dir, &canonical, &[], "thread-symlink")
        .expect("symlink lookup must remain a clean miss");

    assert!(resolved.is_none());
    assert!(!canonical.exists());
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn arbitrary_external_resume_home_remains_authoritative() {
    let root = tempdir("external-resume");
    let session_dir = root.join(".nexus/codex-sessions/s_current");
    let canonical = root.join("machine/.codex");
    let external = root.join("operator-profile");
    let source = write_thread_rollout(&external, "thread-external");

    let resolved = resolve_resume_codex_home(
        &session_dir,
        &canonical,
        std::slice::from_ref(&external),
        "thread-external",
    )
    .expect("resolve external profile")
    .expect("matching home");

    assert_eq!(resolved, external);
    assert!(source.exists());
    assert!(!canonical.join("sessions").exists());
    let _ = std::fs::remove_dir_all(&root);
}
