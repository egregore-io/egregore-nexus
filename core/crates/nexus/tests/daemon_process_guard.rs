use std::fs;
use std::io;

use nexus::daemon::lifecycle::linux_proc_stat_is_alive;
use nexus::daemon::process_guard::DaemonSingleton;

#[test]
fn singleton_rejects_second_live_owner() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir_path = dir.path().join("nexus");
    let state_dir = state_dir_path.to_str().unwrap();

    let first = DaemonSingleton::acquire(state_dir).unwrap();
    let err = match DaemonSingleton::acquire(state_dir) {
        Ok(_) => panic!("second singleton acquisition should fail"),
        Err(err) => err,
    };

    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    assert_eq!(
        err.to_string(),
        format!("nexus daemon already running (pid {})", std::process::id())
    );
    assert_eq!(
        read_pid(&state_dir_path.join("daemon.lock")),
        Some(std::process::id()),
        "failed second acquire must not clobber the first daemon's lock file"
    );
    assert_eq!(
        read_pid(&state_dir_path.join("daemon.pid")),
        Some(std::process::id()),
        "failed second acquire must not clobber the first daemon's pid file"
    );
    drop(first);
    DaemonSingleton::acquire(state_dir).unwrap();
}

#[test]
fn singleton_reclaims_stale_lock_file() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("nexus");
    fs::create_dir_all(&state_dir).unwrap();
    let lock = state_dir.join("daemon.lock");
    fs::write(&lock, "99999999\n").unwrap();

    let guard = DaemonSingleton::acquire(state_dir.to_str().unwrap()).unwrap();

    assert_eq!(read_pid(&lock), Some(std::process::id()));
    drop(guard);
    assert!(!lock.exists());
}

#[test]
fn linux_proc_stat_rejects_zombie_and_dead_process_states() {
    assert_eq!(linux_proc_stat_is_alive("42 (nexus) S 1 2 3"), Some(true));
    assert_eq!(
        linux_proc_stat_is_alive("42 (nexus worker) R 1 2 3"),
        Some(true)
    );
    assert_eq!(linux_proc_stat_is_alive("42 (nexus) Z 1 2 3"), Some(false));
    assert_eq!(linux_proc_stat_is_alive("42 (nexus) X 1 2 3"), Some(false));
    assert_eq!(linux_proc_stat_is_alive("malformed"), None);
}

fn read_pid(path: &std::path::Path) -> Option<u32> {
    fs::read_to_string(path)
        .ok()?
        .split_whitespace()
        .next()
        .and_then(|s| s.parse().ok())
}
