use nexus::update::install_context::InstallMethod;
use nexus::update::lock::{ProcessInspector, UpdateLock, UpdateLockError, UpdateLockRecord};
use std::cell::Cell;
use std::fs;
use std::time::Duration;

#[test]
fn only_one_updater_can_hold_the_lock() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("update.lock");
    let inspector = FakeInspector::new(1_000, true);
    let first = UpdateLock::acquire_with(
        &path,
        record(42, 1_000, "first"),
        &inspector,
        Duration::from_secs(60),
    )
    .unwrap();

    let error = UpdateLock::acquire_with(
        &path,
        record(43, 1_001, "second"),
        &inspector,
        Duration::from_secs(60),
    )
    .unwrap_err();
    assert!(matches!(error, UpdateLockError::Busy { pid: 42, .. }));

    drop(first);
    assert!(!path.exists());
}

#[test]
fn an_old_lock_with_a_dead_owner_is_reclaimed() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("update.lock");
    fs::write(
        &path,
        serde_json::to_vec(&record(42, 1_000, "stale")).unwrap(),
    )
    .unwrap();
    let inspector = FakeInspector::new(100_000, false);

    let lock = UpdateLock::acquire_with(
        &path,
        record(43, 100_000, "replacement"),
        &inspector,
        Duration::from_secs(60),
    )
    .unwrap();

    assert_eq!(lock.record().pid, 43);
    assert_eq!(read_record(&path).owner_token, "replacement");
}

#[test]
fn a_live_or_young_lock_is_never_reclaimed() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("update.lock");
    fs::write(
        &path,
        serde_json::to_vec(&record(42, 1_000, "owner")).unwrap(),
    )
    .unwrap();

    let live = FakeInspector::new(100_000, true);
    assert!(matches!(
        UpdateLock::acquire_with(
            &path,
            record(43, 100_000, "live-attempt"),
            &live,
            Duration::from_secs(60),
        ),
        Err(UpdateLockError::Busy { pid: 42, .. })
    ));

    let young = FakeInspector::new(1_030, false);
    assert!(matches!(
        UpdateLock::acquire_with(
            &path,
            record(43, 1_030, "young-attempt"),
            &young,
            Duration::from_secs(60),
        ),
        Err(UpdateLockError::Busy { pid: 42, .. })
    ));
}

#[test]
fn dropping_an_old_guard_does_not_delete_a_successor_lock() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("update.lock");
    let inspector = FakeInspector::new(1_000, true);
    let lock = UpdateLock::acquire_with(
        &path,
        record(42, 1_000, "old"),
        &inspector,
        Duration::from_secs(60),
    )
    .unwrap();
    fs::write(
        &path,
        serde_json::to_vec(&record(43, 1_001, "new")).unwrap(),
    )
    .unwrap();

    drop(lock);

    assert_eq!(read_record(&path).owner_token, "new");
}

fn record(pid: u32, started_at_ms: i64, token: &str) -> UpdateLockRecord {
    UpdateLockRecord {
        pid,
        started_at_ms,
        install_method: InstallMethod::Npm,
        managed_package: Some("@egregore/nexus".into()),
        target_version: Some("0.1.3".into()),
        owner_token: token.into(),
    }
}

fn read_record(path: &std::path::Path) -> UpdateLockRecord {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

struct FakeInspector {
    now_ms: i64,
    alive: Cell<bool>,
}

impl FakeInspector {
    fn new(now_ms: i64, alive: bool) -> Self {
        Self {
            now_ms,
            alive: Cell::new(alive),
        }
    }
}

impl ProcessInspector for FakeInspector {
    fn now_ms(&self) -> i64 {
        self.now_ms
    }

    fn process_alive(&self, _pid: u32) -> bool {
        self.alive.get()
    }
}
