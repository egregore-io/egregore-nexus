//! Exclusive updater lock.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(windows)]
use std::process::Command;

use serde::{Deserialize, Serialize};

#[cfg(windows)]
use crate::lifecycle_process;

use super::install_context::InstallMethod;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateLockRecord {
    pub pid: u32,
    pub started_at_ms: i64,
    pub install_method: InstallMethod,
    pub managed_package: Option<String>,
    pub target_version: Option<String>,
    pub owner_token: String,
}

#[derive(Debug)]
pub struct UpdateLock {
    path: PathBuf,
    record: UpdateLockRecord,
}

#[derive(Debug, thiserror::Error)]
pub enum UpdateLockError {
    #[error("another Nexus update is running (pid {pid}, started {started_at_ms})")]
    Busy { pid: u32, started_at_ms: i64 },
    #[error("update lock is unreadable: {0}")]
    Corrupt(String),
    #[error("update lock I/O error: {0}")]
    Io(#[from] io::Error),
}

impl UpdateLock {
    pub fn acquire(
        path: &Path,
        install_method: InstallMethod,
        managed_package: Option<String>,
        target_version: Option<String>,
    ) -> Result<Self, UpdateLockError> {
        let inspector = SystemProcessInspector;
        let record = UpdateLockRecord {
            pid: std::process::id(),
            started_at_ms: inspector.now_ms(),
            install_method,
            managed_package,
            target_version,
            owner_token: uuid::Uuid::new_v4().to_string(),
        };
        Self::acquire_with(path, record, &inspector, Duration::from_secs(60 * 60))
    }

    pub fn record(&self) -> &UpdateLockRecord {
        &self.record
    }

    #[doc(hidden)]
    pub fn acquire_with(
        path: &Path,
        record: UpdateLockRecord,
        inspector: &dyn ProcessInspector,
        stale_after: Duration,
    ) -> Result<Self, UpdateLockError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        for _ in 0..3 {
            match create_lock_file(path, &record) {
                Ok(()) => {
                    return Ok(Self {
                        path: path.to_path_buf(),
                        record,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    let existing = read_record(path)?;
                    let age_ms = inspector
                        .now_ms()
                        .saturating_sub(existing.started_at_ms)
                        .max(0) as u128;
                    if inspector.process_alive(existing.pid) || age_ms <= stale_after.as_millis() {
                        return Err(UpdateLockError::Busy {
                            pid: existing.pid,
                            started_at_ms: existing.started_at_ms,
                        });
                    }
                    quarantine_stale_lock(path, &existing)?;
                }
                Err(error) => return Err(UpdateLockError::Io(error)),
            }
        }
        Err(UpdateLockError::Corrupt(
            "lock changed repeatedly during stale recovery".into(),
        ))
    }
}

impl Drop for UpdateLock {
    fn drop(&mut self) {
        if read_record(&self.path)
            .map(|record| record.owner_token == self.record.owner_token)
            .unwrap_or(false)
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn create_lock_file(path: &Path, record: &UpdateLockRecord) -> io::Result<()> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    serde_json::to_writer(&mut file, record).map_err(io::Error::other)?;
    file.write_all(b"\n")?;
    file.sync_all()
}

fn read_record(path: &Path) -> Result<UpdateLockRecord, UpdateLockError> {
    let file = File::open(path)?;
    serde_json::from_reader(file).map_err(|error| UpdateLockError::Corrupt(error.to_string()))
}

fn quarantine_stale_lock(path: &Path, expected: &UpdateLockRecord) -> Result<(), UpdateLockError> {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("update.lock");
    let quarantine = path.with_file_name(format!("{file_name}.stale.{}", expected.owner_token));
    match fs::rename(path, &quarantine) {
        Ok(()) => {
            let moved = read_record(&quarantine)?;
            if moved.owner_token != expected.owner_token {
                return Err(UpdateLockError::Corrupt(
                    "lock ownership changed during stale recovery".into(),
                ));
            }
            fs::remove_file(quarantine)?;
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(UpdateLockError::Io(error)),
    }
}

#[doc(hidden)]
pub trait ProcessInspector {
    fn now_ms(&self) -> i64;
    fn process_alive(&self, pid: u32) -> bool;
}

struct SystemProcessInspector;

impl ProcessInspector for SystemProcessInspector {
    fn now_ms(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(i64::MAX as u128) as i64
    }

    fn process_alive(&self, pid: u32) -> bool {
        process_alive(pid)
    }
}

#[cfg(unix)]
fn process_alive(pid: u32) -> bool {
    let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
    result == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(windows)]
fn process_alive(pid: u32) -> bool {
    let mut command = Command::new("tasklist");
    command.args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"]);
    lifecycle_process::run_bounded(
        &mut command,
        "Windows update-lock process probe",
        Duration::from_secs(5),
        64 * 1024,
    )
    .map(|output| !String::from_utf8_lossy(&output.stdout).contains("No tasks are running"))
    .unwrap_or(false)
}

#[cfg(not(any(unix, windows)))]
fn process_alive(_pid: u32) -> bool {
    false
}
