//! Daemon singleton guard for one local state directory.
//!
//! A second daemon must not start against the same local state directory, because both instances
//! would claim the same store, process-owned harnesses, and gateway/session edge. This guard takes a
//! held exclusive lock on `daemon.lock` before the daemon binds public surfaces, records the current
//! PID in both the lock and `daemon.pid`, and removes only its own files on drop.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// Held for the daemon lifetime so only one daemon owns a local state directory.
pub struct DaemonSingleton {
    lock_path: PathBuf,
    pid_path: PathBuf,
    pid: u32,
    _lock_file: File,
}

impl DaemonSingleton {
    /// Atomically claim `state_dir` for the current daemon process.
    ///
    /// If another live process owns the lock, this returns [`io::ErrorKind::AlreadyExists`]. If the
    /// lock file is stale, it is removed and acquisition is retried.
    pub fn acquire(state_dir: &str) -> io::Result<Self> {
        let state_dir = Path::new(state_dir);
        fs::create_dir_all(state_dir)?;

        let lock_path = state_dir.join("daemon.lock");
        let pid_path = state_dir.join("daemon.pid");
        let pid = std::process::id();
        let mut lock_file = acquire_lock_file(&lock_path)?;

        lock_file.set_len(0)?;
        writeln!(lock_file, "{pid}")?;
        lock_file.sync_all()?;
        fs::write(&pid_path, format!("{pid}\n"))?;

        Ok(Self {
            lock_path,
            pid_path,
            pid,
            _lock_file: lock_file,
        })
    }
}

impl Drop for DaemonSingleton {
    fn drop(&mut self) {
        let _ = remove_if_owned_pid(&self.pid_path, self.pid);
        let _ = remove_if_owned_pid(&self.lock_path, self.pid);
    }
}

#[cfg(unix)]
fn acquire_lock_file(lock_path: &Path) -> io::Result<File> {
    use std::os::fd::AsRawFd;

    const LOCK_EX: i32 = 2;
    const LOCK_NB: i32 = 4;

    extern "C" {
        fn flock(fd: i32, operation: i32) -> i32;
    }

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(lock_path)?;
    let rc = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
    if rc == 0 {
        return Ok(file);
    }
    let err = io::Error::last_os_error();
    if lock_would_block(&err) {
        let pid = read_pid(lock_path)?
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("nexus daemon already running (pid {pid})"),
        ));
    }
    Err(err)
}

#[cfg(unix)]
fn lock_would_block(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock || matches!(err.raw_os_error(), Some(11 | 35))
}

#[cfg(not(unix))]
fn acquire_lock_file(lock_path: &Path) -> io::Result<File> {
    loop {
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(lock_path)
        {
            Ok(file) => return Ok(file),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                if let Some(pid) = read_pid(lock_path)? {
                    if process_alive(pid) {
                        return Err(io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            format!("nexus daemon already running (pid {pid})"),
                        ));
                    }
                }
                fs::remove_file(lock_path)?;
            }
            Err(e) => return Err(e),
        }
    }
}

fn read_pid(path: &Path) -> io::Result<Option<u32>> {
    let mut body = String::new();
    match File::open(path) {
        Ok(mut file) => {
            file.read_to_string(&mut body)?;
            Ok(body.split_whitespace().next().and_then(|s| s.parse().ok()))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn remove_if_owned_pid(path: &Path, pid: u32) -> io::Result<()> {
    if read_pid(path)? == Some(pid) {
        fs::remove_file(path)?;
    }
    Ok(())
}

#[cfg(windows)]
fn process_alive(pid: u32) -> bool {
    nexus_common::process_ids::runtime_process_ids_for_pid(pid).is_some()
}

#[cfg(not(any(unix, windows)))]
fn process_alive(_pid: u32) -> bool {
    false
}
