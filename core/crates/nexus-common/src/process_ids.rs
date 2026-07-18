//! OS process-id helpers shared by launchers and the store.
//!
//! Runtime rows persist the root process PID and process-group id so operator/debug tooling can see
//! which OS process tree Nexus spawned and stop paths can target the same process group.

/// Durable OS process ids for a launched runtime process group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeProcessIds {
    pub os_pid: u32,
    pub os_pgid: u32,
}

impl RuntimeProcessIds {
    /// Build process ids only when both stored columns are present and in range.
    pub fn from_parts(os_pid: Option<i64>, os_pgid: Option<i64>) -> Option<Self> {
        Some(Self {
            os_pid: u32::try_from(os_pid?).ok()?,
            os_pgid: u32::try_from(os_pgid?).ok()?,
        })
    }
}

/// Read the current process ids for `pid`.
#[cfg(target_os = "linux")]
pub fn runtime_process_ids_for_pid(pid: u32) -> Option<RuntimeProcessIds> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    parse_linux_proc_stat_process_ids(&stat, pid)
}

/// Read the current process ids for `pid`.
#[cfg(windows)]
pub fn runtime_process_ids_for_pid(pid: u32) -> Option<RuntimeProcessIds> {
    use windows_sys::Win32::Foundation::{CloseHandle, STILL_ACTIVE};
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    // SAFETY: OpenProcess is called with a non-inheritable, query-only handle. The handle is
    // checked for null, passed to GetExitCodeProcess, and closed exactly once before returning.
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle.is_null() {
        return None;
    }
    let mut exit_code = 0;
    // SAFETY: `handle` is valid and `exit_code` points to writable stack storage.
    let queried = unsafe { GetExitCodeProcess(handle, &mut exit_code) } != 0;
    // SAFETY: `handle` was returned by OpenProcess and has not been closed yet.
    unsafe {
        CloseHandle(handle);
    }
    if !queried || exit_code != STILL_ACTIVE as u32 {
        return None;
    }
    Some(RuntimeProcessIds {
        os_pid: pid,
        // Windows has no POSIX process-group id. The root PID is the stable tree token used by
        // the Windows supervisor, which owns and terminates the direct child handle.
        os_pgid: pid,
    })
}

/// Read the current process ids for `pid`.
#[cfg(not(any(target_os = "linux", windows)))]
pub fn runtime_process_ids_for_pid(_pid: u32) -> Option<RuntimeProcessIds> {
    None
}

/// Parse Linux `/proc/<pid>/stat` into a PID/PGID tuple.
#[cfg(target_os = "linux")]
fn parse_linux_proc_stat_process_ids(stat: &str, pid: u32) -> Option<RuntimeProcessIds> {
    let close = stat.rfind(") ")?;
    let after = stat.get(close + 2..)?;
    let fields: Vec<&str> = after.split_whitespace().collect();
    // Field 3 (`state`) is fields[0] and field 5 (`pgrp`) is fields[2].
    let os_pgid = fields.get(2)?.parse::<u32>().ok()?;
    Some(RuntimeProcessIds {
        os_pid: pid,
        os_pgid,
    })
}
