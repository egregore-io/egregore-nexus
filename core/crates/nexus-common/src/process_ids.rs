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
#[cfg(not(target_os = "linux"))]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(target_os = "linux")]
    fn parses_proc_stat_with_spaces_in_comm() {
        let stat = "123 (fake process) S 1 456 456 0 -1 4194304 0 0 0 0 1 2 0 0 20 0 1 0 789 0 0";

        assert_eq!(
            parse_linux_proc_stat_process_ids(stat, 123),
            Some(RuntimeProcessIds {
                os_pid: 123,
                os_pgid: 456,
            })
        );
    }

    #[test]
    fn from_parts_rejects_incomplete_or_out_of_range_rows() {
        assert_eq!(
            RuntimeProcessIds::from_parts(Some(1), Some(2))
                .unwrap()
                .os_pid,
            1
        );
        assert!(RuntimeProcessIds::from_parts(None, Some(2)).is_none());
        assert!(RuntimeProcessIds::from_parts(Some(-1), Some(2)).is_none());
    }
}
