use nexus_common::process_ids::{runtime_process_ids_for_pid, RuntimeProcessIds};

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

#[test]
#[cfg(target_os = "linux")]
fn linux_reports_the_current_live_process() {
    let process_ids = runtime_process_ids_for_pid(std::process::id())
        .expect("the current Linux process must remain queryable through procfs");

    assert_eq!(process_ids.os_pid, std::process::id());
    assert!(process_ids.os_pgid > 0);
}

#[test]
#[cfg(windows)]
fn windows_reports_the_current_live_process() {
    let pid = std::process::id();
    assert_eq!(
        runtime_process_ids_for_pid(pid),
        Some(RuntimeProcessIds {
            os_pid: pid,
            os_pgid: pid,
        })
    );
    assert!(runtime_process_ids_for_pid(0).is_none());
}
