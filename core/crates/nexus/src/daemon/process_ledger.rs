//! Daemon boot cleanup for runtime process ids.
//!
//! The store persists `(pid, pgid)` for spawned runtime process groups. On boot the daemon uses
//! that generic ledger to reap known daemon-owned or stopped process groups. Active headed and
//! app-server rows are intentionally excluded because their terminals can be adopted after a daemon
//! restart; daemon-owned ACP stdio rows cannot be adopted and are reaped as orphans.

use std::time::Duration;

use nexus_common::process_ids::runtime_process_ids_for_pid;
use nexus_common::{NexusError, RuntimeProcessIds};
use nexus_store::repos::AgentRuntimes;
use nexus_store::Store;

const SWEEP_TERM_GRACE: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeProcessSweepReport {
    pub candidates: usize,
    pub groups_signalled: usize,
    pub cleared: usize,
    pub failures: usize,
}

/// Reap process groups recorded in `agent_runtimes` that are no longer adoptable.
pub async fn reap_runtime_process_ledger(
    store: &Store,
) -> Result<RuntimeProcessSweepReport, NexusError> {
    let runtimes = AgentRuntimes::new(store)
        .list_boot_process_reap_candidates()
        .await?;
    let mut report = RuntimeProcessSweepReport {
        candidates: runtimes.len(),
        ..RuntimeProcessSweepReport::default()
    };
    for runtime in runtimes {
        let Some(ids) = runtime.runtime_process_ids() else {
            continue;
        };
        if !process_ids_match(ids) {
            AgentRuntimes::new(store)
                .clear_process_ids(&runtime.runtime_id)
                .await?;
            report.cleared += 1;
            continue;
        }
        match terminate_process_group(ids) {
            Ok(()) => report.groups_signalled += 1,
            Err(error) => {
                report.failures += 1;
                tracing::warn!(
                    runtime_id = runtime.runtime_id,
                    pid = ids.os_pid,
                    pgid = ids.os_pgid,
                    error = %error,
                    "failed to terminate runtime process group from durable ledger"
                );
                continue;
            }
        }
        tokio::time::sleep(SWEEP_TERM_GRACE).await;
        if process_ids_match(ids) {
            if let Err(error) = kill_process_group(ids) {
                report.failures += 1;
                tracing::warn!(
                    runtime_id = runtime.runtime_id,
                    pid = ids.os_pid,
                    pgid = ids.os_pgid,
                    error = %error,
                    "failed to kill runtime process group from durable ledger"
                );
                continue;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if !process_ids_match(ids) {
            AgentRuntimes::new(store)
                .clear_process_ids(&runtime.runtime_id)
                .await?;
            report.cleared += 1;
        }
    }
    Ok(report)
}

fn process_ids_match(ids: RuntimeProcessIds) -> bool {
    runtime_process_ids_for_pid(ids.os_pid)
        .map(|current| current.os_pgid == ids.os_pgid)
        .unwrap_or(false)
}

fn terminate_process_group(ids: RuntimeProcessIds) -> std::io::Result<()> {
    signal_process_group(ids.os_pgid, 15)
}

fn kill_process_group(ids: RuntimeProcessIds) -> std::io::Result<()> {
    signal_process_group(ids.os_pgid, 9)
}

#[cfg(unix)]
fn signal_process_group(pgid: u32, sig: i32) -> std::io::Result<()> {
    extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
        fn getpgrp() -> i32;
    }
    let pgid = i32::try_from(pgid).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("process group id {pgid} does not fit in i32"),
        )
    })?;
    // SAFETY: getpgrp has no preconditions.
    let current = unsafe { getpgrp() };
    if pgid <= 0 || pgid == current {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("refusing to signal unsafe process group {pgid}"),
        ));
    }
    // SAFETY: kill(2) with a negative pid signals the process group whose id is abs(pid).
    let rc = unsafe { kill(-pgid, sig) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn signal_process_group(_pgid: u32, _sig: i32) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_common::process_ids::runtime_process_ids_for_pid;
    use nexus_store::repos::{AgentRuntimes, Agents, NewAgent, NewAgentRuntime};

    #[tokio::test]
    #[cfg(target_os = "linux")]
    async fn boot_sweep_reaps_recorded_acp_process_group_and_clears_ledger() {
        use std::process::Stdio;

        let store = Store::open(":memory:").await.unwrap();
        store.migrate().await.unwrap();
        Agents::new(&store)
            .create(NewAgent {
                agent_id: "a_reap".to_string(),
                project: "default".to_string(),
                name: Some("reap".to_string()),
                default_harness: Some("claude".to_string()),
                role: None,
                tier: Some("agent".to_string()),
                owner: None,
            })
            .await
            .unwrap();
        let runtimes = AgentRuntimes::new(&store);
        runtimes
            .create(NewAgentRuntime {
                runtime_id: "s_reap".to_string(),
                agent_id: "a_reap".to_string(),
                harness: "claude".to_string(),
                cwd: None,
                transport: Some("acp".to_string()),
                presence: Some("online".to_string()),
                active: true,
            })
            .await
            .unwrap();

        let mut child = tokio::process::Command::new("sh");
        child
            .arg("-c")
            .arg("sleep 60 & wait")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        child.process_group(0);
        let mut child = child.spawn().unwrap();
        let pid = child.id().unwrap();
        let ids = runtime_process_ids_for_pid(pid).expect("child should be visible in /proc");
        assert_eq!(ids.os_pid, pid);
        extern "C" {
            fn getpgrp() -> i32;
        }
        // SAFETY: getpgrp has no preconditions.
        let current_pgrp = unsafe { getpgrp() as u32 };
        assert_ne!(ids.os_pgid, current_pgrp);
        runtimes.set_process_ids("s_reap", ids).await.unwrap();

        let report = reap_runtime_process_ledger(&store).await.unwrap();

        assert_eq!(report.candidates, 1);
        assert_eq!(report.groups_signalled, 1);
        assert_eq!(report.failures, 0);
        let _ = child.wait().await;
        let cleanup = reap_runtime_process_ledger(&store).await.unwrap();
        assert_eq!(report.cleared + cleanup.cleared, 1);
        assert!(
            runtime_process_ids_for_pid(pid).is_none(),
            "recorded root process should be gone"
        );
        let row = runtimes
            .find_by_runtime_id("s_reap")
            .await
            .unwrap()
            .expect("runtime row remains");
        assert_eq!(row.runtime_process_ids(), None);
    }
}
