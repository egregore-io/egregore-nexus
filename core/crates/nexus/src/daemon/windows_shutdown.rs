//! Windows has no SIGTERM for a detached process. Use boot-authenticated local IPC to enter
//! the normal daemon drain, then wait on an open process handle. Force is explicit only.

use std::{io, path::Path, time::Duration};

use nexus_contracts::{
    DaemonIpcCall, DaemonIpcCaller, DaemonIpcRequest, Kind, Tier, DAEMON_IPC_PROTOCOL_VERSION,
};
use windows_sys::Win32::{
    Foundation::{CloseHandle, WAIT_OBJECT_0},
    System::Threading::{
        OpenProcess, TerminateProcess, WaitForSingleObject, PROCESS_SYNCHRONIZE, PROCESS_TERMINATE,
    },
};

pub(super) fn stop(home: &Path, pid: u32, force: bool, grace: Duration) -> io::Result<()> {
    stop_after(home, pid, force, grace, || Ok(()))
}

/// Pin the child before cancelling its supervisor. A stopped wrapper is not proof its child exited.
pub(super) fn stop_after(
    home: &Path,
    pid: u32,
    force: bool,
    grace: Duration,
    stop_manager: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    // Pin the process object before waiting: a later PID reuse must never be force-terminated.
    let rights = PROCESS_SYNCHRONIZE | if force { PROCESS_TERMINATE } else { 0 };
    let handle = unsafe { OpenProcess(rights, 0, pid) };
    if handle.is_null() {
        let error = io::Error::last_os_error();
        // A process that exited before capture has no handle; still cancel its manager.
        if error.raw_os_error()
            == Some(windows_sys::Win32::Foundation::ERROR_INVALID_PARAMETER as i32)
        {
            return stop_manager();
        }
        return Err(error);
    }
    let result = (|| {
        // Do not let task restart-on-failure race a child-only shutdown. Manager failure must
        // prevent IPC/force, even when force was requested or the captured child already exited.
        stop_manager()?;
        if unsafe { WaitForSingleObject(handle, 0) } == WAIT_OBJECT_0 {
            return Ok(());
        }
        let home = home.to_path_buf();
        // Lifecycle is synchronous but can be called inside the CLI's Tokio runtime.
        let request = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    let response = super::daemon_ipc::call_daemon_ipc(
                        &home,
                        DaemonIpcRequest {
                            version: DAEMON_IPC_PROTOCOL_VERSION,
                            token: String::new(),
                            request_id: format!("shutdown-{}", uuid::Uuid::new_v4()),
                            caller: Some(DaemonIpcCaller {
                                name: Some("operator".into()),
                                project: "default".into(),
                                session_id: Some(
                                    crate::local_operator::LOCAL_OPERATOR_SESSION_ID.into(),
                                ),
                                runtime_id: Some(
                                    crate::local_operator::LOCAL_OPERATOR_SESSION_ID.into(),
                                ),
                                agent_id: None,
                                client_key: None,
                                kind: Kind::Human,
                                locality: Default::default(),
                                access: None,
                                principal_id: None,
                                tier: Tier::Admin,
                            }),
                            call: DaemonIpcCall::Query {
                                method: "local.daemon.shutdown".into(),
                                params: serde_json::json!({"processId":pid}),
                            },
                        },
                        Duration::from_secs(2),
                    )
                    .await?;
                    if let Some(error) = response.error {
                        return Err(io::Error::other(error.message));
                    }
                    if response
                        .result
                        .as_ref()
                        .and_then(|v| v.get("accepted"))
                        .and_then(|v| v.as_bool())
                        != Some(true)
                    {
                        return Err(io::Error::other("daemon did not acknowledge shutdown"));
                    }
                    Ok(())
                })
        })
        .join()
        .map_err(|_| io::Error::other("shutdown IPC thread failed"))?;
        // A lost response can accompany successful shutdown. Actual exit is the authority.
        let wait = unsafe {
            WaitForSingleObject(handle, grace.as_millis().min(u32::MAX as u128 - 1) as u32)
        };
        if wait == WAIT_OBJECT_0 {
            return Ok(());
        }
        if !force {
            return Err(request.err().unwrap_or_else(|| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "daemon graceful shutdown timed out; process left running",
                )
            }));
        }
        if unsafe { TerminateProcess(handle, 1) } == 0 {
            return Err(io::Error::last_os_error());
        }
        if unsafe { WaitForSingleObject(handle, 5000) } != WAIT_OBJECT_0 {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "forced daemon exit was not observed",
            ));
        }
        Ok(())
    })();
    unsafe {
        CloseHandle(handle);
    }
    result
}

#[path = "../../tests/unit/windows_shutdown.rs"]
mod windows_shutdown_contracts;
