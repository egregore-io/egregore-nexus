//! Shared operating-system process boundaries for CLI lifecycle commands.

use std::io::{self, Read};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

const POLL_INTERVAL: Duration = Duration::from_millis(10);
const TERMINATE_GRACE: Duration = Duration::from_millis(100);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub(crate) struct BoundedOutput {
    pub(crate) status: ExitStatus,
    pub(crate) stdout: Vec<u8>,
    pub(crate) stderr: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BoundedProcessError {
    #[error("{operation} could not start: {source}")]
    Spawn {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    #[error("{operation} could not be observed: {source}")]
    Wait {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    #[error("{operation} timed out after {timeout:?}")]
    Timeout {
        operation: &'static str,
        timeout: Duration,
    },
    #[error("{operation} process-tree cleanup failed: {source}")]
    Cleanup {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    #[error("{operation} exited unsuccessfully with {status}")]
    Exit {
        operation: &'static str,
        status: ExitStatus,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    },
}

pub(crate) fn spawn_detached(command: &mut Command) -> io::Result<Child> {
    configure_detached(command);
    command.spawn()
}

pub(crate) fn run_bounded(
    command: &mut Command,
    operation: &'static str,
    timeout: Duration,
    output_limit: usize,
) -> Result<BoundedOutput, BoundedProcessError> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_bounded(command);
    let mut child = command
        .spawn()
        .map_err(|source| BoundedProcessError::Spawn { operation, source })?;
    let mut boundary = ProcessBoundary::attach(&child).map_err(|source| {
        let _ = child.kill();
        let _ = child.wait();
        BoundedProcessError::Cleanup { operation, source }
    })?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| BoundedProcessError::Wait {
            operation,
            source: io::Error::other("child stdout unavailable"),
        })?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| BoundedProcessError::Wait {
            operation,
            source: io::Error::other("child stderr unavailable"),
        })?;
    let stdout_rx = spawn_reader(stdout, output_limit);
    let stderr_rx = spawn_reader(stderr, output_limit);
    let deadline = Instant::now() + timeout;
    let mut status = None;
    let mut stdout = None;
    let mut stderr = None;

    loop {
        if status.is_none() {
            status = child
                .try_wait()
                .map_err(|source| BoundedProcessError::Wait { operation, source })?;
        }
        poll_reader(&stdout_rx, &mut stdout)
            .map_err(|source| BoundedProcessError::Wait { operation, source })?;
        poll_reader(&stderr_rx, &mut stderr)
            .map_err(|source| BoundedProcessError::Wait { operation, source })?;

        if let (Some(status), Some(stdout), Some(stderr)) =
            (status, stdout.as_ref(), stderr.as_ref())
        {
            boundary
                .finish()
                .map_err(|source| BoundedProcessError::Cleanup { operation, source })?;
            let output = BoundedOutput {
                status,
                stdout: stdout.clone(),
                stderr: stderr.clone(),
            };
            return if output.status.success() {
                Ok(output)
            } else {
                Err(BoundedProcessError::Exit {
                    operation,
                    status: output.status,
                    stdout: output.stdout,
                    stderr: output.stderr,
                })
            };
        }

        if Instant::now() >= deadline {
            boundary
                .terminate()
                .map_err(|source| BoundedProcessError::Cleanup { operation, source })?;
            wait_for_child(&mut child, Instant::now() + CLEANUP_TIMEOUT)
                .map_err(|source| BoundedProcessError::Cleanup { operation, source })?;
            let cleanup_deadline = Instant::now() + CLEANUP_TIMEOUT;
            wait_for_reader(&stdout_rx, &mut stdout, cleanup_deadline)
                .map_err(|source| BoundedProcessError::Cleanup { operation, source })?;
            wait_for_reader(&stderr_rx, &mut stderr, cleanup_deadline)
                .map_err(|source| BoundedProcessError::Cleanup { operation, source })?;
            boundary.disarm();
            return Err(BoundedProcessError::Timeout { operation, timeout });
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn spawn_reader(
    mut input: impl Read + Send + 'static,
    limit: usize,
) -> Receiver<io::Result<Vec<u8>>> {
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let mut kept = Vec::with_capacity(limit.min(8 * 1024));
        let mut chunk = [0_u8; 8 * 1024];
        let result = loop {
            match input.read(&mut chunk) {
                Ok(0) => break Ok(kept),
                Ok(read) => {
                    let remaining = limit.saturating_sub(kept.len());
                    kept.extend_from_slice(&chunk[..read.min(remaining)]);
                }
                Err(error) => break Err(error),
            }
        };
        let _ = sender.send(result);
    });
    receiver
}

fn poll_reader(
    receiver: &Receiver<io::Result<Vec<u8>>>,
    slot: &mut Option<Vec<u8>>,
) -> io::Result<()> {
    if slot.is_some() {
        return Ok(());
    }
    match receiver.try_recv() {
        Ok(result) => *slot = Some(result?),
        Err(TryRecvError::Empty) => {}
        Err(TryRecvError::Disconnected) => {
            return Err(io::Error::other("bounded output reader disconnected"))
        }
    }
    Ok(())
}

fn wait_for_reader(
    receiver: &Receiver<io::Result<Vec<u8>>>,
    slot: &mut Option<Vec<u8>>,
    deadline: Instant,
) -> io::Result<()> {
    while slot.is_none() && Instant::now() < deadline {
        poll_reader(receiver, slot)?;
        if slot.is_none() {
            thread::sleep(POLL_INTERVAL);
        }
    }
    if slot.is_some() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "bounded output pipe did not close during cleanup",
        ))
    }
}

fn wait_for_child(child: &mut Child, deadline: Instant) -> io::Result<()> {
    while Instant::now() < deadline {
        if child.try_wait()?.is_some() {
            return Ok(());
        }
        thread::sleep(POLL_INTERVAL);
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "bounded child did not exit during cleanup",
    ))
}

fn configure_detached(command: &mut Command) {
    #[cfg(unix)]
    unsafe {
        use std::os::unix::process::CommandExt;
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        command.creation_flags(CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS);
    }
}

fn configure_bounded(command: &mut Command) {
    #[cfg(unix)]
    unsafe {
        use std::os::unix::process::CommandExt;
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        command.creation_flags(CREATE_NEW_PROCESS_GROUP);
    }
}

#[cfg(unix)]
struct ProcessBoundary {
    pgid: u32,
    armed: bool,
}

#[cfg(unix)]
impl ProcessBoundary {
    fn attach(child: &Child) -> io::Result<Self> {
        Ok(Self {
            pgid: child.id(),
            armed: true,
        })
    }

    fn terminate(&mut self) -> io::Result<()> {
        signal_process_group(self.pgid, libc::SIGTERM)?;
        thread::sleep(TERMINATE_GRACE);
        signal_process_group(self.pgid, libc::SIGKILL)?;
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        if process_group_exists(self.pgid) {
            self.terminate()?;
        }
        self.disarm();
        Ok(())
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

#[cfg(unix)]
impl Drop for ProcessBoundary {
    fn drop(&mut self) {
        if self.armed {
            let _ = signal_process_group(self.pgid, libc::SIGKILL);
        }
    }
}

#[cfg(unix)]
fn signal_process_group(pgid: u32, signal: i32) -> io::Result<()> {
    let pgid = i32::try_from(pgid)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "process group id overflow"))?;
    let result = unsafe { libc::kill(-pgid, signal) };
    if result == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(error)
    }
}

#[cfg(unix)]
fn process_group_exists(pgid: u32) -> bool {
    let Ok(pgid) = i32::try_from(pgid) else {
        return false;
    };
    let result = unsafe { libc::kill(-pgid, 0) };
    result == 0 || io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

#[cfg(windows)]
struct ProcessBoundary {
    job: windows_sys::Win32::Foundation::HANDLE,
    armed: bool,
}

#[cfg(windows)]
impl ProcessBoundary {
    fn attach(child: &Child) -> io::Result<Self> {
        use std::mem::{size_of, zeroed};
        use std::os::windows::io::AsRawHandle;
        use std::ptr;
        use windows_sys::Win32::System::JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
            SetInformationJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
            JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        };

        let job = unsafe { CreateJobObjectW(ptr::null(), ptr::null()) };
        if job.is_null() {
            return Err(io::Error::last_os_error());
        }
        let mut information: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { zeroed() };
        information.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        let configured = unsafe {
            SetInformationJobObject(
                job,
                JobObjectExtendedLimitInformation,
                &information as *const _ as *const _,
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        let assigned = configured != 0
            && unsafe { AssignProcessToJobObject(job, child.as_raw_handle() as _) } != 0;
        if !assigned {
            unsafe {
                windows_sys::Win32::Foundation::CloseHandle(job);
            }
            return Err(io::Error::last_os_error());
        }
        Ok(Self { job, armed: true })
    }

    fn terminate(&mut self) -> io::Result<()> {
        let result =
            unsafe { windows_sys::Win32::System::JobObjects::TerminateJobObject(self.job, 1) };
        if result == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    fn finish(&mut self) -> io::Result<()> {
        // Closing a kill-on-close job is also the normal cleanup boundary for helper descendants.
        self.terminate()?;
        self.disarm();
        Ok(())
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

#[cfg(windows)]
impl Drop for ProcessBoundary {
    fn drop(&mut self) {
        if self.armed {
            let _ =
                unsafe { windows_sys::Win32::System::JobObjects::TerminateJobObject(self.job, 1) };
        }
        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(self.job);
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/lifecycle_process.rs"]
mod lifecycle_process_contracts;
