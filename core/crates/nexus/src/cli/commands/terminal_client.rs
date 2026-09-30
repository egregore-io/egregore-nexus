//! `nexus terminal-client <session>` — interactive terminal for a raw daemon-owned PTY runtime.
//!
//! The de-tmux (#22) attach path: the daemon owns the harness PTY and serves it over the
//! per-session terminal socket; this client turns ANY terminal emulator (Alacritty, kitty, your
//! current shell, Windows Terminal) into the viewer. Hidden plumbing — humans reach it through
//! `nexus attach` / `nexus pty-attach`, whose descriptor execs this subcommand for `raw-pty`
//! runtimes.
//!
//! The socket protocol and the relay loop are platform-neutral. Only the console plumbing
//! differs: Unix dials a Unix-domain socket, puts the tty into termios raw mode, and learns about
//! resizes from `SIGWINCH`; Windows dials the daemon's named pipe, switches the console into
//! virtual-terminal mode, and polls the window size because the console has no resize signal.
//!
//! Detach with `Ctrl-]` — the harness keeps running daemon-owned, exactly like a tmux detach.

use std::process::ExitCode;

use nexus_contracts::SessionId;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::daemon::terminal_socket::{
    read_terminal_endpoint_manifest, read_terminal_frame, write_terminal_frame, TerminalFrame,
};

/// `nexus terminal-client <session>` (hidden) — dial the session's terminal socket raw-mode.
#[derive(clap::Args, Debug)]
pub struct TerminalClientArgs {
    /// The Nexus session id whose daemon-owned PTY to view.
    pub session: String,
}

const DETACH_BYTE: u8 = 0x1d; // Ctrl-]

pub async fn terminal_client(a: TerminalClientArgs) -> ExitCode {
    let session = SessionId(a.session);
    let Some(endpoint) = read_terminal_endpoint_manifest(&session) else {
        eprintln!(
            "error: no terminal endpoint manifest for {} — the runtime is not a live raw-PTY \
             headed session (daemon down, session dead, or a tmux-backed runtime)",
            session.0
        );
        return ExitCode::from(1);
    };

    let stream = match console::connect(&endpoint.path).await {
        Ok(stream) => stream,
        Err(e) => {
            eprintln!(
                "error: connect terminal socket {}: {e}",
                endpoint.path.display()
            );
            return ExitCode::from(1);
        }
    };
    let (mut sock_read, mut sock_write) = tokio::io::split(stream);

    if let Err(e) =
        write_terminal_frame(&mut sock_write, TerminalFrame::Auth(endpoint.token.clone())).await
    {
        eprintln!("error: terminal socket auth: {e}");
        return ExitCode::from(1);
    }
    match tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_terminal_frame(&mut sock_read),
    )
    .await
    {
        Ok(Ok(TerminalFrame::Hello { session_id })) if session_id == session.0 => {}
        Ok(Ok(TerminalFrame::Hello { session_id })) => {
            eprintln!(
                "error: terminal socket identity mismatch: requested {}, endpoint says {}",
                session.0, session_id
            );
            return ExitCode::from(1);
        }
        Ok(Ok(TerminalFrame::Error(message))) => {
            eprintln!("error: terminal socket auth failed: {message}");
            return ExitCode::from(1);
        }
        Ok(Ok(frame)) => {
            eprintln!(
                "error: terminal socket identity check failed for {}: expected hello, got {frame:?}",
                session.0
            );
            return ExitCode::from(1);
        }
        Ok(Err(e)) => {
            eprintln!("error: read terminal socket identity: {e}");
            return ExitCode::from(1);
        }
        Err(_) => {
            eprintln!(
                "error: timed out waiting for terminal socket identity for {}",
                session.0
            );
            return ExitCode::from(1);
        }
    }
    if let Some((cols, rows)) = console::terminal_size() {
        let _ = write_terminal_frame(&mut sock_write, TerminalFrame::Resize { cols, rows }).await;
    }

    eprintln!(
        "[attach] raw-pty terminal {} via {} — detach: Ctrl-] (agent keeps running)",
        session.0,
        endpoint.path.display()
    );
    let raw_guard = match console::RawModeGuard::enable() {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!("error: enable raw terminal mode: {e}");
            return ExitCode::from(1);
        }
    };

    let mut resize = match console::ResizeWatch::new() {
        Ok(resize) => resize,
        Err(e) => {
            eprintln!("error: watch terminal size: {e}");
            return ExitCode::from(1);
        }
    };
    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut input = [0u8; 4096];

    let code = loop {
        tokio::select! {
            frame = read_terminal_frame(&mut sock_read) => match frame {
                Ok(TerminalFrame::Output(bytes)) => {
                    if stdout.write_all(&bytes).await.is_err() {
                        break 1;
                    }
                    let _ = stdout.flush().await;
                }
                Ok(TerminalFrame::Error(message)) => {
                    drop_raw_and_eprintln(&raw_guard, &format!("terminal socket error: {message}"));
                    break 1;
                }
                Ok(_) => {} // Auth/Hello/Input/Resize are not terminal output; ignore if echoed.
                Err(_) => {
                    drop_raw_and_eprintln(&raw_guard, "connection closed by daemon");
                    break 0;
                }
            },
            read = stdin.read(&mut input) => match read {
                Ok(0) => break 0,
                Ok(n) => {
                    if let Some(pos) = input[..n].iter().position(|b| *b == DETACH_BYTE) {
                        if pos > 0 {
                            let _ = write_terminal_frame(
                                &mut sock_write,
                                TerminalFrame::Input(input[..pos].to_vec()),
                            )
                            .await;
                        }
                        drop_raw_and_eprintln(&raw_guard, "detached (agent keeps running)");
                        break 0;
                    }
                    if write_terminal_frame(
                        &mut sock_write,
                        TerminalFrame::Input(input[..n].to_vec()),
                    )
                    .await
                    .is_err()
                    {
                        drop_raw_and_eprintln(&raw_guard, "connection closed by daemon");
                        break 0;
                    }
                }
                Err(_) => break 1,
            },
            _ = resize.changed() => {
                if let Some((cols, rows)) = console::terminal_size() {
                    let _ = write_terminal_frame(
                        &mut sock_write,
                        TerminalFrame::Resize { cols, rows },
                    )
                    .await;
                }
            }
        }
    };
    drop(raw_guard);
    ExitCode::from(code)
}

/// Leave raw mode before printing a status line so it renders on a sane terminal.
fn drop_raw_and_eprintln(guard: &console::RawModeGuard, message: &str) {
    guard.restore();
    eprintln!("\r\n[attach] {message}");
}

/// Unix console plumbing: Unix-domain socket, termios raw mode, `SIGWINCH`.
#[cfg(unix)]
mod console {
    use std::path::Path;

    use tokio::net::UnixStream;
    use tokio::signal::unix::{signal, Signal, SignalKind};

    pub async fn connect(path: &Path) -> std::io::Result<UnixStream> {
        UnixStream::connect(path).await
    }

    pub fn terminal_size() -> Option<(u16, u16)> {
        let mut size: libc::winsize = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut size) };
        (rc == 0 && size.ws_col > 0 && size.ws_row > 0).then_some((size.ws_col, size.ws_row))
    }

    /// Puts stdin into raw mode for the client's lifetime; restores the original termios on drop
    /// so the user's shell is never left raw, whichever way the loop exits.
    pub struct RawModeGuard {
        original: libc::termios,
    }

    impl RawModeGuard {
        pub fn enable() -> std::io::Result<RawModeGuard> {
            let mut original: libc::termios = unsafe { std::mem::zeroed() };
            if unsafe { libc::tcgetattr(libc::STDIN_FILENO, &mut original) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
            let mut raw = original;
            unsafe { libc::cfmakeraw(&mut raw) };
            if unsafe { libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &raw) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(RawModeGuard { original })
        }

        pub fn restore(&self) {
            unsafe { libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &self.original) };
        }
    }

    impl Drop for RawModeGuard {
        fn drop(&mut self) {
            self.restore();
        }
    }

    /// Resolves each time the terminal emulator reports a window-size change.
    pub struct ResizeWatch {
        winch: Signal,
    }

    impl ResizeWatch {
        pub fn new() -> std::io::Result<ResizeWatch> {
            Ok(ResizeWatch {
                winch: signal(SignalKind::window_change())?,
            })
        }

        pub async fn changed(&mut self) {
            if self.winch.recv().await.is_none() {
                // The signal stream closed; resizes can no longer be observed, so never wake.
                std::future::pending::<()>().await;
            }
        }
    }
}

/// Windows console plumbing: named pipe, virtual-terminal console modes, size polling.
#[cfg(windows)]
mod console {
    use std::path::Path;
    use std::time::Duration;

    use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient};
    use windows_sys::Win32::Foundation::{ERROR_PIPE_BUSY, HANDLE, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Console::{
        GetConsoleMode, GetConsoleScreenBufferInfo, GetStdHandle, SetConsoleMode, CONSOLE_MODE,
        CONSOLE_SCREEN_BUFFER_INFO, DISABLE_NEWLINE_AUTO_RETURN, ENABLE_ECHO_INPUT,
        ENABLE_LINE_INPUT, ENABLE_PROCESSED_INPUT, ENABLE_PROCESSED_OUTPUT,
        ENABLE_VIRTUAL_TERMINAL_INPUT, ENABLE_VIRTUAL_TERMINAL_PROCESSING, STD_INPUT_HANDLE,
        STD_OUTPUT_HANDLE,
    };

    /// How long to keep retrying a pipe whose instances are all mid-accept. The daemon creates
    /// the next instance as soon as it accepts the previous one, so this is a short window.
    const PIPE_BUSY_RETRY_BUDGET: Duration = Duration::from_secs(2);
    const PIPE_BUSY_RETRY_INTERVAL: Duration = Duration::from_millis(50);

    pub async fn connect(path: &Path) -> std::io::Result<NamedPipeClient> {
        let deadline = std::time::Instant::now() + PIPE_BUSY_RETRY_BUDGET;
        loop {
            match ClientOptions::new().open(path) {
                Ok(client) => return Ok(client),
                Err(e)
                    if e.raw_os_error() == Some(ERROR_PIPE_BUSY as i32)
                        && std::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(PIPE_BUSY_RETRY_INTERVAL).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn std_handle(which: u32) -> std::io::Result<HANDLE> {
        let handle = unsafe { GetStdHandle(which) };
        if handle == INVALID_HANDLE_VALUE || handle.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        Ok(handle)
    }

    fn console_mode(handle: HANDLE) -> std::io::Result<CONSOLE_MODE> {
        let mut mode: CONSOLE_MODE = 0;
        if unsafe { GetConsoleMode(handle, &mut mode) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(mode)
    }

    fn set_console_mode(handle: HANDLE, mode: CONSOLE_MODE) -> std::io::Result<()> {
        if unsafe { SetConsoleMode(handle, mode) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    /// Terminal order (columns, rows) of the visible console window — the ConPTY the daemon
    /// owns must match this, not the (larger) scrollback buffer.
    pub fn terminal_size() -> Option<(u16, u16)> {
        let handle = std_handle(STD_OUTPUT_HANDLE).ok()?;
        let mut info: CONSOLE_SCREEN_BUFFER_INFO = unsafe { std::mem::zeroed() };
        if unsafe { GetConsoleScreenBufferInfo(handle, &mut info) } == 0 {
            return None;
        }
        let cols = i32::from(info.srWindow.Right) - i32::from(info.srWindow.Left) + 1;
        let rows = i32::from(info.srWindow.Bottom) - i32::from(info.srWindow.Top) + 1;
        (cols > 0 && rows > 0).then(|| (cols as u16, rows as u16))
    }

    /// The console-mode equivalent of termios raw mode: stdin stops cooking lines, echoing, and
    /// turning Ctrl-C into a signal (every key reaches the harness as VT input bytes, so the
    /// `Ctrl-]` detach byte is seen here), and stdout renders the ConPTY's VT output. Both
    /// original modes are restored on drop.
    pub struct RawModeGuard {
        // Raw console handles are `*mut c_void`; kept as integers so the guard stays `Send`
        // across the relay loop's awaits.
        stdin: usize,
        stdout: usize,
        stdin_mode: CONSOLE_MODE,
        stdout_mode: CONSOLE_MODE,
    }

    impl RawModeGuard {
        pub fn enable() -> std::io::Result<RawModeGuard> {
            let stdin = std_handle(STD_INPUT_HANDLE)?;
            let stdout = std_handle(STD_OUTPUT_HANDLE)?;
            let stdin_mode = console_mode(stdin)?;
            let stdout_mode = console_mode(stdout)?;

            let raw_in = (stdin_mode
                & !(ENABLE_LINE_INPUT | ENABLE_ECHO_INPUT | ENABLE_PROCESSED_INPUT))
                | ENABLE_VIRTUAL_TERMINAL_INPUT;
            set_console_mode(stdin, raw_in)?;
            let raw_out = stdout_mode
                | ENABLE_PROCESSED_OUTPUT
                | ENABLE_VIRTUAL_TERMINAL_PROCESSING
                | DISABLE_NEWLINE_AUTO_RETURN;
            if let Err(e) = set_console_mode(stdout, raw_out) {
                let _ = set_console_mode(stdin, stdin_mode);
                return Err(e);
            }
            Ok(RawModeGuard {
                stdin: stdin as usize,
                stdout: stdout as usize,
                stdin_mode,
                stdout_mode,
            })
        }

        pub fn restore(&self) {
            let _ = set_console_mode(self.stdin as HANDLE, self.stdin_mode);
            let _ = set_console_mode(self.stdout as HANDLE, self.stdout_mode);
        }
    }

    impl Drop for RawModeGuard {
        fn drop(&mut self) {
            self.restore();
        }
    }

    /// Resolves each time the sampled console window size differs from the last sample.
    pub struct ResizeWatch {
        poll: super::PolledResize,
    }

    impl ResizeWatch {
        pub fn new() -> std::io::Result<ResizeWatch> {
            Ok(ResizeWatch {
                poll: super::PolledResize::new(terminal_size),
            })
        }

        pub async fn changed(&mut self) {
            self.poll.changed().await;
        }
    }
}

/// Polling is shared with portable tests so cancellation by busy relay branches is exercised.
#[cfg(any(windows, test))]
struct PolledResize {
    sample: fn() -> Option<(u16, u16)>,
    last: Option<(u16, u16)>,
    ticks: tokio::time::Interval,
}

#[cfg(any(windows, test))]
impl PolledResize {
    fn new(sample: fn() -> Option<(u16, u16)>) -> Self {
        let period = std::time::Duration::from_millis(250);
        let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Self {
            sample,
            last: sample(),
            ticks,
        }
    }

    async fn changed(&mut self) {
        loop {
            // Interval::tick is cancellation-safe: output/input activity cannot reset the
            // next sample deadline when the relay drops this changed() future.
            self.ticks.tick().await;
            let now = (self.sample)();
            if now.is_some() && now != self.last {
                self.last = now;
                return;
            }
        }
    }
}

#[path = "../../../tests/unit/terminal_client.rs"]
mod tests;

#[cfg(not(any(unix, windows)))]
mod console {
    use std::path::Path;

    fn unsupported() -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "the raw-pty terminal client requires Unix sockets or Windows named pipes",
        )
    }

    pub async fn connect(_path: &Path) -> std::io::Result<tokio::io::DuplexStream> {
        Err(unsupported())
    }

    pub fn terminal_size() -> Option<(u16, u16)> {
        None
    }

    pub struct RawModeGuard;

    impl RawModeGuard {
        pub fn enable() -> std::io::Result<RawModeGuard> {
            Err(unsupported())
        }

        pub fn restore(&self) {}
    }

    pub struct ResizeWatch;

    impl ResizeWatch {
        pub fn new() -> std::io::Result<ResizeWatch> {
            Err(unsupported())
        }

        pub async fn changed(&mut self) {
            std::future::pending::<()>().await
        }
    }
}
