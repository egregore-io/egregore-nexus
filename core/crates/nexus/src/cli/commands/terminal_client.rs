//! `nexus terminal-client <session>` — interactive terminal for a raw daemon-owned PTY runtime.
//!
//! The de-tmux (#22) attach path: the daemon owns the harness PTY and serves it over the
//! per-session terminal socket; this client turns ANY terminal emulator (Alacritty, kitty, your
//! current shell) into the viewer. Hidden plumbing — humans reach it through `nexus attach` /
//! `nexus pty-attach`, whose descriptor execs this subcommand for `raw-pty` runtimes.
//!
//! Detach with `Ctrl-]` — the harness keeps running daemon-owned, exactly like a tmux detach.

use std::process::ExitCode;

use nexus_contracts::SessionId;

/// `nexus terminal-client <session>` (hidden) — dial the session's terminal socket raw-mode.
#[derive(clap::Args, Debug)]
pub struct TerminalClientArgs {
    /// The Nexus session id whose daemon-owned PTY to view.
    pub session: String,
}

const DETACH_BYTE: u8 = 0x1d; // Ctrl-]

#[cfg(unix)]
pub async fn terminal_client(a: TerminalClientArgs) -> ExitCode {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixStream;
    use tokio::signal::unix::{signal, SignalKind};

    use crate::daemon::terminal_socket::{
        read_terminal_endpoint_manifest, read_terminal_frame, write_terminal_frame, TerminalFrame,
    };

    let session = SessionId(a.session);
    let Some(endpoint) = read_terminal_endpoint_manifest(&session) else {
        eprintln!(
            "error: no terminal endpoint manifest for {} — the runtime is not a live raw-PTY \
             headed session (daemon down, session dead, or a tmux-backed runtime)",
            session.0
        );
        return ExitCode::from(1);
    };

    let stream = match UnixStream::connect(&endpoint.path).await {
        Ok(stream) => stream,
        Err(e) => {
            eprintln!(
                "error: connect terminal socket {}: {e}",
                endpoint.path.display()
            );
            return ExitCode::from(1);
        }
    };
    let (mut sock_read, mut sock_write) = stream.into_split();

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
    if let Some((cols, rows)) = terminal_size() {
        let _ = write_terminal_frame(&mut sock_write, TerminalFrame::Resize { cols, rows }).await;
    }

    eprintln!(
        "[attach] raw-pty terminal {} via {} — detach: Ctrl-] (agent keeps running)",
        session.0,
        endpoint.path.display()
    );
    let raw_guard = match RawModeGuard::enable() {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!("error: enable raw terminal mode: {e}");
            return ExitCode::from(1);
        }
    };

    let mut winch = match signal(SignalKind::window_change()) {
        Ok(winch) => winch,
        Err(e) => {
            eprintln!("error: install SIGWINCH handler: {e}");
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
            _ = winch.recv() => {
                if let Some((cols, rows)) = terminal_size() {
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

#[cfg(not(unix))]
pub async fn terminal_client(_a: TerminalClientArgs) -> ExitCode {
    eprintln!("error: the raw-pty terminal client is only implemented on Unix");
    ExitCode::from(1)
}

#[cfg(unix)]
fn terminal_size() -> Option<(u16, u16)> {
    let mut size: libc::winsize = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut size) };
    (rc == 0 && size.ws_col > 0 && size.ws_row > 0).then_some((size.ws_col, size.ws_row))
}

/// Puts stdin into raw mode for the client's lifetime; restores the original termios on drop so
/// the user's shell is never left raw, whichever way the loop exits.
#[cfg(unix)]
struct RawModeGuard {
    original: libc::termios,
}

#[cfg(unix)]
impl RawModeGuard {
    fn enable() -> std::io::Result<RawModeGuard> {
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

    fn restore(&self) {
        unsafe { libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &self.original) };
    }
}

#[cfg(unix)]
impl Drop for RawModeGuard {
    fn drop(&mut self) {
        self.restore();
    }
}

/// Leave raw mode before printing a status line so it renders on a sane terminal.
#[cfg(unix)]
fn drop_raw_and_eprintln(guard: &RawModeGuard, message: &str) {
    guard.restore();
    eprintln!("\r\n[attach] {message}");
}
