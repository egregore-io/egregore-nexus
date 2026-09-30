use std::sync::{Arc, Mutex};

use portable_pty::{Child, CommandBuilder, MasterPty, PtySize};
use tokio::sync::broadcast;

#[derive(Debug, thiserror::Error)]
pub enum PtyError {
    #[error("pty spawn failed: {0}")]
    Spawn(String),
    #[error("pty io error: {0}")]
    Io(String),
    #[error("pty closed")]
    Closed,
}

/// A native harness running in a daemon-owned PTY. Owns the master end: write injects input,
/// `subscribe` fans the harness's terminal output to any number of readers (the operator's
/// attached terminal AND the transcript path's not needed here — output bytes are the raw TTY).
#[derive(Clone)]
pub struct PtySession {
    master: Arc<Mutex<Box<dyn MasterPty + Send>>>,
    writer: Arc<Mutex<Box<dyn std::io::Write + Send>>>,
    child: Arc<Mutex<Box<dyn Child + Send + Sync>>>,
    out_tx: broadcast::Sender<Vec<u8>>,
    child_pid: Option<u32>,
}

impl PtySession {
    pub fn spawn(cmd: CommandBuilder, size: PtySize) -> Result<PtySession, PtyError> {
        let pty = portable_pty::native_pty_system()
            .openpty(size)
            .map_err(|e| PtyError::Spawn(e.to_string()))?;
        let child = pty
            .slave
            .spawn_command(cmd)
            .map_err(|e| PtyError::Spawn(e.to_string()))?;
        let child_pid = child.process_id();
        // The slave handle must be dropped after spawn so the child owns the only slave fd
        // (otherwise EOF never propagates when the child exits).
        drop(pty.slave);

        let mut reader = pty
            .master
            .try_clone_reader()
            .map_err(|e| PtyError::Io(e.to_string()))?;
        let writer = pty
            .master
            .take_writer()
            .map_err(|e| PtyError::Io(e.to_string()))?;
        let (out_tx, _) = broadcast::channel::<Vec<u8>>(1024);

        // Blocking reader on a dedicated thread → broadcast. PTY reads are blocking; keep them off
        // the async runtime.
        let tx = out_tx.clone();
        std::thread::spawn(move || {
            use std::io::Read;
            let mut buf = [0u8; 4096];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let _ = tx.send(buf[..n].to_vec());
                    }
                }
            }
        });

        Ok(PtySession {
            master: Arc::new(Mutex::new(pty.master)),
            writer: Arc::new(Mutex::new(writer)),
            child: Arc::new(Mutex::new(child)),
            out_tx,
            child_pid,
        })
    }

    pub fn write(&self, bytes: &[u8]) -> Result<(), PtyError> {
        use std::io::Write;
        let mut w = self.writer.lock().map_err(|_| PtyError::Closed)?;
        w.write_all(bytes)
            .map_err(|e| PtyError::Io(e.to_string()))?;
        w.flush().map_err(|e| PtyError::Io(e.to_string()))
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Vec<u8>> {
        self.out_tx.subscribe()
    }

    pub fn resize(&self, rows: u16, cols: u16) -> Result<(), PtyError> {
        let m = self.master.lock().map_err(|_| PtyError::Closed)?;
        m.resize(PtySize {
            rows,
            cols,
            pixel_width: 0,
            pixel_height: 0,
        })
        .map_err(|e| PtyError::Io(e.to_string()))
    }

    /// The pty's current winsize — the authoritative terminal size for viewers.
    pub fn winsize(&self) -> Option<PtySize> {
        let m = self.master.lock().ok()?;
        m.get_size().ok()
    }

    pub fn child_pid(&self) -> Option<u32> {
        self.child_pid
    }

    pub fn kill(&self) -> Result<(), PtyError> {
        let mut c = self.child.lock().map_err(|_| PtyError::Closed)?;
        c.kill().map_err(|e| PtyError::Io(e.to_string()))
    }

    /// Whether the spawned child is still running. `try_wait` returning `Ok(None)` means it has not
    /// exited; any exit status or a poisoned lock counts as not-alive.
    pub fn child_alive(&self) -> bool {
        match self.child.lock() {
            Ok(mut c) => matches!(c.try_wait(), Ok(None)),
            Err(_) => false,
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use portable_pty::{CommandBuilder, PtySize};

    #[tokio::test]
    async fn echoes_written_input_to_the_output_stream() {
        // `cat` echoes stdin to stdout — a deterministic stand-in for a harness.
        let size = PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        };
        let session = PtySession::spawn(CommandBuilder::new("cat"), size).unwrap();
        let mut out = session.subscribe();

        session.write(b"hello\n").unwrap();

        // Collect output until we see "hello" or time out.
        let got = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            let mut buf = Vec::new();
            while let Ok(chunk) = out.recv().await {
                buf.extend_from_slice(&chunk);
                if String::from_utf8_lossy(&buf).contains("hello") {
                    return true;
                }
            }
            false
        })
        .await
        .unwrap_or(false);

        assert!(
            got,
            "PTY output stream must carry what was written to its input"
        );
    }

    #[tokio::test]
    async fn terminal_backend_exposes_raw_pty_bytes_both_directions() {
        use crate::TerminalBackend as _;

        let size = PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        };
        let session = PtySession::spawn(CommandBuilder::new("cat"), size).unwrap();
        let mut attached = session.attach();

        attached.writer.write_bytes(b"web-terminal-raw\n").unwrap();

        let got = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            let mut buf = Vec::new();
            while let Ok(chunk) = attached.reader.recv().await {
                buf.extend_from_slice(&chunk);
                if String::from_utf8_lossy(&buf).contains("web-terminal-raw") {
                    return true;
                }
            }
            false
        })
        .await
        .unwrap_or(false);

        assert!(got, "terminal attach must read raw PTY output");
        crate::TerminalBackend::resize(&session, 100, 30).unwrap();
    }
}
