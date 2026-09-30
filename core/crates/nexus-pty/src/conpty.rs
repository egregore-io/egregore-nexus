//! Windows ConPTY terminal backend.
//!
//! [`portable_pty::native_pty_system`] uses Windows ConPTY on supported Windows hosts. This wrapper
//! gives Nexus an explicit Windows terminal backend type while preserving the same raw byte,
//! resize, and [`crate::HarnessInput`] contracts used by Unix raw PTYs.

use std::sync::Arc;

use portable_pty::{CommandBuilder, PtySize};

use crate::{
    HarnessInput, PtyError, PtySession, TerminalAttachment, TerminalBackend, TerminalWriter,
};

/// A Windows ConPTY-backed runtime terminal.
///
/// The inner [`PtySession`] owns the portable-pty master. On Windows, portable-pty's native backend
/// is ConPTY, so this type is the Windows implementation of Nexus's terminal backend abstraction.
#[derive(Clone)]
pub struct ConPtyBackend {
    session: Arc<PtySession>,
}

impl ConPtyBackend {
    /// Spawn `cmd` in a Windows ConPTY and return the backend plus its tracked session.
    pub fn spawn(cmd: CommandBuilder, size: PtySize) -> Result<Self, PtyError> {
        Ok(Self::from_session(Arc::new(PtySession::spawn(cmd, size)?)))
    }

    /// Wrap an already-spawned Windows native PTY session.
    pub fn from_session(session: Arc<PtySession>) -> Self {
        Self { session }
    }

    /// Return the underlying tracked PTY session.
    pub fn session(&self) -> Arc<PtySession> {
        self.session.clone()
    }
}

#[async_trait::async_trait]
impl HarnessInput for ConPtyBackend {
    async fn send_turn(&self, text: &str) -> Result<(), String> {
        self.session.send_turn(text).await
    }

    async fn interrupt_active_turn(&self) -> Result<(), String> {
        self.session.interrupt_active_turn().await
    }

    fn is_alive(&self) -> bool {
        self.session.is_alive()
    }
}

impl TerminalWriter for ConPtyBackend {
    fn write_bytes(&self, bytes: &[u8]) -> Result<(), String> {
        self.session.write_bytes(bytes)
    }
}

impl TerminalBackend for ConPtyBackend {
    fn attach(&self) -> TerminalAttachment {
        TerminalAttachment {
            reader: self.session.subscribe(),
            writer: Arc::new(self.clone()),
        }
    }

    fn resize(&self, cols: u16, rows: u16) -> Result<(), String> {
        crate::TerminalBackend::resize(self.session.as_ref(), cols, rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_traits<T: HarnessInput + TerminalBackend + TerminalWriter + Send + Sync>() {}

    #[test]
    fn conpty_backend_satisfies_terminal_contracts() {
        assert_traits::<ConPtyBackend>();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn conpty_backend_delegates_terminal_bytes_to_wrapped_session() {
        let size = PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        };
        let session = Arc::new(PtySession::spawn(CommandBuilder::new("cat"), size).unwrap());
        let backend = ConPtyBackend::from_session(session);
        let mut attached = backend.attach();

        attached.writer.write_bytes(b"conpty-wrapper\n").unwrap();

        let got = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            let mut buf = Vec::new();
            while let Ok(chunk) = attached.reader.recv().await {
                buf.extend_from_slice(&chunk);
                if String::from_utf8_lossy(&buf).contains("conpty-wrapper") {
                    return true;
                }
            }
            false
        })
        .await
        .unwrap_or(false);

        assert!(got, "ConPTY wrapper must expose inner terminal bytes");
        backend.resize(100, 30).unwrap();
    }
}
