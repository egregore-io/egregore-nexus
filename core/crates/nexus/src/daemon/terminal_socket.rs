//! Local terminal byte side-channel for web terminal attach.
//!
//! PTY bytes are not durable bus/store data, so gateway websocket code dials this local socket
//! (Unix domain socket on Unix, named pipe on Windows) and relays framed terminal bytes to/from the
//! runtime's [`nexus_pty::TerminalBackend`]. The daemon owns the backend; the gateway owns network
//! auth and websocket framing.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use nexus_contracts::SessionId;
use nexus_pty::TerminalBackend;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
#[cfg(windows)]
use tokio::net::windows::named_pipe::{NamedPipeServer, PipeMode, ServerOptions};
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use uuid::Uuid;

const FRAME_AUTH: u8 = 0x01;
const FRAME_INPUT: u8 = 0x02;
const FRAME_RESIZE: u8 = 0x03;
const FRAME_HELLO: u8 = 0x10;
const FRAME_OUTPUT: u8 = 0x11;
const FRAME_ERROR: u8 = 0x7f;
const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;

/// Local terminal socket endpoint for one PTY-backed runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalSocketEndpoint {
    /// Nexus session id this endpoint is bound to.
    pub session_id: SessionId,
    /// Local endpoint path the gateway should dial.
    ///
    /// Unix builds use a filesystem Unix-domain socket. Windows builds use a named-pipe path such
    /// as `\\.\pipe\nexus-terminal-...`.
    pub path: PathBuf,
    /// Bearer token expected as the first `Auth` frame on the socket.
    pub token: String,
}

/// Framed protocol carried over the local terminal socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalFrame {
    /// First client frame. Must match [`TerminalSocketEndpoint::token`].
    Auth(String),
    /// Daemon -> client identity proof sent immediately after successful auth.
    Hello { session_id: String },
    /// Client -> daemon terminal input bytes.
    Input(Vec<u8>),
    /// Daemon -> client terminal output bytes.
    Output(Vec<u8>),
    /// Client -> daemon terminal resize control. Terminal order: columns, rows.
    Resize { cols: u16, rows: u16 },
    /// Daemon -> client terminal/socket error.
    Error(String),
}

/// Write one terminal protocol frame.
pub async fn write_terminal_frame<W>(writer: &mut W, frame: TerminalFrame) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let (kind, payload) = encode_frame(frame);
    writer.write_u8(kind).await?;
    writer.write_u32(payload.len() as u32).await?;
    writer.write_all(&payload).await?;
    writer.flush().await
}

/// Read one terminal protocol frame.
pub async fn read_terminal_frame<R>(reader: &mut R) -> std::io::Result<TerminalFrame>
where
    R: AsyncRead + Unpin,
{
    let kind = reader.read_u8().await?;
    let len = reader.read_u32().await? as usize;
    if len > MAX_FRAME_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("terminal frame too large: {len}"),
        ));
    }
    let mut payload = vec![0; len];
    reader.read_exact(&mut payload).await?;
    decode_frame(kind, payload)
}

/// Registry of per-session terminal sockets. Cloned supervisors share the same socket map.
#[derive(Default, Clone)]
pub struct TerminalSocketRegistry {
    sockets: Arc<Mutex<HashMap<SessionId, TerminalSocketHandle>>>,
}

impl TerminalSocketRegistry {
    /// Bind a terminal backend to a fresh local socket for `session`.
    pub fn bind(
        &self,
        session: SessionId,
        backend: Arc<dyn TerminalBackend>,
    ) -> Result<TerminalSocketEndpoint, String> {
        self.unbind(&session);
        let path = terminal_socket_path(&session);
        cleanup_terminal_socket_path(&path);
        let endpoint = TerminalSocketEndpoint {
            session_id: session.clone(),
            path: path.clone(),
            token: Uuid::new_v4().to_string(),
        };
        let token = endpoint.token.clone();
        let task = spawn_terminal_socket(path.clone(), session.clone(), token, backend)
            .map_err(|e| format!("bind terminal socket {}: {e}", path.display()))?;
        write_terminal_endpoint_manifest(&session, &endpoint);
        self.sockets.lock().unwrap().insert(
            session,
            TerminalSocketHandle {
                endpoint: endpoint.clone(),
                task,
            },
        );
        Ok(endpoint)
    }

    /// Return the endpoint for a bound terminal backend.
    pub fn endpoint(&self, session: &SessionId) -> Option<TerminalSocketEndpoint> {
        self.sockets
            .lock()
            .unwrap()
            .get(session)
            .map(|handle| handle.endpoint.clone())
    }

    /// Remove one terminal socket and stop accepting new terminal clients.
    pub fn unbind(&self, session: &SessionId) {
        remove_terminal_endpoint_manifest(session);
        self.sockets.lock().unwrap().remove(session);
    }

    /// Remove every terminal socket. Used during supervisor shutdown.
    pub fn clear(&self) {
        let sessions: Vec<SessionId> = self.sockets.lock().unwrap().keys().cloned().collect();
        for session in &sessions {
            remove_terminal_endpoint_manifest(session);
        }
        self.sockets.lock().unwrap().clear();
    }
}

/// Directory holding per-session terminal endpoint manifests.
///
/// The daemon writes `{session_id, path, token}` here at bind time so LOCAL same-user tooling (the store-backed
/// `nexus attach`/`pty-attach` CLI) can dial a raw daemon-owned PTY without a daemon RPC. Files are
/// 0600 in `~/.nexus` (same trust domain as the client keys stored there);
/// `NEXUS_TERMINAL_MANIFEST_DIR` overrides for tests.
pub fn terminal_endpoint_manifest_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("NEXUS_TERMINAL_MANIFEST_DIR") {
        if !dir.trim().is_empty() {
            return PathBuf::from(dir);
        }
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
    PathBuf::from(home)
        .join(".nexus")
        .join("terminal-endpoints")
}

/// Manifest path for one session's terminal endpoint.
pub fn terminal_endpoint_manifest_path(session: &SessionId) -> PathBuf {
    terminal_endpoint_manifest_dir().join(format!("{}.json", safe_session_id(session)))
}

/// Read a session's endpoint manifest, if the daemon currently exposes its terminal socket.
pub fn read_terminal_endpoint_manifest(session: &SessionId) -> Option<TerminalSocketEndpoint> {
    let raw = std::fs::read_to_string(terminal_endpoint_manifest_path(session)).ok()?;
    let value: serde_json::Value = serde_json::from_str(&raw).ok()?;
    let manifest_session = value.get("session_id")?.as_str()?;
    if manifest_session != session.0 {
        return None;
    }
    let path = value.get("path")?.as_str()?;
    let token = value.get("token")?.as_str()?;
    Some(TerminalSocketEndpoint {
        session_id: session.clone(),
        path: PathBuf::from(path),
        token: token.to_string(),
    })
}

fn write_terminal_endpoint_manifest(session: &SessionId, endpoint: &TerminalSocketEndpoint) {
    let dir = terminal_endpoint_manifest_dir();
    if std::fs::create_dir_all(&dir).is_err() {
        return;
    }
    let manifest = serde_json::json!({
        "session_id": session.0,
        "path": endpoint.path.to_string_lossy(),
        "token": endpoint.token,
    });
    let path = terminal_endpoint_manifest_path(session);
    let _ = std::fs::write(&path, manifest.to_string());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
}

fn remove_terminal_endpoint_manifest(session: &SessionId) {
    let _ = std::fs::remove_file(terminal_endpoint_manifest_path(session));
}

/// Process-wide lock for tests that mutate `NEXUS_TERMINAL_MANIFEST_DIR` (env is global).
///
/// Delegates to the CLI ambient env-test lock so terminal-manifest tests serialize with identity
/// env tests instead of maintaining a second lock.
pub fn manifest_env_lock() -> &'static std::sync::Mutex<()> {
    &crate::cli::ambient::ENV_TEST_LOCK
}

struct TerminalSocketHandle {
    endpoint: TerminalSocketEndpoint,
    task: JoinHandle<()>,
}

impl Drop for TerminalSocketHandle {
    fn drop(&mut self) {
        self.task.abort();
        cleanup_terminal_socket_path(&self.endpoint.path);
    }
}

#[cfg(unix)]
fn spawn_terminal_socket(
    path: PathBuf,
    session: SessionId,
    token: String,
    backend: Arc<dyn TerminalBackend>,
) -> std::io::Result<JoinHandle<()>> {
    let listener = UnixListener::bind(&path)?;
    Ok(tokio::spawn(serve_terminal_socket(
        listener, session, token, backend,
    )))
}

#[cfg(unix)]
async fn serve_terminal_socket(
    listener: UnixListener,
    session: SessionId,
    token: String,
    backend: Arc<dyn TerminalBackend>,
) {
    loop {
        let Ok((stream, _addr)) = listener.accept().await else {
            break;
        };
        let session = session.clone();
        let token = token.clone();
        let backend = backend.clone();
        tokio::spawn(async move {
            let _ = handle_terminal_client(stream, session, token, backend).await;
        });
    }
}

#[cfg(windows)]
fn spawn_terminal_socket(
    path: PathBuf,
    session: SessionId,
    token: String,
    backend: Arc<dyn TerminalBackend>,
) -> std::io::Result<JoinHandle<()>> {
    let pipe_name = path.to_string_lossy().into_owned();
    let first_server = ServerOptions::new()
        .pipe_mode(PipeMode::Byte)
        .first_pipe_instance(true)
        .create(&pipe_name)?;
    Ok(tokio::spawn(serve_terminal_pipe(
        pipe_name,
        session,
        token,
        backend,
        first_server,
    )))
}

#[cfg(windows)]
async fn serve_terminal_pipe(
    pipe_name: String,
    session: SessionId,
    token: String,
    backend: Arc<dyn TerminalBackend>,
    mut server: NamedPipeServer,
) {
    loop {
        if server.connect().await.is_err() {
            break;
        }
        let connected = server;
        let next = match ServerOptions::new()
            .pipe_mode(PipeMode::Byte)
            .create(&pipe_name)
        {
            Ok(next) => next,
            Err(_) => {
                let session = session.clone();
                let token = token.clone();
                let backend = backend.clone();
                tokio::spawn(async move {
                    let _ = handle_terminal_client(connected, session, token, backend).await;
                });
                break;
            }
        };
        let session = session.clone();
        let token = token.clone();
        let backend = backend.clone();
        tokio::spawn(async move {
            let _ = handle_terminal_client(connected, session, token, backend).await;
        });
        server = next;
    }
}

async fn handle_terminal_client<S>(
    mut stream: S,
    session: SessionId,
    token: String,
    backend: Arc<dyn TerminalBackend>,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    use tokio::io::split;

    match read_terminal_frame(&mut stream).await {
        Ok(TerminalFrame::Auth(got)) if got == token => {}
        Ok(_) | Err(_) => {
            let _ = write_terminal_frame(&mut stream, TerminalFrame::Error("unauthorized".into()))
                .await;
            return Ok(());
        }
    }

    write_terminal_frame(
        &mut stream,
        TerminalFrame::Hello {
            session_id: session.0.clone(),
        },
    )
    .await?;

    let attached = backend.attach();
    let terminal_writer = attached.writer.clone();
    let resize_backend = backend.clone();
    let mut terminal_reader = attached.reader;
    let (mut socket_reader, mut socket_writer) = split(stream);

    // A fresh attach starts from a coherent screen, not a mid-stream byte tap: declare the
    // authoritative pty size (a Resize frame daemon->client; legacy clients ignore it), then
    // one repaint burst of the current screen. Subscribe-before-snapshot above means a byte
    // racing the snapshot can be applied twice — a TUI redraw absorbs that; the reverse order
    // would LOSE bytes.
    if let Some(snapshot) = backend.snapshot() {
        let declared_size = write_terminal_frame(
            &mut socket_writer,
            TerminalFrame::Resize {
                cols: snapshot.cols,
                rows: snapshot.rows,
            },
        )
        .await;
        if declared_size.is_err()
            || write_terminal_frame(&mut socket_writer, TerminalFrame::Output(snapshot.bytes))
                .await
                .is_err()
        {
            return Ok(());
        }
    }

    // One forwarding task per client: live output bytes, plus winsize changes made by ANY
    // viewer (tmux-client semantics — the resizer drives the pty, everyone else adopts the
    // new authoritative size as a Resize frame instead of rendering a stale grid).
    let mut size_reader = backend.subscribe_size();
    let output_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                output = terminal_reader.recv() => match output {
                    Ok(bytes) => {
                        if write_terminal_frame(&mut socket_writer, TerminalFrame::Output(bytes))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                (cols, rows) = recv_size(&mut size_reader) => {
                    if write_terminal_frame(
                        &mut socket_writer,
                        TerminalFrame::Resize { cols, rows },
                    )
                    .await
                    .is_err()
                    {
                        break;
                    }
                }
            }
        }
    });

    loop {
        let frame = match read_terminal_frame(&mut socket_reader).await {
            Ok(frame) => frame,
            Err(_) => break,
        };
        match frame {
            TerminalFrame::Input(bytes) => {
                let writer = terminal_writer.clone();
                let _ = tokio::task::spawn_blocking(move || writer.write_bytes(&bytes)).await;
            }
            TerminalFrame::Resize { cols, rows } => {
                let backend = resize_backend.clone();
                let _ = tokio::task::spawn_blocking(move || backend.resize(cols, rows)).await;
            }
            TerminalFrame::Auth(_)
            | TerminalFrame::Hello { .. }
            | TerminalFrame::Output(_)
            | TerminalFrame::Error(_) => {
                // Ignore client frames that are not meaningful after auth.
            }
        }
    }
    output_task.abort();
    Ok(())
}

/// Await the next winsize change. Backends without size tracking (`None`) — and a closed
/// channel after a rebind — pend forever, so the select loop follows output only.
async fn recv_size(reader: &mut Option<broadcast::Receiver<(u16, u16)>>) -> (u16, u16) {
    loop {
        match reader {
            Some(rx) => match rx.recv().await {
                Ok(size) => return size,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => *reader = None,
            },
            None => std::future::pending::<()>().await,
        }
    }
}

fn encode_frame(frame: TerminalFrame) -> (u8, Vec<u8>) {
    match frame {
        TerminalFrame::Auth(token) => (FRAME_AUTH, token.into_bytes()),
        TerminalFrame::Hello { session_id } => (FRAME_HELLO, session_id.into_bytes()),
        TerminalFrame::Input(bytes) => (FRAME_INPUT, bytes),
        TerminalFrame::Output(bytes) => (FRAME_OUTPUT, bytes),
        TerminalFrame::Resize { cols, rows } => {
            let mut payload = Vec::with_capacity(4);
            payload.extend_from_slice(&cols.to_be_bytes());
            payload.extend_from_slice(&rows.to_be_bytes());
            (FRAME_RESIZE, payload)
        }
        TerminalFrame::Error(message) => (FRAME_ERROR, message.into_bytes()),
    }
}

fn decode_frame(kind: u8, payload: Vec<u8>) -> std::io::Result<TerminalFrame> {
    match kind {
        FRAME_AUTH => Ok(TerminalFrame::Auth(payload_to_string(payload)?)),
        FRAME_HELLO => Ok(TerminalFrame::Hello {
            session_id: payload_to_string(payload)?,
        }),
        FRAME_INPUT => Ok(TerminalFrame::Input(payload)),
        FRAME_OUTPUT => Ok(TerminalFrame::Output(payload)),
        FRAME_RESIZE => {
            if payload.len() != 4 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "resize frame must be 4 bytes",
                ));
            }
            Ok(TerminalFrame::Resize {
                cols: u16::from_be_bytes([payload[0], payload[1]]),
                rows: u16::from_be_bytes([payload[2], payload[3]]),
            })
        }
        FRAME_ERROR => Ok(TerminalFrame::Error(payload_to_string(payload)?)),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unknown terminal frame kind: {kind}"),
        )),
    }
}

fn payload_to_string(payload: Vec<u8>) -> std::io::Result<String> {
    String::from_utf8(payload)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
}

fn terminal_socket_path(session: &SessionId) -> PathBuf {
    #[cfg(windows)]
    {
        return PathBuf::from(format!(
            r"\\.\pipe\nexus-terminal-{}",
            safe_session_id(session)
        ));
    }
    #[cfg(not(windows))]
    std::env::temp_dir().join(format!("nexus-terminal-{}.sock", safe_session_id(session)))
}

#[cfg(unix)]
fn cleanup_terminal_socket_path(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
}

#[cfg(not(unix))]
fn cleanup_terminal_socket_path(_path: &PathBuf) {}

fn safe_session_id(session: &SessionId) -> String {
    session
        .0
        .chars()
        .map(|ch| match ch {
            '/' | ':' | '.' => '-',
            other => other,
        })
        .collect()
}
