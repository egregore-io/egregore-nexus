//! ScreenModelBackend behavior: a daemon-side vt100 model that snapshots the CURRENT screen
//! (grid, size, cursor) for fresh attachers. Moved out of src/ per the core test-layout policy.

use nexus_pty::{ByteWriter, TerminalWriter};
use nexus_pty::{ScreenModelBackend, TerminalAttachment, TerminalBackend};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

struct FakeBackend {
    tx: broadcast::Sender<Vec<u8>>,
    size: Mutex<(u16, u16)>,
}

struct NoopWriter;
impl TerminalWriter for NoopWriter {
    fn write_bytes(&self, _bytes: &[u8]) -> Result<(), String> {
        Ok(())
    }
}

impl TerminalBackend for FakeBackend {
    fn attach(&self) -> TerminalAttachment {
        TerminalAttachment {
            reader: self.tx.subscribe(),
            writer: Arc::new(NoopWriter) as ByteWriter,
        }
    }
    fn resize(&self, cols: u16, rows: u16) -> Result<(), String> {
        *self.size.lock().unwrap() = (cols, rows);
        Ok(())
    }
    fn current_size(&self) -> Option<(u16, u16)> {
        Some(*self.size.lock().unwrap())
    }
}

fn wait_until(deadline: Duration, check: impl Fn() -> bool) -> bool {
    let end = Instant::now() + deadline;
    while Instant::now() < end {
        if check() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    false
}

#[test]
fn snapshot_reproduces_streamed_screen_at_backend_size() {
    let (tx, _keep) = broadcast::channel::<Vec<u8>>(64);
    let backend = Arc::new(FakeBackend {
        tx: tx.clone(),
        size: Mutex::new((100, 30)),
    });
    let model = ScreenModelBackend::wrap(backend as Arc<dyn TerminalBackend>);

    tx.send(b"hello \x1b[31mred\x1b[m world\r\n".to_vec())
        .unwrap();

    assert!(
        wait_until(Duration::from_secs(2), || {
            model
                .snapshot()
                .map(|s| String::from_utf8_lossy(&s.bytes).contains("hello"))
                .unwrap_or(false)
        }),
        "model never absorbed streamed bytes"
    );

    let snapshot = model.snapshot().expect("screen model always snapshots");
    assert_eq!((snapshot.cols, snapshot.rows), (100, 30));
    let text = String::from_utf8_lossy(&snapshot.bytes).into_owned();
    assert!(text.contains("hello"), "snapshot lost screen text: {text}");
    assert!(text.contains("red"), "snapshot lost colored text: {text}");
    // The burst must start from a clean canvas and end with an explicit cursor pin.
    assert!(text.starts_with("\x1b[H\x1b[2J"));
    assert!(
        text.ends_with("H"),
        "snapshot must end with a cursor move: {text:?}"
    );
}

#[test]
fn resize_updates_model_grid_and_inner_backend() {
    let (tx, _keep) = broadcast::channel::<Vec<u8>>(8);
    let backend = Arc::new(FakeBackend {
        tx,
        size: Mutex::new((80, 24)),
    });
    let model = ScreenModelBackend::wrap(backend.clone() as Arc<dyn TerminalBackend>);

    model.resize(132, 43).unwrap();

    assert_eq!(model.current_size(), Some((132, 43)));
    assert_eq!(*backend.size.lock().unwrap(), (132, 43));
    let snapshot = model.snapshot().unwrap();
    assert_eq!((snapshot.cols, snapshot.rows), (132, 43));
}

#[test]
fn cursor_position_survives_snapshot() {
    let (tx, _keep) = broadcast::channel::<Vec<u8>>(8);
    let backend = Arc::new(FakeBackend {
        tx: tx.clone(),
        size: Mutex::new((80, 24)),
    });
    let model = ScreenModelBackend::wrap(backend as Arc<dyn TerminalBackend>);

    // Park the cursor at row 5, col 11 (1-indexed) after drawing.
    tx.send(b"line one\r\n\x1b[5;11H".to_vec()).unwrap();

    assert!(wait_until(Duration::from_secs(2), || {
        model
            .snapshot()
            .map(|s| String::from_utf8_lossy(&s.bytes).ends_with("\x1b[5;11H"))
            .unwrap_or(false)
    }));
}

#[test]
fn resize_broadcasts_the_new_size_to_subscribers() {
    let (tx, _keep) = broadcast::channel::<Vec<u8>>(8);
    let backend = Arc::new(FakeBackend {
        tx,
        size: Mutex::new((80, 24)),
    });
    let model = ScreenModelBackend::wrap(backend as Arc<dyn TerminalBackend>);
    let mut sizes = model
        .subscribe_size()
        .expect("screen model tracks winsize changes");

    model.resize(150, 40).unwrap();

    // Every attached socket client gets this as a Resize frame — the tmux-client
    // semantic: one viewer resizes, all viewers adopt the new authoritative size.
    assert_eq!(sizes.try_recv().unwrap(), (150, 40));
}
