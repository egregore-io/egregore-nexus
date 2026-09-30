//! Fixture-only clocks. Parent receipt times include pipe/reader scheduling delay;
//! child elapsed zero is after interpreter startup, not the parent's spawn origin.
use std::collections::VecDeque;
use std::io;
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Default)]
struct Capture {
    stderr: Vec<u8>,
    line: Vec<u8>,
    line_overflow: bool,
    phases: VecDeque<(Duration, String)>,
}

impl Capture {
    fn record(&mut self, received: Duration, phase: String) {
        if self.phases.len() == 128 {
            self.phases.pop_front();
        }
        self.phases.push_back((received, phase));
    }

    fn stderr_chunk(&mut self, received: Duration, chunk: &[u8]) {
        self.stderr.extend_from_slice(chunk);
        let excess = self.stderr.len().saturating_sub(64 * 1024);
        self.stderr.drain(..excess);
        for &byte in chunk {
            if byte == b'\n' {
                if !self.line_overflow && self.line.starts_with(b"parked-hook phase=") {
                    self.record(
                        received,
                        format!(
                            "stderr-phase-received {}",
                            String::from_utf8_lossy(&self.line)
                        ),
                    );
                }
                self.line.clear();
                self.line_overflow = false;
            } else if self.line.len() < 1024 {
                self.line.push(byte);
            } else {
                self.line_overflow = true;
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct Timeline {
    origin: Instant,
    capture: Arc<Mutex<Capture>>,
}

impl Timeline {
    pub(super) fn before_spawn() -> Self {
        let timeline = Self {
            origin: Instant::now(),
            capture: Arc::default(),
        };
        timeline.record("spawn-start");
        timeline
    }

    pub(super) fn record(&self, phase: &str) {
        let received = self.origin.elapsed();
        self.capture
            .lock()
            .unwrap()
            .record(received, phase.to_owned());
    }

    pub(super) fn stderr_chunk(&self, chunk: &[u8]) {
        // Timestamp before taking the diagnostic mutex; this is receipt, not emission.
        let received = self.origin.elapsed();
        self.capture.lock().unwrap().stderr_chunk(received, chunk);
    }

    pub(super) fn receive_marker(
        &self,
        receiver: &mpsc::Receiver<io::Result<String>>,
    ) -> Result<io::Result<String>, mpsc::RecvTimeoutError> {
        self.record("capture-watchdog-start");
        let captured = receiver.recv_timeout(Duration::from_secs(3));
        self.record(match &captured {
            Ok(Ok(line)) if line.trim() == "exact native row captured" => "exact-marker-received",
            Ok(Ok(_)) => "wrong-marker-received",
            Ok(Err(_)) => "stdout-read-failed",
            Err(mpsc::RecvTimeoutError::Timeout) => "capture-watchdog-timeout",
            Err(mpsc::RecvTimeoutError::Disconnected) => "stdout-reader-disconnected",
        });
        captured
    }

    pub(super) fn report(&self) -> String {
        let capture = self.capture.lock().unwrap();
        format!("parent_since_spawn={:?}; phase_receipts_include_reader_delay=true; child_zero_is_after_interpreter_start=true; stderr={}", capture.phases, String::from_utf8_lossy(&capture.stderr))
    }
}

#[test]
fn parked_hook_timing_records_complete_phase_receipts_and_bounds_output() {
    let mut capture = Capture::default();
    capture.stderr_chunk(Duration::from_millis(5), b"parked-hook phase=python-");
    assert!(capture.phases.is_empty());
    capture.stderr_chunk(Duration::from_millis(8), b"started elapsed=0.0\n");
    assert_eq!(
        capture.phases.back(),
        Some(&(
            Duration::from_millis(8),
            "stderr-phase-received parked-hook phase=python-started elapsed=0.0".to_owned()
        ))
    );
    for _ in 0..200 {
        capture.stderr_chunk(
            Duration::from_millis(9),
            b"parked-hook phase=row-read-entered elapsed=1.8814\n",
        );
    }
    assert_eq!(capture.phases.len(), 128);
    capture.stderr_chunk(Duration::from_secs(1), &vec![b'x'; 128 * 1024]);
    assert_eq!(capture.stderr.len(), 64 * 1024);
    assert_eq!(capture.line.len(), 1024);
    capture.stderr_chunk(
        Duration::from_secs(2),
        b"\nparked-hook phase=handle-complete elapsed=2.0\n",
    );
    assert!(capture.phases.back().unwrap().1.contains("handle-complete"));
}

#[test]
fn parked_hook_timing_records_wrong_marker_without_accepting_it() {
    let (sender, receiver) = mpsc::channel();
    sender.send(Ok("wrong marker\n".to_owned())).unwrap();
    let timeline = Timeline::before_spawn();
    let captured = timeline.receive_marker(&receiver);
    assert!(matches!(captured, Ok(Ok(line)) if line == "wrong marker\n"));
    let capture = timeline.capture.lock().unwrap();
    assert_eq!(capture.phases[1].1, "capture-watchdog-start");
    assert_eq!(capture.phases.back().unwrap().1, "wrong-marker-received");
}

#[test]
fn parked_hook_timing_records_missing_marker_with_unchanged_watchdog() {
    let (_sender, receiver) = mpsc::channel();
    let timeline = Timeline::before_spawn();
    let captured = timeline.receive_marker(&receiver);
    assert!(matches!(captured, Err(mpsc::RecvTimeoutError::Timeout)));
    let capture = timeline.capture.lock().unwrap();
    assert_eq!(capture.phases[1].1, "capture-watchdog-start");
    assert_eq!(capture.phases.back().unwrap().1, "capture-watchdog-timeout");
}
