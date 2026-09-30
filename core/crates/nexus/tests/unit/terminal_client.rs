#![cfg(test)]

use super::PolledResize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[tokio::test]
async fn resize_poll_survives_continuous_relay_activity() {
    static COLS: AtomicUsize = AtomicUsize::new(80);
    fn sample() -> Option<(u16, u16)> {
        Some((COLS.load(Ordering::SeqCst) as u16, 24))
    }
    let mut watch = PolledResize::new(sample);
    COLS.store(120, Ordering::SeqCst);
    let deadline = tokio::time::sleep(Duration::from_secs(2));
    tokio::pin!(deadline);
    let mut relay_wakes = 0;
    loop {
        tokio::select! {
            _ = &mut deadline => panic!("active relay traffic starved resize polling"),
            _ = watch.changed() => break,
            _ = tokio::time::sleep(Duration::from_millis(10)) => relay_wakes += 1,
        }
    }
    assert!(
        relay_wakes > 0,
        "fixture must cancel polling before it completes"
    );
    assert_eq!(watch.last, Some((120, 24)));
}
