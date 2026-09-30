//! QueryScanner/reply_for behavior: chunk-boundary-safe recognition of TUI terminal
//! interrogations and canonical daemon-side replies. Moved out of src/ per test-layout policy.

use nexus_pty::query_responder::{reply_for, QueryScanner, TerminalQuery};

fn scan_all(chunks: &[&[u8]]) -> Vec<TerminalQuery> {
    let mut scanner = QueryScanner::new();
    chunks.iter().flat_map(|c| scanner.scan(c)).collect()
}

#[test]
fn recognizes_the_query_vocabulary() {
    assert_eq!(scan_all(&[b"\x1b[6n"]), vec![TerminalQuery::CursorPosition]);
    assert_eq!(scan_all(&[b"\x1b[5n"]), vec![TerminalQuery::DeviceStatus]);
    assert_eq!(
        scan_all(&[b"\x1b[c\x1b[0c"]),
        vec![
            TerminalQuery::PrimaryDeviceAttributes,
            TerminalQuery::PrimaryDeviceAttributes
        ]
    );
    assert_eq!(
        scan_all(&[b"\x1b[>c"]),
        vec![TerminalQuery::SecondaryDeviceAttributes]
    );
    assert_eq!(
        scan_all(&[b"\x1b[?u"]),
        vec![TerminalQuery::KittyKeyboardFlags]
    );
    assert_eq!(
        scan_all(&[b"\x1b[?2026$p"]),
        vec![TerminalQuery::DecPrivateMode(2026)]
    );
    assert_eq!(
        scan_all(&[b"\x1b]10;?\x07\x1b]11;?\x1b\\"]),
        vec![
            TerminalQuery::ForegroundColor,
            TerminalQuery::BackgroundColor
        ]
    );
    assert_eq!(
        scan_all(&[b"\x1bP+q544e\x1b\\"]),
        vec![TerminalQuery::TermcapCapability]
    );
}

#[test]
fn ignores_ordinary_output_and_non_queries() {
    assert!(scan_all(&[b"plain text, no escapes"]).is_empty());
    // cursor moves, SGR colors, erase — output, not queries
    assert!(scan_all(&[b"\x1b[2J\x1b[H\x1b[31mred\x1b[m\x1b[5;11H"]).is_empty());
    // CPR RESPONSE shape must not be re-detected as a query
    assert!(scan_all(&[b"\x1b[12;40R"]).is_empty());
}

#[test]
fn reassembles_queries_split_across_chunks() {
    assert_eq!(
        scan_all(&[b"\x1b", b"[6n"]),
        vec![TerminalQuery::CursorPosition]
    );
    assert_eq!(
        scan_all(&[b"drawing\x1b[?20", b"26$p tail"]),
        vec![TerminalQuery::DecPrivateMode(2026)]
    );
    assert_eq!(
        scan_all(&[b"\x1b]11;?", b"\x1b", b"\\"]),
        vec![TerminalQuery::BackgroundColor]
    );
}

#[test]
fn queries_embedded_in_surrounding_output_are_found() {
    assert_eq!(
        scan_all(&[b"\x1b[2Jhello\x1b[6nworld\x1b[31m"]),
        vec![TerminalQuery::CursorPosition]
    );
}

#[test]
fn replies_are_canonical() {
    assert_eq!(
        reply_for(&TerminalQuery::CursorPosition, (4, 10)),
        b"\x1b[5;11R".to_vec()
    );
    assert_eq!(reply_for(&TerminalQuery::DeviceStatus, (0, 0)), b"\x1b[0n");
    assert_eq!(
        reply_for(&TerminalQuery::DecPrivateMode(2026), (0, 0)),
        b"\x1b[?2026;2$y".to_vec()
    );
    assert_eq!(
        reply_for(&TerminalQuery::DecPrivateMode(1000), (0, 0)),
        b"\x1b[?1000;0$y".to_vec()
    );
    let bg = reply_for(&TerminalQuery::BackgroundColor, (0, 0));
    assert!(bg.starts_with(b"\x1b]11;rgb:"));
}

#[test]
fn oversized_partial_sequences_are_dropped_not_buffered_forever() {
    // Behavioral version of the internal pending-cap check (the cap is private): an
    // unterminated OSC far beyond any sane sequence length must be abandoned — its
    // eventual "terminator" must NOT complete it into a query — and the scanner must
    // keep recognizing fresh queries afterwards.
    let mut scanner = QueryScanner::new();
    let mut big = b"\x1b]".to_vec();
    big.extend(std::iter::repeat(b'x').take(64 * 1024));
    assert!(scanner.scan(&big).is_empty());
    // If the oversized tail were still buffered, this BEL would terminate the OSC and
    // could surface as a (garbage) query; a dropped tail yields nothing.
    assert!(scanner.scan(b"\x07").is_empty());
    // Scanner still works afterwards
    assert_eq!(
        scanner.scan(b"\x1b[6n"),
        vec![TerminalQuery::CursorPosition]
    );
}
