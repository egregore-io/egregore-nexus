//! Terminal query responder — the daemon acts as the attached terminal emulator.
//!
//! Full-screen TUIs interrogate their terminal (cursor-position reports, device attributes,
//! color queries, mode probes) and stall or degrade when nobody answers. That is the real
//! reason "a claude on a raw PTY never completes an unattended turn" — under tmux it worked
//! because tmux IS a terminal emulator answering these; the raw daemon-owned pty had no
//! answering side. This module is that side: it scans the pty OUTPUT stream for queries and
//! writes canonical replies to the pty INPUT, using the screen model for cursor state.
//!
//! Enable it ONLY for raw pty backends. tmux answers its own pane's queries — a second
//! responder would double-reply and corrupt the harness's input stream.

/// A terminal query decoded from the application's output stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalQuery {
    /// `CSI 6 n` — Device Status Report: cursor position (CPR).
    CursorPosition,
    /// `CSI 5 n` — Device Status Report: operating status.
    DeviceStatus,
    /// `CSI c` / `CSI 0 c` — Primary Device Attributes.
    PrimaryDeviceAttributes,
    /// `CSI > c` / `CSI > 0 c` — Secondary Device Attributes.
    SecondaryDeviceAttributes,
    /// `CSI ? u` — kitty keyboard protocol flags query.
    KittyKeyboardFlags,
    /// `CSI ? {mode} $ p` — DECRQM: request DEC private mode state.
    DecPrivateMode(u16),
    /// `OSC 10 ; ?` — default foreground color query.
    ForegroundColor,
    /// `OSC 11 ; ?` — default background color query.
    BackgroundColor,
    /// `DCS + q …` — XTGETTCAP terminfo string query.
    TermcapCapability,
}

/// The canonical reply bytes for `query`. `cursor` is the screen model's current
/// (row, col), 0-indexed, used for CPR.
pub fn reply_for(query: &TerminalQuery, cursor: (u16, u16)) -> Vec<u8> {
    match query {
        TerminalQuery::CursorPosition => {
            let (row, col) = cursor;
            format!("\x1b[{};{}R", row + 1, col + 1).into_bytes()
        }
        TerminalQuery::DeviceStatus => b"\x1b[0n".to_vec(),
        // VT220 with ANSI color — what xterm-256color-era TUIs expect at minimum.
        TerminalQuery::PrimaryDeviceAttributes => b"\x1b[?62;22c".to_vec(),
        // xterm-compatible terminal, made-up-but-plausible firmware version.
        TerminalQuery::SecondaryDeviceAttributes => b"\x1b[>41;354;0c".to_vec(),
        // Kitty protocol recognized, no flags active.
        TerminalQuery::KittyKeyboardFlags => b"\x1b[?0u".to_vec(),
        TerminalQuery::DecPrivateMode(mode) => {
            // 2026 (synchronized output): supported, currently reset — lets TUIs batch
            // repaints. Everything else: not recognized (0), which is an honest answer
            // that unblocks the prober.
            let state = if *mode == 2026 { 2 } else { 0 };
            format!("\x1b[?{mode};{state}$y").into_bytes()
        }
        TerminalQuery::ForegroundColor => b"\x1b]10;rgb:e5e5/e5e5/e5e5\x1b\\".to_vec(),
        TerminalQuery::BackgroundColor => b"\x1b]11;rgb:1e1e/1e1e/1e1e\x1b\\".to_vec(),
        // XTGETTCAP "invalid/unsupported" — a definitive no beats silence.
        TerminalQuery::TermcapCapability => b"\x1bP0+r\x1b\\".to_vec(),
    }
}

/// Cap for a partial escape sequence carried across chunk boundaries. Anything longer is
/// not a query we answer; drop it rather than buffer unboundedly.
const MAX_PENDING: usize = 4096;

/// Incremental scanner: feed pty output chunks, get decoded queries. Stateful so escape
/// sequences split across chunk boundaries are still recognized.
#[derive(Default)]
pub struct QueryScanner {
    pending: Vec<u8>,
}

impl QueryScanner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn scan(&mut self, chunk: &[u8]) -> Vec<TerminalQuery> {
        let mut queries = Vec::new();
        let buf: Vec<u8> = if self.pending.is_empty() {
            chunk.to_vec()
        } else {
            let mut b = std::mem::take(&mut self.pending);
            b.extend_from_slice(chunk);
            b
        };

        let mut i = 0;
        while i < buf.len() {
            if buf[i] != 0x1b {
                i += 1;
                continue;
            }
            match parse_sequence(&buf[i..]) {
                Parse::Complete { len, query } => {
                    if let Some(query) = query {
                        queries.push(query);
                    }
                    i += len;
                }
                Parse::Partial => {
                    // Sequence continues in the next chunk — carry the tail over.
                    let tail = &buf[i..];
                    if tail.len() <= MAX_PENDING {
                        self.pending = tail.to_vec();
                    }
                    return queries;
                }
                Parse::NotASequence => {
                    i += 1;
                }
            }
        }
        queries
    }
}

enum Parse {
    /// A full escape sequence of `len` bytes; `query` when it's one we answer.
    Complete {
        len: usize,
        query: Option<TerminalQuery>,
    },
    /// The buffer ends mid-sequence.
    Partial,
    /// ESC followed by something we don't track as a sequence (treat as lone byte).
    NotASequence,
}

/// Parse one escape sequence starting at `buf[0] == ESC`.
fn parse_sequence(buf: &[u8]) -> Parse {
    debug_assert_eq!(buf[0], 0x1b);
    let Some(&kind) = buf.get(1) else {
        return Parse::Partial;
    };
    match kind {
        // CSI: ESC '[' params (0x30-0x3F) intermediates (0x20-0x2F) final (0x40-0x7E)
        b'[' => {
            let mut j = 2;
            while j < buf.len() && matches!(buf[j], 0x20..=0x3f) {
                j += 1;
            }
            let Some(&fin) = buf.get(j) else {
                return Parse::Partial;
            };
            if !(0x40..=0x7e).contains(&fin) {
                // Malformed CSI; skip the introducer and move on.
                return Parse::Complete {
                    len: j + 1,
                    query: None,
                };
            }
            let body = &buf[2..j]; // params + intermediates
            let query = csi_query(body, fin);
            Parse::Complete { len: j + 1, query }
        }
        // OSC: ESC ']' … BEL | ESC '\'
        b']' => match find_string_terminator(&buf[2..]) {
            Some((body_len, term_len)) => {
                let body = &buf[2..2 + body_len];
                Parse::Complete {
                    len: 2 + body_len + term_len,
                    query: osc_query(body),
                }
            }
            None => Parse::Partial,
        },
        // DCS: ESC 'P' … ESC '\'
        b'P' => match find_string_terminator(&buf[2..]) {
            Some((body_len, term_len)) => {
                let body = &buf[2..2 + body_len];
                let query = body
                    .starts_with(b"+q")
                    .then_some(TerminalQuery::TermcapCapability);
                Parse::Complete {
                    len: 2 + body_len + term_len,
                    query,
                }
            }
            None => Parse::Partial,
        },
        _ => Parse::NotASequence,
    }
}

/// Find a BEL or ESC-\ terminator; returns (body_len, terminator_len).
fn find_string_terminator(buf: &[u8]) -> Option<(usize, usize)> {
    let mut j = 0;
    while j < buf.len() {
        match buf[j] {
            0x07 => return Some((j, 1)),
            0x1b if buf.get(j + 1) == Some(&b'\\') => return Some((j, 2)),
            0x1b if j + 1 == buf.len() => return None, // ESC at end: maybe half a terminator
            _ => {}
        }
        j += 1;
    }
    None
}

fn csi_query(body: &[u8], fin: u8) -> Option<TerminalQuery> {
    match (body, fin) {
        (b"6", b'n') => Some(TerminalQuery::CursorPosition),
        (b"5", b'n') => Some(TerminalQuery::DeviceStatus),
        (b"" | b"0", b'c') => Some(TerminalQuery::PrimaryDeviceAttributes),
        (b">" | b">0", b'c') => Some(TerminalQuery::SecondaryDeviceAttributes),
        (b"?", b'u') => Some(TerminalQuery::KittyKeyboardFlags),
        _ if fin == b'p' && body.starts_with(b"?") && body.ends_with(b"$") => {
            let digits = &body[1..body.len() - 1];
            std::str::from_utf8(digits)
                .ok()?
                .parse::<u16>()
                .ok()
                .map(TerminalQuery::DecPrivateMode)
        }
        _ => None,
    }
}

fn osc_query(body: &[u8]) -> Option<TerminalQuery> {
    match body {
        b"10;?" => Some(TerminalQuery::ForegroundColor),
        b"11;?" => Some(TerminalQuery::BackgroundColor),
        _ => None,
    }
}
