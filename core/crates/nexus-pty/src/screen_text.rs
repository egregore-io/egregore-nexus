//! Terminal-text extractor: turns raw PTY bytes (ANSI escapes, alternate-screen, cursor moves,
//! redraws) into the visible reply TEXT that a human would read.
//!
//! # How committed-line detection works
//!
//! `vt100::Parser` models a full terminal emulator. After each `process()` call, we read the
//! decoded (escape-free) text of every visible row via [`Screen::rows`] and the current cursor
//! row via [`Screen::cursor_position`].
//!
//! A row is considered **committed** — ready to emit — if and only if the cursor is *strictly
//! below* that row (i.e., `cursor_row > row_index`). The cursor being on or above a row means
//! the harness may still be redrawing it in place, so we hold it back.
//!
//! We maintain a counter `emitted_rows` of how many rows from the top of the visible screen
//! have already been returned by previous `feed` calls. On each `feed`:
//! 1. `process` the new bytes into the parser.
//! 2. Determine `commit_up_to = cursor_row` (exclusive).
//! 3. For rows `emitted_rows .. commit_up_to`, read their text, trim trailing whitespace, drop
//!    empties, and collect them as newly-committed lines.
//! 4. Update `emitted_rows = commit_up_to`.
//!
//! **No-re-emit guarantee:** `emitted_rows` is monotonically non-decreasing, so once a row is
//! returned it is never re-examined.
//!
//! **Chunk-boundary stability:** whether `"PONG\r\n"` arrives in one chunk or two (`"PO"` +
//! `"NG\r\n"`), the `\n` is what advances the cursor below the row. Until `\n` arrives the
//! cursor is on or above the row and it is not committed, so partial lines are never emitted.
//!
//! **Alternate screen / redraws:** entering the alternate screen clears it and resets the cursor
//! to (0,0). Those rows' cursor_row is ≥ 0 so they are not committed until the cursor moves
//! below them. `emitted_rows` is also reset when we detect the alternate screen has exited,
//! because the normal screen's content is what we want.
//!
//! # vt100 version
//! `vt100 = "0.15"` (resolved to 0.15.2 in the lockfile).

/// Incrementally consumes raw PTY bytes and yields newly-committed visible text lines.
///
/// See the [module-level documentation](self) for the committed-line detection algorithm.
pub struct ScreenText {
    /// The underlying vt100 terminal emulator parser.
    parser: vt100::Parser,
    /// How many rows (from the top of the visible screen) have already been returned by
    /// previous [`feed`](ScreenText::feed) calls.
    emitted_rows: u16,
    /// Whether we were in alternate screen mode at the end of the last `feed` call.
    was_alternate_screen: bool,
}

impl ScreenText {
    /// Create a new extractor for a terminal of the given dimensions.
    ///
    /// `rows` / `cols` should match the PTY's window size; 24 × 80 is a safe default for
    /// unattended agents.
    pub fn new(rows: u16, cols: u16) -> Self {
        // A scrollback_len > 0 lets vt100 store lines that scroll off the top, but we don't
        // need scrollback for our approach (cursor-row watermark). Zero is fine.
        let parser = vt100::Parser::new(rows, cols, 0);
        Self {
            parser,
            emitted_rows: 0,
            was_alternate_screen: false,
        }
    }

    /// Feed a chunk of raw PTY bytes; return any text lines that have **committed** (the cursor
    /// has moved past them) since the last call.
    ///
    /// Never returns partial or redrawn lines. Feeding the same byte stream split at different
    /// chunk boundaries yields the same committed lines in the same order.
    ///
    /// Empty `bytes` is a no-op and returns `[]`.
    pub fn feed(&mut self, bytes: &[u8]) -> Vec<String> {
        if bytes.is_empty() {
            return Vec::new();
        }

        self.parser.process(bytes);
        let screen = self.parser.screen();

        // Detect alternate-screen transition. When exiting alternate screen (was_in → not_in),
        // we reset emitted_rows because we are now back on the normal screen where the reply
        // text lives. We do NOT reset on enter, since we might want to read lines that were
        // already in the normal screen before the alt screen was entered.
        let now_alternate = screen.alternate_screen();
        if self.was_alternate_screen && !now_alternate {
            // Exited alt screen — normal screen content is fresh again; restart emission.
            self.emitted_rows = 0;
        }
        self.was_alternate_screen = now_alternate;

        let (cursor_row, _cursor_col) = screen.cursor_position();
        let (total_rows, total_cols) = screen.size();

        // Rows strictly above the cursor are committed.
        let commit_up_to = cursor_row.min(total_rows);

        if commit_up_to <= self.emitted_rows {
            return Vec::new();
        }

        // Collect text for rows [emitted_rows .. commit_up_to].
        let mut result = Vec::new();
        for row_idx in self.emitted_rows..commit_up_to {
            // `Screen::rows(start_col, width)` returns an iterator over visible rows.
            // We want the full row: start=0, width=total_cols.
            let row_text = screen
                .rows(0, total_cols)
                .nth(usize::from(row_idx))
                .unwrap_or_default();
            let trimmed = row_text.trim_end().to_owned();
            if !trimmed.is_empty() {
                result.push(trimmed);
            }
        }

        self.emitted_rows = commit_up_to;
        result
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::ScreenText;

    /// Helper: create a ScreenText with a sensible default size.
    fn st() -> ScreenText {
        ScreenText::new(24, 80)
    }

    // ------------------------------------------------------------------
    // Test 1: simple stream
    // `feed(b"hello\r\nPONG\r\n")` must return lines containing "hello" and "PONG".
    // ------------------------------------------------------------------
    #[test]
    fn simple_stream_extracts_lines() {
        let mut s = st();
        let lines = s.feed(b"hello\r\nPONG\r\n");
        assert!(
            lines.iter().any(|l| l.contains("hello")),
            "expected 'hello' in committed lines, got: {lines:?}"
        );
        assert!(
            lines.iter().any(|l| l.contains("PONG")),
            "expected 'PONG' in committed lines, got: {lines:?}"
        );
    }

    // ------------------------------------------------------------------
    // Test 2: ANSI-noisy stream
    // Color codes, clear-screen, cursor-home, redraw — extracted text must
    // contain "PONG" and must NOT contain any ESC bytes.
    // ------------------------------------------------------------------
    #[test]
    fn ansi_noisy_stream_strips_escapes() {
        let mut s = st();
        // ESC[2J = clear screen, ESC[H = cursor home, ESC[32m = green, ESC[0m = reset
        let input = b"\x1b[2J\x1b[H\x1b[32mthinking...\x1b[0m\r\nPONG\r\n";
        let lines = s.feed(input);
        assert!(
            lines.iter().any(|l| l.contains("PONG")),
            "expected 'PONG' in output, got: {lines:?}"
        );
        assert!(
            !lines.iter().any(|l| l.contains('\x1b')),
            "output must not contain raw ESC bytes, got: {lines:?}"
        );
    }

    // ------------------------------------------------------------------
    // Test 3: chunk-boundary stability
    // Feeding "PO" then "NG\r\n" across two calls must yield a single
    // committed "PONG" line, not "PO" then "NG".
    // ------------------------------------------------------------------
    #[test]
    fn chunk_boundary_stability() {
        let mut s = st();
        let first = s.feed(b"PO");
        // "PO" is on the cursor row — not committed yet.
        assert!(
            first.is_empty(),
            "partial line 'PO' should not be emitted yet, got: {first:?}"
        );
        let second = s.feed(b"NG\r\n");
        assert_eq!(
            second,
            vec!["PONG".to_string()],
            "full line should be committed after newline, got: {second:?}"
        );
    }

    // ------------------------------------------------------------------
    // Test 4: idempotence / no-re-emit
    // After a line is returned, a subsequent feed (even with no new content)
    // must NOT return it again.
    // ------------------------------------------------------------------
    #[test]
    fn no_re_emit_after_committed() {
        let mut s = st();
        let first = s.feed(b"hello\r\n");
        assert!(
            first.iter().any(|l| l.contains("hello")),
            "expected 'hello' on first feed, got: {first:?}"
        );
        // Second feed: no new bytes.
        let second = s.feed(b"");
        assert!(
            second.is_empty(),
            "empty feed must not re-emit already-committed lines, got: {second:?}"
        );
        // Third feed: more bytes that don't touch the already-committed row.
        let third = s.feed(b"world\r\n");
        assert!(
            !third.iter().any(|l| l.contains("hello")),
            "'hello' must not be re-emitted on third feed, got: {third:?}"
        );
        assert!(
            third.iter().any(|l| l.contains("world")),
            "expected 'world' on third feed, got: {third:?}"
        );
    }

    // ------------------------------------------------------------------
    // Test 5: in-place redraw suppressed until finalized
    // The harness may write "thinking...\r" and overwrite with "PONG\r\n".
    // Only the final "PONG" must be committed, not the "thinking..." version.
    // ------------------------------------------------------------------
    #[test]
    fn in_place_redraw_suppressed() {
        let mut s = st();
        // Write "thinking..." and return cursor to start of line without newline.
        let partial = s.feed(b"thinking...\r");
        assert!(
            partial.is_empty(),
            "in-progress redraw line must not be committed, got: {partial:?}"
        );
        // Overwrite with "PONG" and finalize.
        let final_lines = s.feed(b"PONG\r\n");
        assert!(
            final_lines.iter().any(|l| l.contains("PONG")),
            "expected 'PONG' after overwrite, got: {final_lines:?}"
        );
        assert!(
            !final_lines.iter().any(|l| l.contains("thinking")),
            "'thinking...' must not appear in committed output, got: {final_lines:?}"
        );
    }
}
