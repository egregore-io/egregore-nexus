//! Prompt correlation for headed input receipts.

/// Raw PTY and tmux append one LF to the pasted prompt. Native queued input may
/// report the original text instead. Preserve all user-authored bytes in either
/// case; arbitrary trailing whitespace or control characters are not equivalent.
pub fn matches_submitted_prompt(expected: &str, observed: &str) -> bool {
    observed == expected || observed.strip_suffix('\n') == Some(expected)
}
