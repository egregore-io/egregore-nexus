use nexus_harness_claude::native::receipt::matches_submitted_prompt;

#[test]
fn exact_or_one_added_lf_preserves_all_original_bytes() {
    for text in [
        "",
        "prompt",
        "prompt\n",
        "prompt\n\n",
        "prompt\t",
        "prompt\r",
        "é\n内容",
    ] {
        assert!(matches_submitted_prompt(text, text), "exact {text:?}");
        assert!(
            matches_submitted_prompt(text, &format!("{text}\n")),
            "added LF {text:?}"
        );
    }
}

#[test]
fn differing_content_and_other_control_suffixes_are_not_receipts() {
    for (expected, observed) in [
        ("prompt", "other\n"),
        ("prompt", "prompt\n\n"),
        ("prompt", "prompt\r\n"),
        ("prompt", "prompt\n\u{1}"),
        ("prompt", "prompt\t"),
        ("prompt", "prompt\0"),
        ("prompt", "prompt\u{85}"),
        ("prompt", "prompt \n"),
        ("prompt\n", "prompt"),
        ("prompt\n\n", "prompt\n"),
        ("prompt\t", "prompt\n"),
    ] {
        assert!(
            !matches_submitted_prompt(expected, observed),
            "{expected:?} vs {observed:?}"
        );
    }
}
