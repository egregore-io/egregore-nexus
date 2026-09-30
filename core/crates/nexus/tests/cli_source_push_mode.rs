use nexus::cli::commands::source::{select_push_input_mode, PushInputMode};

#[test]
fn inline_message_wins_when_global_json_output_sets_the_shared_json_flag() {
    assert_eq!(
        select_push_input_mode(true, Some("release probe"), false),
        PushInputMode::InlineMessage,
        "`nexus --json push source -m body` must not reinterpret empty stdin as JSON input"
    );
}

#[test]
fn explicit_json_stdin_still_wins_without_an_inline_message() {
    assert_eq!(
        select_push_input_mode(true, None, false),
        PushInputMode::JsonStdin
    );
}

#[test]
fn piped_lines_and_interactive_missing_body_remain_distinct() {
    assert_eq!(
        select_push_input_mode(false, None, false),
        PushInputMode::PipedLines
    );
    assert_eq!(
        select_push_input_mode(false, None, true),
        PushInputMode::MissingInteractiveBody
    );
}
