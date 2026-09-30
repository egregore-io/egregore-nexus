use nexus_harness_core::{native_harness_program, native_npm_runner, NativeProcessPlatform};

#[test]
fn windows_uses_native_harness_executable_names() {
    let platform = NativeProcessPlatform::Windows;

    assert_eq!(
        native_harness_program("claude", platform),
        Some("claude.exe")
    );
    assert_eq!(native_harness_program("codex", platform), Some("codex.exe"));
    assert_eq!(
        native_harness_program("opencode", platform),
        Some("opencode.exe")
    );
    assert_eq!(
        native_harness_program("hermes", platform),
        Some("hermes.exe")
    );
    assert_eq!(native_npm_runner(platform), "npx.cmd");
}

#[cfg(windows)]
#[test]
fn a_windows_build_selects_the_windows_process_contract() {
    assert_eq!(
        NativeProcessPlatform::current(),
        NativeProcessPlatform::Windows
    );
}

#[test]
fn unix_platforms_use_their_native_path_tokens() {
    let platform = NativeProcessPlatform::Unix;

    assert_eq!(native_harness_program("claude", platform), Some("claude"));
    assert_eq!(native_harness_program("codex", platform), Some("codex"));
    assert_eq!(
        native_harness_program("opencode", platform),
        Some("opencode")
    );
    assert_eq!(native_harness_program("hermes", platform), Some("hermes"));
    assert_eq!(native_npm_runner(platform), "npx");
}

#[test]
fn non_headed_harnesses_have_no_native_program() {
    for platform in [NativeProcessPlatform::Unix, NativeProcessPlatform::Windows] {
        assert_eq!(native_harness_program("pi", platform), None);
        assert_eq!(native_harness_program("other", platform), None);
    }
}
