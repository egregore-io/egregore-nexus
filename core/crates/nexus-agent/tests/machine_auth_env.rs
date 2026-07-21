use nexus_agent::adapter::engine::should_scrub_inherited_env;

#[test]
fn provider_machine_auth_environment_remains_inherited() {
    for key in [
        "HOME",
        "USERPROFILE",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
    ] {
        assert!(
            !should_scrub_inherited_env(std::ffi::OsStr::new(key)),
            "{key} selects machine auth and must remain inherited"
        );
    }
}

#[test]
fn daemon_owned_runtime_state_environment_is_still_scrubbed() {
    for key in [
        "NEXUS_HOME",
        "CODEX_HOME",
        "CLAUDE_CONFIG_DIR",
        "HERMES_HOME",
        "OPENCODE_HOME",
        "OPENCODE_CONFIG_DIR",
        "OPENCODE_CONFIG_CONTENT",
        "OPENCODE_DB",
    ] {
        assert!(
            should_scrub_inherited_env(std::ffi::OsStr::new(key)),
            "{key} is daemon-owned runtime state and must not leak"
        );
    }
}
