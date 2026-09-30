use nexus::initial_prompt::{render_initial_prompt, InitialPromptVars};

fn vars() -> InitialPromptVars {
    InitialPromptVars {
        name: Some("otto".to_string()),
        role: Some("lead".to_string()),
        harness: "codex".to_string(),
        agent_id: "a_s_otto".to_string(),
        session_id: "s_otto".to_string(),
        runtime_id: "r_otto".to_string(),
        cwd: Some("/work".to_string()),
    }
}

#[test]
fn rejects_missing_name_when_referenced() {
    let mut vars = vars();
    vars.name = None;

    let err = render_initial_prompt("You are <var.name>.", &vars).unwrap_err();

    assert!(err.to_string().contains("<var.name>"));
}

#[test]
fn expands_supported_launch_variables() {
    let rendered = render_initial_prompt(
        "You are <var.name> / <var.role> / <var.harness> / <var.agentId> / <var.sessionId> / <var.runtimeId> / <var.cwd>.",
        &vars(),
    )
    .unwrap();

    assert_eq!(
        rendered,
        "You are otto / lead / codex / a_s_otto / s_otto / r_otto / /work."
    );
}

#[test]
fn preserves_unrelated_angle_brackets() {
    let rendered = render_initial_prompt("Keep <xml> and <varish.name> as text.", &vars()).unwrap();

    assert_eq!(rendered, "Keep <xml> and <varish.name> as text.");
}

#[test]
fn rejects_unknown_var_namespace_members() {
    let err = render_initial_prompt("Project is <var.project>.", &vars()).unwrap_err();

    assert!(err.to_string().contains("<var.project>"));
}

#[test]
fn rejects_missing_role_when_referenced() {
    let mut vars = vars();
    vars.role = None;

    let err = render_initial_prompt("Role is <var.role>.", &vars).unwrap_err();

    assert!(err.to_string().contains("<var.role>"));
}

#[test]
fn rejects_missing_cwd_when_referenced() {
    let mut vars = vars();
    vars.cwd = None;

    let err = render_initial_prompt("Work in <var.cwd>.", &vars).unwrap_err();

    assert!(err.to_string().contains("<var.cwd>"));
}

#[test]
fn rejects_blank_rendered_prompt() {
    let err = render_initial_prompt("   \n\t  ", &vars()).unwrap_err();

    assert!(err.to_string().contains("empty"));
}
