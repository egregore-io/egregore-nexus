use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitialPromptVars {
    pub name: Option<String>,
    pub role: Option<String>,
    pub harness: String,
    pub agent_id: String,
    pub session_id: String,
    pub runtime_id: String,
    pub cwd: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InitialPromptError {
    Empty,
    UnknownVariable(String),
    MissingVariable(String),
}

impl fmt::Display for InitialPromptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "initial prompt rendered empty"),
            Self::UnknownVariable(token) => {
                write!(f, "unknown initial prompt variable {token}")
            }
            Self::MissingVariable(token) => {
                write!(f, "initial prompt variable {token} is unavailable")
            }
        }
    }
}

impl std::error::Error for InitialPromptError {}

pub fn render_initial_prompt(
    template: &str,
    vars: &InitialPromptVars,
) -> Result<String, InitialPromptError> {
    let mut rendered = String::with_capacity(template.len());
    let mut rest = template;

    while let Some(start) = rest.find("<var.") {
        rendered.push_str(&rest[..start]);
        let after_start = &rest[start..];
        let Some(end) = after_start.find('>') else {
            rendered.push_str(after_start);
            rest = "";
            break;
        };
        let token = &after_start[..=end];
        rendered.push_str(resolve_token(token, vars)?);
        rest = &after_start[end + 1..];
    }

    rendered.push_str(rest);
    if rendered.trim().is_empty() {
        return Err(InitialPromptError::Empty);
    }
    Ok(rendered)
}

fn resolve_token<'a>(
    token: &str,
    vars: &'a InitialPromptVars,
) -> Result<&'a str, InitialPromptError> {
    match token {
        "<var.name>" => vars
            .name
            .as_deref()
            .ok_or_else(|| InitialPromptError::MissingVariable(token.to_string())),
        "<var.role>" => vars
            .role
            .as_deref()
            .ok_or_else(|| InitialPromptError::MissingVariable(token.to_string())),
        "<var.harness>" => Ok(vars.harness.as_str()),
        "<var.agentId>" => Ok(vars.agent_id.as_str()),
        "<var.sessionId>" => Ok(vars.session_id.as_str()),
        "<var.runtimeId>" => Ok(vars.runtime_id.as_str()),
        "<var.cwd>" => vars
            .cwd
            .as_deref()
            .ok_or_else(|| InitialPromptError::MissingVariable(token.to_string())),
        _ => Err(InitialPromptError::UnknownVariable(token.to_string())),
    }
}
