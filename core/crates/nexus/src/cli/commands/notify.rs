//! Explicit one-shot notifications. Typing or constructing a draft has no side effect; one
//! complete request reaches the daemon only when the CLI command is invoked.

use std::io::Read;
use std::process::ExitCode;

use clap::Args;
use nexus_contracts::{
    codes, validate_send_body, Ack, AgentId, ContractError, NotifySendRequest, NotifyTarget,
};

use crate::cli::render::finish_with;
use crate::cli::store_client::StoreClient;

#[derive(Args, Debug)]
pub struct NotifyArgs {
    /// Agent id/name, group, or thread. Prefix with agent:, group:, or thread: to disambiguate.
    #[arg(long)]
    pub target: String,
    /// Attribution label. Defaults to the invoking Nexus identity.
    #[arg(long)]
    pub source: Option<String>,
    /// Stable retry key scoped to the invoking identity.
    #[arg(long)]
    pub idempotency_key: Option<String>,
    /// Complete notification body.
    #[arg(conflicts_with = "stdin")]
    pub message: Option<String>,
    /// Read the complete notification body from stdin.
    #[arg(long)]
    pub stdin: bool,
}

pub fn parse_target(raw: &str) -> Result<NotifyTarget, ContractError> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(invalid("notification target must not be empty"));
    }
    let prefixed = |prefix: &str| raw.strip_prefix(prefix).map(str::trim);
    if let Some(agent) = prefixed("agent:") {
        require_target_value("agent", agent)?;
        return Ok(if agent.starts_with("a_") {
            NotifyTarget::Agent {
                agent_id: AgentId(agent.into()),
            }
        } else {
            NotifyTarget::Name { name: agent.into() }
        });
    }
    if let Some(group) = prefixed("group:") {
        require_target_value("group", group)?;
        return Ok(NotifyTarget::Group {
            group: group.into(),
        });
    }
    if let Some(thread) = prefixed("thread:") {
        require_target_value("thread", thread)?;
        return Ok(NotifyTarget::Thread {
            thread: thread.into(),
        });
    }
    if raw.starts_with("a_") {
        return Ok(NotifyTarget::Agent {
            agent_id: AgentId(raw.into()),
        });
    }
    Ok(NotifyTarget::Auto { value: raw.into() })
}

pub fn build_request_with_reader(
    args: NotifyArgs,
    mut reader: impl Read,
) -> Result<NotifySendRequest, ContractError> {
    let body = match args.message {
        Some(message) => message,
        None if args.stdin => {
            let mut body = String::new();
            reader
                .read_to_string(&mut body)
                .map_err(|error| invalid(format!("reading notification stdin failed: {error}")))?;
            body
        }
        None => {
            return Err(invalid(
                "notification message is required — pass <message> or --stdin",
            ))
        }
    };
    validate_send_body(&body)?;
    if args
        .source
        .as_deref()
        .is_some_and(|source| source.trim().is_empty())
    {
        return Err(invalid("notification source must not be empty"));
    }
    Ok(NotifySendRequest {
        target: parse_target(&args.target)?,
        source: args.source.map(|source| source.trim().to_string()),
        body,
        idempotency_key: args.idempotency_key,
    })
}

pub async fn notify(client: &StoreClient, args: NotifyArgs, json: bool) -> ExitCode {
    let request = match build_request_with_reader(args, std::io::stdin()) {
        Ok(request) => request,
        Err(error) => return finish_with::<Ack>(Err(error), json, |_| String::new()),
    };
    finish_with(client.notification_send(&request).await, json, |ack| {
        ack.message_id.0.clone()
    })
}

fn require_target_value(kind: &str, value: &str) -> Result<(), ContractError> {
    if value.is_empty() {
        Err(invalid(format!("{kind} notification target is empty")))
    } else {
        Ok(())
    }
}

fn invalid(message: impl Into<String>) -> ContractError {
    ContractError {
        code: codes::INVALID_PARAMS,
        message: message.into(),
    }
}
