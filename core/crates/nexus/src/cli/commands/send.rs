//! Send commands: `dm`, `post`, `reply` (context-aware), `publish`, and the underlying `send --to`
//! All build one [`SendRequest`] and enqueue a store-backed `message.post.send`
//! command intent for the daemon worker. Every send command supports `--stdin` for exact body
//! capture when shell quoting would otherwise mangle backticks, `$()` or `$vars`.

use std::io::Read;
use std::process::ExitCode;

use clap::Args;
use nexus_contracts::{codes, validate_send_body, Ack, ContractError, SendRequest, SendTarget};

use crate::cli::render::finish_with;
use crate::cli::store_client::StoreClient;

/// Body source: `-m <text>`, explicit `--stdin`, or legacy stdin fallback when `-m` is absent.
pub fn read_body_from_reader(
    message: Option<String>,
    stdin: bool,
    mut reader: impl Read,
) -> Result<String, ContractError> {
    if let Some(message) = message {
        validate_send_body(&message)?;
        return Ok(message);
    }

    if stdin {
        let mut body = String::new();
        reader
            .read_to_string(&mut body)
            .map_err(|e| ContractError {
                code: codes::INVALID_PARAMS,
                message: format!("reading stdin body failed: {e}"),
            })?;
        validate_send_body(&body)?;
        return Ok(body);
    }

    // Preserve the historical fallback: omitting `-m` reads stdin. This keeps scripts that already
    // pipe bodies into `nexus dm/post/reply` working while `--stdin` gives humans an explicit mode.
    let mut body = String::new();
    reader
        .read_to_string(&mut body)
        .map_err(|e| ContractError {
            code: codes::INVALID_PARAMS,
            message: format!("reading stdin body failed: {e}"),
        })?;
    validate_send_body(&body)?;
    Ok(body)
}

/// Issue one send and render its durable message id. Recipient expansion is daemon-owned and is
/// deliberately not part of the public acknowledgement envelope.
async fn do_send(client: &StoreClient, req: SendRequest, json: bool) -> ExitCode {
    let res: Result<Ack, ContractError> = client.message_post_send(&req).await;
    finish_with(res, json, |a| a.message_id.0.clone())
}

/// `nexus dm <name> (-m <msg> | --stdin)`.
#[derive(Args, Debug)]
pub struct DmArgs {
    pub name: String,
    #[arg(short = 'm', long = "message", conflicts_with = "stdin")]
    pub message: Option<String>,
    /// Read the message body from stdin verbatim.
    #[arg(long)]
    pub stdin: bool,
    #[arg(long)]
    pub summary: Option<String>,
}
/// Private 2-party DM.
pub async fn dm(client: &StoreClient, a: DmArgs, json: bool) -> ExitCode {
    dm_with_reader(client, a, json, std::io::stdin()).await
}

/// Private 2-party DM with an injectable reader for external CLI-body tests.
pub async fn dm_with_reader(
    client: &StoreClient,
    a: DmArgs,
    json: bool,
    reader: impl Read,
) -> ExitCode {
    let body = match read_body_from_reader(a.message, a.stdin, reader) {
        Ok(body) => body,
        Err(e) => return finish_with::<Ack>(Err(e), json, |_| String::new()),
    };
    do_send(
        client,
        SendRequest {
            to: SendTarget::dm_name(a.name),
            summary: a.summary,
            body,
            mention: vec![],
            idempotency_key: None,
        },
        json,
    )
    .await
}

/// `nexus post <thread> (-m <msg> | --stdin)`.
#[derive(Args, Debug)]
pub struct PostArgs {
    pub thread: String,
    #[arg(short = 'm', long = "message", conflicts_with = "stdin")]
    pub message: Option<String>,
    /// Read the message body from stdin verbatim.
    #[arg(long)]
    pub stdin: bool,
    #[arg(long)]
    pub mention: Vec<String>,
    #[arg(long)]
    pub summary: Option<String>,
}
/// Post to a named thread (fan-out to members).
pub async fn post(client: &StoreClient, a: PostArgs, json: bool) -> ExitCode {
    post_with_reader(client, a, json, std::io::stdin()).await
}

/// Post to a named thread with an injectable reader for external CLI-body tests.
pub async fn post_with_reader(
    client: &StoreClient,
    a: PostArgs,
    json: bool,
    reader: impl Read,
) -> ExitCode {
    let body = match read_body_from_reader(a.message, a.stdin, reader) {
        Ok(body) => body,
        Err(e) => return finish_with::<Ack>(Err(e), json, |_| String::new()),
    };
    do_send(
        client,
        SendRequest {
            to: SendTarget::Post { thread: a.thread },
            summary: a.summary,
            body,
            mention: a.mention,
            idempotency_key: None,
        },
        json,
    )
    .await
}

/// `nexus reply (-m <msg> | --stdin)` — context-aware (no target).
#[derive(Args, Debug)]
pub struct ReplyArgs {
    #[arg(short = 'm', long = "message", conflicts_with = "stdin")]
    pub message: Option<String>,
    /// Read the message body from stdin verbatim.
    #[arg(long)]
    pub stdin: bool,
    #[arg(long)]
    pub mention: Vec<String>,
    #[arg(long)]
    pub summary: Option<String>,
}
/// Build the context-aware reply request (no target — the daemon resolves the caller's last inbound
/// scope, the `<nexus>` the turn arrived with).
pub fn build_reply_request(
    body: String,
    mention: Vec<String>,
    summary: Option<String>,
) -> SendRequest {
    SendRequest {
        to: SendTarget::Reply,
        summary,
        body,
        mention,
        idempotency_key: None,
    }
}
/// Reply into the current conversation.
pub async fn reply(client: &StoreClient, a: ReplyArgs, json: bool) -> ExitCode {
    reply_with_reader(client, a, json, std::io::stdin()).await
}

/// Reply into the current conversation with an injectable reader for external CLI-body tests.
pub async fn reply_with_reader(
    client: &StoreClient,
    a: ReplyArgs,
    json: bool,
    reader: impl Read,
) -> ExitCode {
    let body = match read_body_from_reader(a.message, a.stdin, reader) {
        Ok(body) => body,
        Err(e) => return finish_with::<Ack>(Err(e), json, |_| String::new()),
    };
    do_send(
        client,
        build_reply_request(body, a.mention, a.summary),
        json,
    )
    .await
}

/// `nexus publish <topic> (-m <msg> | --stdin)`.
#[derive(Args, Debug)]
pub struct PublishArgs {
    pub topic: String,
    #[arg(short = 'm', long = "message", conflicts_with = "stdin")]
    pub message: Option<String>,
    /// Read the message body from stdin verbatim.
    #[arg(long)]
    pub stdin: bool,
    #[arg(long)]
    pub summary: Option<String>,
}
/// Publish to a topic (pub/sub fan-out).
pub async fn publish(client: &StoreClient, a: PublishArgs, json: bool) -> ExitCode {
    publish_with_reader(client, a, json, std::io::stdin()).await
}

/// Publish to a topic with an injectable reader for external CLI-body tests.
pub async fn publish_with_reader(
    client: &StoreClient,
    a: PublishArgs,
    json: bool,
    reader: impl Read,
) -> ExitCode {
    let body = match read_body_from_reader(a.message, a.stdin, reader) {
        Ok(body) => body,
        Err(e) => return finish_with::<Ack>(Err(e), json, |_| String::new()),
    };
    do_send(
        client,
        SendRequest {
            to: SendTarget::Publish { topic: a.topic },
            summary: a.summary,
            body,
            mention: vec![],
            idempotency_key: None,
        },
        json,
    )
    .await
}

/// `nexus send --to <name-or-thread> (-m <msg> | --stdin)` — the single underlying contract. The
/// CLI emits a `Dm` target; the daemon's resolver (`Router::resolve`) treats a `to` that matches a
/// thread name as a thread send regardless of the verb, so DM-vs-thread disambiguation is
/// server-side.
#[derive(Args, Debug)]
pub struct SendArgs {
    #[arg(long)]
    pub to: String,
    #[arg(short = 'm', long = "message", conflicts_with = "stdin")]
    pub message: Option<String>,
    /// Read the message body from stdin verbatim.
    #[arg(long)]
    pub stdin: bool,
    #[arg(long)]
    pub summary: Option<String>,
    #[arg(long)]
    pub mention: Vec<String>,
}
/// Generic send (`--to` resolved DM-vs-thread by the daemon).
pub async fn send(client: &StoreClient, a: SendArgs, json: bool) -> ExitCode {
    send_with_reader(client, a, json, std::io::stdin()).await
}

/// Generic send with an injectable reader for external CLI-body tests.
pub async fn send_with_reader(
    client: &StoreClient,
    a: SendArgs,
    json: bool,
    reader: impl Read,
) -> ExitCode {
    let body = match read_body_from_reader(a.message, a.stdin, reader) {
        Ok(body) => body,
        Err(e) => return finish_with::<Ack>(Err(e), json, |_| String::new()),
    };
    do_send(
        client,
        SendRequest {
            to: SendTarget::dm_name(a.to),
            summary: a.summary,
            body,
            mention: a.mention,
            idempotency_key: None,
        },
        json,
    )
    .await
}
