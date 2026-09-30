//! Source commands: `nexus source register|ls|show|enable|disable|rotate|rm` and `nexus push`
//! Source reads use store read views; source writes and pushes
//! use daemon-managed command intents.

use std::io::{BufRead, IsTerminal, Read};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Args, Subcommand};
use nexus_contracts::{
    PushRequest, PushResponse, Source, SourceListResponse, SourceRef, SourceRegisterRequest,
    SourceRegisterResponse, SourceTokenResponse,
};

use crate::cli::read_client::ReadClient;
use crate::cli::render::{finish_with, print_human, print_json};
use crate::cli::store_client::StoreClient;

/// `nexus source <op>`.
#[derive(Subcommand, Debug)]
pub enum SourceCmd {
    /// Register a new notification source (prints token once — copy it).
    Register {
        /// Unique name for the source (e.g. `github-ci`).
        name: String,
        /// Default topic pushes from this source are routed to.
        #[arg(long)]
        topic: Option<String>,
    },
    /// List all registered sources.
    Ls,
    /// Show a single source's details.
    Show { name: String },
    /// Enable a source.
    Enable { name: String },
    /// Disable a source.
    Disable { name: String },
    /// Rotate a source's token (prints new token once — copy it).
    Rotate { name: String },
    /// Remove a source.
    Rm { name: String },
}

/// Dispatch a `source` op through store read views or daemon-managed command intents.
pub async fn run(store: &StoreClient, read: &ReadClient, cmd: SourceCmd, json: bool) -> ExitCode {
    match cmd {
        SourceCmd::Register { name, topic } => {
            let res: Result<SourceRegisterResponse, _> = store
                .command(
                    nexus_store::command_kinds::source::REGISTER,
                    &SourceRegisterRequest { name, topic },
                )
                .await;
            finish_with(res, json, |r| {
                format!(
                    "source {} → topic {}\ntoken {}",
                    r.source.name, r.source.topic, r.token
                )
            })
        }
        SourceCmd::Ls => {
            let res: Result<SourceListResponse, _> = read.sources().await;
            finish_with(res, json, |l| {
                if l.sources.is_empty() {
                    return "(no sources)".to_string();
                }
                let mut s =
                    String::from("NAME                 TOPIC                ENABLED  LAST FIRED");
                for src in &l.sources {
                    s.push_str(&format!(
                        "\n{:<20} {:<20} {:<8} {}",
                        src.name,
                        src.topic,
                        if src.enabled { "yes" } else { "no" },
                        fmt_relative(src.last_fired_at)
                    ));
                }
                s
            })
        }
        SourceCmd::Show { name } => {
            let res: Result<Source, _> = read.source(&name).await;
            finish_with(res, json, |s| {
                format!(
                    "name    {}\ntopic   {}\nenabled {}\ncreated {}\nlast fired {}",
                    s.name,
                    s.topic,
                    s.enabled,
                    fmt_relative(Some(s.created_at)),
                    fmt_relative(s.last_fired_at)
                )
            })
        }
        SourceCmd::Enable { name } => {
            let res: Result<Source, _> = store
                .command(
                    nexus_store::command_kinds::source::ENABLE,
                    &SourceRef { name },
                )
                .await;
            finish_with(res, json, |s| format!("{} enabled={}", s.name, s.enabled))
        }
        SourceCmd::Disable { name } => {
            let res: Result<Source, _> = store
                .command(
                    nexus_store::command_kinds::source::DISABLE,
                    &SourceRef { name },
                )
                .await;
            finish_with(res, json, |s| format!("{} enabled={}", s.name, s.enabled))
        }
        SourceCmd::Rotate { name } => {
            let res: Result<SourceTokenResponse, _> = store
                .command(
                    nexus_store::command_kinds::source::ROTATE,
                    &SourceRef { name },
                )
                .await;
            finish_with(res, json, |r| format!("{} new token {}", r.name, r.token))
        }
        SourceCmd::Rm { name } => {
            let res: Result<SourceRef, _> = store
                .command(
                    nexus_store::command_kinds::source::REMOVE,
                    &SourceRef { name },
                )
                .await;
            finish_with(res, json, |r| format!("removed {}", r.name))
        }
    }
}

// ── nexus push ───────────────────────────────────────────────────────────────────────────────────

/// `nexus push <source> [--topic <t>] [--summary <s>] [-m <body> | --json | <stdin>]`
#[derive(Args, Debug)]
pub struct PushArgs {
    /// Name of the registered source making the push.
    pub source: String,
    /// Override topic for this push; absent → source's default topic.
    #[arg(long)]
    pub topic: Option<String>,
    /// Short human-readable summary (used as notification title).
    #[arg(long)]
    pub summary: Option<String>,
    /// Event body (inline; mutually exclusive with `--json` / pipe).
    #[arg(short = 'm', long = "message")]
    pub message: Option<String>,
    /// Read `{summary,body,meta}` from stdin as JSON; push once.
    #[arg(long)]
    pub json: bool,
}

/// The JSON shape expected when `--json` is given (stdin is read fully and parsed).
#[derive(serde::Deserialize)]
struct JsonPush {
    summary: Option<String>,
    body: String,
    #[serde(default)]
    meta: Option<serde_json::Value>,
}

/// Resolved body source for one `nexus push` invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushInputMode {
    /// Literal `-m/--message` body.
    InlineMessage,
    /// One JSON object read from stdin.
    JsonStdin,
    /// One event per non-empty piped line.
    PipedLines,
    /// No body source was supplied from an interactive terminal.
    MissingInteractiveBody,
}

/// Resolve the body input mode while preserving the global `--json` output contract.
///
/// Clap exposes the root output flag and `push`'s historical JSON-stdin flag under the same long
/// name. Consequently `nexus --json push source -m body` sets `json_input` too. An explicit inline
/// body is unambiguous and must win; without a message, `--json` retains its existing stdin meaning.
pub fn select_push_input_mode(
    json_input: bool,
    message: Option<&str>,
    stdin_is_terminal: bool,
) -> PushInputMode {
    if message.is_some() {
        PushInputMode::InlineMessage
    } else if json_input {
        PushInputMode::JsonStdin
    } else if !stdin_is_terminal {
        PushInputMode::PipedLines
    } else {
        PushInputMode::MissingInteractiveBody
    }
}

// ── pure builders (no I/O — unit-testable) ───────────────────────────────────────────────────────

/// Build a [`PushRequest`] from an inline `-m <body>` string.
pub fn build_push_from_message(
    source: &str,
    topic: Option<String>,
    summary: Option<String>,
    body: String,
) -> PushRequest {
    PushRequest {
        source: source.into(),
        topic,
        summary,
        body,
        meta: None,
    }
}

/// Build a [`PushRequest`] by parsing a JSON blob from `--json` mode. `flag_summary` is used when
/// the JSON object itself omits the `summary` field.
pub fn build_push_from_json(
    source: &str,
    topic: Option<String>,
    flag_summary: Option<String>,
    raw: &str,
) -> Result<PushRequest, serde_json::Error> {
    let parsed: JsonPush = serde_json::from_str(raw)?;
    let summary = parsed.summary.or(flag_summary);
    Ok(PushRequest {
        source: source.into(),
        topic,
        summary,
        body: parsed.body,
        meta: parsed.meta,
    })
}

/// Build a [`PushRequest`] from a single line of piped stdin.
pub fn build_push_from_line(
    source: &str,
    topic: Option<String>,
    summary: Option<String>,
    line: &str,
) -> PushRequest {
    PushRequest {
        source: source.into(),
        topic,
        summary,
        body: line.into(),
        meta: None,
    }
}

/// `nexus push` handler.
///
/// Mode resolution (priority):
/// 1. `-m <body>`: push once with the literal body. This wins when the root `--json` output flag
///    also sets the historical push JSON-input flag.
/// 2. `--json`: read all stdin, parse as [`JsonPush`], push once.
/// 3. stdin is NOT a TTY (piped): each non-empty line → one push RPC.
/// 4. Interactive TTY with no `-m` / `--json`: error.
pub async fn push(client: &StoreClient, args: PushArgs, json_output: bool) -> ExitCode {
    let mode = select_push_input_mode(
        args.json,
        args.message.as_deref(),
        std::io::stdin().is_terminal(),
    );
    if mode == PushInputMode::InlineMessage {
        // Mode 1: inline -m body. This is checked before JSON stdin because the global `--json`
        // output flag shares the same Clap flag name with this command's legacy input mode.
        let req = build_push_from_message(
            &args.source,
            args.topic,
            args.summary,
            args.message.expect("inline mode requires message"),
        );
        let res: Result<PushResponse, _> = client
            .command(nexus_store::command_kinds::source::PUSH, &req)
            .await;
        return finish_with(res, json_output, |r| {
            format!(
                "accepted → topic {} (queued_to {}, message_id {})",
                r.topic,
                r.queued_to,
                r.message_id.as_ref().map(|id| id.0.as_str()).unwrap_or("-")
            )
        });
    }

    if mode == PushInputMode::JsonStdin {
        // Mode 1: read full stdin as a JSON object.
        let mut raw = String::new();
        if let Err(e) = std::io::stdin().read_to_string(&mut raw) {
            eprintln!("error: reading stdin: {e}");
            return ExitCode::from(1);
        }
        let req = match build_push_from_json(&args.source, args.topic, args.summary, &raw) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("error: invalid JSON push body: {e}");
                return ExitCode::from(1);
            }
        };
        let res: Result<PushResponse, _> = client
            .command(nexus_store::command_kinds::source::PUSH, &req)
            .await;
        return finish_with(res, json_output, |r| {
            format!(
                "accepted → topic {} (queued_to {}, message_id {})",
                r.topic,
                r.queued_to,
                r.message_id.as_ref().map(|id| id.0.as_str()).unwrap_or("-")
            )
        });
    }

    if mode == PushInputMode::PipedLines {
        // Mode 3: piped stdin — each non-empty line is one push.
        let stdin = std::io::stdin();
        let reader = stdin.lock();
        let mut count: u32 = 0;
        let mut last_err: Option<String> = None;
        for line_result in reader.lines() {
            let line = match line_result {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("error: reading stdin line: {e}");
                    return ExitCode::from(1);
                }
            };
            let line = line.trim().to_string();
            if line.is_empty() {
                continue;
            }
            let req = build_push_from_line(
                &args.source,
                args.topic.clone(),
                args.summary.clone(),
                &line,
            );
            match client
                .command::<_, PushResponse>(nexus_store::command_kinds::source::PUSH, &req)
                .await
            {
                Ok(r) => {
                    count += 1;
                    if json_output {
                        print_json(&r);
                    }
                }
                Err(e) => {
                    last_err = Some(e.message.clone());
                    eprintln!("error: push failed: {}", e.message);
                }
            }
        }
        if let Some(err) = last_err {
            eprintln!("error: last push failed: {err}");
            return ExitCode::from(1);
        }
        if !json_output {
            print_human(&format!("pushed {count} event(s)"));
        }
        return ExitCode::SUCCESS;
    }

    // Mode 4: interactive TTY with no body source.
    eprintln!("error: provide a body with -m <text>, --json, or pipe lines on stdin");
    ExitCode::from(1)
}

/// Format an `Option<i64>` Unix-millisecond timestamp as a human-readable relative time.
/// Returns `"-"` when `None`, or a string like `"5m ago"` / `"2h ago"` / `"just now"`.
fn fmt_relative(ts_ms: Option<i64>) -> String {
    let ms = match ts_ms {
        None => return "-".to_string(),
        Some(ms) => ms,
    };
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let diff_secs = (now_ms - ms) / 1000;
    if diff_secs < 5 {
        "just now".to_string()
    } else if diff_secs < 60 {
        format!("{}s ago", diff_secs)
    } else if diff_secs < 3600 {
        format!("{}m ago", diff_secs / 60)
    } else if diff_secs < 86400 {
        format!("{}h ago", diff_secs / 3600)
    } else {
        format!("{}d ago", diff_secs / 86400)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    use crate::cli::Cli;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("nexus").chain(args.iter().copied())).unwrap()
    }

    // ── arg-parse tests ──────────────────────────────────────────────────────

    #[test]
    fn register_with_topic_parses() {
        let cli = parse(&["source", "register", "myapp", "--topic", "deploys"]);
        match cli.command {
            crate::cli::Command::Source(SourceCmd::Register { name, topic }) => {
                assert_eq!(name, "myapp");
                assert_eq!(topic, Some("deploys".into()));
            }
            other => panic!("expected Source(Register), got {:?}", other),
        }
    }

    #[test]
    fn register_without_topic_parses() {
        let cli = parse(&["source", "register", "myapp"]);
        match cli.command {
            crate::cli::Command::Source(SourceCmd::Register { name, topic }) => {
                assert_eq!(name, "myapp");
                assert_eq!(topic, None);
            }
            other => panic!("expected Source(Register), got {:?}", other),
        }
    }

    #[test]
    fn ls_parses() {
        let cli = parse(&["source", "ls"]);
        assert!(matches!(
            cli.command,
            crate::cli::Command::Source(SourceCmd::Ls)
        ));
    }

    #[test]
    fn rm_parses() {
        let cli = parse(&["source", "rm", "x"]);
        match cli.command {
            crate::cli::Command::Source(SourceCmd::Rm { name }) => {
                assert_eq!(name, "x");
            }
            other => panic!("expected Source(Rm), got {:?}", other),
        }
    }

    #[test]
    fn show_enable_disable_rotate_parse() {
        assert!(matches!(
            parse(&["source", "show", "foo"]).command,
            crate::cli::Command::Source(SourceCmd::Show { .. })
        ));
        assert!(matches!(
            parse(&["source", "enable", "foo"]).command,
            crate::cli::Command::Source(SourceCmd::Enable { .. })
        ));
        assert!(matches!(
            parse(&["source", "disable", "foo"]).command,
            crate::cli::Command::Source(SourceCmd::Disable { .. })
        ));
        assert!(matches!(
            parse(&["source", "rotate", "foo"]).command,
            crate::cli::Command::Source(SourceCmd::Rotate { .. })
        ));
    }

    // ── fmt_relative unit tests ──────────────────────────────────────────────

    #[test]
    fn fmt_relative_none_is_dash() {
        assert_eq!(fmt_relative(None), "-");
    }

    #[test]
    fn fmt_relative_just_now() {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        assert_eq!(fmt_relative(Some(now_ms)), "just now");
    }

    #[test]
    fn fmt_relative_minutes() {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let five_min_ago = now_ms - 5 * 60 * 1000;
        assert_eq!(fmt_relative(Some(five_min_ago)), "5m ago");
    }

    // ── push builder tests ───────────────────────────────────────────────────

    #[test]
    fn push_from_message_builds_correct_request() {
        let req = build_push_from_message("myapp", Some("deploys".into()), None, "hi".into());
        assert_eq!(req.source, "myapp");
        assert_eq!(req.topic, Some("deploys".into()));
        assert_eq!(req.summary, None);
        assert_eq!(req.body, "hi");
        assert!(req.meta.is_none());
    }

    #[test]
    fn push_from_json_parses_full_object() {
        let raw = r#"{"body":"x","summary":"s","meta":{"k":1}}"#;
        let req = build_push_from_json("myapp", Some("ci".into()), None, raw).unwrap();
        assert_eq!(req.source, "myapp");
        assert_eq!(req.topic, Some("ci".into()));
        assert_eq!(req.summary, Some("s".into()));
        assert_eq!(req.body, "x");
        assert_eq!(req.meta, Some(serde_json::json!({"k": 1})));
    }

    #[test]
    fn push_from_json_no_summary_uses_flag_summary() {
        let raw = r#"{"body":"x"}"#;
        let req = build_push_from_json("myapp", None, Some("flag-summary".into()), raw).unwrap();
        assert_eq!(req.body, "x");
        assert_eq!(req.summary, Some("flag-summary".into()));
        assert!(req.meta.is_none());
    }

    #[test]
    fn push_from_line_builds_correct_request() {
        let req = build_push_from_line("myapp", None, None, "line one");
        assert_eq!(req.source, "myapp");
        assert_eq!(req.topic, None);
        assert_eq!(req.summary, None);
        assert_eq!(req.body, "line one");
    }

    #[test]
    fn push_args_with_message_parses() {
        let cli = parse(&["push", "myapp", "--topic", "deploys", "-m", "hello"]);
        match cli.command {
            crate::cli::Command::Push(a) => {
                assert_eq!(a.source, "myapp");
                assert_eq!(a.topic, Some("deploys".into()));
                assert_eq!(a.message, Some("hello".into()));
                assert!(!a.json);
            }
            other => panic!("expected Push, got {:?}", other),
        }
    }

    #[test]
    fn push_args_with_json_flag_parses() {
        let cli = parse(&["push", "myapp", "--json"]);
        match cli.command {
            crate::cli::Command::Push(a) => {
                assert_eq!(a.source, "myapp");
                assert!(a.json);
                assert!(a.message.is_none());
            }
            other => panic!("expected Push, got {:?}", other),
        }
    }

    #[test]
    fn push_args_with_summary_parses() {
        let cli = parse(&[
            "push",
            "myapp",
            "--summary",
            "deploy event",
            "-m",
            "details",
        ]);
        match cli.command {
            crate::cli::Command::Push(a) => {
                assert_eq!(a.summary, Some("deploy event".into()));
                assert_eq!(a.message, Some("details".into()));
            }
            other => panic!("expected Push, got {:?}", other),
        }
    }
}
