use std::io::Cursor;

use clap::Parser;
use nexus::cli::commands::notify::{build_request_with_reader, NotifyArgs};
use nexus::cli::{Cli, Command};
use nexus_contracts::{AgentId, NotifyTarget};

#[test]
fn notify_cli_parses_the_explicit_public_surface() {
    let cli = Cli::try_parse_from([
        "nexus",
        "notify",
        "--target",
        "group:reviewers",
        "--source",
        "watchdog",
        "--idempotency-key",
        "gate:42",
        "checkpoint failed",
    ])
    .unwrap();
    let Command::Notify(args) = cli.command else {
        panic!("expected notify command")
    };
    assert_eq!(args.target, "group:reviewers");
    assert_eq!(args.source.as_deref(), Some("watchdog"));
    assert_eq!(args.idempotency_key.as_deref(), Some("gate:42"));
    assert_eq!(args.message.as_deref(), Some("checkpoint failed"));
}

#[test]
fn notify_target_prefixes_and_stdin_build_the_daemon_request() {
    let cases = [
        (
            "a_stable",
            NotifyTarget::Agent {
                agent_id: AgentId("a_stable".into()),
            },
        ),
        (
            "agent:alice",
            NotifyTarget::Name {
                name: "alice".into(),
            },
        ),
        (
            "group:reviewers",
            NotifyTarget::Group {
                group: "reviewers".into(),
            },
        ),
        (
            "thread:release",
            NotifyTarget::Thread {
                thread: "release".into(),
            },
        ),
        (
            "release",
            NotifyTarget::Auto {
                value: "release".into(),
            },
        ),
    ];
    for (target, expected) in cases {
        let request = build_request_with_reader(
            NotifyArgs {
                target: target.into(),
                source: None,
                idempotency_key: None,
                message: None,
                stdin: true,
            },
            Cursor::new("callback payload"),
        )
        .unwrap();
        assert_eq!(request.target, expected);
        assert_eq!(request.body, "callback payload");
    }
}
