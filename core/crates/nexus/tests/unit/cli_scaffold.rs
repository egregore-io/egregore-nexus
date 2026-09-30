#![cfg(test)]

use super::*;
use clap::Parser;

fn parse(args: &[&str]) -> Cli {
    Cli::try_parse_from(std::iter::once("nexus").chain(args.iter().copied())).unwrap()
}

#[test]
fn clap_parses_every_documented_subcommand() {
    parse(&["launch", "claude"]);
    parse(&["launch", "claude", "--detach"]);
    parse(&["register", "--name", "ben"]);
    parse(&["whoami"]);
    parse(&["resume", "claude"]);
    parse(&["resume", "s_123"]);
    parse(&["attach", "claude"]);
    parse(&["members"]);
    parse(&["members", "--include-offline", "--presence"]);
    parse(&["threads", "--mine"]);
    parse(&["topics", "--subscribed"]);
    parse(&["dm", "ben", "-m", "hi"]);
    parse(&["dm", "ben", "--stdin"]);
    parse(&["post", "backend", "-m", "plan"]);
    parse(&["post", "backend", "--stdin"]);
    parse(&["reply", "-m", "on it"]);
    parse(&["reply", "--stdin"]);
    parse(&["publish", "ci", "-m", "green"]);
    parse(&["publish", "ci", "--stdin"]);
    parse(&["send", "--to", "ben", "-m", "hi"]);
    parse(&["send", "--to", "ben", "--stdin"]);
    parse(&["thread", "new", "backend", "--member", "dylan"]);
    parse(&["thread", "join", "backend"]);
    parse(&["thread", "leave", "backend"]);
    parse(&["thread", "archive", "backend"]);
    parse(&["thread", "delete", "backend"]);
    parse(&["thread", "members", "backend"]);
    parse(&["status", "paused", "--work", "compacting"]);
    parse(&["status"]);
    parse(&["subscribe", "ci", "--group", "workers"]);
    parse(&["unsubscribe", "ci"]);
    parse(&["search", "auth refactor", "--hybrid", "--with", "ben"]);
    parse(&["history", "--with", "ben", "--limit", "50"]);
    parse(&["read", "m_01"]);
    parse(&["admin", "route", "n_01", "--to", "ben"]);
    parse(&["admin", "spawn", "codex", "--name", "dylan"]);
    parse(&["admin", "remove", "dylan"]);
    parse(&["admin", "delete", "dylan"]);
    parse(&["admin", "channel", "create", "ci"]);
    parse(&["admin", "assign-role", "ben", "lead"]);
    parse(&["admin", "assign-project", "ben", "--project", "lens"]);
    parse(&["admin", "grant-tier", "ben", "admin"]);
    parse(&["admin", "monitor", "--follow"]);
    parse(&["source", "register", "gh-ci", "--topic", "ci.events"]);
    parse(&["source", "ls"]);
    parse(&["source", "show", "gh-ci"]);
    parse(&["source", "enable", "gh-ci"]);
    parse(&["source", "disable", "gh-ci"]);
    parse(&["source", "rotate", "gh-ci"]);
    parse(&["source", "rm", "gh-ci"]);
    parse(&["push", "myapp", "-m", "deploy done"]);
    parse(&[
        "push",
        "myapp",
        "--topic",
        "ci",
        "--summary",
        "s",
        "-m",
        "body",
    ]);
    parse(&["push", "myapp", "--json"]);
    parse(&["agents", "list"]);
    parse(&["listen"]);
    parse(&["listen", "--timeout-ms", "5000", "--max", "10", "--once"]);
    parse(&["mcp", "--as", "ada", "--project", "lens"]);
    parse(&["--json", "whoami"]);
}

#[test]
fn assign_project_parses_name_and_project_flag() {
    let cli = parse(&["admin", "assign-project", "ben", "--project", "lens"]);
    match cli.command {
        Command::Admin(commands::admin::AdminCmd::AssignProject { name, project }) => {
            assert_eq!(name, "ben");
            assert_eq!(project, "lens");
        }
        other => panic!("expected AssignProject, got {:?}", other),
    }
}

#[test]
fn admin_route_help_names_notification_audit_id() {
    use clap::CommandFactory;

    let mut command = Cli::command();
    let admin = command
        .find_subcommand_mut("admin")
        .expect("admin subcommand");
    let route = admin
        .find_subcommand_mut("route")
        .expect("admin route subcommand");
    let mut help = Vec::new();
    route.write_long_help(&mut help).unwrap();
    let help = String::from_utf8(help).unwrap();

    assert!(help.contains("notification audit id"), "{help}");
}

#[test]
fn agents_list_parses_correctly() {
    let cli = parse(&["agents", "list"]);
    assert!(matches!(
        cli.command,
        Command::Agents(commands::agents::AgentsCmd::List)
    ));
}

#[test]
fn json_flag_is_global_and_parsed() {
    let cli = parse(&["--json", "whoami"]);
    assert!(cli.global.json);
    let cli2 = parse(&["whoami", "--json"]);
    assert!(cli2.global.json);
}
