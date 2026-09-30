use clap::Parser;
use nexus::cli::Cli;

fn parse(args: &[&str]) -> Cli {
    Cli::try_parse_from(std::iter::once("nexus").chain(args.iter().copied())).unwrap()
}

#[test]
fn agents_identity_commands_parse() {
    parse(&["agents", "create", "blake", "--harness", "codex"]);
    parse(&["agents", "show", "blake"]);
    parse(&[
        "agents",
        "credentials",
        "create",
        "blake",
        "--purpose",
        "runtime",
        "--label",
        "local",
    ]);
    parse(&["agents", "credentials", "revoke", "cred_123"]);
    parse(&["agents", "runtimes", "blake"]);
}
