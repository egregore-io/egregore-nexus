use async_trait::async_trait;
use nexus::first_run::{run_setup, should_prompt, Components, Decision, SetupBackend};

#[derive(Default)]
struct Fake {
    components: Components,
    answer: Option<bool>,
    fail: Option<&'static str>,
    calls: Vec<&'static str>,
}

#[async_trait(?Send)]
impl SetupBackend for Fake {
    fn components(&mut self) -> Components {
        self.components
    }
    fn ask(&mut self, _: Components) -> Result<bool, String> {
        self.calls.push("ask");
        self.answer.ok_or("input closed".into())
    }
    async fn daemon(&mut self) -> Result<(), String> {
        self.step("daemon")
    }
    async fn gateway(&mut self) -> Result<(), String> {
        self.step("gateway")
    }
    async fn webconsole(&mut self) -> Result<(), String> {
        self.step("webconsole")
    }
    fn remember(&mut self, accepted: bool) -> Result<(), String> {
        self.step(if accepted { "accepted" } else { "declined" })
    }
}

impl Fake {
    fn step(&mut self, name: &'static str) -> Result<(), String> {
        self.calls.push(name);
        if self.fail == Some(name) {
            Err(format!("{name} failed"))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn installs_complete_stack_in_order_before_remembering_consent() {
    let mut fake = Fake {
        components: Components {
            gateway: true,
            webconsole: true,
        },
        answer: Some(true),
        ..Fake::default()
    };
    run_setup(&mut fake).await.unwrap();
    assert_eq!(
        fake.calls,
        ["ask", "daemon", "gateway", "webconsole", "accepted"]
    );
}

#[tokio::test]
async fn decline_remembers_without_starting_and_eof_remembers_nothing() {
    let mut fake = Fake {
        answer: Some(false),
        ..Fake::default()
    };
    run_setup(&mut fake).await.unwrap();
    assert_eq!(fake.calls, ["ask", "declined"]);
    let mut eof = Fake::default();
    assert!(run_setup(&mut eof)
        .await
        .unwrap_err()
        .contains("input closed"));
    assert_eq!(eof.calls, ["ask"]);
}

#[tokio::test]
async fn partial_install_does_not_request_missing_components() {
    let mut fake = Fake {
        answer: Some(true),
        ..Fake::default()
    };
    run_setup(&mut fake).await.unwrap();
    assert_eq!(fake.calls, ["ask", "daemon", "accepted"]);
}

#[tokio::test]
async fn failures_stop_sequence_and_do_not_record_completed_setup() {
    for (fail, expected) in [
        ("daemon", vec!["ask", "daemon"]),
        ("gateway", vec!["ask", "daemon", "gateway"]),
        ("webconsole", vec!["ask", "daemon", "gateway", "webconsole"]),
    ] {
        let mut fake = Fake {
            components: Components {
                gateway: true,
                webconsole: true,
            },
            answer: Some(true),
            fail: Some(fail),
            ..Fake::default()
        };
        assert!(run_setup(&mut fake).await.unwrap_err().contains(fail));
        assert_eq!(fake.calls, expected);
    }
}

#[test]
fn prompt_is_only_for_interactive_operator_entrypoints() {
    for args in [
        vec![],
        vec!["members"],
        vec!["launch", "codex"],
        vec!["agents", "list"],
    ] {
        let args: Vec<String> = args.into_iter().map(String::from).collect();
        assert!(should_prompt(&args, true, true, false), "{args:?}");
        assert!(!should_prompt(&args, false, true, false));
        assert!(!should_prompt(&args, true, false, false));
        assert!(!should_prompt(&args, true, true, true));
    }
    for args in [
        vec!["--help"],
        vec!["--version"],
        vec!["members", "--json"],
        vec!["members", "-q"],
        vec!["daemon", "run"],
        vec!["gateway", "start"],
        vec!["webconsole", "start"],
        vec!["mcp"],
        vec!["listen"],
        vec!["register"],
        vec!["reply", "--stdin"],
        vec!["launch", "--help"],
    ] {
        let args: Vec<String> = args.into_iter().map(String::from).collect();
        assert!(!should_prompt(&args, true, true, false), "{args:?}");
    }
}

#[tokio::test]
async fn missing_gateway_cannot_record_webconsole_as_configured() {
    let mut fake = Fake {
        components: Components {
            gateway: false,
            webconsole: true,
        },
        answer: Some(true),
        ..Fake::default()
    };
    assert!(run_setup(&mut fake).await.unwrap_err().contains("Gateway"));
    assert_eq!(fake.calls, ["ask"]);
}

#[test]
fn decision_lock_serializes_and_releases_without_stale_lock_recovery() {
    let home = tempfile::tempdir().unwrap();
    let first = Decision::acquire(home.path()).unwrap().unwrap();
    assert!(!first.remembered().unwrap());
    assert!(Decision::acquire(home.path()).is_err());
    first.remember(false).unwrap();
    assert!(first.remembered().unwrap());
    drop(first);
    let second = Decision::acquire(home.path()).unwrap().unwrap();
    assert!(second.remembered().unwrap());
    let value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(home.path().join("first-run.json")).unwrap())
            .unwrap();
    assert_eq!(value["background"], false);
}

#[test]
fn corrupt_decision_is_not_silently_overwritten_or_treated_as_success() {
    let home = tempfile::tempdir().unwrap();
    std::fs::write(home.path().join("first-run.json"), "broken").unwrap();
    let decision = Decision::acquire(home.path()).unwrap().unwrap();
    assert!(decision.remembered().is_err());
    assert_eq!(
        std::fs::read_to_string(home.path().join("first-run.json")).unwrap(),
        "broken"
    );
}
