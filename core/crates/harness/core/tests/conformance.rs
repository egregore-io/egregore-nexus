use nexus_harness_core::{harness_conformance, Harness};

#[derive(Debug, Clone, Copy)]
struct FutureHarness;

impl Harness for FutureHarness {
    fn program(&self) -> &'static str {
        "future"
    }

    fn agent_token(&self) -> &'static str {
        "future"
    }
}

harness_conformance!(FutureHarness);
