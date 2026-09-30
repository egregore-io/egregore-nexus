use nexus_contracts::Harness as HarnessKind;
use nexus_harness_core::{harness_conformance, Harness};

#[derive(Debug, Clone, Copy)]
struct FutureHarness;

impl Harness for FutureHarness {
    fn kind(&self) -> HarnessKind {
        HarnessKind::Other
    }

    fn program(&self) -> &'static str {
        "future"
    }
}

harness_conformance!(FutureHarness);
