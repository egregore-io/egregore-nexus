//! The `nexus-acceptance` crate is a **test-only** workspace member: it carries no library code, it
//! only hosts the end-to-end acceptance suite under `core/tests/` (the AionUi-adapted scenarios from
//! spec §11, run against a real in-test daemon + the hermetic `MockAdapter`). The scenarios live in
//! the `[[test]]` targets declared in `Cargo.toml`; they share the `common` harness module.
