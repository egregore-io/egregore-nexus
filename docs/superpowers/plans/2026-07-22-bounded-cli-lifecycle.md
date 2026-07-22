# Bounded CLI Lifecycle Commands Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Guarantee that `nexus gateway`, `nexus webconsole`, and `nexus update` either complete or return a typed failure within an explicit deadline, while preserving attached behavior only for `logs --follow`.

**Architecture:** Extract the daemon's platform detach behavior into one crate-local process lifecycle module. The same module also owns a bounded child-process runner that creates an isolated process group/tree, drains capped output concurrently, terminates the whole owned tree on timeout, and never waits indefinitely for descendant-held pipes. Gateway, Webconsole, update, and daemon lifecycle code become consumers of this one boundary.

**Tech Stack:** Rust standard process APIs, `libc` on Unix, `windows-sys` process/job APIs on Windows, Tokio only at existing async lifecycle boundaries, and the existing external test-layout convention under `core/crates/nexus/tests/unit`.

---

## Task 1: Build the shared process boundary tests-first

**Files:**
- Create: `core/crates/nexus/src/lifecycle_process.rs`
- Create: `core/crates/nexus/tests/unit/lifecycle_process.rs`
- Modify: `core/crates/nexus/src/lib.rs`
- Modify: `core/crates/nexus/Cargo.toml`

- [ ] Add external unit contracts for a successful bounded command, bounded stdout/stderr truncation, a direct timeout, a child whose descendant retains stdout, and process-tree cleanup after timeout. On Unix, use real `sh` processes and assert the recorded descendant PID disappears. Add target-gated Windows command-shim/job tests so the same contract runs in Windows CI.
- [ ] Run `cargo test -p egregore-nexus lifecycle_process -- --nocapture` and confirm the new tests fail because the module/API does not exist.
- [ ] Implement the smallest shared API:

  ```rust
  pub(crate) fn spawn_detached(command: &mut Command) -> io::Result<Child>;

  pub(crate) fn run_bounded(
      command: &mut Command,
      operation: &'static str,
      timeout: Duration,
      output_limit: usize,
  ) -> Result<BoundedOutput, BoundedProcessError>;
  ```

  `run_bounded` must close stdin, own a new process group/job, drain both output pipes without exceeding the returned byte cap, terminate the whole tree on deadline or abandoned pipe EOF, and distinguish spawn, wait, timeout, cleanup, and non-zero exit errors. Reader threads communicate through bounded-deadline channels; they are never joined without a deadline.
- [ ] Use `setsid` and negative-PGID signals on Unix. On Windows, create a kill-on-close Job Object, assign the child, and terminate the job on timeout; add only the target-specific `windows-sys` features required for that implementation.
- [ ] Add an operation-scoped cancellation registry used by the update command's Ctrl-C handler so an interrupted updater kills the currently owned process tree and subsequent rollback/helper launches fail fast rather than keeping the shell occupied.
- [ ] Re-run the focused test and commit: `feat(cli): add bounded process lifecycle boundary`.

## Task 2: Make daemon and Gateway share the lifecycle boundary

**Files:**
- Modify: `core/crates/nexus/src/daemon/lifecycle.rs`
- Modify: `core/crates/nexus/src/gateway_lifecycle.rs`
- Modify: `core/crates/nexus/tests/unit/daemon_lifecycle.rs`
- Modify: `core/crates/nexus/tests/unit/gateway_lifecycle.rs`

- [ ] Add RED source/behavior contracts proving daemon and Gateway no longer define private detach helpers, Gateway migration calls the shared bounded runner with a 120-second production budget, and migration timeout/non-zero exit prevents `spawn`.
- [ ] Run `cargo test -p egregore-nexus gateway_lifecycle_contracts daemon_lifecycle_contracts -- --nocapture` and record the expected failures.
- [ ] Replace both duplicated detach implementations with `lifecycle_process::spawn_detached` while preserving null stdin and owned log files.
- [ ] Replace Gateway migration's unbounded `Command::output` with `run_bounded`; map timeout and exit output into `GATEWAY_LIFECYCLE_FAILED` without leaking unbounded stderr.
- [ ] Route Gateway's platform process probes and signal helpers through the five-second bounded helper boundary where an OS command is required. Keep native health socket deadlines and existing 8-second/3-second stop budgets unchanged.
- [ ] Re-run focused Gateway/daemon lifecycle suites and commit: `fix(gateway): bound lifecycle child processes`.

## Task 3: Make Webconsole launch and helpers non-blocking

**Files:**
- Modify: `core/crates/nexus/src/webconsole_lifecycle.rs`
- Modify: `core/crates/nexus/tests/unit/webconsole_lifecycle.rs`
- Modify: `core/crates/nexus/tests/unit/webconsole_health.rs`

- [ ] Add RED contracts proving Webconsole uses the shared detach function, macOS/Windows process identity and liveness helpers are bounded, and browser opening returns after successful spawn without waiting for the opener to exit.
- [ ] Add a real Unix browser-opener regression using a fake `xdg-open` that sleeps after recording its PID; assert the lifecycle call returns promptly while the opener continues independently.
- [ ] Run `cargo test -p egregore-nexus webconsole_lifecycle -- --nocapture` and confirm the browser regression fails against `.status()`.
- [ ] Replace Webconsole's private detach helper with `spawn_detached`. Configure the platform opener with null stdin/stdout/stderr and detach it after readiness; report only spawn failure.
- [ ] Route `ps`, PowerShell, `tasklist`, and `taskkill` helpers through five-second bounded execution. Preserve Linux `/proc` and native `kill(2)` paths where they already avoid subprocesses.
- [ ] Re-run focused suites and commit: `fix(webconsole): detach browser and bound helpers`.

## Task 4: Make update foreground, bounded, and cancellation-safe

**Files:**
- Modify: `core/crates/nexus/src/update/system.rs`
- Modify: `core/crates/nexus/src/cli/commands/update.rs`
- Modify: `core/crates/nexus/tests/update_system_smoke.rs`
- Modify: `core/crates/nexus/tests/update_cli.rs`

- [ ] Add RED regressions with a fake package manager that (a) never exits and (b) exits after leaving a descendant holding stdout. Inject short test-only phase deadlines and assert the CLI returns, the descendant is gone, the receipt is redacted, and rollback/failure status remains truthful.
- [ ] Add a Ctrl-C subprocess regression: interrupt the updater while the fake package manager is running, assert bounded CLI exit, no recorded descendant remains, and no secret reaches stdout, stderr, or the receipt.
- [ ] Run `cargo test -p egregore-nexus --test update_system_smoke --test update_cli -- --nocapture` and confirm the hanging cases fail or exceed their guard deadline before implementation.
- [ ] Delete the updater's local `run_command` reader-thread implementation and delegate to `lifecycle_process::run_bounded`. Keep registry lookup at 60 seconds and verification at 30 seconds; set install and rollback to 15 minutes each.
- [ ] Install the shared cancellation handler only for mutating `nexus update`. On Ctrl-C, cancel the owned process boundary, make later transaction phases fail fast, write the existing redacted failure receipt when possible, and return a non-zero status without orphaning package-manager children.
- [ ] Preserve exact package/version selection, service capture/restore, rollback, and recovery-command behavior.
- [ ] Re-run both update suites and commit: `fix(update): bound package manager process trees`.

## Task 5: Pin the one intentional attached surface

**Files:**
- Modify: `core/crates/nexus/tests/cli_release_surface.rs`
- Modify: `core/crates/nexus/tests/unit/gateway_lifecycle.rs`
- Modify: `core/crates/nexus/tests/unit/webconsole_lifecycle.rs`

- [ ] Add contract tests that ordinary `gateway logs` and `webconsole logs` read a finite tail, while `--follow` alone routes into each polling loop.
- [ ] Add source guards that no Gateway/Webconsole/update call site uses raw `.output()` or `.status()` for platform helpers, migration, package management, or browser launch.
- [ ] Run the lifecycle surface tests and commit: `test(cli): pin bounded lifecycle surfaces`.

## Task 6: Verify every supported operating-system contract

**Files:**
- Modify only if a test exposes a defect in the scoped implementation.

- [ ] Run formatting and focused Linux gates:

  ```bash
  cargo fmt --all -- --check
  cargo test -p egregore-nexus lifecycle_process -- --nocapture
  cargo test -p egregore-nexus --test webconsole_lifecycle --test update_system_smoke --test update_cli --test cli_release_surface
  cargo test -p egregore-nexus gateway_lifecycle_contracts daemon_lifecycle_contracts
  ```

- [ ] Run `cargo test --workspace`, `git diff --check`, and repository checks `scripts/check core-test-layout`, `release-identity`, `architecture`, `boundaries`, and `rust-workspace`.
- [ ] Run the target-specific lifecycle test set in Linux, macOS, and Windows CI. Treat platform compilation as necessary but insufficient: each runner must execute its native process/tree and opener tests.
- [ ] Confirm no unrelated worktree file was staged, especially `docs/superpowers/specs/2026-07-22-gateway-durable-projections-design.md`.
- [ ] Request a bounded independent review of the final diff, address only demonstrated blockers, then commit any final correction separately.

