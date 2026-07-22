# Bounded CLI Lifecycle Commands

**Status:** approved design
**Date:** 2026-07-22
**Scope:** `nexus gateway`, `nexus webconsole`, and `nexus update`

## Problem

Gateway and Webconsole already spawn their long-lived servers in detached process groups, and the
updater already places deadlines around registry, install, and verification commands. Those local
protections do not yet form one end-to-end guarantee that the operator gets their shell back.

The remaining blocking seams are:

- Gateway store migration uses an unbounded child-process `output()` call.
- Webconsole browser launch and several operating-system process probes wait without a deadline.
- Gateway and Webconsole duplicate the daemon's detach logic instead of sharing one audited
  implementation.
- Update kills only the immediate package-manager child on timeout. A descendant may survive with
  inherited stdout or stderr open, causing the reader-thread joins to block forever even after the
  nominal timeout.
- Windows and macOS helper processes used for liveness, identity, termination, and browser launch
  can hold the CLI indefinitely if the operating-system command stalls.

The operator requirement is simple: every lifecycle invocation returns success or a typed error
within a documented deadline. The sole deliberate foreground stream is an explicit
`logs --follow`, which remains attached until interrupted.

## Decision

Use the same lifecycle model as `nexus daemon start` for all three surfaces:

1. Long-lived services run in a new process group/session with stdin closed and stdout/stderr sent
   to owned log files.
2. The CLI waits only for a bounded readiness or shutdown receipt, then exits.
3. Short-lived helper commands run in their own killable process boundary with bounded output.
4. Timeout cleanup targets the entire child process tree, not only the direct child.
5. Every blocking phase reports a stable operation name and deadline in its error.

A shared Rust module owns these mechanics. Gateway, Webconsole, update, and the daemon lifecycle
call that module instead of carrying separate detach/timeout implementations.

## Public behavior

### Gateway

- `gateway start` resolves the installation, ensures the daemon, runs migration with a bounded
  deadline, spawns the Gateway detached, and waits at most the existing 60-second readiness budget.
- `gateway stop` and `gateway restart` retain their current graceful and forced shutdown budgets.
  Every operating-system probe or signal helper inside those budgets is itself bounded.
- `gateway status` and `gateway logs` return finite snapshots.
- `gateway logs --follow` remains attached and exits on the operator's interrupt.

### Webconsole

- `webconsole start` and `restart` spawn the server detached and wait only for bounded readiness.
- `webconsole stop`, adoption, liveness, and executable verification cannot wait indefinitely on
  `ps`, PowerShell, `tasklist`, or `taskkill`.
- Browser opening is fire-and-forget after verified readiness. Failure to spawn the platform opener
  remains reportable; the CLI does not wait for the browser process to exit.
- `webconsole status` and `webconsole logs` return finite snapshots.
- `webconsole logs --follow` remains attached and exits on the operator's interrupt.

### Update

- `update` remains foreground so its exit status truthfully reports updated, rolled back, or failed.
- Registry lookup, installation, verification, rollback, and service restoration retain explicit
  phase deadlines and gain one process-tree cleanup boundary.
- On timeout, Nexus terminates the command's entire process group/tree, drains only bounded output,
  writes the normal redacted receipt, and returns a typed failure. A descendant retaining an output
  handle cannot keep the CLI alive.
- Ctrl-C leaves no updater-owned package-manager descendants. The update lock remains governed by
  its existing crash-safe ownership rules.

## Shared process boundary

Add a focused process-control module with two operations:

```rust
pub(crate) fn spawn_detached(command: &mut Command) -> io::Result<Child>;

pub(crate) fn run_bounded(
    command: &mut Command,
    operation: &'static str,
    timeout: Duration,
    output_limit: usize,
) -> Result<BoundedOutput, BoundedProcessError>;
```

`spawn_detached` applies the daemon's existing platform behavior:

- Unix: `setsid()` before exec.
- Windows: `CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS`.

`run_bounded` closes stdin, captures stdout/stderr up to the caller's limit, and creates a separate
kill boundary:

- Unix: a new session/process group; timeout sends TERM to the group, waits a short cleanup grace,
  then sends KILL to the group.
- Windows: a new process group; timeout invokes bounded tree termination and force termination for
  the PID and descendants.

Output readers may never extend the operation deadline. If the direct child exits while a
descendant retains a pipe, the runner treats the missing EOF as an unterminated process tree,
terminates the group, and returns the direct child's result only after bounded cleanup.

The error type distinguishes spawn, wait, timeout, cleanup, and non-zero exit. User-facing
lifecycles map it into their existing stable error envelopes rather than leaking provider or OS
details.

## Deadline policy

Deadlines stay close to the owning operation rather than becoming a global CLI timeout:

- Gateway migration: 120 seconds.
- Gateway readiness: existing 60 seconds.
- Gateway/Webconsole process probes and helper commands: 5 seconds each.
- Gateway/Webconsole graceful/forced stop: existing 8-second and 3-second windows.
- Webconsole readiness and health: existing bounded windows.
- Update registry query: existing 60 seconds.
- Update package installation or rollback: 15 minutes per phase.
- Update binary verification: existing 30 seconds per probe.

These are upper bounds, not sleeps. Successful commands return immediately. Tests use injected
short budgets while exercising the identical production runner.

## Cancellation and ownership

The CLI owns helper process groups but does not own successfully detached services. Consequently:

- dropping or timing out `run_bounded` cleans up its process tree;
- dropping the CLI after a successful `spawn_detached` does not stop Gateway or Webconsole;
- readiness failure does not silently convert the server into a foreground child;
- lifecycle commands continue to validate discovery/PID ownership before signaling a service;
- no timeout path deletes or rewrites another process's discovery record.

## Cross-platform verification

Tests must prove behavior, not merely inspect command strings:

1. a bounded helper that never exits returns within its deadline;
2. a helper that spawns a descendant retaining stdout/stderr cannot wedge the caller;
3. timeout cleanup removes the descendant process tree;
4. successful helpers preserve bounded stdout/stderr and exit status;
5. detached service children survive CLI return and do not inherit the CLI's stdin;
6. Gateway migration timeout prevents service spawn;
7. Webconsole browser launch returns after spawning rather than browser exit;
8. update timeout produces the expected failed/rolled-back receipt without leaked secrets;
9. normal `logs` returns and `logs --follow` remains the only intentional attached path.

Unix tests use real child processes and process groups. Windows tests use native command shims and
tree termination. macOS exercises the Unix process-group implementation plus its platform opener
and process-identity helper. CI must run the target-specific lifecycle tests on Linux, macOS, and
Windows; a Linux container is not accepted as proof of the other two operating systems.

## Rejected alternatives

### Background every update

Returning before an installation finishes loses the truthful exit status, complicates rollback,
and makes lock recovery the primary user interface. A bounded foreground transaction is clearer
and safer.

### Add timers only at call sites

A timer around `child.kill()` does not close descendant-held output pipes and therefore does not
guarantee shell return. The process tree must be the cleanup unit.

### Keep three independent launchers

Gateway, Webconsole, and daemon detach code has already drifted. One shared boundary makes the
cross-platform guarantees reviewable and prevents future lifecycle commands from reintroducing an
unbounded child.

## Non-goals

- Changing `logs --follow` into a background subscription.
- Backgrounding the update transaction.
- Changing Gateway or Webconsole ports, discovery schemas, or service ownership.
- Replacing systemd, launchd, or Windows Task Scheduler.
- Expanding this hardening into harness runtime or terminal-attach commands.
