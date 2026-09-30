# Model and status reporting

The beta.4 implementation reports **observed model metadata**, not model control or guaranteed
provider resolution. Runtime status and evidence travel through the canonical Gateway projection,
REST reads and `runtime.snapshot` / `runtime.unavailable` WebSocket subscriptions. Consumers must
keep the exact runtime/agent ownership, report revision and connection/subscription sequence.

## Evidence by actual execution mode

| Harness | Mode | Reported evidence | Native source exercised by automated integration |
| --- | --- | --- | --- |
| Claude | Headless | Configured | Actual ACP adapter, fixture `session/new` / `session/load` replies |
| Codex | Headless | Configured | Actual ACP adapter, fixture `session/new` / `session/load` replies |
| OpenCode | Headless | Configured | Actual ACP adapter, fixture `session/new` / `session/load` replies |
| Hermes | Headless | Configured | Actual ACP adapter, fixture `session/new` / `session/load` replies |
| Codex | Headed | Configured | Fake app-server process through actual fresh/cold setup and daemon publisher |
| Claude | Headed | Response-reported | Actual attachment/SessionStart sidecar and transcript poller with exact-root assistant records |
| OpenCode | Headed | Turn-selected | Generated plugin, authenticated native bridge and actual daemon launch/resume fixture |
| Hermes | Headed | Configured | Generated framework handler, exact SQLite session row and actual daemon launch/resume fixture |

Configured, turn-selected and response-reported are separate slots; one must not be relabeled as
another. Requested model names are not reported as native evidence. Missing, invalid, synthetic,
foreign-root or unsupported metadata must not become a guessed model. Capabilities describe the
**selected backend**, not merely a requested headed/headless mode. Surviving legacy native processes
without a captured reporter are not retroactively certified by adoption.

The matrix exercises production collectors against native-shaped fixtures. It does **not** certify
installed provider versions, actual tmux sessions, every native HookRegistry dispatch path, or live
provider requests. Claude's second exported observation is a later response under the same owner;
it is not described as a cold resume. Hermes cold startup preserves its isolated profile and checks
the prior native root; process startup is not proof that conversation context was restored.
The Claude ACP case requests headed mode on the actual root without a PTY supervisor and verifies
that the constructed backend still reports `claude.acp`; the other ACP cases request headless mode.

## Lifetime and display rules

- A report belongs to a captured runtime/agent/native-source lifetime. Rebind, stop, replacement,
  source loss and failed setup revoke the captured observer; old callbacks must not publish as NEW.
- Stop settlement preserves historical evidence with `observerActive=false`. Historical model
  evidence is not a live status signal. Disconnected/unavailable UI state must remain explicit.
- Delayed full projections with older reports cannot restore old liveness while borrowing newer
  evidence. Same-owner equal-revision status updates and report-absent legacy frames retain their
  existing compatibility behavior.
- Gateway lifecycle timestamps are normalized descriptor timestamps. In particular, canonical
  `stoppedAt` currently denotes the accepted stop projection's `occurredAt`, not the daemon's
  original SQL stop timestamp. A rejected older report must not advance that timestamp.
- Initial/reconnect snapshots are complete; an empty snapshot means authoritative absence.
  Unavailable frames advance subscription sequence and clear live availability. Neither permits a
  late REST response or previous socket generation to restore stale live evidence.
- Model-enabled headed Claude input waits up to 30 seconds for its captured SessionStart source;
  failure is explicit. Independent display/archive forwarding may continue when model setup fails.

## Usage, context and allowance

The adapter and public report contracts support separate usage, context and account-allowance
capabilities/observations. These model collectors do not yet enable those native metrics. Unsupported
or unknown is not zero, and contract support is not a claim that a collector supplies live values.
Consumers preserve each value's native/derived/estimated basis and timestamp. Usage snapshots retain
per-turn versus native-session scope and counter/reset identity; repeated cumulative values are
replacements, not amounts to add. Remaining context must never be computed from lifetime token
totals. Account allowance retains provider/account/window/reset scope, not per-session cost.

## Reproducible automated gate

Run from the repository root:

```sh
scripts/check model-reporting
```

This command creates a disposable HOME and empty artifact directory, runs the existing four ACP
cases plus the Codex headed case and three headed daemon cases, and requires all eight nonempty
artifacts. Files are created once with a unique run identity. Each carries actual `WsSink` payloads
for fresh, newer and stopped evidence. The production publisher orders those unchanged bodies as
NEW, OLD, STOPPED, OLD, with distinct controlled envelope timestamps. The real Gateway materializer and canonical snapshot source must preserve
NEW, then STOPPED, despite the higher stream sequences on OLD. The command prints SHA-256 hashes
before deleting its temporary artifacts. Missing/malformed inputs fail; no synthetic fallback or
optional skip is permitted. It is a required explicit gate, outside ordinary Vitest discovery.

The read fixture supplies canonical rows to the snapshot source; this composition is not a real
authenticated socket or live provider test. Separate Gateway socket tests cover transport
behavior. External consumers must validate their own hydration, listener-ready subscription,
owner/revision reduction, reconnect and stopped-state handling separately. Actual browser
behavior requires a separate visual smoke test.

Fixture coverage does not establish live-provider or platform-wide acceptance. Installation,
provider execution and release validation are separate checks. Child-context segregation and
interaction classification are not guaranteed by these telemetry fixtures.
