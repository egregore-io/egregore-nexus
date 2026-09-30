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

Headed OpenCode 1.17.17 now has a source candidate for native assistant-response usage and its
live UI context calculation. Usage keeps input/output/cache-read/cache-write/reasoning separate
under `lastResponse`; no total or native turn/reset is inferred. Context sums the same five buckets
and reads capacity from the exact observed provider/model. Used/remaining percentages and tokens
are estimates with an explicit live-TUI basis; capacity is native. A bounded ordered observation
queue preserves cross-message eviction across asynchronous local capacity lookups. The native
100-message live window includes user rows, handles removals, and does not claim equivalence to
the differently ordered history hydration path. Foreign roots cannot consume queue bounds; overflow
revokes reporting without interrupting input. No account-window source exists on this inspected surface.

Headed Claude 2.1.261 now has a separate processed-response usage candidate from the captured root
transcript. Content-block records share an API message ID; the collector counts that response once,
independently of earlier model evidence. Partial, explicit abort/error, synthetic, sidechain and
zeroed-compaction records do not replace usage. Native input/output/cache counters and optional
thinking remain separate under `lastResponse`, with no computed total or native measurement time.
These are native processed snapshots, not a guarantee of successful provider-final billing totals.
Usage does not enable context/allowance: those statusline sources remain unverified until capture can
preserve the user's effective selected command. Neither new headed source is a rollout claim.

The adapter and public report contracts support separate usage, context and account-allowance
capabilities/observations. The headed Codex app-server collector now supplies native-session token
snapshots, last-reported context estimates and passive account-window observations using the pinned
0.154.0 protocol. All four ACP adapters now supply explicitly scoped usage and last-reported context
in the current candidate: Codex/OpenCode last response, Claude last prompt, Hermes resident-session
cumulative. Their context proxies are mode-specific, not interchangeable measurements. Claude ACP
capacity may come from a native default/heuristic and is marked estimated; the others preserve the
supplied native capacity. Headed Hermes additionally samples persisted native session-row counters
at exact-root gateway hooks; no total is manufactured from the independent breakdowns. These new
collector changes are source candidates, not an installation or live-provider acceptance claim.
Coverage varies by category and mode. Unsupported or unknown is not zero, and contract
support is not a claim that every collector supplies live values.
Consumers preserve each value's native/derived/estimated basis and timestamp. Usage snapshots retain
native-ID turn, last prompt, last response, or native-session scope and counter/reset identity; repeated cumulative values are
replacements, not amounts to add. Remaining context must never be computed from lifetime token
totals. Account allowance retains provider/account/window/reset scope, not per-session cost.

Across all eight harness/mode paths, prefer a natively exposed metric or percentage; otherwise
reuse a verified same-version native display calculation and label its derived/estimated basis.
Do not invent a substitute from lifetime counters. A headed native source never enables ACP by
implication. Consumers render supplied canonical percentages without repeating the calculation.

Codex `thread/tokenUsage/updated` supplies `total`, `last` and optional `modelContextWindow`, not a
percentage. Usage preserves `total` as a replacement snapshot, including input/output/cache/reasoning
breakdowns. Missing optional cache-write, model, native timestamp and reset identity stay absent;
counter decreases are not assigned invented epochs. Context token estimates use `last.totalTokens`,
not cumulative usage. The remaining percentage matches Codex 0.154's fixed 12,000-token UI baseline:
round the clamped ratio of remaining user-controllable context to `(capacity - 12000)` (zero when
capacity is at most 12,000). This is a **last-reported display estimate**, not a direct native percent,
guaranteed current occupancy, output reserve, or a user-configurable baseline. Raw remaining tokens
are calculated independently from raw capacity minus the last sample, never from that percentage.
Over-window samples preserve used tokens and the native-display zero percent, but omit negative
remaining tokens rather than clamp them into an invented zero-token observation.

Exact published-owner/root admission and native turn ordering fence telemetry. New turns clear
context; old-turn notifications cannot restore it. Compaction items (and the legacy notification)
or an explicit context-window error block occupancy until a distinct native turn starts. Capacity
can remain observed while occupancy is absent. Identifiable synthetic/inconsistent full-window
counts do not become usage or occupancy. Repeated same-turn samples remain last-reported estimates;
there is no extra cumulative-append prerequisite that the Codex UI itself does not impose.

Version evidence: installed `codex 0.154.0` generated schema, plus tagged
[protocol calculation](https://github.com/openai/codex/blob/rust-v0.154.0/codex-rs/protocol/src/protocol.rs)
and [TUI caller](https://github.com/openai/codex/blob/rust-v0.154.0/codex-rs/tui/src/chatwidget.rs).
Fixtures are native-shaped examples, not recorded live-provider responses. This source change
does not install or restart an existing daemon. Disposable authenticated Gateway WebSocket
hydration checks cover these exact producer reports, including reconnect and stopped state;
live-provider and rendered-client acceptance remain separate.

### Headed Codex account windows

Existing `account/rateLimits/updated` notifications supply native used percentages, rolling-window
minutes and Unix-second reset times. Nexus converts only the units to seconds/milliseconds, retains
primary/secondary windows by native limit ID, and does not infer remaining balances, prices or costs.
Missing account identity stays absent: these rolling notifications do not include an account ID.
The separately available `account/rateLimits/read` RPC is not automatically invoked, so this
collector adds no provider request. Its OpenAI account scope is distinct from the configured model's
provider and from per-session token counters.

Sparse updates retain previously available windows, bounded to eight buckets/sixteen windows per
captured connection. Each retained window keeps its sample time internally; the combined wire
snapshot uses the oldest retained time, so updating one window or bucket cannot refresh another.
Metadata-only updates do not refresh window age. Account-change notifications
or invalid/overflowing snapshots clear retained evidence; native over-limit percentages are not
clamped. Provisional, disconnected and revoked owners cannot publish. No global account snapshot
is relabeled as native thread/turn status.

### Claude ACP selected account window

ACP 0.58.1 can forward the SDK rate-limit event in `_meta["_claude/rateLimit"]` once assistant
usage exists. This is a selected status/window, not a complete account inventory or an initial
account query. The candidate converts native utilization fractions to percentages and Unix-second
reset times to milliseconds; five-hour and distinct seven-day bucket identities retain their
durations. It does not infer account identity, remaining balance, limit, cost, or overage entitlement.
Only an explicit extension updates quota; one without utilization becomes Unknown rather than
zero. A changed selected window replaces the earlier one instead of accumulating account windows.
Repeated identical root/window/duration/reset/measurement tuples retain the earlier observation
time because native status-only rejections can replay older values. No native measurement timestamp
is exposed. The backing source is pinned to the bridge's bundled SDK 0.3.205/Claude 2.1.205;
arbitrary executable overrides and live account-dashboard correspondence remain unverified.

### Headed Hermes session usage

The launch-local hook reads only the exact framework-selected session row in a read-only database
transaction. Native input, output, cache-read, cache-write and reasoning counters are independent
session-cumulative fields; they replace previous snapshots, even on decreases. No total, native turn
ID or reset is synthesized. A native API-call count of zero leaves the fresh-row defaults Unknown;
missing telemetry columns preserve independent model reporting. The timestamp records the row read,
not a native API measurement timestamp. Context occupancy/capacity exists inside the native gateway
footer logic but is not exported by these hooks or rows, and account allowances are absent from this
surface. Those categories are unsupported here, not zero and not inferred from usage or costs.

Source pins: the 0.154.0 [account protocol](https://github.com/openai/codex/blob/rust-v0.154.0/codex-rs/app-server-protocol/src/protocol/v2/account.rs)
and [account processor](https://github.com/openai/codex/blob/rust-v0.154.0/codex-rs/app-server/src/request_processors/account_processor.rs)
define the sparse notification and Codex/ChatGPT authentication boundary. Native-shaped fixtures
exercise the real captured reporter, daemon/store and Gateway socket; they are not
live account measurements.

### Codex headless ACP context and usage

The pinned `@agentclientprotocol/codex-acp@1.1.2` emits structured `usage_update` notifications
with its last model sample's `used` tokens and effective context `size`. The captured ACP
connection/root publishes these through the same context slot. Capacity is native; occupancy,
remaining tokens and remaining percentage are explicitly last-reported estimates. Percentage
matches the bridge's native rounded `(size - used) / size * 100` display, not the headed TUI's
12000-token baseline. Overfull used tokens remain visible; negative remaining values are omitted.
No turn/reset/compaction identity or native timestamp is invented from an ordered notification
that does not supply one. Duplicates and decreases replace the prior sample, never add to it.

The pinned bridge's prompt `usage` is built from its **last model sample**, despite generic ACP
schema comments about cumulative usage. It is reported as `scope: lastResponse`, not a whole
prompt/turn or cumulative session total. Input/output/cache/reasoning and supplied total are
forwarded unchanged; cache breakdowns must not be added again to the supplied total. There is no
exported native turn/reset ID, so neither is invented from JSON-RPC request correlation. The
captured connection, request and native root govern admission; canceled/replaced requests cannot
publish a late response. Repeated or decreasing samples replace previous observations.

The additive `lastResponse` and `lastPrompt` scope values require coordinated consumer updates:
older validators reject the complete runtime snapshot on an unknown scope. A new contract value
does not itself enable another producer. `lastPrompt` is reserved for a native prompt aggregate
that may include several responses, without pretending the protocol exports a native turn ID.

This does not enable cumulative token usage or account windows for Codex ACP.
Account windows remain internal to its `/status` text; there is no
verified structured quota export on this path. Requested command/package overrides do not
establish additional capabilities. Other harness category enablement is tracked separately.

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

The composition uses a real loopback WebSocket and the real authenticated REST dispatcher against
the disposable canonical database. It checks read scope, missing/invalid/revoked credentials and
fresh-subscription rehydration; no operator credentials or daemon read-view access are used. It is
still a native-fixture test, not a live provider test. External consumers must validate their own
hydration, listener-ready subscription, owner/revision reduction, reconnect and stopped-state
handling separately; the base gate does not certify those integrations. Actual browser behavior
requires a separate visual smoke test.

Fixture coverage does not establish live-provider or platform-wide acceptance. Installation,
provider execution and release validation are separate checks. Child-context segregation and
interaction classification are not guaranteed by these telemetry fixtures.
