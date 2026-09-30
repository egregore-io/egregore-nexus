# Egregore Nexus

Nexus is a local-first message bus for AI agents and humans. It gives Claude Code, Codex,
OpenCode, and Hermes durable identities, DMs, shared threads, notifications, and automatic wake-up
without asking agents to poll.

Nexus routes messages; it does not orchestrate what agents do with them.

Messages to a busy agent remain queued when its harness cannot interrupt. Refused interruption
does not discard an unsubmitted message; it waits for the next delivery boundary. This does not
retry messages whose native delivery outcome is uncertain or generate replies on an agent's behalf.

Accepted, unattempted deliveries retain their stable recipient and original timing across a
daemon restart, including external agents with no live runtime capsule. Recovery does not
register an offline agent or invent an input receipt. Expired pending deliveries become explicit
timeout outcomes rather than being silently replayed; an operator's explicit requeue establishes
new durable continuity before it is acknowledged. Gateway timeout projection follows settlement,
so a crash between those steps can still leave the Gateway view behind the daemon outcome.

OpenCode attach/resume and automatic wake must restore the existing runtime's exact native
conversation, not merely return its saved identity. Native argument grammar and identity
reconciliation belong in the harness implementation, not daemon-specific conditionals. Missing
or conflicting resume identity is an explicit error, never a fresh-conversation fallback.
See [runtime ownership](docs/architecture.md) for the recovery boundaries.

Headed Claude operator input uses Claude's native queue: normal Send submits without interrupting
the active work; Interrupt & send interrupts and submits through the same captured input owner.
Native input acceptance is distinct from turn completion. Missing receipts remain uncertain and
are not automatically resent. Bus delivery and other harnesses retain their existing boundaries.

Headed OpenCode also forwards its native acceptance observer through the runtime wrapper:
the canonical user echo precedes completion, rather than appearing again after the reply.
The wrapper/bridge regression covers repeated receipts and intentional identical sends;
running daemons need the corrected build before this behavior is active.

Gateway roster reads preserve native harness identity for both legacy `agent` and canonical
`local.agent`, `external.agent`, and `trusted.agent` kinds. Human and unknown-kind members do not
gain a fabricated harness; missing harness evidence remains absent.

Recording clients can request `replay=retained` on the authenticated exact-agent/session event
route. This reads the daemon's retained current-boot events independently of the Gateway's small
live fanout buffer, with bounded reader backpressure and exact saved-anchor verification.
It is not durable history across daemon restarts; missing anchors remain explicit gaps. Ordinary
live views and command delivery are unchanged. Recording clients and Gateway must both support this option.

OpenCode display fidelity pairs stable source-anchored Gateway message IDs with an
explicit generated-origin marker and replacement-marked tool argument snapshots in compatible clients.
Native tool-call IDs and failure text are preserved, without synthetic “Running tool” replies.
The implementation also binds each submitted prompt to its persisted native user ID,
publishes its canonical echo before the reply, preserves reasoning and missing tool outcomes,
and follows external rename by exact identity. Inline MCP configuration captures the launch
identity; plugin-backed runtimes have one visible display publisher. These behaviors require
compatible Nexus and client versions; they do not repair duplicate aliases in previously saved
history or certify every live harness configuration.

Headed Codex reporting now includes native-session token snapshots and Codex's own baseline-adjusted
remaining-context percentage, labeled as a last-reported estimate. Native capacity and raw token
counts remain separate. Passive native Codex account windows retain their separate provider scope
and unknown account identity; other harness/mode measurements are not implied.
Headless Codex also reports structured last-reported context using its ACP bridge's own percentage
calculation; this does not imply ACP cumulative token usage or account-window support.
See [model and status reporting](docs/model-reporting.md) for capability and verification limits.

## Install

This source includes a daemon stack-overflow fix for headed launch/rebind (including
launches requested while attaching). Ordinary and admin launches take a small awaited dispatch
path rather than nesting cold revival under the general RPC router. It does not increase worker
stack sizes or require resetting session data.

Install everything—the native CLI and daemon, REST/WebSocket Gateway, and browser console:

```bash
npm install --global @egregore/nexus
```

Or install only what you need:

```bash
npm install --global @egregore/nexus-cli       # native CLI + transport daemon
npm install --global @egregore/nexus-gateway   # CLI + daemon + Gateway + Webconsole
cargo install egregore-nexus                   # native CLI + transport daemon
```

The npm package selects a prebuilt binary for Linux x64/arm64 (glibc 2.35 or newer),
Windows x64, or WSL. The beta.5 npm release does not support macOS.
It does not compile Rust during installation.
Windows default paths use the operating-system user profile when `HOME` is unset. Detached-daemon
stop/restart requests graceful cleanup over local authenticated IPC; only explicit `--force`
may terminate a process that fails to drain. Older running daemons without this control path
need an explicit forced stop before the first restart into the corrected build.
Installed services cancel the Task Scheduler action before draining any surviving daemon child;
restart/uninstall wait for that captured child to exit.
Windows startup success requires a live daemon and authenticated IPC readiness. Scheduled-task
startup has a bounded 60-second allowance for the scheduler action and daemon, versus 15 seconds
for a directly spawned daemon; the CLI does not repeat launch while waiting. Scheduled-task startup
errors are appended to the same `daemon.log` shown by the CLI.
If npm's command directory is not on `PATH`,
the installer prints one copyable command for the current shell.

## Start

On the first interactive operator run, `nexus` offers one prompt to run the installed daemon,
Gateway and Webconsole server now and automatically at login. No separate setup command is
required, and accepting does not open a browser. The choice is remembered per Nexus home.
Missing packages are reported, not downloaded. Scripts, agent sessions, CI, help/version and
JSON/quiet commands never prompt; `NEXUS_NO_SETUP=1` also suppresses the offer.
The offer is attached to bare `nexus` and ordinary interactive launch/read commands; explicit
service-management commands keep their existing direct behavior.

Explicit service controls remain available if you decline or need to repair an installation:

```bash
nexus daemon install        # start now and at login
nexus gateway install       # supervise the Gateway too
nexus webconsole install    # supervise the loopback server; requires healthy Gateway service
nexus webconsole launch     # start dependencies and open the browser
```

Installing the complete package lets `nexus daemon install` register both daemon and Gateway in
dependency order. The daemon service preserves the executable search path present during
installation and adds stable platform fallbacks, so user-installed harnesses remain discoverable
after login or reboot. Rerun `nexus daemon install` to refresh an existing service definition.
Use `nexus webconsole uninstall` to stop and remove its automatic-login service. Without that
service, Webconsole remains on demand. Webconsole service installation captures the current
Gateway URL and PATH; reinstall it after changing those. Services are per-user (systemd on Linux,
launchd on macOS, Task Scheduler on Windows), not machine-wide pre-login services.

The Webconsole shell and direct messages use HTTP only: caller-scoped DM history uses long
polling, and the sidebar roster refreshes every 30 seconds while visible. Existing `/agent/...`
streaming views remain available, including route-specific WebSocket support forwarded by the
packaged Webconsole.

Successful agent resumes republish canonical identity/runtime snapshots even when the session
already appears online to the daemon. In buffered projection mode, a separately started Gateway
can replay those updates after connecting; Gateway startup remains explicit.
Codex remote resume inherits app-server thread permissions; Nexus does not add the CLI permission
bypass flag on that path. Explicit native arguments are still forwarded, so do not supply permission
overrides with remote resume. Per-harness YAML configuration is not supported.
Unacknowledged snapshots retain their assigned sequence positions, so repeated updates do not
create silent replay gaps that strand the Gateway directory offline.

Lost submission acknowledgements can be recovered through exact caller-owned queue reads using
the original `clientMessageId`, `agentId`, and `expectedSessionId`. This lookup does not enqueue
or resend input; a missing retained row is not proof that the original attempt had no effect.
Clients can persist an inspection-only journal for prompt and direct redirect recovery across restart.
Queued/started recovery proves daemon admission, not native receipt or completion. Credential or
target changes leave unmatched inspections unresolved rather than transferring ownership.

Session composers should use queued prompts for ordinary Send, not infer strict steer from
displayed activity. Unsupported delivery/model options are rejected rather than discarded,
including on redirected queued prompts.
Daemon and Gateway HTTP callers can opt into exact dispatch with stable `agentId` plus
`expectedSessionId`: prompt, steer, interrupt, compact, and new queue mutations reject a
replaced/absent runtime instead of reviving or retargeting it. Omission preserves
legacy HTTP/IPC/CLI routing. Agent-session WebSocket commands require the captured exact
pair, supplied by the retained browser producer only after `session.bound`; missing or
foreign response bindings are not reported as success. Binding is transport/routing
evidence, not native admission proof. External client integration remains a separate step.
Exact queue reads and reconnects can inspect a retained earlier session without
warming it or substituting the agent's newer session. Queue recovery preserves
original command binding and authenticated caller correlation; it does not certify
PACT transcript paging or native delivery.
Failed queue transitions also refresh that existing snapshot. A lost ACK or later
HTTP error is not safe-retry evidence: clients retain the original input identity
until a validated ACK or terminal fact settles transport pending, without
inventing a fresh send ID.
Codex native activity is ingested before display processing, so a blocked display write
does not hold a subsequently read completed turn busy. Private binding ownership also
prevents a superseded request from entering native submission; an already admitted request
remains potentially accepted after cancellation, not safe to resend automatically.
Ordinary queued prompts and explicit steers retain a durable attempt fence: a timeout after adapter
entry, restart, or unresolved lease expiry cannot automatically replay that command.
Such outcomes remain delivery-uncertain under their original identity, not rejected
or delivered. A deadline that wins before adapter entry prevents later invocation.
Codex steer presentation waits for the matching native input record, not its queue
acknowledgement. A closed turn without that receipt settles unconfirmed and releases
the waiter without resending; recorded input still succeeds. Native history admission
is not itself proof of provider consumption.
Observed ACP prompts wait for their correlated protocol response rather than relay
spawn. This is completion-bound evidence, not an early native acknowledgement;
strict output follows the accepted-input event, while legacy streaming is unchanged.
Headed Claude activity uses ordered native hook records before display processing.
Receipt matching retains the binding owner, native session and hook offset; an old
hook or delayed forwarder cannot settle a newer binding. Terminal writes alone do
not prove acceptance, and direct human input cannot be atomically reserved by Nexus.
Adapters expose a turn observation separately from queue position and presence:
verified idle, native open, unknown, or unavailable. Exact queue snapshots carry
that evidence and actual redirect capability, refreshing subscribed lanes even
without queue transitions. Missing evidence is not idle, and an observation does
not reserve the next native input operation.
See [composer delivery and retry safety](docs/composer-delivery.md).

Check or update the exact npm/Cargo installation that launched the command:

```bash
nexus update --check
nexus update
```

Updates are locked and transactional. Nexus preserves which services were running, verifies the
replacement, and restores the exact previous package version if a health check fails.

## Message agents

```bash
nexus launch --name writer --headless --detach claude
nexus launch --name reviewer --tui --backend tmux codex

nexus thread new release --member writer --member reviewer
nexus post release -m "Review the release boundary."
nexus dm writer -m "Summarize the open risks."
nexus notify --target thread:release "The build finished."
```

An offline daemon-owned agent is revived with its stored harness, headed/headless mode, backend,
working directory, and native resume correlation when the harness exposes one. Names are mutable
aliases; stable agent IDs are the routing identity.

Gateway launch callers can pass headed/headless mode and an optional initial prompt through the same
authenticated Gateway launch endpoint. Update Gateway and clients together for these options;
older Gateways reject them rather than silently changing mode or sending a second prompt.
Native harness and platform launch support still belongs to the daemon.

## Operate

```bash
nexus daemon status
nexus gateway status
nexus webconsole status

nexus gateway logs --follow
nexus webconsole logs --follow

nexus gateway restart
nexus webconsole restart
```

The daemon is the lightweight transport authority. The Gateway is the persistent REST/WebSocket
backend and the only backend used by Webconsole. Webconsole never connects directly to the daemon.

Gateway transport-host shutdown stops new bridge work, waits for owned child processes and
in-flight authority/log operations, and reports cleanup failures before its store is closed.
A start requested during shutdown waits for that shutdown before starting a new host generation.

Live stream storage is anonymous memory by default. If a nonstandard deployment sets
`NEXUS_STREAM_DB_PATH`, only the daemon's transport connection attaches that named stream file;
the identity connection keeps an independent anonymous stream schema so transport transactions
cannot block identity writes through a shared SQLite attachment.

Local presentation services bind to loopback by default. Binding Webconsole to another interface
prints a warning; use a firewall or trusted private network.

## Platforms

Linux and Windows/WSL are the beta.5 release platforms. macOS is deferred and unsupported
in this npm release; no macOS native binaries are bundled.

## Documentation

Canonical runtime reads preserve optional, validated `modelReport` evidence, including native
usage, context and allowance snapshots when supplied. Missing data is unavailable, not zero;
headless Claude, Codex, OpenCode and Hermes report configured models from ACP metadata through
the canonical runtime projection. Headed Codex also reports configured-model evidence from its
captured app-server setup response across fresh launch and cold resume. Configured evidence is not
a response-model claim. Headed Claude reports response models from newly observed, exact-root
assistant transcript records. Model-enabled Claude input waits up to 30 seconds for its captured
SessionStart source before native input handoff; unavailable sources return an error, not a guessed
model. Rejected model attachment does not suppress independent native display/archive forwarding.
Historical/synthetic/child records do not activate reporting. Headed OpenCode reports native
assistant-message turn selection under its captured plugin/ready-handshake owner; it does not
promote requested configuration into provider response evidence. Headed Hermes reads configured
metadata from the exact session selected by its launch-local framework hook. Missing metadata stays
unknown; native root replacement, disconnect or compaction closes the captured reporter. Cold
Gateway resume retains the isolated native profile and requires its prior exact root evidence;
process startup alone does not certify restored context. Final live session-brief verification
remains open. Model evidence does not imply usage or quota. Separate telemetry collectors preserve
headed Codex usage/context/allowance, mode-specific usage/context for all four ACP adapters, and
headed Hermes native session-row usage, headed OpenCode response usage/live-context estimates,
and headed Claude processed-response usage. Headed Claude context/allowance capture remains
separate pending safe statusline composition. Context does not derive from lifetime tokens. New usage
scopes require paired consumer updates; current source candidates are not a live rollout claim.
`scripts/check model-reporting` composes all eight headed/headless collector paths with captured
native fixtures, the Rust publisher and Gateway canonical snapshots in disposable storage. It
requires fresh artifacts for every mode and verifies that delayed older reports cannot restore a
stopped runtime's status. This is not live-provider or visual acceptance. See the
[model/status reporting matrix](docs/model-reporting.md) for exact evidence and remaining limits.

- [Getting started](docs/getting-started.md)
- [CLI reference](docs/cli.md)
- [Distribution and platform matrix](docs/distribution.md)
- [Architecture](docs/architecture.md)
- [REST API](docs/rest-api.md)
- [Adding a harness](docs/adding-a-harness.md)

## License

Apache-2.0. See [LICENSE](LICENSE).
