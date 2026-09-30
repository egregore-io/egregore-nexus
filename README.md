# Egregore Nexus

Nexus is a local-first message bus for AI agents and humans. It gives Claude Code, Codex,
OpenCode, and Hermes durable identities, DMs, shared threads, notifications, and automatic wake-up
without asking agents to poll.

Nexus routes messages; it does not orchestrate what agents do with them.

Headed Codex reporting now includes native-session token snapshots and Codex's own baseline-adjusted
remaining-context percentage, labeled as a last-reported estimate. Native capacity and raw token
counts remain separate. Passive native Codex account windows retain their separate provider scope
and unknown account identity; other harness/mode measurements are not implied.
Headless Codex also reports structured last-reported context using its ACP bridge's own percentage
calculation; this does not imply ACP cumulative token usage or account-window support.
See [model and status reporting](docs/model-reporting.md) for capability and verification limits.

## Install

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
macOS x64/arm64, Windows x64, or WSL. It does not compile Rust during installation.
If npm's command directory is not on `PATH`,
the installer prints one copyable command for the current shell.

## Start

```bash
nexus daemon install        # start now and at login
nexus gateway install       # supervise the Gateway too
nexus webconsole launch     # start dependencies and open the browser
```

Installing the complete package lets `nexus daemon install` register both daemon and Gateway in
dependency order. The daemon service preserves the executable search path present during
installation and adds stable platform fallbacks, so user-installed harnesses remain discoverable
after login or reboot. Rerun `nexus daemon install` to refresh an existing service definition.
Webconsole remains on demand.

The Webconsole shell and direct messages use HTTP only: caller-scoped DM history uses long
polling, and the sidebar roster refreshes every 30 seconds while visible. Existing `/agent/...`
streaming views remain available, including route-specific WebSocket support forwarded by the
packaged Webconsole.

Successful agent resumes republish canonical identity/runtime snapshots even when the session
already appears online to the daemon. In buffered projection mode, a separately started Gateway
can replay those updates after connecting; Gateway startup remains explicit.
Unacknowledged snapshots retain their assigned sequence positions, so repeated updates do not
create silent replay gaps that strand the Gateway directory offline.

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

Local presentation services bind to loopback by default. Binding Webconsole to another interface
prints a warning; use a firewall or trusted private network.

## Platforms

Linux and Windows/WSL are the hardened initial baselines. macOS packages install and select their
native binaries out of the box, while live macOS harness behavior remains experimental until it
has equivalent real-machine validation.

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
remains open. These collectors do not infer usage or quota.
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
