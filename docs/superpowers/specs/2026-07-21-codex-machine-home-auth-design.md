# Machine-Owned Harness Authentication Design

**Date:** 2026-07-22

## Problem

Nexus has gradually mixed two different concerns:

- session-scoped runtime state, which must be isolated; and
- provider authentication, which must remain owned by the machine installation.

Codex and Hermes crossed that boundary by copying `auth.json` into Nexus-owned
session/profile directories. Every copy then became an independent OAuth writer.
A login or refresh in the machine home did not update existing copies, and refresh
rotation could invalidate another session's copy. This is why Paul and Bob could
both ask for login even after Codex had already been authenticated on the host.

The machine already has one working authentication authority for each installed
harness. Nexus must launch the harness against that authority directly. It must
not copy, synchronize, hard-link, or symlink credential files.

## Reference behavior

AionUi's runtime builds an agent environment from the machine environment and
launches each agent with that environment plus runtime-only overrides. It does
not manufacture a private credential store for each conversation. Nexus adopts
the same ownership model while keeping its stronger per-session process and bus
isolation.

## Provider-wide boundary

Normal host launches use the provider authority already selected by the machine:

- Codex: an explicit external `CODEX_HOME`, otherwise machine `CODEX_HOME`,
  otherwise `HOME/.codex`;
- Claude: `CLAUDE_CONFIG_DIR` when set, otherwise Claude's normal machine-home
  resolution;
- Hermes: machine `HERMES_HOME` or `HOME/.hermes` remains the credential
  authority even when Nexus generates a runtime-only gateway plugin profile;
- OpenCode: machine `HOME`, provider environment variables, and OpenCode's normal
  auth resolution remain authoritative; `NEXUS_OPENCODE_HOME` is runtime data,
  not an authentication home.

Provider-home values beneath a Nexus-managed session root are legacy runtime
homes, not machine authority. In particular, a daemon launched from a Nexus
Codex session may inherit that session's `CODEX_HOME`; Nexus ignores it and
falls back to the daemon's machine `HOME/.codex`. This prevents one agent's
credential copy from becoming the source for every later agent.

An explicit provider home supplied as a launch option remains authoritative and
untouched. This preserves operator-selected external profiles and hermetic tests.

## Runtime isolation

Nexus continues to isolate everything it owns per session: socket or loopback
endpoint, PID sidecar, stderr log, bridge token, plugin code, temporary files,
Nexus identity, client key, working directory, native thread/session id, and
runtime handles.

Nexus runtime configuration is supplied through command arguments or environment
overrides. It is never persisted into the machine's provider config merely to
support one Nexus session. Sharing provider authentication does not merge Nexus
bus identities or harness conversations.

## Codex home resolution and migration

For a default Codex launch, the bridge and supervisor resolve the same canonical
machine home. They create the directory if required, but never seed `auth.json`
or `config.toml`. All Nexus sessions therefore observe the same Codex login and
refresh state immediately.

Known-thread lookup checks the canonical home first. If a requested thread exists
only under a legacy Nexus-owned `<codex-sessions>/<session>/codex-home`, Nexus
migrates only the matching rollout file into the same relative location beneath
the canonical home's `sessions` tree. The migration uses a temporary regular
file, file sync, atomic rename, and parent-directory sync. It refuses path escape,
symlinked or non-regular sources, and conflicting destination bytes. Credentials
and configuration are never migrated.

Already-running processes retain their launch environment until a normal managed
restart. New launches and revived sessions use the canonical home.

## Hermes gateway profile

The Hermes gateway still needs a Nexus-generated profile containing its platform
plugin and runtime bridge configuration. That profile no longer receives an
`auth.json` copy. Hermes resolves authentication from the machine-owned global
home through its supported global-auth fallback, while Nexus owns only the
generated plugin/profile files.

## Claude and OpenCode

Claude and OpenCode already principally use direct machine authentication. Their
launch contracts are pinned so future isolation work cannot silently redirect or
copy credentials. Headless launches explicitly preserve machine provider-home
selection where the ACP layer otherwise scrubs environment variables.

## Packed labs

Packed release labs remain hermetic. Their daemon receives a lab-owned `HOME` and,
where required, lab-owned provider-home variables. The lab builder may read the
operator's active provider homes through explicit read-only provisioning mounts and
copy the minimum provider state into those isolated lab-owned homes. The running
candidate resolves only the copies beneath the lab root; it never selects or writes
the host paths directly.

## Failure behavior

- Missing machine-home authority fails before process spawn; Nexus never uses a
  relative provider directory.
- Provider-home creation or access failure aborts before spawn.
- Legacy rollout migration failure leaves the source intact and aborts before
  app-server spawn.
- No credential contents or credential hashes are logged.
- Explicit external profiles remain untouched.
- A packed lab cannot fall back through to the host's home.

## Verification contract

Acceptance requires proof that:

- two Codex sessions use one canonical machine home but distinct endpoints and
  Nexus identities;
- an inherited Nexus-session `CODEX_HOME` is rejected as machine authority;
- no harness creates or copies a session-local credential file;
- host credential changes are immediately visible to later launches;
- explicit external homes remain authoritative;
- legacy Codex rollouts migrate without moving credentials or config;
- Hermes uses a runtime plugin profile without a copied auth store;
- Claude and OpenCode retain direct machine auth selection;
- packed labs remain isolated from host homes; and
- focused harness tests, the Rust workspace gates, formatting, and repository
  policy checks pass before the v0.1.5 packed candidate is rebuilt.

## Out of scope

- provider OAuth UI or refresh implementation;
- a Nexus credential broker, keyring, or synchronization service;
- changing the packed lab's existing read-only credential provisioning boundary;
- changing Nexus bus identity, routing, or session ownership.
