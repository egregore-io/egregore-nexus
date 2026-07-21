# Codex Machine-Home Authentication Design

**Date:** 2026-07-21

## Problem

Nexus currently starts each normal Codex app-server with a private
`<session>/codex-home` directory. On first launch it copies the operator's
`auth.json` into that directory and never refreshes the copy.

That makes every Nexus session an independent OAuth credential writer. A login
or token refresh in the machine's real Codex home does not update the copied
files, and refresh-token rotation can leave another copy unusable. The visible
symptom is repeated Codex login prompts for otherwise healthy Nexus agents such
as Paul and Bob even though the operator already authenticated Codex on the
machine.

Nexus must not copy, synchronize, hard-link, or symlink OAuth credentials.
Normal host-managed Codex sessions must use the machine's canonical Codex home
directly.

## Chosen architecture

For a normal launch where `SupervisorOpts.codex_home` is absent, the supervisor
resolves one canonical machine Codex home:

1. the daemon's nonempty `CODEX_HOME`, when set; otherwise
2. the daemon's nonempty `HOME` joined with `.codex`.

The app-server receives that directory unchanged as `CODEX_HOME`. Nexus no
longer creates a per-session `codex-home`, copies `auth.json`, or copies
`config.toml` for normal launches. Codex therefore reads and refreshes exactly
the same credential and configuration files as the operator's machine Codex
installation.

Nexus isolation remains per session for its Unix socket or loopback endpoint,
PID sidecar, stderr log, Nexus identity, client key, working directory, and
runtime handles. Sharing Codex's own home does not merge Nexus sessions or bus
identities. Runtime-only Nexus configuration continues to be supplied through
`-c` arguments and sanitized subprocess environment variables, so Nexus does
not persist its identity or MCP overrides in machine `config.toml`.

An explicit `SupervisorOpts.codex_home` remains authoritative and is never
rewritten. This preserves hermetic tests and intentional external-profile
resumes. A packed lab remains isolated because its daemon receives a lab-owned
`HOME` or `CODEX_HOME`; its canonical machine home is therefore inside the
lab, not the host operator's home.

## Existing session migration

New sessions immediately use the canonical machine home. A known-thread resume
first looks for that thread in the canonical home. If the thread exists only in
a legacy Nexus-owned `<codex-sessions>/<session>/codex-home`, Nexus copies only
the matching rollout file into the same relative location under the canonical
home using a temporary file and atomic rename. It never copies credentials or
configuration. The legacy rollout remains intact until the canonical copy is
durable, so a migration failure is non-destructive and aborts before app-server
spawn.

Only homes beneath the same Nexus codex-session root qualify for automatic
migration. An arbitrary explicit external Codex home is treated as an
intentional separate profile and remains authoritative.

Already-running app-server processes keep the home they were launched with.
They pick up this change on the normal managed restart boundary; Nexus does not
rewrite a live process's environment.

## Data flow

1. A daemon launch request creates a per-session runtime directory.
2. The bridge resolves a requested thread, preferring the canonical machine
   home and migrating a legacy Nexus-owned rollout when required.
3. The supervisor resolves the effective Codex home and validates that the
   path can be created or opened.
4. The supervisor starts `codex app-server` with the effective `CODEX_HOME`,
   a session-specific endpoint, and Nexus runtime overrides.
5. Runtime storage records the effective canonical home so revive and
   transcript discovery stay deterministic.
6. Codex alone reads, writes, and refreshes `auth.json` in that home.

## Failure behavior

- Missing or empty `CODEX_HOME` and `HOME` fails launch with a typed connection
  error; Nexus never falls back to a relative `.codex` directory.
- A machine-home creation or access failure aborts before process spawn.
- Legacy rollout migration refuses path escape, symlinked source files,
  non-regular files, and destination collisions with different bytes.
- Migration write, sync, or rename failure leaves the legacy source untouched
  and fails before process spawn.
- Nexus never logs credential contents or hashes.
- Explicit external homes remain untouched, including their authentication,
  configuration, and rollout files.

## Compatibility

The public command surface is unchanged. `SupervisorOpts.codex_home: Some(...)`
keeps its existing explicit-home meaning. Only the default `None` behavior
changes from a private copied home to the machine-owned home.

Codex rollout files from legacy Nexus-owned homes are migrated lazily on the
first known-thread resume. Fresh sessions require no migration. Packed labs
continue using their own isolated Codex home through their lab-owned daemon
environment.

## Verification

Tests must prove all of the following before the change is accepted:

- a default launch uses `CODEX_HOME` when set and otherwise `HOME/.codex`;
- two Nexus sessions use the same machine home but different endpoints and
  runtime identities;
- no per-session `auth.json` or `config.toml` copy is created;
- changing machine `auth.json` is immediately visible to later launches
  without any Nexus copy or synchronization step;
- an explicit external home is still used unchanged;
- missing home authority fails before spawn;
- runtime state persists the effective machine home;
- known-thread lookup prefers the machine home;
- a rollout in a legacy Nexus-owned home migrates atomically and resumes from
  the machine home;
- arbitrary external homes are not migrated;
- the full Codex harness tests, Nexus Codex launch tests, formatting, and
  repository policy gates remain green.

## Out of scope

- OAuth login UI or token refresh logic inside Codex;
- a Nexus credential broker, keyring, or token synchronization service;
- sharing host authentication into an isolated packed lab;
- changing Claude or other harness authentication;
- changing Nexus bus identity, routing, or session ownership.
