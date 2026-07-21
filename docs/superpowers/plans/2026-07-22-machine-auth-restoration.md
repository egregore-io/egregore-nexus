# Machine Authentication Restoration Execution Plan

**Goal:** Restore machine-owned authentication for every Nexus harness without
weakening session isolation or packed-lab hermeticity.

This is an implementation sequence, not a second release tracker. Canonical
release progress remains in `2026-07-20-v015-release-burndown.md`.

## Sequence

1. Replace Codex's session-home tests with contracts for canonical machine-home
   resolution, rejection of Nexus-owned inherited homes, explicit-home
   preservation, and absence of credential/config copies.
2. Add known-thread tests for canonical-home preference and lazy migration of a
   rollout from a legacy Nexus session home. Pin path containment, no-follow
   source handling, atomic publication, and collision behavior.
3. Change the Codex bridge and supervisor to share one resolver, remove credential
   and config seeding, migrate only rollout data, and persist the effective home.
4. Replace Hermes auth-copy tests with a runtime-profile/global-machine-auth
   contract and remove the copy implementation.
5. Add structural and launch-level contracts proving Claude and OpenCode retain
   the machine provider authority and never gain a credential-copy seam.
6. Preserve provider-home variables in headless launches where ACP environment
   scrubbing would otherwise drop an explicit machine selection.
7. Run focused Codex, Hermes, Claude, OpenCode, and headless gates; then run format,
   workspace, architecture, boundary, and diff checks.
8. Obtain a bounded review of only the auth restoration, rebuild the immutable
   packed lab, and resume Task 5 from the approved release candidate.

## Safety constraints

- Normal Nexus launches never read, print, hash, copy, link, or rewrite credential
  contents; the existing explicit packed-lab provisioning boundary remains unchanged.
- Preserve all unrelated working-tree files.
- Do not touch the running host daemon or Gateway during code verification.
- Keep Webconsole deferred from the v0.1.5 ship gate.
- Update the existing release tracker only after the corresponding packed gate
  actually passes.
