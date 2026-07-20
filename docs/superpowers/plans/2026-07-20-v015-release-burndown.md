# Nexus v0.1.5 Release Burn-Down Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce one immutable packed Nexus v0.1.5 candidate whose complete required evidence validates and can receive a final ship/no-ship decision.

**Architecture:** Preserve the daemon as transport/identity authority and Gateway as browser REST/WebSocket authority. The packed candidate, readiness decision, and source ledger contain Nexus only. The existing Lens working tree remains the real CDP ocular client for Nexus WebSockets and user-facing chat surfaces; it may be dirty or uncommitted because it is validation tooling, not a Nexus release source, and its exact ocular artifacts bind what actually ran. Every visual chat action is driven through `lens.nexus`, never Egregore chat. Egregore or Pactree packages may remain present as Lens implementation dependencies, but their repositories, revisions, cleanliness, and product behavior are outside the Nexus release gate. Freeze the already-reviewed Nexus source changes first, then build exactly once and run every required Nexus gate against that same packed candidate; any Nexus source or packed-artifact mutation after the build invalidates the run and returns to the candidate-build task.

**Tech Stack:** Rust/Cargo, TypeScript/Node/pnpm/Vitest, Python/pytest, Electron/CDP, Docker, SQLite/libsql, Bash, npm packed artifacts.

---

### Task 1: Freeze the implementation and review ledger

**Files:**
- Modify: `docs/superpowers/plans/2026-07-20-v015-release-burndown.md`
- Inspect: `docs/v0.1.5-acceptance.md`
- Inspect: `/home/earldennison/Projects/egregore-lens/tests/ocular/live-plugin.mjs`
- Inspect: `/home/earldennison/Projects/egregore-lens/tests/ocular/live-drivers.mjs`
- Inspect: `/home/earldennison/Projects/egregore-lens/tests/ocular/aggregate.mjs`

- [x] **Step 1: Record the exact approved hashes for B1, C2/C3, D2, F2, credential r5, SQLite initialization, and Lens Stop/reconnect.**

  Copy the final reviewer-approved SHA-256 lines into the execution notes beneath this task; do not replace them with branch names or commit IDs.

  Approved SHA-256 ledger:

  ```text
  B1 final prepared-send closure
  9f810a46179210106f539c4da324e90c5abeb8a4ee33b98b8be91c1acbdeefc8  core/crates/nexus-contracts/src/ports.rs
  20477667ee16e22a0c86dbc0ecd8bd319fbfa6eca9d163c9d634bbde34d0b31c  core/crates/nexus-bus/src/service.rs
  97422c8926fab7ab3db723ac34ac9c07c8759b75d70031d196327e07cf3c272f  core/crates/nexus-bus/tests/prepared_send.rs
  9e3d9920587dcd6d6ad88f0b8fd5698ecc2b84ab02b96556ba9e8e826f300129  core/crates/nexus-notify/src/service.rs
  37894cef4e03101cc25a65519464cdc4e3eebce2f466a19ecafe2ff88b2989df  core/crates/nexus-notify/tests/idempotency.rs
  f8f9f3a3cdc264460599a47f399b923ec1f3a2541117e18be5733c5ac0fdb2fb  core/crates/nexus/src/daemon/command_worker.rs

  C2/C3 final backpressure/client recovery closure
  6f2044511180cc6e3b043c94d326c34ff9afdae7643316f5c23e4e865d8c3046  gateway/src/server/agui/ws.mjs
  6e178c02ff4eba209cce0c988719fd8b4c978f2d45d2d9291462775bfa77993c  gateway/src/server/agui/ws.d.mts
  95ea949056fbcd1933b566d7ff4266272a54e811404e533042939060f100e9be  gateway/src/server/agui/ws.test.ts
  1f693767b6ec772a4b0fa5a053797336f84ad03d6058f5abf5d045323e31be7e  gateway/src/server/agui/developerEventWsBackpressure.test.ts
  fc762ff60b3699c13264ac2fe1fb9839f290bd58d173681bdf6b79f5c4de3cfe  gateway/src/server/agui/sessionWsBackpressure.test.ts
  e978b01581eba1a18565c5cffb407df08355e1d87ea963c8ed9b18588308c9c9  gateway/src/server/stream/sessionEvents.ts
  549500ac145e1f6fc415e15651937c2c5a216d71d9910fd3851b4cf092fb9289  gateway/src/server/stream/sessionEvents.test.ts
  e54cd730fed6aa864a78a9c545917e61bd61492df8a38465ce80989df00e29f4  gateway/src/server/agui/daemonPushRelay.mjs
  44b678eba82d871953d5d2bec0fbf911fca5d9e1fdfc94d8bf2836da8b2a3b69  gateway/src/server/agui/daemonPushRelay.d.mts
  5a4638c336e516570370816da0b13bf475271379ca8a4b1c640525d32c24f1a1  gateway/src/server/agui/daemonPushRelay.test.ts
  46a679dccac4b96075e16eb2eeae1a61a926885d8a6a12f2b3d113739aacc0ba  gateway/src/modules/pane/aguiConversation.ts
  793c1a6a940a154a67c66096c1774b27a06823bdceee0b7e06ef363f4c9013b2  gateway/src/modules/pane/aguiConversation.test.ts
  29f5612f0c4edbc9315f7c316e33cab7bd132418e2889315abd36771e2fbc57e  gateway/src/modules/pane/aguiWebSocketParity.test.tsx

  D2 final packed Webconsole
  1330d35fd4b73a771c369f15dcfd5bb49236658dd1779dee6e85c5e04f64b6e9  scripts/nexus-v015-webconsole-acceptance
  83bbf43a9ae79ba729b43250e17b8ca2a09cdcf362c641951c13530786367437  scripts/test-nexus-v015-webconsole-acceptance

  F2 final packed hooks
  fdead91fcbf4d8325a318dc5ba441c386ae6523e035d2cbf2f7267046e323fff  scripts/nexus-hooks-practical-gate
  2b846d8276ae3ef3115a7091ea584e4d282a48c5e7d3722ccdfc8b97e1cf38c8  scripts/test-nexus-hooks-practical-gate
  9986d1e3ccffca64cff187a07a45912d03584bb121d88e00d10cde40a827cbe7  scripts/nexus-docker-test-env
  09b0ec3ce0cd96ac8d6768785662980dcb7cdc40013991023abae6ab7aefb668  scripts/test-nexus-docker-test-env

  Credential r5
  902e37d0fe1733a61f4c0e8a1684b57f5de381e063533b3c95e1743c8ac1e4c2  scripts/nexus-v015-lab-credential
  524b194ac5e8b1e7f6d08e4cb649bc500315da4d59a1ced97594d5ea19da4ff3  scripts/test-nexus-v015-lab-credential

  SQLite initialization
  d7ecc7ea1d4579d85513a1c0dfb1f0cec5957892e338cf0a9eaff6585bd9d968  gateway/src/server/store/client.ts
  45555328fc4aa81c05e7abb6075e193c54b091f3b1e0e4fc0c03926751d26335  gateway/src/server/store/client.test.ts
  09bea35f4ff2639029d5c98a4d9b820cb6637049121eb5dd61302a154f2c5d7a  gateway/src/server/gateway/packedWsSmoke.test.ts

  Lens Stop/reconnect final integrated bytes
  b0e1fe1201f38feedecbb667a32c564c280b3181e4bf367d7fda3942c237dda8  lens/plugins/lens.nexus/lens_nexus/backend.py
  046ccadb1b26080000000e1482774e4d24a746115b65c6435938205b83b69d22  lens/plugins/lens.nexus/lens_nexus/observe.py
  7449222f3c58fa0a7fcda93c4deb96a6b41ca0aff8517a2668a5cf0b4364dd9e  lens/plugins/lens.nexus/lens_nexus/schemas.py
  7f6955dd38235d568a81c4c19b8fc0bea501fab143de2de1dadc6d0686f3a9ca  lens/plugins/lens.nexus/lens_nexus/test_backend_routes.py
  66e57536cdfd8d738e1c15103d983f38e5c63fb926f38ea5cbb171aa63dfcaa7  lens/plugins/lens.nexus/lens_nexus/test_observe_stream.py
  44571ace76ba1d3e18ba60a824d65a7635c6a4298f20c522e20107bc0c40e275  lens/plugins/lens.nexus/ui/chatContract.ts
  9bf6da915bd715f3a88e2d78f9c3d673a7c172c01c86bf7565f10ee1c76bc853  lens/plugins/lens.nexus/ui/runtime.ts
  bd500291bd74a91bafa6a399639ea1fd47eae52429a28440d5b8ac278d9dff27  lens/plugins/lens.nexus/ui/components/Conversation.tsx
  b122e6466d0a9c9b63a853428a277813eb6b8ae485b3c18ff8c7fc7c82e9ddb0  lens/plugins/lens.nexus/ui/index.test.tsx
  7f190775cff2d9f8ea004bb3222c9f6b46a42fde691dd33632378bde41b6ce7f  lens/src/plugin/chat-plane.ts
  1b510c05fbf11103ea41bea8ba6f0324d5aefc23e792f50a0b3c0b143c3d38b9  lens/src/plugin/chat-plane.test.ts
  ```

- [x] **Step 2: Obtain binary reviewer verdicts for the complete LP1–LP16 driver and the release assembler/validator boundary.**

  Expected: `APPROVED` or a demonstrated blocker with an executable regression. A partial green is not a verdict.

- [x] **Step 3: Close the import-safe help regression.**

  Run:

  ```bash
  cd /home/earldennison/Projects/egregore-lens
  node tests/ocular/live-plugin.mjs --help
  node tests/ocular/aggregate.mjs --help
  ```

  Expected: each command prints its own usage, exits zero, and writes no evidence.

- [x] **Step 4: Mark Task 1 complete only when no implementation or review lane is still moving.**

### Task 2: Run complete local verification before committing

**Files:**
- Test: `core/crates/**/tests/*.rs`
- Test: `gateway/src/**/*.test.ts`
- Test: `/home/earldennison/Projects/egregore-lens/plugins/lens.nexus/**`
- Test: `/home/earldennison/Projects/egregore-lens/tests/ocular/**`

- [x] **Step 1: Run Nexus contract generation and architecture checks.**

  ```bash
  cd /home/earldennison/Projects/egregore-nexus
  pnpm --dir gateway gen:contracts
  pnpm --dir gateway check:contracts
  scripts/check core-test-layout
  scripts/check release-identity
  scripts/check architecture
  scripts/check boundaries
  ```

  Expected: every command exits zero; contract generation leaves no new diff.

- [x] **Step 2: Run Rust and Gateway gates.**

  ```bash
  cd /home/earldennison/Projects/egregore-nexus
  cargo test --manifest-path core/Cargo.toml --workspace
  pnpm --dir gateway typecheck
  pnpm --dir gateway test
  pnpm --dir gateway build
  ```

  Expected: zero failed tests and zero type/build errors.

- [x] **Step 3: Run packaging and release-script contracts.**

  ```bash
  cd /home/earldennison/Projects/egregore-nexus
  bash scripts/test-nexus-pack-native-npm
  bash scripts/test-nexus-docker-test-env
  bash scripts/test-nexus-v015-webconsole-acceptance
  python3 scripts/test-nexus-v015-lab-credential
  ```

  Expected: each external contract reports its explicit `ok`/PASS result.


- [x] **Step 4: Run Lens backend, UI, chat-plane, and ocular gates.**

  ```bash
  cd /home/earldennison/Projects/egregore-lens
  .venv/bin/python -m pytest plugins/lens.nexus/lens_nexus -q
  npx vitest run plugins/lens.nexus src/plugin/chat-plane.test.ts tests/ocular
  npx tsc --noEmit
  ```

  Expected: Python 238 or more passing, plugin UI 271 or more passing, ocular 177 or more passing, and no TypeScript errors.

### Task 3: Produce coherent source commits

**Files:**
- Modify: only files named by the approved release ledger in both repositories
- Exclude: `.cdp*.mjs`, `tests/ocular/evidence/`, build caches, screenshots, `*.tsbuildinfo`, `.nexus/`, `.codex/`, and unrelated user work

- [x] **Step 1: Generate scoped file lists and compare them with approved hashes.**

  ```bash
  cd /home/earldennison/Projects/egregore-nexus
  git diff --name-only
  git ls-files --others --exclude-standard
  cd /home/earldennison/Projects/egregore-lens
  git diff --name-only
  git ls-files --others --exclude-standard
  ```

  Expected: every staged release file has an approved owner/scope; unrelated files remain unstaged.

- [x] **Step 2: Commit each coherent concern with imperative subjects and no attribution trailers.**

  Required concerns: identity/delivery, interrupt receipts, Gateway SQLite startup, packed acceptance, Lens auth/chat/Stop, and ocular producers/drivers.

- [x] **Step 3: Verify committed trees.**

  ```bash
  git show --check --oneline HEAD
  git status --short
  ```

  Expected: `git show --check` is clean. Remaining status entries are documented unrelated user/scratch work and cannot enter the candidate snapshot.

### Task 4: Build one immutable packed candidate

**Files:**
- Execute: `scripts/nexus-docker-test-env`
- Produce: Nexus candidate tarballs, native binary, readiness receipt, manifest, and container image under the isolated lab

- [x] **Step 1: Remove only the previous disposable validator.**

  ```bash
  cd /home/earldennison/Projects/egregore-nexus
  scripts/nexus-docker-test-env lab-down --mode packed
  ```

  Expected: the disposable validator is absent; host isolation checks remain unchanged.

- [ ] **Step 2: Build and create the packed lab.**

  ```bash
  scripts/nexus-docker-test-env build
  scripts/nexus-docker-test-env lab-up --mode packed
  scripts/nexus-docker-test-env lab-readiness --mode packed
  scripts/nexus-docker-test-env lab-status --mode packed
  ```

  Expected: one 6-CPU/8-GiB/1,024-PID validator, candidate-bound readiness, healthy daemon/Gateway/Lens ocular client, and no host runtime mutation. The Nexus source ledger is independent of the Lens/Egregore/Pactree working-tree state; any Egregore/Pactree packages present support the Lens shell only and no Egregore chat/product gate runs.

- [ ] **Step 3: Freeze the candidate identity.**

  ```bash
  scripts/nexus-docker-test-env lab-evidence --mode packed
  ```

  Expected: one run ID, readiness SHA-256, candidate digest, exact native hash, three npm tarball hashes, image digest, a Nexus-only source ledger, and the toolchain ledger.

### Task 5: Run all packed functional gates

**Files:**
- Execute: packed CLI/Gateway/Webconsole/WebUI/hook producers
- Execute: the existing Lens LP1–LP16 ocular validator against the ready candidate
- Produce: canonical Nexus evidence, W1–W8 WebUI evidence, and candidate-bound Lens ocular evidence without grading Lens/Egregore/Pactree source revisions

- [ ] **Step 1: Run the complete packed gate without soak.**

  ```bash
  cd /home/earldennison/Projects/egregore-nexus
  scripts/nexus-docker-test-env lab-gate --mode packed
  ```

  Expected: backend, D2, F2, W1–W8, Lens LP1–LP16, and pre-soak sealing producers all report PASS for Nexus behavior. The gate records the actual Lens ocular bytes that ran but does not require clean/committed Lens, Egregore, or Pactree repositories and does not run their separate product suites.

- [ ] **Step 2: Verify every chat surface explicitly.**

  Required evidence: human thread creation/post/reply, agent thread reply, two-way DM, session prompt/progressive output, queued send/drain, steer, Stop/interrupt receipt, compact, reload, Gateway restart, daemon restart, auth failure, offline state, and queue-authority degradation/recovery.

- [ ] **Step 3: Verify both real harnesses.**

  Expected: Codex and Claude each produce a correlated progressive response and terminal event. Provider authentication/credits failure is recorded as external unavailability and cannot be counted as PASS.

### Task 6: Run endurance and Windows gates

**Files:**
- Execute: `scripts/nexus-v015-ws-soak`
- Execute: `scripts/test-nexus-windows-validation`

- [ ] **Step 1: Run the mandatory 30-minute packed soak.**

  ```bash
  cd /home/earldennison/Projects/egregore-nexus
  scripts/nexus-docker-test-env lab-gate --mode packed --soak 1800
  ```

  Expected: 30 minutes of correlated thread/DM/session traffic with repeated independent daemon/Gateway restarts, zero lost/duplicate accepted messages, zero dead letters, bounded resources, and final healthy topology.

- [ ] **Step 2: Run Windows package/launcher validation.**

  ```bash
  scripts/test-nexus-windows-validation
  ```

  Expected: `.cmd` and `.bat` spaced-prefix execution evidence is PASS. If a real Windows VM is required by the gate, retain its candidate-bound result under the same run evidence root before proceeding.

### Task 7: Assemble, validate, and seal evidence

**Files:**
- Execute: `scripts/nexus-v015-release-assemble`
- Execute: `scripts/nexus-v015-evidence-validate`
- Produce: final detached evidence seal

- [ ] **Step 1: Assemble candidate-bound aggregate evidence.**

  ```bash
  scripts/nexus-v015-release-assemble --run-root "$NEXUS_LAB_ROOT/evidence/$NEXUS_V015_RUN_ID"
  ```

  Expected: canonical A1–F3 plus DIFF/REVIEW results, with no missing/extra result or artifact.

- [ ] **Step 2: Validate without mutation.**

  ```bash
  scripts/nexus-v015-evidence-validate "$NEXUS_LAB_ROOT/evidence/$NEXUS_V015_RUN_ID"
  ```

  Expected: PASS for exact run/readiness/candidate identity, ownership, modes, link counts, inventories, semantic receipts, ocular cases, and soak.

- [ ] **Step 3: Publish the final evidence seal.**

  ```bash
  scripts/nexus-v015-evidence-validate --seal "$NEXUS_LAB_ROOT/evidence/$NEXUS_V015_RUN_ID"
  ```

  Expected: a verifier-valid, immutable final seal published last.

### Task 8: Issue the release decision

**Files:**
- Inspect: `docs/v0.1.5-acceptance.md`
- Update: `CHANGELOG.md` only after every required gate is PASS

- [ ] **Step 1: Reconcile every A1–G2 requirement against one sealed run.**

  Expected: no skipped, refused, flaky, partial, stale, or prior-candidate evidence.

- [ ] **Step 2: Produce the final verdict.**

  `SHIP` requires the sealed run, clean intended source snapshot, and zero unresolved review blocker. Otherwise report `NO-SHIP` with the first causal failing gate and return to the corresponding task.

- [ ] **Step 3: Stop before pushing tags or publishing packages.**

  Tagging, pushing, npm publication, and release creation require the operator's explicit final authorization after the `SHIP` verdict.
