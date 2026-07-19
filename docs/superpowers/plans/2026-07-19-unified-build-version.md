# Unified Build Version Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `VERSION` the canonical Nexus distribution version and prevent Cargo or npm builds from emitting artifacts with a different version.

**Architecture:** An extensionless Node script performs side-effect-free drift checks and explicit atomic synchronization across Cargo, npm, and their lockfiles. Native `build.rs`, npm build/pack commands, and the release identity gate independently invoke the check so every artifact boundary fails closed.

**Tech Stack:** Node.js 20+ standard library, Cargo/Rust build scripts, npm/pnpm manifests, Bash release gates.

---

### Task 1: Canonical version synchronizer

**Files:**
- Create: `VERSION`
- Create: `scripts/nexus-version`
- Create: `scripts/test-nexus-version`
- Modify: `core/Cargo.toml`
- Modify: `core/Cargo.lock`
- Modify: `gateway/package.json`
- Modify: `gateway/gateway/package.json`
- Modify: `gateway/webconsole/package.json`
- Modify: `gateway/pnpm-lock.yaml`
- Modify: `packages/nexus-cli/package.json`
- Modify: `packages/nexus/package.json`

- [ ] **Step 1: Write the failing fixture test**

Create a test that copies the required version files to a temporary root, writes `VERSION=0.1.5`,
and proves `check` detects the existing Cargo/npm drift without modifying fixture hashes. It must
then prove `sync` updates workspace versions, exact internal pins, Cargo.lock workspace entries,
pnpm's internal specifier, and all five npm manifests; a second `sync` must make no changes.

Run: `scripts/test-nexus-version`

Expected: FAIL because `scripts/nexus-version` does not exist.

- [ ] **Step 2: Implement the synchronizer**

Implement:

```text
scripts/nexus-version check
scripts/nexus-version sync
```

The command reads a stable SemVer from root `VERSION`. `check` accumulates all drift and exits 1.
Malformed input or missing/ambiguous files exits 2. `sync` preflights every transformation, writes
temporary sibling files, atomically renames them, and rolls back originals if a rename fails.
`NEXUS_VERSION_ROOT` is accepted only as a fixture root override.

- [ ] **Step 3: Verify RED becomes GREEN and synchronize v0.1.5**

Run:

```bash
scripts/test-nexus-version
scripts/nexus-version sync
scripts/nexus-version check
scripts/nexus-version sync
scripts/nexus-version check
```

Expected: tests PASS; both checks PASS; the fixture test proves the second sync has byte-identical
output to the first.

### Task 2: Fail-closed build integration

**Files:**
- Modify: `core/crates/nexus/build.rs`
- Modify: `core/crates/nexus/tests/cli_release_surface.rs`
- Modify: `gateway/package.json`
- Modify: `gateway/webconsole/package.json`
- Modify: `packages/nexus-cli/package.json`
- Modify: `packages/nexus/package.json`
- Modify: `.github/workflows/npm-native-artifacts.yml`
- Test: `scripts/test-nexus-version`

- [ ] **Step 1: Add failing build-boundary assertions**

Extend the script fixture test to assert that the native build script, Gateway build scripts, all
three public package prepack surfaces, and the native artifact workflow reference
`nexus-version check`.

Run: `scripts/test-nexus-version`

Expected: FAIL because build boundaries are not wired.

- [ ] **Step 2: Wire native and npm builds**

Extend `build.rs` to read `../../../VERSION` when building from a checkout and compare it with
`CARGO_PKG_VERSION`; emit `cargo:rerun-if-changed` and panic on mismatch. Packaged crates without
the repository root keep working from their Cargo metadata.

Prepend the checker to Gateway/Webconsole builds, add checker-backed `prepack` scripts to CLI and
umbrella npm packages, and run the checker before native workflow compilation.

- [ ] **Step 3: Verify the boundary test**

Run: `scripts/test-nexus-version`

Expected: PASS.

### Task 3: Derive release tooling and documentation from `VERSION`

**Files:**
- Modify: `scripts/nexus-v010-release-identity-gate`
- Modify: `scripts/nexus-v010-release-evidence`
- Modify: `scripts/nexus-pack-native-npm`
- Modify: `scripts/nexus-three-facet-package-smoke`
- Modify: `scripts/test-nexus-npm-launcher`
- Modify: `scripts/test-nexus-npm-publication-contract`
- Modify: `scripts/test-nexus-v01-release-validation`
- Modify: `docs/cli.md`
- Modify: `docs/distribution.md`
- Modify: `docs/getting-started.md`
- Modify: `docs/release-regression.md`

- [ ] **Step 1: Add failing release-gate assertions**

Extend `scripts/test-nexus-version` to require the release identity gate and native packer to read
the root version, and to reject separate Cargo/npm release constants.

Run: `scripts/test-nexus-version`

Expected: FAIL on the current hard-coded `0.1.0`/`0.1.5` split.

- [ ] **Step 2: Replace distribution-version constants**

Read `VERSION` in release scripts, use it for package expectations and native version matching,
and update current user documentation to `0.1.5`. Leave protocol fixture versions that are not
distribution versions unchanged.

- [ ] **Step 3: Verify focused release surfaces**

Run:

```bash
scripts/test-nexus-version
scripts/check release-identity
scripts/test-nexus-npm-launcher
scripts/test-nexus-npm-publication-contract
scripts/test-nexus-release-entrypoint-help
```

Expected: every command PASS.

### Task 4: Capped candidate verification

**Files:**
- Verify only; do not modify the operator runtime.

- [ ] **Step 1: Run formatting and static gates**

Run in the sole capped Docker validator:

```bash
scripts/nexus-version check
scripts/check core-test-layout
scripts/check release-identity
scripts/check architecture
scripts/check boundaries
pnpm --dir gateway typecheck
pnpm --dir gateway test
pnpm --dir gateway build
```

Expected: all green.

- [ ] **Step 2: Build the exact native candidate**

Run with `CARGO_BUILD_JOBS=2` and `NEXUS_BUILD_REVISION=$(git rev-parse HEAD)`:

```bash
cargo build --manifest-path core/Cargo.toml -p egregore-nexus --bin nexus --release
/tmp/nexus-target/release/nexus --version
```

Expected: `nexus 0.1.5 (revision <exact HEAD>)`; the binary is stripped.

- [ ] **Step 3: Commit the implementation**

Stage only files owned by this plan. Commit with an imperative subject and no attribution trailer:

```bash
git commit -m "fix(release): keep build versions synchronized"
```
