# Unified build version design

## Goal

Nexus has one distribution version across Cargo, npm, the native CLI, the Gateway, and the
Webconsole. A build must fail before producing an artifact when any version-bearing manifest has
drifted from that version.

The immediate canonical version is `0.1.5`. Consequently, a native binary built from this tree
must report `nexus 0.1.5 (revision <revision>)`.

## Canonical source

The repository-root `VERSION` file is the only version source. It contains one stable SemVer value
without a `v` prefix or surrounding whitespace, followed by a newline.

An ecosystem manifest is deliberately not canonical. Making Cargo canonical would subordinate npm
releases to Rust packaging, while making an npm manifest canonical would subordinate Cargo and
native builds to JavaScript packaging. A neutral root file keeps the contract symmetric.

## Command surface

`scripts/nexus-version` exposes two commands:

- `scripts/nexus-version check` reads `VERSION`, verifies its SemVer shape, and exits unsuccessfully
  with an exact list of drifted files or fields. It is side-effect free.
- `scripts/nexus-version sync` reads `VERSION` and updates every supported version-bearing field.
  It is idempotent and does not build, publish, tag, or modify runtime state.

Release operators change `VERSION` once, run `sync`, review the resulting diff, and commit the
version bump. Normal builds run `check`; they never rewrite the worktree.

The script synchronizes:

- `core/Cargo.toml` workspace version and exact internal workspace dependency requirements;
- workspace-package entries in `core/Cargo.lock`;
- the public npm manifests for `@egregore/nexus`, `@egregore/nexus-cli`, and
  `@egregore/nexus-gateway`;
- the private Gateway and Webconsole workspace manifests;
- exact internal npm dependency requirements between those packages;
- the matching internal dependency specifier in `gateway/pnpm-lock.yaml`.

It preserves unrelated manifest content and formatting. Unknown files are not scanned or rewritten.

## Build enforcement

The existing Nexus `build.rs` checks `VERSION` against `CARGO_PKG_VERSION` and fails the native
build on drift. It retains its existing source-revision behavior.

Gateway and Webconsole build/package commands run `scripts/nexus-version check` before creating an
artifact. The release-identity gate also runs the same check and derives its expected version from
`VERSION` rather than embedding separate Cargo and npm constants.

This provides three independent failure points:

1. native Cargo builds cannot emit a wrongly stamped binary;
2. npm builds and packs cannot emit mismatched packages;
3. the release gate reports the complete cross-ecosystem mismatch before publication.

## Failure behavior

`check` reports all mismatches in one run and returns exit code 1. Invalid or missing `VERSION`, a
missing required manifest, malformed JSON, or an ambiguous Cargo edit returns exit code 2 and does
not alter files.

`sync` stages all transformed content in memory first. It writes only after every required file has
parsed and every expected field has been found exactly once, preventing a partially synchronized
tree. A second `sync` produces no diff.

## Verification

Tests live outside production files and cover:

- drift is detected without changing fixture files;
- `sync` updates Cargo, Cargo.lock, npm package versions, and internal dependency pins;
- `sync` is idempotent;
- malformed versions and incomplete fixtures fail without partial writes;
- the real repository passes `scripts/nexus-version check`;
- a freshly built binary reports `0.1.5` plus the requested source revision;
- release identity and npm publication gates remain green.

All compile-heavy verification runs in the single capped Docker validator. This work does not
restart or replace the operator's live daemon or Gateway.
