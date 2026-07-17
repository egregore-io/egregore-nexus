# Release recovery

[← Distribution](distribution.md)

Nexus 0.1.0 is the first public release. There is no earlier public package or database contract to
restore, and the release does not ingest pre-release stores. Recovery means withdrawing an affected
artifact, preserving its immutable source tag and evidence, and publishing a corrected version.

## Before publication

Generate the release evidence from the exact clean candidate checkout and the final artifact
directory:

```bash
scripts/nexus-v010-release-evidence \
  --artifacts output/v0.1.0/artifacts \
  --output output/v0.1.0/evidence \
  --revision "$(git rev-parse HEAD)"
```

Publish only when `SHA256SUMS`, `nexus-0.1.0.cdx.json`, and
`nexus-0.1.0.intoto.jsonl` all name the same complete artifact set. The annotated `v0.1.0` tag must
point at the revision recorded in the provenance statement.

## Withdraw a Cargo release

Yank the application first, followed by its internal crates in reverse dependency order. Yanking
prevents new dependency resolution without deleting the immutable crate archive:

```bash
cargo yank --vers 0.1.0 egregore-nexus
cargo yank --vers 0.1.0 nexus-pty
cargo yank --vers 0.1.0 nexus-harness-codex
cargo yank --vers 0.1.0 nexus-harness-claude
cargo yank --vers 0.1.0 nexus-harness-core
cargo yank --vers 0.1.0 nexus-admin
cargo yank --vers 0.1.0 nexus-identity
cargo yank --vers 0.1.0 nexus-search
cargo yank --vers 0.1.0 nexus-dispatch
cargo yank --vers 0.1.0 nexus-bus
cargo yank --vers 0.1.0 nexus-agent
cargo yank --vers 0.1.0 nexus-acp-stream
cargo yank --vers 0.1.0 egregore-nexus-notify
cargo yank --vers 0.1.0 nexus-store
cargo yank --vers 0.1.0 egregore-nexus-common
cargo yank --vers 0.1.0 nexus-transcript
cargo yank --vers 0.1.0 nexus-contracts
```

If a crate was never published, skip it and record that fact in the release incident. Do not move
or delete the source tag.

## Withdraw npm releases

Deprecate every affected public package with the same corrective-version instruction:

```bash
npm deprecate @egregore/nexus@0.1.1 "Withdrawn; install the announced corrective release"
npm deprecate @egregore/nexus-cli@0.1.1 "Withdrawn; install the announced corrective release"
npm deprecate @egregore/nexus-gateway@0.1.1 "Withdrawn; install the announced corrective release"
```

Because 0.1.0 has no earlier public version, never point `latest` at a private or pre-release build.
Publish a corrected public version, verify its clean install and evidence, and only then move the
`latest` dist-tag.

## Operator recovery

Stop the Gateway and daemon, archive the current Nexus home, install the corrected packages, and
start with a fresh home when the correction changes the database baseline. Never run an implicit
database conversion. Preserve the withdrawn artifacts, checksums, SBOM, provenance, logs, and the
archived home until the incident is closed.
