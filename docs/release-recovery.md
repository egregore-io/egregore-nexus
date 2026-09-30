# Release recovery

[← Distribution](distribution.md)

Release recovery means withdrawing an affected artifact, preserving its immutable source tag and
evidence, and publishing a corrected version. Never point a public dist-tag at a private build.

## Before publication

Generate the release evidence from the exact clean candidate checkout and the final artifact
directory:

```bash
RELEASE_VERSION=$(tr -d '\n' < VERSION)
scripts/nexus-v010-release-evidence \
  --artifacts "output/v${RELEASE_VERSION}/artifacts" \
  --output "output/v${RELEASE_VERSION}/evidence" \
  --revision "$(git rev-parse HEAD)"
```

Publish only when `SHA256SUMS`, `nexus-${RELEASE_VERSION}.cdx.json`, and
`nexus-${RELEASE_VERSION}.intoto.jsonl` all name the same complete artifact set. The annotated
`v${RELEASE_VERSION}` tag must point at the revision recorded in the provenance statement.

The source `VERSION` is not evidence that a version was published. Before withdrawing anything,
identify the actual affected published version and packages from the release manifest and
publication records; do not infer them from the current checkout.

## Withdraw a Cargo release

Yank the affected published application first. Set `AFFECTED_VERSION` to its actual published
version before running this example. Yanking prevents new dependency resolution without deleting
the immutable crate archive. The guard requires a successful read and an exact SemVer version;
blank input, ranges, wildcards, and surrounding whitespace perform no withdrawal:

```bash
SEMVER_CORE='(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)'
SEMVER_PRERELEASE='(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)'
SEMVER_BUILD='[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*'
SEMVER_PATTERN="^${SEMVER_CORE}(-${SEMVER_PRERELEASE}(\.${SEMVER_PRERELEASE})*)?(\+${SEMVER_BUILD})?$"
if IFS= read -r -p 'Actual affected published version: ' AFFECTED_VERSION &&
   [[ $AFFECTED_VERSION =~ $SEMVER_PATTERN ]]; then
  cargo yank --vers "$AFFECTED_VERSION" egregore-nexus
else
  printf '%s\n' 'No withdrawal performed: enter an exact published SemVer version.' >&2
fi
```

Then use the affected release manifest to identify its internal crates and their versions. Yank
only those actually published, in reverse dependency order (dependents before dependencies),
using the same guarded example with each confirmed crate name and its exact published version.
If a crate was never published, skip it and record that fact in the release incident. Do not move
or delete the source tag.

## Withdraw npm releases

Deprecate every affected public package with the same corrective-version instruction. Set
`AFFECTED_VERSION` to the actual published version being withdrawn, and run only the commands
for packages confirmed published at that version. The same exact-version guard applies here:

```bash
SEMVER_CORE='(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)'
SEMVER_PRERELEASE='(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)'
SEMVER_BUILD='[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*'
SEMVER_PATTERN="^${SEMVER_CORE}(-${SEMVER_PRERELEASE}(\.${SEMVER_PRERELEASE})*)?(\+${SEMVER_BUILD})?$"
if IFS= read -r -p 'Actual affected published version: ' AFFECTED_VERSION &&
   [[ $AFFECTED_VERSION =~ $SEMVER_PATTERN ]]; then
  npm deprecate "@egregore/nexus@$AFFECTED_VERSION" "Withdrawn; install the announced corrective release"
  npm deprecate "@egregore/nexus-cli@$AFFECTED_VERSION" "Withdrawn; install the announced corrective release"
  npm deprecate "@egregore/nexus-gateway@$AFFECTED_VERSION" "Withdrawn; install the announced corrective release"
else
  printf '%s\n' 'No withdrawal performed: enter an exact published SemVer version.' >&2
fi
```

Publish a corrected public version, verify its clean install and evidence, and only then move the
`latest` dist-tag.

## Operator recovery

Stop the Gateway and daemon, archive the current Nexus home, install the corrected packages, and
start with a fresh home when the correction changes the database baseline. Never run an implicit
database conversion. Preserve the withdrawn artifacts, checksums, SBOM, provenance, logs, and the
archived home until the incident is closed.
