# Nexus v0.1.5 Beta Publication Design

## Objective

Publish the frozen Nexus candidate as an opt-in npm prerelease without
consuming the final `0.1.5` version or changing npm's `latest` tag.

The prerelease version is `0.1.5-beta.1` and the npm dist-tag is `beta`.
The existing `v015-candidate-freeze` branch remains unchanged; beta-specific
version and packaging changes live on the isolated `v015-beta1` branch.

## Package topology

The beta preserves the existing three-package public topology:

- `@egregore/nexus-cli@0.1.5-beta.1`
- `@egregore/nexus-gateway@0.1.5-beta.1`
- `@egregore/nexus@0.1.5-beta.1`

Every internal npm dependency uses the exact prerelease version. Cargo
workspace packages, exact internal Cargo dependency pins, package manifests,
and lockfiles derive from the same root `VERSION` value. The version
synchronizer accepts canonical SemVer prereleases while continuing to reject
malformed versions and build metadata.

## Artifact production

The existing `npm-native-artifacts` workflow builds the same five native
targets and assembles exactly three npm tarballs. Its install and command
surface smokes remain mandatory. Webconsole assets remain packaged for
compatibility with the current gateway package, but Webconsole behavior is not
a beta acceptance gate.

Before publication, the exact downloaded tarballs are checked for:

- complete five-target native inventory;
- exact `0.1.5-beta.1` package identities and dependencies;
- successful local and global umbrella installation;
- working `nexus --version` and command help surfaces;
- registry absence of the three beta package versions.

## Publication

Publish the verified tarballs in dependency order: CLI, Gateway, umbrella.
Every `npm publish` uses `--tag beta --access public`. Publication must stop if
npm authentication is unavailable or if a package/version already exists.

After publication, verify all three `beta` tags resolve to `0.1.5-beta.1` and
all three `latest` tags still resolve to `0.1.4`. A partial publication is
reported explicitly and resumed only for packages that were not published;
published package versions are never overwritten or reused.

## Release boundary

This beta is an opt-in Nexus package release. It does not claim the final
v0.1.5 packed Lens LP3-LP16, 30-minute soak, aggregate evidence, or release
seal. Those remain required before publishing stable `0.1.5` under `latest`.
