# Contributing to Nexus

## How to open a pull request

1. Fork this repository and clone your fork. Core contributors can use a branch in this repository.
2. Create a short-lived branch from the latest `main`. Use a descriptive name such as
   `fix/launch-banner` or `feat/cursor-harness`.
3. Make one focused change. Add relevant tests and update the documentation and changelog for
   user-visible changes. Regenerate TypeScript contracts if a Rust contract changes.
4. Run the checks under [Verification](#verification). Record the commands and results in your PR.
5. Push your branch and open a pull request against `egregore-io/egregore-nexus`, targeting `main`.
   Use a title that describes the change.
6. Fill in the PR template: explain the problem, what changes, and how you verified it. Describe
   compatibility and architecture changes when relevant. Link an existing issue if there is one.
7. For fork PRs, enable "Allow edits by maintainers". Address review comments on the same branch.
   A maintainer squash-merges the approved PR, then the branch is deleted.

For larger changes, open a draft PR early so review can happen as the work develops.

## Git workflow

- `main` is the only long-lived branch and stays releasable.
- All changes land through pull requests. Keep each PR to one concern; split large features into
  small PRs that can be reviewed independently.
- Pull requests are squash-merged. Do not push directly to `main` or rewrite published history.
- Long-running private work rebases onto each release tag and lands through small PRs.
- Releases are annotated tags on `main`: `vX.Y.Z`, or `vX.Y.Z-beta.N` for a prerelease.

## Architecture invariants

- The daemon is the lightweight transport authority. It owns durable agent identity, runtime
  resurrection descriptors, and only the bounded journal needed to settle accepted deliveries.
- The Gateway is a separately installed process and the canonical REST/WebSocket backend. It owns
  durable product history, search, event projections, and browser-facing authentication.
- The WebUI talks only to the Gateway. It must not open the daemon database, daemon IPC, or runtime
  files directly.
- Projects are metadata labels, not routing authorities or durable first-class entities.
- DMs, threads, topics, and notifications share transport primitives but retain their distinct
  delivery and visibility rules.
- Agent-session presentation has two explicit views: normalized model text and converted AG-UI
  events. Raw terminal bytes are a separate attach stream, never canonical message history.
- Nexus routes and wakes; it does not orchestrate agent behavior.

## Contracts and generated code

`core/crates/nexus-contracts` is the source of truth for public request, response, event, and error
shapes. TypeScript contracts in `gateway/src/shared/types/contracts.gen.ts` are generated and must
not be edited by hand.

After a Rust contract change, run:

```bash
pnpm --dir gateway gen:contracts
pnpm --dir gateway check:contracts
```

Commit the Rust change, generated TypeScript, and matching golden fixtures together.

## Documentation

Documentation is part of the change:

- update rustdoc or source comments when an implementation boundary changes;
- update the relevant file under `docs/` for behavior, API, installation, or operations changes;
- update `README.md` and `CHANGELOG.md` for user-visible changes;
- describe compatibility and failure behavior precisely—do not promise unsupported provider or
  platform behavior.

## Verification

Use the narrowest relevant test while developing, then run the repository gates before proposing a
merge:

```bash
scripts/check core-test-layout
scripts/check release-identity
scripts/check architecture
scripts/check boundaries
scripts/check rust-workspace
pnpm --dir gateway typecheck
pnpm --dir gateway test
pnpm --dir gateway build
```

Release-oriented scripts must implement a successful, side-effect-free `--help`. Runtime and
endurance tests belong in the disposable Docker validator; never point them at an operator's live
Nexus home.

## Review

- Routine changes need one maintainer approval. For paths listed in `.github/CODEOWNERS`, that
  approval must come from a listed owner.
- The core team is three people. Expect a first response within a week; a pull request with no
  activity for sixty days may be closed and can be reopened.
- Tick "Allow edits by maintainers" on your pull request. For small remaining fixes we may push to
  your branch rather than ask for another round.
- You are responsible for every line you submit, whatever tools helped write it. A change its
  author cannot explain in review is closed.

## Commits

Use an imperative subject and keep each commit to one concern. Keep commit messages focused on the change and its rationale. Do not add tool, model, or session attribution
trailers; the author of record is the person who opens the pull request.
