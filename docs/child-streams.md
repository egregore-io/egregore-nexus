# Child streams (subagent identity)

The observation paths listed below attribute native output before parent publication. Output
identified as belonging to a subagent (a child session, thread or transcript under the captured
root) is kept out of the owner's `agent.update` stream. Child and unresolved observations use
separate `child_agent.update` events on a bounded, volatile lane, keyed by owner session,
harness, captured native root and child identity or observation locator. This is parent-stream
isolation, not a claim that every native record is captured or durably delivered.

## Wire shape

```json
{
  "type": "child_agent.update",
  "sessionId": "s_owner",
  "child": {
    "harness": "claude",
    "root": "<native root id>",
    "id": "<native child id>",
    "locator": "claude:subagents/agent-<id>.jsonl@<generation>",
    "parent": "<native parent id, when the harness records one>",
    "parentRef": "<parent-side reference to the spawning call, when recorded>",
    "depth": 1,
    "resolution": "root_verified | lineage_verified | unresolved",
    "evidence": "<the native fields that proved the relationship>"
  },
  "kind": "text | thinking | tool_call | user_input | turn_end | ...",
  "sourceRef": "<harness>:<native id>@<generation>#<occurrence>",
  "data": { }
}
```

- `resolution` says how much native evidence established the child's relationship to the root.
  `root_verified`: root affiliation proven by native ids, immediate parent and depth unknown.
  `lineage_verified`: the parent chain to the root proven by native lineage; `parent` and
  `depth` are set. `unresolved`: the record could not be attributed positively; it is kept on a
  separately bounded unresolved lane, never merged into the owner's stream and never guessed.
- Nothing is inferred from names, text or timing. `parent` and `depth` appear only when the
  harness records them natively.
- `sourceRef` names the source occurrence within a source generation (a file's first record
  uuid, a session row's start time, a forwarder instance). It is provenance for consumers, not a
  unique event key, not a global sequence and not a restart dedupe guarantee: where a harness
  decodes several events from one native record (Hermes message rows) they share that record's
  reference. A daemon restart clears the lane; consumers must not treat these references as a
  guarantee that earlier output can be replayed.
- `kind` and `data` follow the same contract as `agent.update` (`docs/tool-call-contract.md` for
  tool calls). OpenCode child text additionally carries `delivery: "delta" | "snapshot"` and,
  when the native role was never observed or was evicted, `role: "unknown"`.

## Per harness

| Harness | Child source | Verified identity | Ancestry |
|---|---|---|---|
| Claude (headed, hooks bridge) | `<transcript dir>/<owned session>/subagents/agent-*.jsonl`, plus sidechain rows found in the main transcript | `root_verified` when the directory, the file name, `agentId` and `sessionId` agree | not recorded natively; `parent` and `depth` absent |
| OpenCode (plugin) | every part, status and idle event of a session that is not the root, sent by the plugin to the bridge's `/child` route | `lineage_verified` when the `session.created` `parentID` chain reaches the launch-captured root | `parent`, `depth` from the chain; `parentRef` from the root's own task part (`state.metadata.sessionId` + `callID`) |
| Codex (app server) | notifications whose `threadId` is not the bound main thread | `lineage_verified` when the rollout meta `thread_spawn` chain walks to the main thread | `parent`, `depth` from rollout meta |
| Hermes (headed, sqlite) | `messages` rows of sessions whose `parent_session_id` chain reaches the root, discovered by a keyset-resumable walk from the root whose frontier at each depth rotates fairly across all known sessions at that depth (page cuts, the registration cap and depth-limit sessions are counted as `truncated`, deferred frontier parents as `deferred`; no pass is claimed complete), served under the root they were verified for even after the runtime's root changes; payload columns over 256 KiB by exact byte length are withheld by the store and halt that child with `record_exceeds_byte_budget` | `lineage_verified` by construction: descendants are discovered by walking native parent links from the root | `parent`, `depth` from the walk |

Observed foreign or unattributed plugin parts and app-server notifications use unresolved
lanes rather than the parent stream, subject to the lane bounds. A part with no session id at
all (OpenCode) goes to one unresolved lane per captured root under `opencode:unidentified`.
File/database discovery does not enumerate every unrelated native session.

## Bounds and loss

The child lane is volatile (it lives with the daemon's boot epoch) and bounded per owner
session: rows, bytes, lanes and unresolved lanes (`child_stream_max_rows_per_session`,
`child_stream_max_bytes_per_session`, `child_stream_max_lanes_per_session`,
`child_stream_max_unresolved_lanes_per_session` in the daemon config). When the row or byte bound is hit the
oldest retained rows across the whole owner session are evicted, whichever lane they belong to (a
quiet lane is not protected from busier siblings), and each eviction is counted on the lane it came
from
(`evictedThrough`, `evictedRows`, `evictedBytes`); a record the lane refuses is counted as
`refusedRows` on a per-session sentinel. Lane-count limits do not evict retained rows. New-lane admission may reclaim an empty tombstone;
otherwise it is refused. The unresolved-lane limit refuses records that would create another
unresolved lane. Lane eviction and refusal accounting is exposed on
the affected lane or the owner's session-loss summary; it does not account for native output
that never reached this observation path.

Claude child-file and Hermes child-database readers keep durable per-child cursors. Emission
and cursor persistence are separate operations, so a failure between them can permit repeated
observations. Neither this ordering nor a saved cursor guarantees at-least-once delivery:
the lane is volatile, retention is bounded, and restart recovery is not implemented.
These readers bound each pass and schedule registered children least recently served first.
Their discovery bounds and assumptions still limit coverage. Hermes discovery cuts and
deferrals are counted and logged by the daemon.

When these readers cannot continue a consumed cursor safely, they declare
`coverage { generation, from, unknown_before: true, reason }` and halt that cursor. Reasons
include detected source replacement/truncation, malformed or oversized records, and a previous
daemon boot. This declaration does not recover the missing output. A refused declaration is
retried; it does not authorize skipping the rejected record.

Codex app-server and OpenCode plugin child streams instead observe live notifications; they do
not have these durable per-child tail cursors. Their instance-scoped state and source references
do not promise replay after disconnection or restart.

## Lookup

`agent.child_streams` returns the lane state for one owner session:

```json
{ "session": "s_owner", "harness": "claude", "root": "...", "child": "n:<id>",
  "cursor": { "epoch": "<boot epoch>", "afterId": 0 }, "limit": 200,
  "lanesAfter": { "epoch": "<boot epoch>", "childKey": "n:abc", "harness": "claude", "root": "..." },
  "lanesLimit": 100 }
```

Result: `{ epoch, cursorStatus: "fresh" | "valid" | "boot_mismatch", lanes, lanesNextAfter, page,
sessionLoss }`. Both cursors are epoch-bearing: one from another daemon boot is answered with
`cursorStatus: "boot_mismatch"` and both rows and lanes start from the beginning, never a silent
empty page and never a lane hidden behind an old lane cursor. A cursor without an epoch is
refused as malformed. Row and lane limits default to 200 and 100 and are capped at 1000.
The optional `harness`, `root` and `child` filters narrow `page.rows`, not `lanes`; lane
enumeration is separately paginated across the authorized owner session.

The caller must be the owner session itself, an admin-tier caller, or hold an agent access grant
on the owner's agent. A grant found by the caller's durable agent id authorizes it under any
name. A grant found by principal name and project authorizes only when its explicit bindings,
where present, name this caller (bound principal agent id equal to the caller's agent id, bound
principal session id equal to the caller's session); a name never overrides a mismatching
durable binding. A legacy grant with neither binding authorizes by name and project alone.
Anything else is refused; an unknown session is not found.

## Limitations

- Excluded child content lives in the bounded volatile child lane only. There is no separate
  durable recording per child; eviction and refusal are reported per lane, and
  filtered output is not promised to be recoverable.
- After a daemon restart the child lane is empty. Replay of retained native child records is
  not implemented. A consumed Claude/Hermes child cursor from a previous boot declares unknown coverage
  and does not advance. This is not a recovery policy for the live plugin/app-server paths.
- Claude: immediate parent, depth and the Task call a child belongs to are not verifiable by id
  in the current Claude Code transcripts; lanes are `root_verified` at most.
  Child-file opens reject symlinks and avoid blocking on special files on Unix. Other platforms
  currently check that the opened handle is a regular file, but do not provide that no-follow
  guarantee.
- OpenCode legacy DB-observer mode does not tail child sessions (the plugin path carries them).
  Parts observed before the plugin knows its root are logged and not attributable.
- ACP-mode child attribution is not established by these headed-mode fixtures.
- Codex: parent/child notification isolation is covered by protocol fixtures; live native
  subagent acceptance has not been validated.
- Hermes discovery assumes a newly visible child sorts after the newest registered child of
  its parent by (`started_at`, `id`). Hermes sets `started_at` at creation; a backdated or
  imported child that sorts before that keyset is not discovered. The walk stops at 8 hops and
  registers at most 256 descendants per pass; sessions at the depth limit are counted as cut.
