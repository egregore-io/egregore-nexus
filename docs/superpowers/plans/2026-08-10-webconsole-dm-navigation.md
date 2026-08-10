# Webconsole DM-First Navigation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Webconsole Direct Messages rail open `/dm/<agent_id>` while preserving every existing route, transport, and API endpoint.

**Architecture:** Carry the stable `MemberRow.agentId` into the shell navigation model and use it as the DM link key and route parameter. Resolve `/dm/$agent` display metadata by stable id first, with display-name fallback for existing `/dm/<agent_name>` deep links. The DM pane continues using the existing HTTP history/cursor long-poll and `POST /api/v1/messages`; `/agent/$handle` remains untouched as the explicit live-session observer.

**Tech Stack:** React 19, TypeScript, TanStack Router, TanStack Query, Vitest, Testing Library.

---

### Task 1: Route Direct Message rail rows by stable agent id

**Files:**
- Modify: `gateway/src/modules/shell/nav.ts`
- Modify: `gateway/src/modules/shell/useShellNav.ts`
- Modify: `gateway/src/modules/shell/Sidebar/Sidebar.tsx`
- Test: `gateway/src/modules/shell/shellNav.test.tsx`
- Test: `gateway/src/modules/shell/liveNav.test.tsx`

- [ ] **Step 1: Write the failing navigation tests**

Update the DM fixtures to carry stable ids, click a DM row, and assert that the memory router lands on the stable-id DM route rather than a session route:

```tsx
const FX_DMS: DmNavItem[] = [
  {
    id: "a_ben",
    agentId: "a_ben",
    name: "ben",
    kind: "agent",
    presence: "online",
    kindLabel: "agent",
  },
];

it("opens a durable DM by stable agent id", async () => {
  renderRouted(
    <Sidebar channels={[]} dms={FX_DMS} teams={[]} />,
  );

  const link = await screen.findByRole("link", { name: /ben/i });
  expect(link).toHaveAttribute("href", "/dm/a_ben");
  expect(link).not.toHaveAttribute("href", "/agent/ben:s_ben");
});
```

Give the live member fixtures stable ids and include one online member without an id, then assert that the id-less member is omitted:

```tsx
const MEMBERS = [
  { name: "ben", agentId: "a_ben", sessionId: "s_ben", agent: "claude", presence: "online" },
  { name: "legacy", sessionId: "s_legacy", agent: "claude", presence: "online" },
];

expect(within(rail).queryByText("legacy")).not.toBeInTheDocument();
```

- [ ] **Step 2: Run the focused shell tests and verify RED**

Run:

```bash
cd gateway
npx vitest run src/modules/shell/shellNav.test.tsx src/modules/shell/liveNav.test.tsx
```

Expected: failures because `DmNavItem` has no `agentId`, the row still links to `/agent/$handle`, and id-less members are still shown.

- [ ] **Step 3: Make stable identity mandatory in the DM navigation model**

Replace the session-addressing field with the stable agent identity:

```ts
export interface DmNavItem {
  id: string;
  agentId: string;
  name: string;
  kind: AgentKind;
  presence: PresenceValue;
  kindLabel?: string;
}
```

Filter members without a nonempty stable id and map `id` and `agentId` from that authority:

```ts
type AddressableMember = MemberRow & { agentId: string };

function isAddressableMember(m: MemberRow): m is AddressableMember {
  return typeof m.agentId === "string" && m.agentId.trim().length > 0;
}

function toDm(m: AddressableMember): DmNavItem {
  return {
    id: m.agentId,
    agentId: m.agentId,
    name: m.name,
    kind: memberKind(m),
    presence: presence(m.presence),
    kindLabel: m.agent,
  };
}
```

The query selector must require both eligibility and stable identity:

```ts
select: (rows) => rows
  .filter((m): m is AddressableMember =>
    m.kind !== "human"
      && presence(m.presence) !== "offline"
      && isAddressableMember(m))
  .map(toDm),
```

- [ ] **Step 4: Link the rail row to the existing DM route**

Replace the `/agent/$handle` construction in `DmRow` with:

```tsx
const active = Boolean(
  matchRoute({ to: "/dm/$agent", params: { agent: dm.agentId } }),
);

return (
  <Link
    to="/dm/$agent"
    params={{ agent: dm.agentId }}
    className={cn(rowBase, active && rowActive)}
    aria-current={active ? "page" : undefined}
  >
    <PresenceDot presence={dm.presence} size="sm" />
    <span className="min-w-0 flex-1 truncate">{dm.name}</span>
    {dm.kindLabel && (
      <span className="rounded-2 border border-border-subtle px-1 text-[10px] tracking-[0.03em] text-text-faint">
        {dm.kindLabel}
      </span>
    )}
  </Link>
);
```

Do not modify `/agent/$handle`, its route registration, or its transports.

- [ ] **Step 5: Run the focused shell tests and verify GREEN**

Run:

```bash
cd gateway
npx vitest run src/modules/shell/shellNav.test.tsx src/modules/shell/liveNav.test.tsx
```

Expected: all focused shell tests pass.

### Task 2: Resolve id-addressed DM chrome by exact stable identity

**Files:**
- Modify: `gateway/src/modules/pane/liveData.ts`
- Test: `gateway/src/modules/pane/liveData.test.tsx`

- [ ] **Step 1: Write the failing exact-id resolution test**

Add a member roster with a stable id and assert that an id-addressed DM renders the display name while preserving the exact send authority:

```tsx
import {
  useAddThreadMember,
  useAgentFacts,
  useDmView,
  usePubFeed,
  usePubRuleFacts,
  useRenameThread,
} from "./liveData";

it("resolves an id-addressed DM to its display name and stable send target", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify([
    {
      name: "ben",
      agentId: "a_ben",
      sessionId: "s_ben",
      presence: "online",
      agent: "claude",
    },
  ]), { status: 200 })));

  const hook = renderHook(() => ({
    view: useDmView("a_ben"),
    facts: useAgentFacts("a_ben"),
  }), { wrapper });

  await waitFor(() => {
    expect(hook.result.current.view.title).toBe("ben");
    expect(hook.result.current.view.target).toEqual({
      verb: "dm",
      name: "ben",
      agentId: "a_ben",
    });
    expect(hook.result.current.facts.found).toBe(true);
  });
});
```

Keep the existing name-addressed compatibility test green.

- [ ] **Step 2: Run the focused pane test and verify RED**

Run:

```bash
cd gateway
npx vitest run src/modules/pane/liveData.test.tsx
```

Expected: the new test fails because the current selectors only match `MemberRow.name`.

- [ ] **Step 3: Add exact-id-first member resolution**

Add one private helper and use it in both DM selectors:

```ts
function memberForAgentReference(
  rows: MemberRow[] | undefined,
  agentReference: string,
): MemberRow | undefined {
  return rows?.find((row) => row.agentId === agentReference)
    ?? rows?.find((row) => row.name === agentReference);
}
```

Update the facts selector:

```ts
export function useAgentFacts(agentReference: string) {
  const q = useMembers();
  const m = memberForAgentReference(q.data, agentReference);
  const facts: Fact[] = m
    ? [
        { dt: "Name", dd: m.name },
        { dt: "Harness", dd: m.agent ?? "—" },
        { dt: "Status", dd: presenceState(m) },
      ]
    : [];
  return { facts, isLoading: q.isLoading, found: !!m };
}
```

Update the DM view so exact-id links display the human-readable name and send using the stable target, while unresolved/name-addressed deep links retain the old behavior:

```ts
export function useDmView(agentReference: string): ConversationView {
  const { data } = useMembers();
  const m = memberForAgentReference(data, agentReference);
  const displayName = m?.name ?? agentReference;
  return {
    key: m?.agentId ?? agentReference,
    title: displayName,
    target: {
      verb: "dm",
      name: displayName,
      ...(m?.agentId ? { agentId: m.agentId } : {}),
    },
    presence: presenceOf(m?.presence),
    composerPlaceholder: `Message ${displayName}`,
    composerLabel: `Message ${displayName}`,
  };
}
```

- [ ] **Step 4: Run the focused pane test and verify GREEN**

Run:

```bash
cd gateway
npx vitest run src/modules/pane/liveData.test.tsx
```

Expected: the exact-id and legacy-name DM tests pass.

### Task 3: Verify endpoint and transport compatibility

**Files:**
- Verify only: `gateway/src/routes/agent.$handle.tsx`
- Verify only: `gateway/src/routes/dm.$agent.tsx`
- Verify only: `gateway/src/modules/pane/messageHistory.test.tsx`
- Verify only: `gateway/src/modules/pane/aguiWebSocketParity.test.tsx`

- [ ] **Step 1: Run the complete focused regression set**

Run:

```bash
cd gateway
npx vitest run \
  src/modules/shell/shellNav.test.tsx \
  src/modules/shell/liveNav.test.tsx \
  src/modules/pane/liveData.test.tsx \
  src/modules/pane/messageHistory.test.tsx \
  src/modules/pane/aguiWebSocketParity.test.tsx
```

Expected: all tests pass, including the existing no-WebSocket ordinary-message guard.

- [ ] **Step 2: Run full Gateway verification**

Run:

```bash
cd gateway
npm test
npm run typecheck
npm run webconsole:build
```

Expected: all Gateway tests pass, TypeScript reports no errors, and the Webconsole production build completes.

- [ ] **Step 3: Verify endpoint preservation from the diff**

Run:

```bash
git diff --check
git diff --name-only 539d02b..HEAD
git diff -- gateway/src/routes gateway/src/server gateway/src/modules/pane/messageHistory.ts
```

Expected: no whitespace errors; no route deletion, route registration change, Gateway endpoint change, WebSocket/SSE handler change, or message-history transport change.

- [ ] **Step 4: Commit the implementation**

```bash
git add \
  gateway/src/modules/shell/nav.ts \
  gateway/src/modules/shell/useShellNav.ts \
  gateway/src/modules/shell/Sidebar/Sidebar.tsx \
  gateway/src/modules/shell/shellNav.test.tsx \
  gateway/src/modules/shell/liveNav.test.tsx \
  gateway/src/modules/pane/liveData.ts \
  gateway/src/modules/pane/liveData.test.tsx
git commit -m "Route Webconsole agent DMs by stable id"
```

The commit must contain no `Co-Authored-By` trailer or automated-tool attribution.
