// ContextViews — the right-panel content per scope (prototype `.ctx-view`s):
//   members (channels/teams) · agent details (DMs) · routing rules (pub) ·
//   tiers (admin). Each is a small composition of a section head + a member list
//   or a facts dl. Presentational: the live data is fetched at the route
//   (`useRoster`/`useAgentFacts`/`usePubRuleFacts`/`useTierFacts`) and passed in;
//   each renders an honest empty state when there is nothing yet. Tokens only.
import * as DropdownMenu from "@radix-ui/react-dropdown-menu";

import { cn } from "@shared/ui";

import type { Fact, MemberItem } from "./liveData";

const dotColor: Record<string, string> = {
  online: "bg-online",
  busy: "bg-busy",
  offline: "bg-offline",
};

function Head({ children, count, mt }: { children: string; count?: string; mt?: boolean }) {
  return (
    <div className={cn("mb-2.5 flex items-center gap-2", mt && "mt-[22px]")}>
      <h2 className="text-[11px] font-semibold tracking-[0.04em] text-text-faint">
        {children}
      </h2>
      {count && <span className="text-[11px] text-text-faint">{count}</span>}
    </div>
  );
}

function Facts({ facts }: { facts: Fact[] }) {
  return (
    <dl className="flex flex-col gap-[9px]">
      {facts.map((f, i) => (
        <div key={i} className="flex justify-between gap-3 text-[12px]">
          <dt className="text-text-faint">{f.dt}</dt>
          <dd className="text-text-read">{f.dd}</dd>
        </div>
      ))}
    </dl>
  );
}

function Empty({ children }: { children: string }) {
  return <p className="text-[12px] text-text-faint">{children}</p>;
}

// ── members (channels / teams) ──────────────────────────────────────────────
export function MembersContext({
  members = [],
  candidates = [],
  onAdd,
  onRemove,
}: {
  members?: MemberItem[];
  /** Names that can be added to the thread (directory minus current members). */
  candidates?: string[];
  /** Add an agent/member to the thread. When provided, the "+" affordance shows. */
  onAdd?: (member: string) => void;
  /** Remove a member from the thread. When provided, each row gets a remove button. */
  onRemove?: (member: string) => void;
}) {
  return (
    <div>
      <div className="mb-2.5 flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <h2 className="text-[11px] font-semibold tracking-[0.04em] text-text-faint">
            Members
          </h2>
          {members.length > 0 && (
            <span className="text-[11px] text-text-faint">{members.length}</span>
          )}
        </div>
        {onAdd && <AddMemberMenu candidates={candidates} onAdd={onAdd} />}
      </div>
      {members.length === 0 ? (
        <Empty>No members yet.</Empty>
      ) : (
        <ul className="flex flex-col gap-0.5">
          {members.map((m) => (
            <li
              key={m.name}
              className="group/m grid grid-cols-[24px_1fr_auto] grid-rows-[auto_auto] items-center gap-x-[9px] rounded-row p-1.5 hover:bg-[color:var(--lens-fill-hover)]"
            >
              <span className="relative row-span-2 grid h-6 w-6 place-items-center rounded-btn border border-border-subtle bg-surface-raised text-[10px] font-semibold text-text-read">
                {m.glyph}
                <span
                  className={cn(
                    "absolute -bottom-0.5 -right-0.5 h-2 w-2 rounded-full border-2 border-bg-secondary",
                    dotColor[String(m.presence)] ?? "bg-offline",
                  )}
                />
              </span>
              <span className="col-start-2 text-[13px] font-semibold text-text-normal">
                {m.name}
              </span>
              <span className="col-start-2 text-[11px] text-text-muted">{m.state}</span>
              {onRemove && (
                <button
                  type="button"
                  aria-label={`Remove ${m.name}`}
                  title={`Remove ${m.name}`}
                  onClick={() => onRemove(m.name)}
                  className="col-start-3 row-span-2 grid h-6 w-6 place-items-center self-center rounded-btn text-[16px] leading-none text-text-faint opacity-0 outline-none transition-opacity hover:bg-[color:var(--lens-fill-active)] hover:text-text-normal focus-visible:opacity-100 focus-visible:ring-2 focus-visible:ring-white/20 group-hover/m:opacity-100"
                >
                  ×
                </button>
              )}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

/** A "+ Add" dropdown listing addable agents for the thread. */
function AddMemberMenu({
  candidates,
  onAdd,
}: {
  candidates: string[];
  onAdd: (member: string) => void;
}) {
  return (
    <DropdownMenu.Root>
      <DropdownMenu.Trigger asChild>
        <button
          type="button"
          aria-label="Add member"
          className="inline-flex items-center gap-1 rounded-row border border-border-subtle px-1.5 py-0.5 text-[11px] font-medium text-text-muted outline-none transition-colors hover:bg-[color:var(--lens-fill-active)] hover:text-text-normal focus-visible:ring-2 focus-visible:ring-white/20"
        >
          <span className="text-[13px] leading-none">+</span> Add
        </button>
      </DropdownMenu.Trigger>
      <DropdownMenu.Portal>
        <DropdownMenu.Content
          align="end"
          sideOffset={6}
          className={cn(
            "z-[200] max-h-[300px] min-w-[12rem] max-w-[16rem] overflow-y-auto",
            "rounded-btn border border-border-subtle bg-bg-overlay p-1 lens-scroll",
            "shadow-[var(--lens-shadow-elevated)]",
          )}
        >
          <DropdownMenu.Label className="px-2 py-1.5 text-[10px] font-semibold uppercase tracking-wide text-text-faint">
            Add to thread
          </DropdownMenu.Label>
          {candidates.length === 0 ? (
            <div className="px-2 py-1 text-[12px] italic text-text-faint">
              Everyone&rsquo;s already in.
            </div>
          ) : (
            candidates.map((name) => (
              <DropdownMenu.Item
                key={name}
                onSelect={() => onAdd(name)}
                className={cn(
                  "flex h-8 cursor-pointer items-center gap-2 rounded-row px-2 text-[13px] outline-none",
                  "text-text-muted data-[highlighted]:bg-[color:var(--lens-fill-active)] data-[highlighted]:text-text-normal",
                )}
              >
                <span className="min-w-0 flex-1 truncate">{name}</span>
              </DropdownMenu.Item>
            ))
          )}
        </DropdownMenu.Content>
      </DropdownMenu.Portal>
    </DropdownMenu.Root>
  );
}

// ── agent details (DMs) ─────────────────────────────────────────────────────
export function AgentContext({
  facts = [],
  session,
}: {
  facts?: Fact[];
  /** The daemon-owned session id this view is bound to (`/agent/<name>:<session_id>`). */
  session?: string;
}) {
  const allFacts: Fact[] = session ? [{ dt: "session", dd: session }, ...facts] : facts;
  return (
    <div>
      <Head>Agent</Head>
      {allFacts.length === 0 ? <Empty>No details yet.</Empty> : <Facts facts={allFacts} />}
    </div>
  );
}

// ── routing rules (pub) ─────────────────────────────────────────────────────
export function PubContext({ rules = [] }: { rules?: Fact[] }) {
  return (
    <div>
      <Head>Routing rules</Head>
      {rules.length === 0 ? <Empty>No routing rules yet.</Empty> : <Facts facts={rules} />}
      <Head mt>Pub</Head>
      <p className="text-[11px] text-text-muted">
        External producers push here. You (or an admin agent) route who gets what.
      </p>
    </div>
  );
}

// ── tiers (admin) ───────────────────────────────────────────────────────────
export function AdminContext({ facts = [] }: { facts?: Fact[] }) {
  return (
    <div>
      <Head>Tiers</Head>
      {facts.length === 0 ? <Empty>No members yet.</Empty> : <Facts facts={facts} />}
    </div>
  );
}
