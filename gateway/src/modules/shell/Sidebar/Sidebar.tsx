// Sidebar — the left rail of the web console (prototype "LEFT RAIL").
//
// Grouped IA, top→bottom: Channels (#-rows + counts, alert counts) · Direct
// messages (presence dot + name + kind label) · Teams · a minor group pinned to
// the bottom (Pub feed w/ alert count, Admin) · a "me" footer (presence + etan +
// operator + a settings cog). Routed rows are TanStack <Link>s that light up via
// the router's active match. Tokens only (white-label); the rail is a <nav>
// landmark for a11y.
import * as DropdownMenu from "@radix-ui/react-dropdown-menu";
import { Link, useMatchRoute } from "@tanstack/react-router";

import { NexusLoader, PresenceDot, ScrollArea, cn } from "@shared/ui";

import {
  type ChannelNavItem,
  type DmNavItem,
  type MeIdentity,
  type TeamNavItem,
} from "../nav";

/** A project group the operator can move a channel into. */
export interface SidebarProject {
  id: string;
  name: string;
}

export interface SidebarProps {
  /** Channel rows (real named threads). Empty array → empty Channels group. */
  channels?: ChannelNavItem[];
  /** DM rows (real members). Empty array → empty Direct messages group. */
  dms?: DmNavItem[];
  /** Team rows. Empty array → empty Teams group. */
  teams?: TeamNavItem[];
  /** The resolved caller for the footer. `undefined` while loading/empty. */
  me?: MeIdentity;
  /** Pub-feed alert count. */
  pubAlert?: number;
  /** True while the live read-view is still loading (renders skeleton rows). */
  loading?: boolean;
  /** Called when the Settings cog button is clicked. */
  onSettings?: () => void;
  /** Open the "new channel" dialog (the Channels group "+"). */
  onCreateChannel?: () => void;
  /** Project groups a channel can be moved into (drives the per-row "Move to" menu). */
  projects?: SidebarProject[];
  /** Move a channel into a project (or pass null to unassign it). */
  onMoveChannel?: (threadName: string, projectId: string | null) => void;
}

const rowBase =
  "flex h-[30px] items-center gap-2 rounded-row px-2 text-[13px] font-medium " +
  "border border-transparent text-text-muted transition-colors " +
  "hover:bg-[color:var(--lens-fill-hover)] hover:text-text-normal " +
  "outline-none focus-visible:ring-2 focus-visible:ring-white/20";
const rowActive =
  "border-border-subtle bg-[color:var(--lens-fill-active)] font-semibold text-text-normal";

export function Sidebar({
  channels = [],
  dms = [],
  teams = [],
  me,
  pubAlert = 0,
  loading = false,
  onSettings,
  onCreateChannel,
  projects = [],
  onMoveChannel,
}: SidebarProps) {
  const matchRoute = useMatchRoute();
  const pubActive = Boolean(matchRoute({ to: "/pub" }));
  const adminActive = Boolean(matchRoute({ to: "/admin" }));
  const sourcesActive = Boolean(matchRoute({ to: "/sources" }));

  return (
    <nav
      aria-label="Primary"
      className="flex flex-col overflow-y-auto border-r border-border-subtle bg-bg-secondary px-2.5 py-2 lens-scroll"
    >
      <ScrollArea className="flex flex-1 flex-col">
        {/* Channels */}
        <Group
          title="Channels"
          onAdd={
            onCreateChannel
              ? { label: "New channel", onClick: onCreateChannel }
              : undefined
          }
        >
          {loading && channels.length === 0 ? (
            <SkeletonRows n={2} />
          ) : channels.length === 0 ? (
            <EmptyRow label="No channels yet" />
          ) : (
            channels.map((c) => (
              <ChannelRow
                key={c.id}
                channel={c}
                matchRoute={matchRoute}
                projects={projects}
                onMove={onMoveChannel}
              />
            ))
          )}
        </Group>

        {/* Direct messages */}
        <Group title="Direct messages">
          {loading && dms.length === 0 ? (
            <SkeletonRows n={2} />
          ) : dms.length === 0 ? (
            <EmptyRow label="No members online" />
          ) : (
            dms.map((d) => <DmRow key={d.id} dm={d} matchRoute={matchRoute} />)
          )}
        </Group>

        {/* Teams */}
        <Group title="Teams">
          {teams.length === 0 ? (
            <EmptyRow label="No teams" />
          ) : (
            teams.map((t) => (
              <TeamRow key={t.id} team={t} matchRoute={matchRoute} />
            ))
          )}
        </Group>
      </ScrollArea>

      {/* Minor group pinned to the bottom */}
      <section className="mb-2 mt-auto">
        <ul className="flex flex-col">
          <li>
            <Link to="/pub" className={cn(rowBase, pubActive && rowActive)}>
              <span className="w-4 text-center text-text-faint" aria-hidden="true">
                ≋
              </span>
              <span className="min-w-0 flex-1 truncate">Pub feed</span>
              <span className="rounded-pill bg-[color:var(--lens-fill-active)] px-[7px] py-px text-[11px] font-semibold text-text-normal">
                {pubAlert}
              </span>
            </Link>
          </li>
          <li>
            <Link to="/admin" className={cn(rowBase, adminActive && rowActive)}>
              <span className="w-4 text-center text-text-faint" aria-hidden="true">
                ⌥
              </span>
              <span className="min-w-0 flex-1 truncate">Admin</span>
            </Link>
          </li>
          <li>
            <Link to="/sources" className={cn(rowBase, sourcesActive && rowActive)}>
              <span className="w-4 text-center text-text-faint" aria-hidden="true">
                ⇥
              </span>
              <span className="min-w-0 flex-1 truncate">Sources</span>
            </Link>
          </li>
        </ul>
      </section>

      {/* "me" footer */}
      <div className="mt-1 flex items-center gap-2 border-t border-border-subtle p-2">
        <PresenceDot presence={me?.presence ?? "offline"} />
        <span className="text-[13px] font-semibold text-text-normal">
          {me?.name ?? (loading ? "…" : "—")}
        </span>
        <span className="mr-auto text-[11px] text-text-faint">
          {me?.role ?? ""}
        </span>
        <button
          type="button"
          aria-label="Settings"
          onClick={onSettings}
          className="grid h-[26px] w-[26px] place-items-center rounded-btn text-text-muted outline-none transition-colors hover:bg-[color:var(--lens-fill-active)] hover:text-text-normal focus-visible:ring-2 focus-visible:ring-white/20"
        >
          <CogIcon />
        </button>
      </div>
    </nav>
  );
}

type MatchRoute = ReturnType<typeof useMatchRoute>;

const menuItem = cn(
  "flex h-8 cursor-pointer items-center gap-2 rounded-row px-2 text-[13px] outline-none",
  "text-text-muted data-[highlighted]:bg-[color:var(--lens-fill-active)] data-[highlighted]:text-text-normal",
);

function ChannelRow({
  channel,
  matchRoute,
  projects = [],
  onMove,
}: {
  channel: ChannelNavItem;
  matchRoute: MatchRoute;
  projects?: SidebarProject[];
  onMove?: (threadName: string, projectId: string | null) => void;
}) {
  const active = Boolean(
    matchRoute({ to: "/c/$channel", params: { channel: channel.name } }),
  );
  return (
    <li className="group/row relative">
      <Link
        to="/c/$channel"
        params={{ channel: channel.name }}
        className={cn(rowBase, active && rowActive, onMove && "pr-7")}
        aria-current={active ? "page" : undefined}
      >
        <span
          className={cn(
            "font-semibold",
            active ? "text-text-muted" : "text-text-faint",
          )}
        >
          #
        </span>
        <span className="min-w-0 flex-1 truncate">{channel.name}</span>
        {channel.count !== undefined &&
          (channel.alert ? (
            <span className="rounded-pill bg-[color:var(--lens-fill-active)] px-[7px] py-px text-[11px] font-semibold text-text-normal">
              {channel.count}
            </span>
          ) : (
            <span className="text-[11px] font-semibold text-text-faint">
              {channel.count}
            </span>
          ))}
      </Link>
      {onMove && (
        <DropdownMenu.Root>
          <DropdownMenu.Trigger asChild>
            <button
              type="button"
              aria-label={`Move ${channel.name} to a project`}
              className="absolute right-1 top-1/2 grid h-[22px] w-[22px] -translate-y-1/2 place-items-center rounded-2 text-text-faint opacity-0 outline-none transition-opacity hover:bg-bg-hover hover:text-text-normal focus-visible:opacity-100 focus-visible:ring-2 focus-visible:ring-white/20 group-hover/row:opacity-100 data-[state=open]:opacity-100"
            >
              <KebabIcon />
            </button>
          </DropdownMenu.Trigger>
          <DropdownMenu.Portal>
            <DropdownMenu.Content
              align="end"
              sideOffset={4}
              className={cn(
                "z-[200] min-w-[10rem] max-w-[16rem] rounded-btn border border-border-subtle bg-bg-overlay p-1",
                "shadow-[var(--lens-shadow-elevated)]",
              )}
            >
              <DropdownMenu.Label className="px-2 py-1.5 text-[10px] font-semibold uppercase tracking-wide text-text-faint">
                Move to project
              </DropdownMenu.Label>
              {projects.length === 0 && (
                <div className="px-2 py-1 text-[12px] italic text-text-faint">
                  No projects yet
                </div>
              )}
              {projects.map((p) => (
                <DropdownMenu.Item
                  key={p.id}
                  onSelect={() => onMove(channel.name, p.id)}
                  className={menuItem}
                >
                  <span className="min-w-0 flex-1 truncate">{p.name}</span>
                </DropdownMenu.Item>
              ))}
              <DropdownMenu.Separator className="my-1 h-px bg-border-subtle" />
              <DropdownMenu.Item
                onSelect={() => onMove(channel.name, null)}
                className={menuItem}
              >
                <span className="min-w-0 flex-1 truncate">Unassign</span>
              </DropdownMenu.Item>
            </DropdownMenu.Content>
          </DropdownMenu.Portal>
        </DropdownMenu.Root>
      )}
    </li>
  );
}

function KebabIcon() {
  return (
    <svg viewBox="0 0 16 16" aria-hidden="true" className="h-[15px] w-[15px]">
      <circle cx="8" cy="3.5" r="1.2" fill="currentColor" />
      <circle cx="8" cy="8" r="1.2" fill="currentColor" />
      <circle cx="8" cy="12.5" r="1.2" fill="currentColor" />
    </svg>
  );
}

function DmRow({ dm, matchRoute }: { dm: DmNavItem; matchRoute: MatchRoute }) {
  const active = Boolean(
    matchRoute({ to: "/dm/$agent", params: { agent: dm.agentId } }),
  );
  return (
    <li>
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
    </li>
  );
}

function TeamRow({
  team,
  matchRoute,
}: {
  team: TeamNavItem;
  matchRoute: MatchRoute;
}) {
  const active = Boolean(
    matchRoute({ to: "/c/$channel", params: { channel: `team-${team.name}` } }),
  );
  return (
    <li>
      <Link
        to="/c/$channel"
        params={{ channel: `team-${team.name}` }}
        className={cn(rowBase, active && rowActive)}
      >
        <span
          aria-hidden="true"
          className="h-2 w-2 rounded-[2px] border-[1.5px] border-text-muted"
        />
        <span className="min-w-0 flex-1 truncate">{team.name}</span>
      </Link>
    </li>
  );
}

/** A faint placeholder row shown when a live group has no rows. */
function EmptyRow({ label }: { label: string }) {
  return (
    <li className="px-2 py-1 text-[12px] italic text-text-faint">{label}</li>
  );
}

/** Loading rows while a live group is still streaming in.
 * Every loading state renders the Nexus travel loader —
 * no pulse-bar skeletons. `n` is kept for call-site compatibility but the
 * loader renders once per group. */
function SkeletonRows({ n: _n }: { n: number }) {
  return (
    <li className="flex justify-center px-2 py-2" aria-hidden="true">
      <NexusLoader size={20} className="opacity-60" />
    </li>
  );
}

function Group({
  title,
  onAdd,
  children,
}: {
  title: string;
  onAdd?: { label: string; onClick: () => void };
  children: React.ReactNode;
}) {
  return (
    <section className="mb-3.5">
      <div className="flex items-center justify-between px-2 pb-1.5 pt-1">
        <span className="text-[11px] font-semibold tracking-[0.04em] text-text-faint">
          {title}
        </span>
        {onAdd && (
          <button
            type="button"
            aria-label={onAdd.label}
            title={onAdd.label}
            onClick={onAdd.onClick}
            className="grid h-[18px] w-[18px] place-items-center rounded-2 text-[14px] leading-none text-text-faint outline-none transition-colors hover:bg-[color:var(--lens-fill-active)] hover:text-text-normal focus-visible:ring-2 focus-visible:ring-white/20"
          >
            +
          </button>
        )}
      </div>
      <ul className="flex flex-col">{children}</ul>
    </section>
  );
}

function CogIcon() {
  return (
    <svg viewBox="0 0 16 16" aria-hidden="true" className="h-[15px] w-[15px]">
      <circle cx="8" cy="8" r="2" fill="none" stroke="currentColor" strokeWidth="1.2" />
      <path
        d="M8 1.5v2M8 12.5v2M1.5 8h2M12.5 8h2M3.4 3.4l1.4 1.4M11.2 11.2l1.4 1.4M12.6 3.4l-1.4 1.4M4.8 11.2l-1.4 1.4"
        stroke="currentColor"
        strokeWidth="1.1"
        strokeLinecap="round"
      />
    </svg>
  );
}
