// ConversationPane — a full channel/team/DM view: PaneHead + Thread + Composer.
// The shared pane layout used by channels, teams, and DMs.
//
// Three flavors:
//   - `ConversationPane` (presentational) — renders a `ConversationView`, with an
//     optional `items` override so a caller can supply the thread content while
//     keeping the head/composer chrome from the view. Pure; no I/O. (`view.items`
//     is seed content used only by tests/stories; live callers pass `items`.)
//   - `LiveConversationPane` — the older direct `useAguiConversation` wrapper.
//   - `LiveChannelPane` — wraps it with `useMessagePosts` (Lane A / Message Post
//     hook): subscribes to committed `message.created` streams, reconciles
//     optimistic echoes, renders attributed posts. Used by `/c` and `/dm`.
import { useIdentityName } from "@app/identity";
import type { ReactNode } from "react";

import { Composer } from "./Composer";
import { PaneHead } from "./PaneHead";
import { Thread, type ThreadTextRenderer } from "./Thread";
import type { AguiAgent } from "./aguiConversation";
import { useGatewayMessageHistory } from "./messageHistory";
import type { ConversationView } from "./conversationView";
import type { ThreadItem } from "./types";

export interface ConversationPaneProps {
  view: ConversationView;
  /** Thread content to render (the live AG-UI rows; `[]` for an empty thread). */
  items: ThreadItem[];
  /** Send the composer's text. Omitted → the composer is inert. */
  onSend?: (text: string) => void | Promise<void>;
  /** Lane-specific paragraph renderer. Defaults to Thread's Streamdown renderer. */
  textRenderer?: ThreadTextRenderer;
  /** Route-owned controls rendered in the pane header. */
  actions?: ReactNode;
}

export function ConversationPane({
  view,
  items,
  onSend,
  textRenderer,
  actions,
}: ConversationPaneProps) {
  return (
    <section className="flex min-h-0 flex-1 flex-col">
      <PaneHead
        title={view.title}
        topic={view.topic}
        glyph={view.glyph}
        presence={view.presence}
        faces={view.faces}
        actions={actions}
      />
      <Thread
        items={items}
        aria-label={`Messages in ${view.title}`}
        textRenderer={textRenderer}
      />
      <Composer
        placeholder={view.composerPlaceholder ?? `Message ${view.title}`}
        label={view.composerLabel ?? `Message ${view.title}`}
        onSend={onSend}
      />
    </section>
  );
}

export interface LiveConversationPaneProps {
  view: ConversationView;
  /**
   * The bus thread to watch over AG-UI `observe`. Defaults to the view key (the
   * channel/team/dm route param) — the same name the daemon threads runs under.
   */
  thread?: string;
  /** Identity to attribute live agent rows to (defaults to a neutral agent). */
  agent?: AguiAgent;
  /** Route-owned controls rendered in the pane header. */
  actions?: ReactNode;
}

/**
 * The conversation pane wired to live data. Subscribes to the thread's AG-UI
 * `observe` stream and renders ONLY the decoded live agent turns — no seed
 * fallback. With no stream / before anything arrives it renders an empty thread
 * (the head + composer chrome remain). The composer drives an AG-UI run for
 * `view.target`; the committed message renders back through this same stream.
 */
export function LiveConversationPane({ view, thread, actions }: LiveConversationPaneProps) {
  if (!view.target) {
    throw new Error("LiveConversationPane requires a Message Post target");
  }
  const operator = useIdentityName();
  const you = operator
    ? { who: operator, glyph: operator.charAt(0).toUpperCase() }
    : undefined;
  // Persistence key: DMs are keyed `dm:<name>`, channels/teams by thread name —
  // so each conversation replays from its own canonical daemon history.
  const conversationId =
    view.target.verb === "dm" ? `dm:${view.target.agentId ?? view.target.name ?? "unknown"}`
    : view.target.verb === "post" ? view.target.thread
    : undefined;
  const { messages, send } = useGatewayMessageHistory({
    target: view.target,
    you,
    conversationId: conversationId ?? thread ?? view.key,
  });
  // Render bounded backlog plus decoded live turns.
  const items: ThreadItem[] = messages;
  return <ConversationPane view={view} items={items} onSend={send} actions={actions} />;
}

export interface LiveChannelPaneProps {
  view: ConversationView;
  /**
   * The channel thread to watch. Defaults to the view key. Passed to
   * `useMessagePosts` as `thread`.
   */
  thread?: string;
  /** Identity to attribute live agent rows to (defaults to a neutral agent). */
  agent?: AguiAgent;
  /** Route-owned controls rendered in the pane header. */
  actions?: ReactNode;
}

/**
 * The Message Post pane wired to `useMessagePosts`. Channels subscribe to
 * `?thread=<channel>` and DMs subscribe to `?dm=<name>` via `observe`, reconcile
 * optimistic "you" echoes against committed self-authored posts, and render other
 * participants' posts as attributed assistant-side rows.
 */
export function LiveChannelPane({ view, thread, agent, actions }: LiveChannelPaneProps) {
  if (!view.target) {
    throw new Error("LiveChannelPane requires a Message Post target");
  }
  const operator = useIdentityName();
  const you = operator
    ? { who: operator, glyph: operator.charAt(0).toUpperCase() }
    : undefined;
  const conversationId =
    view.target.verb === "dm" ? `dm:${view.target.agentId ?? view.target.name ?? "unknown"}`
    : view.target.verb === "post" ? view.target.thread
    : undefined;
  const { messages, send } = useGatewayMessageHistory({
    target: view.target,
    you,
    conversationId: conversationId ?? thread ?? view.key,
  });
  const items: ThreadItem[] = messages;
  return <ConversationPane view={view} items={items} onSend={send} actions={actions} />;
}

/** Shown when a route has no conversation selected. */
export function MissingPane({ title }: { title: string }) {
  return (
    <section className="flex min-h-0 flex-1 flex-col">
      <PaneHead title={title} glyph="#" />
      <div className="flex flex-1 flex-col items-center justify-center gap-2 px-10 text-center text-text-muted">
        <h3 className="text-[15px] font-semibold text-text-normal">
          Nothing here yet
        </h3>
        <p className="max-w-[38ch] text-[13px]">
          Pick a channel, a DM, or a team from the rail to open a conversation.
        </p>
      </div>
    </section>
  );
}
