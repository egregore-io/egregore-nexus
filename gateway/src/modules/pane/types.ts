// Pane view-models — the shape the web console thread renders.
//
// Richer than the bare contract `Message` because the thread shows
// inline code, mentions, code blocks, live streaming, agent thinking, and tool
// calls. These are CLIENT view-models (never sent to the daemon, never DB rows);
// the read-view maps contract messages and stream events into these.
import type { PresenceValue } from "@shared/ui";

/** One inline run inside a message paragraph. */
export type Inline =
  | { t: "text"; v: string }
  | { t: "code"; v: string }
  | { t: "mention"; v: string };

/** A block inside a message body. */
export type Block =
  | { b: "p"; runs: Inline[] }
  | { b: "code"; lines: string[] }
  | { b: "thinking"; text: string }
  | {
      b: "toolcall";
      icon?: string;
      name: string;
      status?: string;
      output?: string;
    };

export type ChipLabel = "you" | "agent" | "notification" | "app";

/** A single message row in a thread. */
export interface PaneMessage {
  id: string;
  who: string;
  /** Avatar glyph (single char). */
  glyph: string;
  /** The author's chip ("you" | "agent" | "notification"). */
  chip: ChipLabel;
  /** Avatar presence dot (omitted for the operator's own messages). */
  presence?: PresenceValue;
  /** "you" rows use the inverted operator avatar. */
  isYou?: boolean;
  time?: string;
  /** A live, streaming turn (shows the streaming indicator + caret). */
  streaming?: boolean;
  /** Internal canonical Message Post resume cursor; never rendered or sent back to the daemon. */
  messageCursor?: { createdAt: number; rowid: number; opaque?: string };
  blocks: Block[];
}

/** A day/section separator in the thread. */
export interface DaySep {
  sep: string;
}

export type ThreadItem = DaySep | PaneMessage;

export function isDaySep(item: ThreadItem): item is DaySep {
  return "sep" in item;
}
