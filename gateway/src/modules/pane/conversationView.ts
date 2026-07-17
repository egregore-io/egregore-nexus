// The conversation descriptor (channel / team / DM pane chrome).
//
// This is the header/composer view-model the pane renders around live thread
// content. It used to be a seeded constant; it is now built at the route from
// live read-view data (see `liveData.ts` `useChannelView`/`useDmView`). The
// thread body itself is the live AG-UI stream, so this type carries NO seeded
// message items — only the chrome.
import type { SendTarget } from "@shared/types";
import type { Face, PresenceValue } from "@shared/ui";

export interface ConversationView {
  /** Route key (channel name or DM agent name) — the bus thread to watch. */
  key: string;
  /**
   * Where the Message Post composer sends. Channels/teams → `{verb:"post",thread}`;
   * DMs → `{verb:"dm",name}`. Agent-session panes omit this because their composer
   * injects session input through the Agent Session lane, not Message Post.
   */
  target?: SendTarget;
  /** Pane head. */
  title: string;
  topic?: string;
  /** A leading glyph (channels/teams). Mutually exclusive with `presence`. */
  glyph?: string;
  /** A leading presence dot (DMs). Mutually exclusive with `glyph`. */
  presence?: PresenceValue;
  faces?: Face[];
  /** Composer chrome. */
  composerPlaceholder?: string;
  composerLabel?: string;
}
