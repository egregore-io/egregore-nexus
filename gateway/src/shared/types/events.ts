// Contract events — the web console WS stream. `WsEvent`/`WsEventWire` are the
// internally-tagged (`type`) unions from the generated mirror. We re-export them
// and add discriminated-union helpers keyed by the `type` tag so the eventRouter
// and the store can narrow exhaustively.
//
// NEVER redefine a contract event shape here — re-export + narrow only.
import type { WsEvent, WsEventWire } from "./contracts.gen";

export type { WsEvent, WsEventWire };

/** Every literal `WsEvent.type` tag (`"message.created" | ...`). */
export type WsEventType = WsEvent["type"];

/** The concrete event variant for a given tag, e.g. `EventOf<"message.created">`. */
export type EventOf<T extends WsEventType> = Extract<WsEvent, { type: T }>;

/**
 * Type-guard that narrows a `WsEvent` to a single variant by its `type` tag.
 * Lets the eventRouter switch and the store reducers see the exact payload.
 */
export function isEvent<T extends WsEventType>(
  ev: WsEvent,
  type: T,
): ev is EventOf<T> {
  return ev.type === type;
}
