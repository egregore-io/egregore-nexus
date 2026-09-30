import { AgentUpdateKind, type WsEvent } from "@shared/types";
import {
  createManifestDaemonPushConnection,
  type DaemonPushConnector,
} from "../agui/daemonPushRelay.mjs";
import { decodeCursor, encodeCursor, type SessionFanoutFrame } from "./sessionFanout";

export interface RetainedSessionSubscriber {
  after?: string;
  onSource(value: {
    version: 1;
    mode: "retained-agent";
    sessionId: string;
    daemonBootId: string;
  }): boolean | void;
  onFrame(frame: SessionFanoutFrame): boolean | void;
  onEnd(): void;
}

const kinds = new Set<string>(Object.values(AgentUpdateKind));

/**
 * Replay only the daemon's retained, current-boot agent lane. This is NOT durable history.
 * A resumed cursor must still exist as an exact source anchor; neither an empty ready
 * handshake nor a later frame proves that missing content can be skipped.
 *
 * The connection is private to this read-only subscription. There is no Gateway replay
 * ring or secondary event queue, and no shared connection high-water mark to skip catch-up.
 */
export function subscribeRetainedSession(
  sessionId: string,
  subscriber: RetainedSessionSubscriber,
  connector: DaemonPushConnector = createManifestDaemonPushConnection,
): { ready: Promise<void>; close(): void; pause(): void; resume(): void } {
  const connection = connector();
  const requested = subscriber.after === undefined ? undefined : decodeCursor(subscriber.after);
  let boot = "unavailable";
  let closed = false;
  let unsubscribe: (() => void) | undefined;
  let anchored = requested === undefined || requested?.id === 0;
  let lastId = requested?.id ?? 0;
  const close = () => {
    if (closed) return;
    closed = true;
    unsubscribe?.();
    connection?.close();
  };
  // An unavailable transport proves no source gap. End it so the recorder can reconnect
  // from its own durable checkpoint, without adopting this connection's speculative tail.
  const end = () => {
    if (closed) return;
    close();
    subscriber.onEnd();
  };
  const fail = (reason: string) => {
    if (closed) return;
    try {
      subscriber.onFrame({
        lane: "gap", epoch: boot, id: lastId, cursor: encodeCursor(boot, lastId), reason,
      });
    } finally {
      close();
      subscriber.onEnd();
    }
  };
  const ready = Promise.resolve().then(async () => {
    if (closed) return;
    if (!connection) { end(); return; }
    // Subscribe only AFTER authentication. The daemon's subscribe operation captures its
    // live receiver before catch-up, so this needs no pre-ready frame buffer or lost-tail guess.
    await connection.ready;
    if (closed) return;
    const authenticatedBoot = connection.daemonBootId;
    if (!authenticatedBoot?.trim()) { fail("source_boot_missing"); return; }
    boot = authenticatedBoot;
    if (requested === null) { fail("malformed_cursor"); return; }
    if (requested && requested.daemonBootId !== boot) { fail("boot_mismatch"); return; }
    if (subscriber.onSource({ version: 1, mode: "retained-agent", sessionId, daemonBootId: boot }) === false) {
      close();
      return;
    }
    if (closed) return;
    unsubscribe = connection.subscribe({
      lane: "agent", sessionId, afterId: requested ? Math.max(0, requested.id - 1) : 0,
    }, {
      onFrame(raw) {
        if (closed) return;
        if (connection.daemonBootId !== boot) { fail("boot_mismatch"); return; }
        if (!raw || typeof raw !== "object" || Array.isArray(raw)) { fail("source_frame_invalid"); return; }
        const frame = raw as Record<string, unknown>;
        if (frame.sessionId !== sessionId) { fail("source_session_mismatch"); return; }
        if (frame.t === "gap") { fail("daemon_gap"); return; }
        if (frame.t !== "agent.update" || !Number.isSafeInteger(frame.streamEventId)
          || Number(frame.streamEventId) <= 0 || typeof frame.kind !== "string" || !kinds.has(frame.kind)
          || !frame.data || typeof frame.data !== "object" || Array.isArray(frame.data)) {
          fail("source_frame_invalid"); return;
        }
        const id = Number(frame.streamEventId);
        if (!anchored) {
          if (id !== requested?.id) { fail("source_cursor_missing"); return; }
          anchored = true;
          return; // Anchor was already recorded; never replay its visible content.
        }
        if (id <= lastId) { fail("source_order_invalid"); return; }
        const data = { ...(frame.data as Record<string, unknown>), streamEventId: id };
        let accepted: boolean | void;
        try {
          accepted = subscriber.onFrame({
            lane: "agent", epoch: boot, id, cursor: encodeCursor(boot, id),
            event: { type: "agent.update", sessionId, kind: frame.kind, data } as WsEvent,
          });
        } catch {
          close();
          subscriber.onEnd();
          return;
        }
        if (accepted === false) { close(); return; }
        lastId = id;
      },
      onError: end,
    });
    // A synchronous fixture/source may fail during subscribe, before its cleanup is returned.
    if (closed) unsubscribe();
  }).catch(end);
  return {
    ready, close,
    pause() { if (!closed) connection?.pause?.(); },
    resume() { if (!closed) connection?.resume?.(); },
  };
}
