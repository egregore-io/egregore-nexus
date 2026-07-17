import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { SendTarget } from "@shared/types";

import {
  historyPath,
  historyRowsToPaneMessages,
  useGatewayMessageHistory,
  type GatewayHistoryRow,
} from "./messageHistory";

afterEach(() => {
  vi.restoreAllMocks();
});

const target: SendTarget = { verb: "post", thread: "design" };

describe("Gateway message history", () => {
  it("builds only Gateway REST history URLs for threads and DMs", () => {
    expect(historyPath("design")).toBe("/api/v1/threads/design/history?limit=100");
    expect(historyPath("dm:ada", { after: "cursor 1", waitMs: 30_000 })).toBe(
      "/api/v1/dms/ada/history?limit=100&after=cursor+1&waitMs=30000",
    );
  });

  it("maps canonical rows into attributed pane rows", () => {
    expect(historyRowsToPaneMessages([
      { messageId: "m1", from: "alex", body: "hello", when: 1 },
      { messageId: "m2", from: "ada", body: "hi", when: 2 },
    ], { who: "alex", glyph: "E" })).toMatchObject([
      { id: "m1", who: "alex", isYou: true },
      { id: "m2", who: "ada", chip: "agent" },
    ]);
  });

  it("loads latest once, then waits from the opaque cursor without opening a socket", async () => {
    const pending = new Promise<GatewayHistoryRow[]>(() => {});
    const load = vi.fn()
      .mockResolvedValueOnce([
        {
          messageId: "m1",
          from: "ada",
          body: "first",
          when: 1,
          cursor: { createdAt: 1, rowid: 0, opaque: "c1" },
        },
      ])
      .mockReturnValueOnce(pending);

    const { result } = renderHook(() => useGatewayMessageHistory({
      conversationId: "design",
      target,
      loadHistory: load,
    }));

    await waitFor(() => expect(result.current.messages).toHaveLength(1));
    expect(load).toHaveBeenNthCalledWith(1, "design");
    expect(load).toHaveBeenNthCalledWith(2, "design", expect.objectContaining({
      after: "c1",
      waitMs: 30_000,
    }));
  });

  it("uses origin after an empty latest page and reconciles the optimistic row to the Ack id", async () => {
    let resolveTail!: (rows: GatewayHistoryRow[]) => void;
    const tail = new Promise<GatewayHistoryRow[]>((resolve) => { resolveTail = resolve; });
    const load = vi.fn().mockResolvedValueOnce([]).mockReturnValueOnce(tail);
    const post = vi.fn().mockResolvedValue({ messageId: "m_committed" });

    const { result } = renderHook(() => useGatewayMessageHistory({
      conversationId: "design",
      target,
      you: { who: "alex", glyph: "E" },
      loadHistory: load,
      postMessage: post,
    }));

    await waitFor(() => expect(load).toHaveBeenCalledTimes(2));
    expect(load).toHaveBeenNthCalledWith(2, "design", expect.objectContaining({
      after: "origin",
      waitMs: 30_000,
    }));

    await act(async () => {
      await result.current.send("hello");
    });
    expect(result.current.messages).toMatchObject([
      { id: "m_committed", isYou: true },
    ]);

    await act(async () => {
      resolveTail([{
        messageId: "m_committed",
        from: "alex",
        body: "hello",
        when: 2,
        cursor: { createdAt: 2, rowid: 0, opaque: "c2" },
      }]);
    });
    await waitFor(() => expect(result.current.messages).toHaveLength(1));
    expect(result.current.messages[0]?.id).toBe("m_committed");
  });

  it("continues cursor polling after focus aborts the current long poll", async () => {
    const abortableTail = (_conversationId: string, cursor?: { signal?: AbortSignal }) =>
      new Promise<GatewayHistoryRow[]>((_resolve, reject) => {
        cursor?.signal?.addEventListener("abort", () => {
          reject(new DOMException("refresh requested", "AbortError"));
        }, { once: true });
      });
    const load = vi.fn()
      .mockResolvedValueOnce([{
        messageId: "m1",
        from: "ada",
        body: "first",
        when: 1,
        cursor: { createdAt: 1, rowid: 0, opaque: "c1" },
      }])
      .mockImplementationOnce(abortableTail)
      .mockResolvedValueOnce([{
        messageId: "m2",
        from: "ben",
        body: "after focus",
        when: 2,
        cursor: { createdAt: 2, rowid: 0, opaque: "c2" },
      }])
      .mockReturnValue(new Promise<GatewayHistoryRow[]>(() => {}));

    const { result } = renderHook(() => useGatewayMessageHistory({
      conversationId: "design",
      target,
      loadHistory: load,
    }));

    await waitFor(() => expect(load).toHaveBeenCalledTimes(2));
    act(() => window.dispatchEvent(new Event("focus")));

    await waitFor(() => expect(load).toHaveBeenCalledTimes(4));
    expect(load).toHaveBeenNthCalledWith(3, "design", expect.objectContaining({
      after: "c1",
      waitMs: 30_000,
    }));
    expect(result.current.messages.map((row) => row.id)).toEqual(["m1", "m2"]);
  });
});
