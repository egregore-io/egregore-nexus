import type { PropsWithChildren } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { GatewayEventInvalidator } from "./providers";
import { qk } from "@server/read/keys";

class FakeWebSocket extends EventTarget {
  static instances: FakeWebSocket[] = [];
  static OPEN = 1;
  readyState = 0;
  sent: string[] = [];
  readonly url: string;

  constructor(url: string | URL) {
    super();
    this.url = String(url);
    FakeWebSocket.instances.push(this);
  }

  open(): void {
    this.readyState = FakeWebSocket.OPEN;
    this.dispatchEvent(new Event("open"));
  }

  send(value: string): void {
    this.sent.push(value);
  }

  message(value: unknown): void {
    this.dispatchEvent(new MessageEvent("message", { data: JSON.stringify(value) }));
  }

  close(): void {
    this.readyState = 3;
    this.dispatchEvent(new Event("close"));
  }
}

function wrapperWithClient(queryClient: QueryClient) {
  return function Wrapper({ children }: PropsWithChildren) {
    return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
  };
}

afterEach(() => {
  FakeWebSocket.instances = [];
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("GatewayEventInvalidator", () => {
  it("subscribes once and invalidates the canonical roster on fleet status", async () => {
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const queryClient = new QueryClient();
    const invalidate = vi.spyOn(queryClient, "invalidateQueries");

    render(<GatewayEventInvalidator />, { wrapper: wrapperWithClient(queryClient) });
    const socket = FakeWebSocket.instances[0]!;
    act(() => socket.open());

    expect(socket.sent).toEqual([
      JSON.stringify({ t: "subscribe", topic: "sys.fleet.status", afterSeq: 0 }),
    ]);

    act(() => {
      socket.message({
        type: "developer.event",
        event: { topic: "sys.fleet.status", lifecycle: "status", seq: 7 },
      });
    });

    await waitFor(() => {
      expect(invalidate).toHaveBeenCalledWith({ queryKey: qk.members("all") });
    });
  });
});
