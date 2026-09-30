import { afterEach, describe, expect, it, vi } from "vitest";
import { act, render, waitFor } from "@testing-library/react";
import { EventType } from "@ag-ui/client";
import type { BaseEvent } from "@ag-ui/client";
import { useEffect } from "react";

import {
  openAguiSource,
  useAguiConversation,
  useAgentSession,
  type AguiEventSource,
  type AguiInputFrame,
} from "./aguiConversation";
import type { PaneMessage } from "./types";

class FakeEventAguiSource implements AguiEventSource {
  onmessage: ((ev: { data: string }) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  closed = false;

  readonly url: string;

  constructor(url: string) {
    this.url = url;
  }

  push(event: BaseEvent): void {
    this.onmessage?.({ data: JSON.stringify(event) });
  }

  close(): void {
    this.closed = true;
  }

  drop(): void {
    this.onerror?.(new Error("websocket dropped"));
  }
}

class FakeEventSource implements AguiEventSource {
  static instances: FakeEventSource[] = [];

  onmessage: ((ev: { data: string }) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  closed = false;
  readonly url: string;

  constructor(url: string) {
    this.url = url;
    FakeEventSource.instances.push(this);
  }

  close(): void {
    this.closed = true;
  }
}

class FakeWebSocket extends EventTarget {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;
  static instances: FakeWebSocket[] = [];

  readyState = FakeWebSocket.CONNECTING;
  sent: string[] = [];
  readonly url: string;

  constructor(url: string) {
    super();
    this.url = url;
    FakeWebSocket.instances.push(this);
  }

  send(data: string): void {
    this.sent.push(data);
  }

  close(): void {
    this.readyState = FakeWebSocket.CLOSED;
    this.dispatchEvent(new Event("close"));
  }

  open(): void {
    this.readyState = FakeWebSocket.OPEN;
    this.dispatchEvent(new Event("open"));
  }

  message(data: string | BaseEvent | Record<string, unknown>): void {
    this.dispatchEvent(
      new MessageEvent("message", {
        data: typeof data === "string" ? data : JSON.stringify(data),
      }),
    );
  }
}

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  FakeEventSource.instances = [];
  FakeWebSocket.instances = [];
});

function CaptureHarness({
  source,
  onMessages,
}: {
  source: FakeEventAguiSource;
  onMessages: (messages: PaneMessage[]) => void;
}) {
  const { messages } = useAgentSession({
    name: "iris",
    openSource: () => source,
  });
  useEffect(() => onMessages(messages), [messages, onMessages]);
  return null;
}

function installFakeWebSocket(): typeof FakeWebSocket {
  FakeWebSocket.instances = [];
  vi.stubGlobal("WebSocket", FakeWebSocket);
  return FakeWebSocket;
}

function installFakeEventSource(): typeof FakeEventSource {
  FakeEventSource.instances = [];
  vi.stubGlobal("EventSource", FakeEventSource);
  return FakeEventSource;
}

function openWsSource(url: string): AguiEventSource | null {
  return openAguiSource(url, "ws");
}

function sessionTurn(streamEventId: number, text: string): BaseEvent[] {
  return [
    { type: EventType.RUN_STARTED, threadId: "iris", runId: "r1" } as BaseEvent,
    {
      type: EventType.TEXT_MESSAGE_START,
      messageId: "m1",
      role: "assistant",
      streamEventId: streamEventId - 1,
      cursor: `epoch:${streamEventId - 1}`,
    } as BaseEvent,
    {
      type: EventType.TEXT_MESSAGE_CONTENT,
      messageId: "m1",
      delta: text,
      streamEventId,
      cursor: `epoch:${streamEventId}`,
    } as BaseEvent,
    { type: EventType.TEXT_MESSAGE_END, messageId: "m1", streamEventId } as BaseEvent,
    { type: EventType.RUN_FINISHED, threadId: "iris", runId: "r1" } as BaseEvent,
  ];
}

function partialSessionTurn(streamEventId: number, text: string): BaseEvent[] {
  return [
    { type: EventType.RUN_STARTED, threadId: "iris", runId: "r1" } as BaseEvent,
    {
      type: EventType.TEXT_MESSAGE_START,
      messageId: "m1",
      role: "assistant",
      streamEventId: streamEventId - 1,
      cursor: `epoch:${streamEventId - 1}`,
    } as BaseEvent,
    {
      type: EventType.TEXT_MESSAGE_CONTENT,
      messageId: "m1",
      delta: text,
      streamEventId,
      cursor: `epoch:${streamEventId}`,
    } as BaseEvent,
  ];
}

function messagePostTurn(body: string): BaseEvent[] {
  return [
    { type: EventType.RUN_STARTED, threadId: "post:design", runId: "r1" } as BaseEvent,
    {
      type: EventType.TEXT_MESSAGE_START,
      messageId: "m1",
      role: "assistant",
      name: "iris",
      createdAt: 1_783_300_001_000,
      cursor: { createdAt: 1_783_300_001_000, rowid: 44 },
    } as BaseEvent,
    { type: EventType.TEXT_MESSAGE_CONTENT, messageId: "m1", delta: body } as BaseEvent,
    { type: EventType.TEXT_MESSAGE_END, messageId: "m1" } as BaseEvent,
    { type: EventType.RUN_FINISHED, threadId: "post:design", runId: "r1" } as BaseEvent,
  ];
}

function rowText(rows: PaneMessage[]): string {
  return rows
    .flatMap((row) => row.blocks)
    .flatMap((block) => (block.b === "p" ? block.runs : []))
    .map((run) => (run.t === "text" ? run.v : ""))
    .join("");
}

describe("AG-UI WebSocket verification source parity", () => {
  it("keeps the SSE lane on EventSource when the transport flag is off", () => {
    const EventSource = installFakeEventSource();
    vi.stubGlobal("WebSocket", FakeWebSocket);

    const source = openAguiSource("/api/agui/observe?session=iris", "sse");

    expect(source).toBeInstanceOf(EventSource);
    expect(EventSource.instances[0]!.url).toBe("/api/agui/observe?session=iris");
    expect(FakeWebSocket.instances).toHaveLength(0);
    source?.close();
  });

  it("renders identical pane rows from SSE EventSource and the real WS client source", async () => {
    const Socket = installFakeWebSocket();
    const sseSource = new FakeEventAguiSource("/api/v1/agent-sessions/iris/events?view=agui");
    let sseRows: PaneMessage[] = [];
    let wsRows: PaneMessage[] = [];

    const rendered = render(
      <>
        <CaptureHarness source={sseSource} onMessages={(messages) => { sseRows = messages; }} />
        <WsCaptureHarness onMessages={(messages) => { wsRows = messages; }} />
      </>,
    );
    const wsSocket = Socket.instances[0]!;
    expect(wsSocket.url).toBe("ws://localhost:3000/api/v1/agent-sessions/iris/events?view=agui");

    act(() => {
      for (const event of sessionTurn(214, "same pane row")) {
        sseSource.push(event);
        wsSocket.message(event);
      }
    });

    await waitFor(() => expect(rowText(wsRows)).toContain("same pane row"));
    expect(wsRows).toEqual(sseRows);
    rendered.unmount();
  });

  it("reuses the opaque Gateway cursor with the real WS client source", () => {
    vi.useFakeTimers();
    const Socket = installFakeWebSocket();
    const opened: string[] = [];

    function Harness() {
      useAgentSession({
        name: "iris",
        reconnectDelayMs: 10,
        openSource: (url) => {
          opened.push(url);
          return openWsSource(url);
        },
      });
      return null;
    }

    render(<Harness />);
    expect(opened[0]).toBe("/api/v1/agent-sessions/iris/events?view=agui");
    expect(Socket.instances[0]!.url).toBe("ws://localhost:3000/api/v1/agent-sessions/iris/events?view=agui");

    act(() => {
      for (const event of sessionTurn(214, "before drop")) {
        Socket.instances[0]!.message(event);
      }
      Socket.instances[0]!.close();
      vi.advanceTimersByTime(10);
    });

    expect(opened[1]).toBe("/api/v1/agent-sessions/iris/events?view=agui&after=epoch%3A214");
    expect(Socket.instances[1]!.url).toBe("ws://localhost:3000/api/v1/agent-sessions/iris/events?view=agui&after=epoch%3A214");
  });

  it("drops stale stream-store afterId on boot-epoch changes with the real WS client source", () => {
    const Socket = installFakeWebSocket();
    const opened: string[] = [];

    function Harness({ epoch }: { epoch: string }) {
      useAgentSession({
        name: "iris",
        observeEpoch: epoch,
        openSource: (url) => {
          opened.push(url);
          return openWsSource(url);
        },
      });
      return null;
    }

    const { rerender } = render(<Harness epoch="boot-a" />);

    act(() => {
      for (const event of sessionTurn(214, "survives")) {
        Socket.instances[0]!.message(event);
      }
    });

    rerender(<Harness epoch="boot-b" />);

    expect(opened[1]).toBe("/api/v1/agent-sessions/iris/events?view=agui");
    expect(Socket.instances[1]!.url).toBe("ws://localhost:3000/api/v1/agent-sessions/iris/events?view=agui");
  });

  it("repairs a dropped open run without duplicate or missing rows after WS reconnect replay", async () => {
    vi.useFakeTimers();
    const Socket = installFakeWebSocket();
    const opened: string[] = [];
    let rows: PaneMessage[] = [];

    function Harness() {
      const { messages } = useAgentSession({
        name: "iris",
        reconnectDelayMs: 10,
        openSource: (url) => {
          opened.push(url);
          return openWsSource(url);
        },
      });
      useEffect(() => {
        rows = messages;
      }, [messages]);
      return null;
    }

    render(<Harness />);

    act(() => {
      for (const event of partialSessionTurn(214, "partial")) {
        Socket.instances[0]!.message(event);
      }
    });
    await act(async () => {
      vi.advanceTimersByTime(16);
    });
    expect(rowText(rows)).toContain("partial");

    act(() => {
      Socket.instances[0]!.close();
      vi.advanceTimersByTime(10);
    });
    expect(opened[1]).toBe("/api/v1/agent-sessions/iris/events?view=agui&after=epoch%3A214");

    act(() => {
      for (const event of sessionTurn(216, "replayed full")) {
        Socket.instances[1]!.message(event);
      }
    });

    expect(rowText(rows)).toContain("replayed full");
    expect(rows.filter((row) => row.id === "agui:r1")).toHaveLength(1);
    expect(rowText(rows)).not.toContain("partialreplayed full");
  });

  it("sends agent-session input over the active WS source and waits for input.ack", async () => {
    const Socket = installFakeWebSocket();
    let send!: (text: string) => Promise<void>;
    const promptCalls: Array<{ name: string; text: string }> = [];

    function Harness() {
      ({ send } = useAgentSession({
        name: "iris",
        openSource: openWsSource,
        postPrompt: async (name, text) => { promptCalls.push({ name, text }); },
      }));
      return null;
    }

    render(<Harness />);
    const socket = Socket.instances[0]!;
    socket.open();

    let sent: Promise<void>;
    act(() => {
      sent = send("socket prompt");
    });

    await waitFor(() => expect(socket.sent).toHaveLength(1));
    const frame = JSON.parse(socket.sent[0]!) as AguiInputFrame;
    expect(frame).toEqual({
      t: "input",
      mode: "session",
      target: "iris",
      text: "socket prompt",
      clientMessageId: expect.stringMatching(/^you:/),
    });
    expect(promptCalls).toHaveLength(0);

    act(() => {
      socket.message(JSON.stringify({
        t: "input.ack",
        clientMessageId: frame.clientMessageId,
        delivered: true,
      }));
    });
    await sent!;
    expect(promptCalls).toHaveLength(0);
  });

  it("does not open a session WebSocket for ordinary Message Post input", async () => {
    const Socket = installFakeWebSocket();
    let send!: (text: string) => Promise<void>;
    const runCalls: Array<{ target: unknown; text: string }> = [];

    function Harness() {
      ({ send } = useAguiConversation({
        thread: "design",
        target: { verb: "post", thread: "design" },
        openSource: openWsSource,
        postRun: async (target, text) => { runCalls.push({ target, text }); },
      }));
      return null;
    }

    render(<Harness />);
    await act(async () => {
      await send("REST bus message");
    });

    expect(Socket.instances).toHaveLength(0);
    expect(runCalls).toEqual([{ target: { verb: "post", thread: "design" }, text: "REST bus message" }]);
  });
});

function WsCaptureHarness({
  onMessages,
}: {
  onMessages: (messages: PaneMessage[]) => void;
}) {
  const { messages } = useAgentSession({
    name: "iris",
    openSource: openWsSource,
  });
  useEffect(() => onMessages(messages), [messages, onMessages]);
  return null;
}
