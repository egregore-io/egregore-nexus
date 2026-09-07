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

  close(code = 1000, reason = ""): void {
    this.readyState = FakeWebSocket.CLOSED;
    const event = new Event("close");
    Object.defineProperties(event, {
      code: { value: code },
      reason: { value: reason },
    });
    this.dispatchEvent(event);
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
    sessionId: "s_iris",
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
    const sseSource = new FakeEventAguiSource("/api/v1/agent-sessions/s_iris/events?view=agui");
    let sseRows: PaneMessage[] = [];
    let wsRows: PaneMessage[] = [];

    const rendered = render(
      <>
        <CaptureHarness source={sseSource} onMessages={(messages) => { sseRows = messages; }} />
        <WsCaptureHarness onMessages={(messages) => { wsRows = messages; }} />
      </>,
    );
    const wsSocket = Socket.instances[0]!;
    expect(wsSocket.url).toBe("ws://localhost:3000/api/v1/agent-sessions/s_iris/events?view=agui");

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

  it.each([false, true])(
    "preserves an unsent draft without name-only fallback before binding or after disconnect: %s",
    async (disconnect) => {
      const Socket = installFakeWebSocket();
      const postPrompt = vi.fn();
      let send: (text: string) => Promise<void> = async () => {};
      function Harness() {
        const session = useAgentSession({
          name: "iris",
          sessionId: "s_iris",
          openSource: openWsSource,
          postPrompt,
        });
        send = session.send;
        return null;
      }
      const rendered = render(<Harness />);
      Socket.instances[0]!.open();
      if (disconnect)
        act(() => {
          Socket.instances[0]!.message({
            t: "session.bound",
            agentId: "a_iris",
            sessionId: "s_iris",
          });
          Socket.instances[0]!.close();
        });
      await act(async () => {
        await expect(send("keep this draft")).rejects.toThrow(/binding/i);
      });
      expect(postPrompt).not.toHaveBeenCalled();
      expect(Socket.instances[0]!.sent).toEqual([]);
      rendered.unmount();
    },
  );

  it("reuses the opaque Gateway cursor with the real WS client source", () => {
    vi.useFakeTimers();
    const Socket = installFakeWebSocket();
    const opened: string[] = [];

    function Harness() {
      useAgentSession({
        name: "iris",
        sessionId: "s_iris",
        reconnectDelayMs: 10,
        openSource: (url) => {
          opened.push(url);
          return openWsSource(url);
        },
      });
      return null;
    }

    render(<Harness />);
    expect(opened[0]).toBe("/api/v1/agent-sessions/s_iris/events?view=agui");
    expect(Socket.instances[0]!.url).toBe("ws://localhost:3000/api/v1/agent-sessions/s_iris/events?view=agui");

    act(() => {
      for (const event of sessionTurn(214, "before drop")) {
        Socket.instances[0]!.message(event);
      }
      Socket.instances[0]!.close();
      vi.advanceTimersByTime(10);
    });

    expect(opened[1]).toBe("/api/v1/agent-sessions/s_iris/events?view=agui&after=epoch%3A214");
    expect(Socket.instances[1]!.url).toBe("ws://localhost:3000/api/v1/agent-sessions/s_iris/events?view=agui&after=epoch%3A214");
  });

  it("rolls back an incomplete same-cursor group before a 1013 replay", async () => {
    vi.useFakeTimers();
    const Socket = installFakeWebSocket();
    const opened: string[] = [];
    let rows: PaneMessage[] = [];

    function Harness() {
      const { messages } = useAgentSession({
        name: "iris",
        sessionId: "s_iris",
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
    const first = Socket.instances[0]!;
    const runStarted = {
      type: EventType.RUN_STARTED,
      threadId: "iris",
      runId: "r1",
      cursor: "epoch:40",
    } as BaseEvent;
    const textStarted = {
      type: EventType.TEXT_MESSAGE_START,
      messageId: "m1",
      role: "assistant",
      cursor: "epoch:40",
    } as BaseEvent;
    const siblingA = {
      type: EventType.TEXT_MESSAGE_CONTENT,
      messageId: "m1",
      delta: "A",
      cursor: "epoch:41",
    } as BaseEvent;
    const siblingB = {
      type: EventType.TEXT_MESSAGE_CONTENT,
      messageId: "m1",
      delta: "B",
      cursor: "epoch:41",
    } as BaseEvent;

    act(() => {
      first.message(runStarted);
      first.message(textStarted);
      first.message(siblingA);
    });
    await act(async () => {
      vi.advanceTimersByTime(16);
    });
    expect(rowText(rows)).toBe("A");

    act(() => {
      first.close(1013, "session.bp:epoch:40");
      vi.advanceTimersByTime(10);
    });

    expect(opened[1]).toBe(
      "/api/v1/agent-sessions/s_iris/events?view=agui&after=epoch%3A40",
    );
    const resumed = Socket.instances[1]!;
    act(() => {
      resumed.message(siblingA);
      resumed.message(siblingB);
      resumed.message({
        type: EventType.TEXT_MESSAGE_END,
        messageId: "m1",
        cursor: "epoch:41",
      } as BaseEvent);
      resumed.message({
        type: EventType.RUN_FINISHED,
        threadId: "iris",
        runId: "r1",
        cursor: "epoch:41",
      } as BaseEvent);
    });
    await act(async () => {
      vi.advanceTimersByTime(16);
    });

    expect(rowText(rows)).toBe("AB");
    expect(rowText(rows).match(/A/g)).toHaveLength(1);
    expect(rowText(rows).match(/B/g)).toHaveLength(1);
  });

  it("preserves an acknowledged optimistic input while rolling back a partial cursor group", async () => {
    vi.useFakeTimers();
    const Socket = installFakeWebSocket();
    const opened: string[] = [];
    let rows: PaneMessage[] = [];
    let send: ((text: string) => Promise<void>) | undefined;

    function Harness() {
      const conversation = useAgentSession({
        name: "iris",
        sessionId: "s_iris",
        you: { who: "test-user", glyph: "T" },
        reconnectDelayMs: 10,
        openSource: (url) => {
          opened.push(url);
          return openWsSource(url);
        },
      });
      send = conversation.send;
      useEffect(() => {
        rows = conversation.messages;
      }, [conversation.messages]);
      return null;
    }

    render(<Harness />);
    const first = Socket.instances[0]!;
    first.open();
    first.message({
      t: "session.bound",
      agentId: "a_iris",
      sessionId: "s_iris",
    });
    const runStarted = {
      type: EventType.RUN_STARTED,
      threadId: "iris",
      runId: "r1",
      cursor: "epoch:40",
    } as BaseEvent;
    const textStarted = {
      type: EventType.TEXT_MESSAGE_START,
      messageId: "m1",
      role: "assistant",
      cursor: "epoch:40",
    } as BaseEvent;
    const siblingA = {
      type: EventType.TEXT_MESSAGE_CONTENT,
      messageId: "m1",
      delta: "A",
      cursor: "epoch:41",
    } as BaseEvent;
    const siblingB = {
      type: EventType.TEXT_MESSAGE_CONTENT,
      messageId: "m1",
      delta: "B",
      cursor: "epoch:41",
    } as BaseEvent;

    act(() => {
      first.message(runStarted);
      first.message(textStarted);
      first.message(siblingA);
    });
    await act(async () => {
      vi.advanceTimersByTime(16);
    });
    expect(rowText(rows)).toBe("A");

    let sendPromise!: Promise<void>;
    act(() => {
      sendPromise = send!("MINE");
    });
    const input = JSON.parse(first.sent.at(-1)!) as {
      t: string;
      text: string;
      clientMessageId: string;
    };
    expect(input).toMatchObject({ t: "input", text: "MINE" });
    act(() => {
      first.message({
        t: "input.ack",
        sessionId: "s_iris",
        clientMessageId: input.clientMessageId,
        delivered: true,
      });
    });
    await act(async () => {
      await sendPromise;
    });
    expect(rowText(rows)).toContain("MINE");

    act(() => {
      first.close(1013, "session.bp:epoch:40");
      vi.advanceTimersByTime(10);
    });

    expect(opened[1]).toBe(
      "/api/v1/agent-sessions/s_iris/events?view=agui&after=epoch%3A40",
    );
    expect(rowText(rows)).toContain("MINE");

    const resumed = Socket.instances[1]!;
    act(() => {
      resumed.message(siblingA);
      resumed.message(siblingB);
      resumed.message({
        type: EventType.TEXT_MESSAGE_END,
        messageId: "m1",
        cursor: "epoch:41",
      } as BaseEvent);
      resumed.message({
        type: EventType.RUN_FINISHED,
        threadId: "iris",
        runId: "r1",
        cursor: "epoch:41",
      } as BaseEvent);
      resumed.message({
        type: EventType.TEXT_MESSAGE_START,
        messageId: input.clientMessageId,
        role: "user",
        name: "test-user",
        kind: "human",
        cursor: "epoch:42",
      } as BaseEvent);
      resumed.message({
        type: EventType.TEXT_MESSAGE_CONTENT,
        messageId: input.clientMessageId,
        delta: "MINE",
        cursor: "epoch:42",
      } as BaseEvent);
      resumed.message({
        type: EventType.TEXT_MESSAGE_END,
        messageId: input.clientMessageId,
        cursor: "epoch:42",
      } as BaseEvent);
    });
    await act(async () => {
      vi.advanceTimersByTime(16);
    });

    expect(rowText(rows).match(/A/g)).toHaveLength(1);
    expect(rowText(rows).match(/B/g)).toHaveLength(1);
    expect(rowText(rows).match(/MINE/g)).toHaveLength(1);
  });

  it("preserves two optimistic inputs through out-of-order acknowledgements and replay echoes", async () => {
    vi.useFakeTimers();
    const Socket = installFakeWebSocket();
    let rows: PaneMessage[] = [];
    let send: ((text: string) => Promise<void>) | undefined;

    function Harness() {
      const conversation = useAgentSession({
        name: "iris",
        sessionId: "s_iris",
        you: { who: "test-user", glyph: "T" },
        reconnectDelayMs: 10,
        openSource: openWsSource,
      });
      send = conversation.send;
      useEffect(() => {
        rows = conversation.messages;
      }, [conversation.messages]);
      return null;
    }

    render(<Harness />);
    const first = Socket.instances[0]!;
    first.open();
    first.message({
      t: "session.bound",
      agentId: "a_iris",
      sessionId: "s_iris",
    });
    const siblingA = {
      type: EventType.TEXT_MESSAGE_CONTENT,
      messageId: "m1",
      delta: "A",
      cursor: "epoch:41",
    } as BaseEvent;
    const siblingB = {
      type: EventType.TEXT_MESSAGE_CONTENT,
      messageId: "m1",
      delta: "B",
      cursor: "epoch:41",
    } as BaseEvent;
    act(() => {
      first.message({
        type: EventType.RUN_STARTED,
        threadId: "iris",
        runId: "r1",
        cursor: "epoch:40",
      } as BaseEvent);
      first.message({
        type: EventType.TEXT_MESSAGE_START,
        messageId: "m1",
        role: "assistant",
        cursor: "epoch:40",
      } as BaseEvent);
      first.message(siblingA);
    });
    await act(async () => {
      vi.advanceTimersByTime(16);
    });

    let firstSend!: Promise<void>;
    let secondSend!: Promise<void>;
    act(() => {
      firstSend = send!("FIRST");
      secondSend = send!("SECOND");
    });
    const inputs = first.sent.slice(-2).map((payload) => JSON.parse(payload) as {
      t: string;
      text: string;
      clientMessageId: string;
    });
    expect(inputs.map((input) => input.text)).toEqual(["FIRST", "SECOND"]);
    act(() => {
      first.message({
        t: "input.ack",
        sessionId: "s_iris",
        clientMessageId: inputs[1]!.clientMessageId,
        delivered: true,
      });
      first.message({
        t: "input.ack",
        sessionId: "s_iris",
        clientMessageId: inputs[0]!.clientMessageId,
        delivered: true,
      });
    });
    await act(async () => {
      await Promise.all([secondSend, firstSend]);
    });

    const expectedInputs = inputs.map((input) => [input.clientMessageId, input.text]);
    expect(
      rows.filter((row) => row.isYou).map((row) => [row.id, rowText([row])]),
    ).toEqual(expectedInputs);

    act(() => {
      first.close(1013, "session.bp:epoch:40");
      vi.advanceTimersByTime(10);
    });
    expect(
      rows.filter((row) => row.isYou).map((row) => [row.id, rowText([row])]),
    ).toEqual(expectedInputs);

    const resumed = Socket.instances[1]!;
    const echo = (input: (typeof inputs)[number], cursor: string): void => {
      resumed.message({
        type: EventType.TEXT_MESSAGE_START,
        messageId: input.clientMessageId,
        role: "user",
        name: "test-user",
        kind: "human",
        cursor,
      } as BaseEvent);
      resumed.message({
        type: EventType.TEXT_MESSAGE_CONTENT,
        messageId: input.clientMessageId,
        delta: input.text,
        cursor,
      } as BaseEvent);
      resumed.message({
        type: EventType.TEXT_MESSAGE_END,
        messageId: input.clientMessageId,
        cursor,
      } as BaseEvent);
    };
    act(() => {
      resumed.message(siblingA);
      resumed.message(siblingB);
      resumed.message({
        type: EventType.TEXT_MESSAGE_END,
        messageId: "m1",
        cursor: "epoch:41",
      } as BaseEvent);
      resumed.message({
        type: EventType.RUN_FINISHED,
        threadId: "iris",
        runId: "r1",
        cursor: "epoch:41",
      } as BaseEvent);
      echo(inputs[1]!, "epoch:42");
      echo(inputs[0]!, "epoch:43");
    });
    await act(async () => {
      vi.advanceTimersByTime(16);
    });

    expect(
      rows.filter((row) => row.isYou).map((row) => [row.id, rowText([row])]),
    ).toEqual(expectedInputs);
    expect(rows.filter((row) => rowText([row]) === "FIRST")).toHaveLength(1);
    expect(rows.filter((row) => rowText([row]) === "SECOND")).toHaveLength(1);
    expect(rowText(rows).match(/A/g)).toHaveLength(1);
    expect(rowText(rows).match(/B/g)).toHaveLength(1);
  });

  it("preserves completed transcript state while rolling back only the partial cursor group", async () => {
    vi.useFakeTimers();
    const Socket = installFakeWebSocket();
    let rows: PaneMessage[] = [];

    function Harness() {
      const { messages } = useAgentSession({
        name: "iris",
        sessionId: "s_iris",
        reconnectDelayMs: 10,
        openSource: openWsSource,
      });
      useEffect(() => {
        rows = messages;
      }, [messages]);
      return null;
    }

    render(<Harness />);
    const first = Socket.instances[0]!;
    const siblingA = {
      type: EventType.TEXT_MESSAGE_CONTENT,
      messageId: "m1",
      delta: "A",
      cursor: "epoch:41",
    } as BaseEvent;
    const siblingB = {
      type: EventType.TEXT_MESSAGE_CONTENT,
      messageId: "m1",
      delta: "B",
      cursor: "epoch:41",
    } as BaseEvent;
    act(() => {
      first.message({
        type: EventType.RUN_STARTED,
        threadId: "iris",
        runId: "prior-run",
        cursor: "epoch:38",
      } as BaseEvent);
      first.message({
        type: EventType.TEXT_MESSAGE_START,
        messageId: "prior-message",
        role: "assistant",
        cursor: "epoch:38",
      } as BaseEvent);
      first.message({
        type: EventType.TEXT_MESSAGE_CONTENT,
        messageId: "prior-message",
        delta: "SEED",
        cursor: "epoch:39",
      } as BaseEvent);
      first.message({
        type: EventType.TEXT_MESSAGE_END,
        messageId: "prior-message",
        cursor: "epoch:39",
      } as BaseEvent);
      first.message({
        type: EventType.RUN_FINISHED,
        threadId: "iris",
        runId: "prior-run",
        cursor: "epoch:39",
      } as BaseEvent);
      first.message({
        type: EventType.RUN_STARTED,
        threadId: "iris",
        runId: "r1",
        cursor: "epoch:40",
      } as BaseEvent);
      first.message({
        type: EventType.TEXT_MESSAGE_START,
        messageId: "m1",
        role: "assistant",
        cursor: "epoch:40",
      } as BaseEvent);
      first.message(siblingA);
    });
    await act(async () => {
      vi.advanceTimersByTime(16);
    });
    expect(rowText(rows)).toBe("SEEDA");

    act(() => {
      first.close(1013, "session.bp:epoch:40");
      vi.advanceTimersByTime(10);
    });
    expect(rowText(rows)).toBe("SEED");

    const resumed = Socket.instances[1]!;
    act(() => {
      resumed.message(siblingA);
      resumed.message(siblingB);
      resumed.message({
        type: EventType.TEXT_MESSAGE_END,
        messageId: "m1",
        cursor: "epoch:41",
      } as BaseEvent);
      resumed.message({
        type: EventType.RUN_FINISHED,
        threadId: "iris",
        runId: "r1",
        cursor: "epoch:41",
      } as BaseEvent);
    });
    await act(async () => {
      vi.advanceTimersByTime(16);
    });

    expect(rowText(rows)).toBe("SEEDAB");
    expect(rows.filter((row) => rowText([row]) === "SEED")).toHaveLength(1);
  });

  it("drops stale stream-store afterId on boot-epoch changes with the real WS client source", () => {
    const Socket = installFakeWebSocket();
    const opened: string[] = [];

    function Harness({ epoch }: { epoch: string }) {
      useAgentSession({
        name: "iris",
        sessionId: "s_iris",
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

    expect(opened[1]).toBe("/api/v1/agent-sessions/s_iris/events?view=agui");
    expect(Socket.instances[1]!.url).toBe("ws://localhost:3000/api/v1/agent-sessions/s_iris/events?view=agui");
  });

  it("repairs a dropped open run without duplicate or missing rows after WS reconnect replay", async () => {
    vi.useFakeTimers();
    const Socket = installFakeWebSocket();
    const opened: string[] = [];
    let rows: PaneMessage[] = [];

    function Harness() {
      const { messages } = useAgentSession({
        name: "iris",
        sessionId: "s_iris",
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
    expect(opened[1]).toBe("/api/v1/agent-sessions/s_iris/events?view=agui&after=epoch%3A214");

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
        sessionId: "s_iris",
        openSource: openWsSource,
        postPrompt: async (name, text) => { promptCalls.push({ name, text }); },
      }));
      return null;
    }

    render(<Harness />);
    const socket = Socket.instances[0]!;
    socket.open();
    socket.message(
      JSON.stringify({
        t: "session.bound",
        agentId: "a_iris",
        sessionId: "s_iris",
      }),
    );

    let sent: Promise<void>;
    act(() => {
      sent = send("socket prompt");
    });

    await waitFor(() => expect(socket.sent).toHaveLength(1));
    const frame = JSON.parse(socket.sent[0]!) as AguiInputFrame;
    expect(frame).toEqual({
      t: "input",
      text: "socket prompt",
      agentId: "a_iris",
      expectedSessionId: "s_iris",
      clientMessageId: expect.stringMatching(/^you:/),
    });
    expect(promptCalls).toHaveLength(0);

    act(() => {
      socket.message(JSON.stringify({
        t: "input.ack",
          sessionId: "s_iris",
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
    sessionId: "s_iris",
    openSource: openWsSource,
  });
  useEffect(() => onMessages(messages), [messages, onMessages]);
  return null;
}
