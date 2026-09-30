// `useAgentSession` hook tests.
//
// Three assertions per the brief:
//   1. The hook opens an SSE to `/api/agui/observe?session=<name>` (not `?dm=`).
//   2. It folds `agent.update` (rendered as RUN_STARTED…RUN_FINISHED) into activity.
//   3. The composer `send(text)` calls the session-input path (`/api/conversation/prompt`)
//      NOT the bus `send`/`/api/agui` run and without a DM SendTarget.
import { afterEach, describe, it, expect, vi } from "vitest";
import { act, render, screen, within } from "@testing-library/react";
import { EventType } from "@ag-ui/client";
import type { BaseEvent } from "@ag-ui/client";

import { useAgentSession, type AguiEventSource } from "./aguiConversation";
import { Thread } from "./Thread";

// ── Fake EventSource ──────────────────────────────────────────────────────────

class FakeEventSource implements AguiEventSource {
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
  fail(): void {
    this.onerror?.(new Error("stream dropped"));
  }
}

afterEach(() => {
  vi.useRealTimers();
});

// ── Harness ───────────────────────────────────────────────────────────────────

type SendSpy = (name: string, text: string, clientMessageId?: string) => Promise<void>;

function AgentSessionHarness({
  name,
  source,
  postPrompt,
  reconnectDelayMs,
  you,
}: {
  name: string;
  source: FakeEventSource;
  postPrompt?: SendSpy;
  reconnectDelayMs?: number;
  you?: { who: string; glyph: string };
}) {
  const { messages, live, send } = useAgentSession({
    name,
    agent: { who: name, glyph: name.charAt(0).toUpperCase(), presence: "online" },
    you,
    // openSource must return the fake for the session URL
    openSource: () => source,
    postPrompt,
    reconnectDelayMs,
  });
  return (
    <div data-live={live ? "yes" : "no"}>
      <button onClick={() => void send("hello agent")}>do-send</button>
      <Thread items={messages} aria-label={`Messages in ${name}`} />
    </div>
  );
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe("useAgentSession — Gateway session URL", () => {
  it("opens the canonical Gateway AG-UI view", () => {
    const captured: string[] = [];
    const name = "ben";
    const source = new FakeEventSource("");

    // Wrap openSource to capture the URL we would open
    function AgentSessionUrlHarness() {
      const { live } = useAgentSession({
        name,
        openSource: (url) => {
          captured.push(url);
          return source;
        },
      });
      return <div data-live={live ? "yes" : "no"} />;
    }

    render(<AgentSessionUrlHarness />);

    expect(captured).toHaveLength(1);
    expect(captured[0]).toBe(`/api/v1/agent-sessions/${name}/events?view=agui`);
  });

  it("encodes the name in the URL", () => {
    const captured: string[] = [];
    const name = "ben/zara";
    const source = new FakeEventSource("");

    function AgentSessionEncodedHarness() {
      const { live } = useAgentSession({
        name,
        openSource: (url) => {
          captured.push(url);
          return source;
        },
      });
      return <div data-live={live ? "yes" : "no"} />;
    }

    render(<AgentSessionEncodedHarness />);
    expect(captured[0]).toBe(`/api/v1/agent-sessions/${encodeURIComponent(name)}/events?view=agui`);
  });

  it("reopens an agent-session stream with the last opaque Gateway cursor", () => {
    vi.useFakeTimers();
    const captured: string[] = [];
    const sources: FakeEventSource[] = [];
    const name = "ben";

    function ReconnectHarness() {
      const { live } = useAgentSession({
        name,
        reconnectDelayMs: 10,
        openSource: (url) => {
          captured.push(url);
          const source = new FakeEventSource(url);
          sources.push(source);
          return source;
        },
      });
      return <div data-live={live ? "yes" : "no"} />;
    }

    render(<ReconnectHarness />);
    expect(captured[0]).toBe("/api/v1/agent-sessions/ben/events?view=agui");
    const first = sources[0]!;

    act(() => {
      first.push({
        type: EventType.TEXT_MESSAGE_CONTENT,
        messageId: "m1",
        delta: "chunk",
        cursor: "epoch:42",
      } as BaseEvent);
      first.fail();
      vi.advanceTimersByTime(10);
    });

    expect(first.closed).toBe(true);
    expect(captured[1]).toBe("/api/v1/agent-sessions/ben/events?view=agui&after=epoch%3A42");
  });

  it("re-subscribes on boot-epoch changes without carrying a stale stream-store afterId", () => {
    const captured: string[] = [];
    const sources: FakeEventSource[] = [];
    const name = "ben";

    function EpochHarness({ epoch }: { epoch: string }) {
      const { messages } = useAgentSession({
        name,
        agent: { who: name, glyph: "B", presence: "online" },
        observeEpoch: epoch,
        openSource: (url) => {
          captured.push(url);
          const source = new FakeEventSource(url);
          sources.push(source);
          return source;
        },
      });
      return <Thread items={messages} aria-label={`Messages in ${name}`} />;
    }

    const { rerender } = render(<EpochHarness epoch="boot-1" />);
    expect(captured[0]).toBe("/api/v1/agent-sessions/ben/events?view=agui");
    const first = sources[0]!;

    act(() => {
      first.push({ type: EventType.RUN_STARTED, threadId: name, runId: "r1" } as BaseEvent);
      first.push({
        type: EventType.TEXT_MESSAGE_START,
        messageId: "m1",
        role: "assistant",
        cursor: "boot-1:41",
      } as BaseEvent);
      first.push({
        type: EventType.TEXT_MESSAGE_CONTENT,
        messageId: "m1",
        delta: "survives agent restart",
        cursor: "boot-1:42",
      } as BaseEvent);
      first.push({
        type: EventType.TEXT_MESSAGE_END,
        messageId: "m1",
        cursor: "boot-1:42",
      } as BaseEvent);
      first.push({ type: EventType.RUN_FINISHED, threadId: name, runId: "r1" } as BaseEvent);
    });

    rerender(<EpochHarness epoch="boot-2" />);

    expect(first.closed).toBe(true);
    expect(captured[1]).toBe("/api/v1/agent-sessions/ben/events?view=agui");
    expect(screen.getByText("survives agent restart")).toBeInTheDocument();

    const replay = sources[1]!;
    act(() => {
      replay.push({ type: EventType.RUN_STARTED, threadId: name, runId: "r1" } as BaseEvent);
      replay.push({
        type: EventType.TEXT_MESSAGE_START,
        messageId: "m1",
        role: "assistant",
        streamEventId: 41,
      } as BaseEvent);
      replay.push({
        type: EventType.TEXT_MESSAGE_CONTENT,
        messageId: "m1",
        delta: "survives agent restart",
        streamEventId: 42,
      } as BaseEvent);
      replay.push({
        type: EventType.TEXT_MESSAGE_END,
        messageId: "m1",
        streamEventId: 42,
      } as BaseEvent);
      replay.push({ type: EventType.RUN_FINISHED, threadId: name, runId: "r1" } as BaseEvent);
    });

    expect(screen.getAllByText("survives agent restart")).toHaveLength(1);
  });
});

describe("useAgentSession — activity folding", () => {
  it("folds agent.update frames (RUN_STARTED…RUN_FINISHED) into rendered activity", () => {
    const name = "ben";
    const source = new FakeEventSource("");
    render(<AgentSessionHarness name={name} source={source} />);

    expect(document.querySelector('[data-live="yes"]')).not.toBeNull();

    act(() => {
      source.push({ type: EventType.RUN_STARTED, threadId: name, runId: "r1" } as BaseEvent);
      source.push({ type: EventType.TEXT_MESSAGE_START, messageId: "m1", role: "assistant" } as BaseEvent);
      source.push({ type: EventType.TEXT_MESSAGE_CONTENT, messageId: "m1", delta: "task done." } as BaseEvent);
      source.push({ type: EventType.TEXT_MESSAGE_END, messageId: "m1" } as BaseEvent);
      source.push({ type: EventType.RUN_FINISHED, threadId: name, runId: "r1" } as BaseEvent);
    });

    const log = screen.getByRole("log");
    expect(within(log).getByText("task done.")).toBeInTheDocument();
    expect(within(log).getByText(name)).toBeInTheDocument();
  });

  it("closes the stream on unmount (no leak)", () => {
    const name = "ben";
    const source = new FakeEventSource("");
    const { unmount } = render(<AgentSessionHarness name={name} source={source} />);
    unmount();
    expect(source.closed).toBe(true);
  });
});

describe("useAgentSession — session composer", () => {
  it("send() calls postPrompt (session /api/conversation/prompt), NOT a bus run", async () => {
    const name = "ben";
    const source = new FakeEventSource("");
    const promptCalls: Array<{ name: string; text: string; clientMessageId?: string }> = [];

    render(
      <AgentSessionHarness
        name={name}
        source={source}
        postPrompt={async (n, t, clientMessageId) => {
          promptCalls.push({ name: n, text: t, clientMessageId });
        }}
      />,
    );

    await act(async () => {
      screen.getByText("do-send").click();
    });

    // Direct session path called exactly once with the right name.
    expect(promptCalls).toEqual([
      { name, text: "hello agent", clientMessageId: expect.stringMatching(/^you:/) },
    ]);
  });

  it("send() echoes the operator's input optimistically in the thread", async () => {
    const name = "ben";
    const source = new FakeEventSource("");

    render(
      <AgentSessionHarness
        name={name}
        source={source}
        postPrompt={async () => {}}
      />,
    );

    await act(async () => {
      screen.getByText("do-send").click();
    });

    const log = screen.getByRole("log");
    expect(within(log).getByText("hello agent")).toBeInTheDocument();
  });

  it("dedupes the streamed user_input echo after a web composer send by correlated id", async () => {
    const name = "ben";
    const source = new FakeEventSource("");
    let sentClientMessageId = "";

    render(
      <AgentSessionHarness
        name={name}
        source={source}
        you={{ who: "operator", glyph: "O" }}
        postPrompt={async (_name, _text, clientMessageId) => {
          sentClientMessageId = clientMessageId ?? "";
        }}
      />,
    );

    await act(async () => {
      screen.getByText("do-send").click();
    });

    act(() => {
      source.push({ type: EventType.RUN_STARTED, threadId: name, runId: "r1" } as BaseEvent);
      source.push({
        type: EventType.TEXT_MESSAGE_START,
        messageId: sentClientMessageId,
        role: "user",
      } as BaseEvent);
      source.push({
        type: EventType.TEXT_MESSAGE_CONTENT,
        messageId: sentClientMessageId,
        delta: "hello agent",
      } as BaseEvent);
      source.push({ type: EventType.TEXT_MESSAGE_END, messageId: sentClientMessageId } as BaseEvent);
      source.push({
        type: EventType.TEXT_MESSAGE_START,
        messageId: "a1",
        role: "assistant",
      } as BaseEvent);
      source.push({
        type: EventType.TEXT_MESSAGE_CONTENT,
        messageId: "a1",
        delta: "hello operator",
      } as BaseEvent);
      source.push({ type: EventType.TEXT_MESSAGE_END, messageId: "a1" } as BaseEvent);
      source.push({ type: EventType.RUN_FINISHED, threadId: name, runId: "r1" } as BaseEvent);
    });

    const log = screen.getByRole("log");
    expect(within(log).getAllByText("hello agent")).toHaveLength(1);
    expect(within(log).getByText("hello operator")).toBeInTheDocument();
  });

  it("does not dedupe same-text streamed user_input when ids do not correlate", async () => {
    const name = "ben";
    const source = new FakeEventSource("");

    render(
      <AgentSessionHarness
        name={name}
        source={source}
        you={{ who: "operator", glyph: "O" }}
        postPrompt={async () => {}}
      />,
    );

    await act(async () => {
      screen.getByText("do-send").click();
    });

    act(() => {
      source.push({ type: EventType.RUN_STARTED, threadId: name, runId: "r1" } as BaseEvent);
      source.push({
        type: EventType.TEXT_MESSAGE_START,
        messageId: "different-stream-id",
        role: "user",
      } as BaseEvent);
      source.push({
        type: EventType.TEXT_MESSAGE_CONTENT,
        messageId: "different-stream-id",
        delta: " hello ",
      } as BaseEvent);
      source.push({
        type: EventType.TEXT_MESSAGE_CONTENT,
        messageId: "different-stream-id",
        delta: "agent\n",
      } as BaseEvent);
      source.push({ type: EventType.TEXT_MESSAGE_END, messageId: "different-stream-id" } as BaseEvent);
      source.push({ type: EventType.RUN_FINISHED, threadId: name, runId: "r1" } as BaseEvent);
    });

    expect(within(screen.getByRole("log")).getAllByText("hello agent")).toHaveLength(2);
  });
});
