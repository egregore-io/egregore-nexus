import { act, render, screen, waitFor, within } from "@testing-library/react";
import { EventType } from "@ag-ui/client";
import type { BaseEvent } from "@ag-ui/client";
import { afterEach, describe, expect, it, vi } from "vitest";

import { useIdentityName, useIdentityStore } from "@app/identity";
import { AppProviders } from "@app/providers";
import type { SendTarget } from "@shared/types";

import {
  useAguiConversation,
  useAgentSession,
  type AguiEventSource,
  type AguiInputFrame,
} from "./aguiConversation";
import { LiveChannelPane } from "./ConversationPane";
import type { ConversationView } from "./conversationView";
import { Thread } from "./Thread";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  window.localStorage.clear();
  useIdentityStore.setState({ name: null });
});

const AUTHENTICATED_HUMAN = {
  name: "alice",
  sessionId: "s_alice",
  kind: "human",
  role: "operator",
  tier: "admin",
  project: "default",
  presence: "online",
};

function IdentityProbe() {
  return <output data-testid="identity-name">{useIdentityName() ?? "none"}</output>;
}

class FakeEventSource implements AguiEventSource {
  onmessage: ((event: { data: string }) => void) | null = null;
  onerror: ((event: unknown) => void) | null = null;
  closed = false;
  inputs: AguiInputFrame[] = [];
  delivered = true;

  push(event: BaseEvent): void {
    this.onmessage?.({ data: JSON.stringify(event) });
  }

  async sendInput(frame: AguiInputFrame): Promise<boolean> {
    this.inputs.push(frame);
    return this.delivered;
  }

  close(): void {
    this.closed = true;
  }
}

function SessionHarness({ source }: { source: FakeEventSource }) {
  const { messages, live } = useAguiConversation({
    thread: "s_ben",
    sessionName: "ben",
    sessionStreamId: "s_ben",
    sendMode: "session",
    agent: { who: "ben", glyph: "B", presence: "online" },
    openSource: () => source,
  });
  return (
    <div data-live={live ? "yes" : "no"}>
      <Thread items={messages} aria-label="Messages in ben" />
    </div>
  );
}

function AuthenticatedSessionHarness({ source }: { source: FakeEventSource }) {
  const operator = useIdentityName();
  const { messages, send } = useAgentSession({
    name: "ben",
    sessionId: "s_ben",
    agent: { who: "ben", glyph: "B", presence: "online" },
    you: operator
      ? { who: operator, glyph: operator.charAt(0).toUpperCase() }
      : undefined,
    openSource: () => source,
    postPrompt: async () => {},
  });
  return (
    <>
      <IdentityProbe />
      <button type="button" onClick={() => void send("authenticated session input")}>Send session input</button>
      <Thread items={messages} aria-label="Messages in ben" />
    </>
  );
}

function pushSessionTurn(source: FakeEventSource): void {
  source.push({ type: EventType.RUN_STARTED, threadId: "s_ben", runId: "r1" } as BaseEvent);
  source.push({ type: EventType.REASONING_MESSAGE_START, messageId: "reason" } as BaseEvent);
  source.push({ type: EventType.REASONING_MESSAGE_CONTENT, messageId: "reason", delta: "Checking the diff…" } as BaseEvent);
  source.push({ type: EventType.REASONING_MESSAGE_END, messageId: "reason" } as BaseEvent);
  source.push({ type: EventType.TOOL_CALL_START, toolCallId: "tool", toolCallName: "git rebase" } as BaseEvent);
  source.push({ type: EventType.TOOL_CALL_RESULT, toolCallId: "tool", content: "rebased clean" } as BaseEvent);
  source.push({ type: EventType.TEXT_MESSAGE_START, messageId: "answer" } as BaseEvent);
  source.push({ type: EventType.TEXT_MESSAGE_CONTENT, messageId: "answer", delta: "all done" } as BaseEvent);
  source.push({ type: EventType.TEXT_MESSAGE_END, messageId: "answer" } as BaseEvent);
  source.push({ type: EventType.RUN_FINISHED, threadId: "s_ben", runId: "r1" } as BaseEvent);
}

describe("agent session AG-UI pane", () => {
  it("renders semantic thinking, tool and text frames through the existing pane", () => {
    const source = new FakeEventSource();
    render(<SessionHarness source={source} />);
    expect(document.querySelector('[data-live="yes"]')).not.toBeNull();

    act(() => pushSessionTurn(source));

    const log = screen.getByRole("log");
    expect(within(log).getByText("Checking the diff…")).toBeInTheDocument();
    expect(within(log).getByText("git rebase")).toBeInTheDocument();
    expect(within(log).getByText("rebased clean")).toBeInTheDocument();
    expect(within(log).getByText("all done")).toBeInTheDocument();
  });

  it("closes the Gateway session source on unmount", () => {
    const source = new FakeEventSource();
    const { unmount } = render(<SessionHarness source={source} />);
    unmount();
    expect(source.closed).toBe(true);
  });

  it("sends session input over the live bidirectional source", async () => {
    const source = new FakeEventSource();
    let send!: (text: string) => Promise<void>;
    const prompt = vi.fn();

    function Harness() {
      ({ send } = useAguiConversation({
        thread: "s_ben",
        sessionName: "ben",
        sessionStreamId: "s_ben",
        sendMode: "session",
        openSource: () => source,
        postPrompt: prompt,
      }));
      return null;
    }

    render(<Harness />);
    await act(async () => send("socket prompt"));

    expect(prompt).not.toHaveBeenCalled();
    expect(source.inputs).toEqual([{
      t: "input",
      text: "socket prompt",
      clientMessageId: expect.stringMatching(/^you:/),
    }]);
  });

  it("attributes optimistic session input to authenticated human, never the target agent", async () => {
    window.localStorage.setItem("nexus.identity", JSON.stringify({
      state: { name: "ben" },
      version: 0,
    }));
    useIdentityStore.setState({ name: "ben" });
    vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL) => {
      if (String(input).includes("/api/v1/whoami")) {
        return Promise.resolve(new Response(JSON.stringify(AUTHENTICATED_HUMAN), { status: 200 }));
      }
      return Promise.resolve(new Response(null, { status: 404 }));
    }));
    const source = new FakeEventSource();

    render(
      <AppProviders>
        <AuthenticatedSessionHarness source={source} />
      </AppProviders>,
    );

    await waitFor(() => {
      expect(screen.getByTestId("identity-name")).toHaveTextContent("alice");
    });
    await act(async () => screen.getByRole("button", { name: "Send session input" }).click());
    const body = await screen.findByText("authenticated session input");
    const row = body.closest("article");
    expect(row).not.toBeNull();
    expect(within(row!).getByText("alice")).toBeInTheDocument();
    expect(within(row!).getByText("you")).toBeInTheDocument();
    expect(within(row!).queryByText("ben")).not.toBeInTheDocument();
  });
});

describe("ordinary Gateway message panes", () => {
  it("attributes hydrated and optimistic DM rows to authenticated human, never the target agent", async () => {
    window.localStorage.setItem("nexus.identity", JSON.stringify({
      state: { name: "ben" },
      version: 0,
    }));
    useIdentityStore.setState({ name: "ben" });
    vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.includes("/api/v1/whoami")) {
        return Promise.resolve(new Response(JSON.stringify(AUTHENTICATED_HUMAN), { status: 200 }));
      }
      if (init?.method === "POST") {
        return Promise.resolve(new Response(JSON.stringify({ messageId: "m_ack" }), { status: 201 }));
      }
      if (!url.includes("after=")) {
        return Promise.resolve(new Response(JSON.stringify([{
          messageId: "m_alice",
          from: "alice",
          fromKind: "human",
          when: 1_700_000_001_000,
          body: "authenticated backlog",
          cursor: { createdAt: 1_700_000_001_000, rowid: 0, opaque: "c1" },
        }]), { status: 200 }));
      }
      return new Promise<Response>(() => {});
    }));
    const view: ConversationView = {
      key: "ben",
      title: "ben",
      target: { verb: "dm", name: "ben" },
      composerLabel: "Message ben",
    };

    render(
      <AppProviders>
        <IdentityProbe />
        <LiveChannelPane view={view} thread="ben" />
      </AppProviders>,
    );

    await waitFor(() => {
      expect(screen.getByTestId("identity-name")).toHaveTextContent("alice");
    });
    const backlogBody = await screen.findByText("authenticated backlog");
    const backlogRow = backlogBody.closest("article");
    expect(backlogRow).not.toBeNull();
    expect(within(backlogRow!).getByText("alice")).toBeInTheDocument();
    expect(within(backlogRow!).getByText("you")).toBeInTheDocument();

    const box = screen.getByRole("textbox", { name: "Message ben" });
    await act(async () => {
      box.textContent = "authenticated optimistic echo";
      screen.getByRole("button", { name: "Send" }).click();
    });
    const optimisticBody = await screen.findByText("authenticated optimistic echo");
    const optimisticRow = optimisticBody.closest("article");
    expect(optimisticRow).not.toBeNull();
    expect(within(optimisticRow!).getByText("alice")).toBeInTheDocument();
    expect(within(optimisticRow!).getByText("you")).toBeInTheDocument();
    expect(within(optimisticRow!).queryByText("ben")).not.toBeInTheDocument();
  });

  it("hydrates a DM from Gateway REST and never opens an AG-UI socket", async () => {
    const requests: string[] = [];
    vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      requests.push(url);
      if (url.includes("/api/v1/whoami")) {
        return Promise.resolve(new Response(JSON.stringify(AUTHENTICATED_HUMAN), { status: 200 }));
      }
      if (!url.includes("after=")) {
        return Promise.resolve(new Response(JSON.stringify([{
          messageId: "m_dm",
          from: "ben",
          when: 1_700_000_001_000,
          body: "canonical DM history",
          cursor: { createdAt: 1_700_000_001_000, rowid: 0, opaque: "c1" },
        }]), { status: 200 }));
      }
      return new Promise<Response>(() => {});
    }));
    const view: ConversationView = {
      key: "ben",
      title: "ben",
      target: { verb: "dm", name: "ben" },
      composerLabel: "Message ben",
    };

    render(
      <AppProviders>
        <IdentityProbe />
        <LiveChannelPane view={view} thread="ben" />
      </AppProviders>,
    );

    await waitFor(() => {
      expect(screen.getByTestId("identity-name")).toHaveTextContent("alice");
    });
    expect(await screen.findByText("canonical DM history")).toBeInTheDocument();
    expect(requests).toContain("/api/v1/dms/ben/history?limit=100");
    expect(requests.some((url) =>
      url.includes("/api/v1/dms/ben/history?limit=100&after=c1&waitMs=30000"),
    )).toBe(true);
    expect(requests.every((url) => !url.includes("/api/agui/"))).toBe(true);
  });

  it("posts once through Gateway REST, renders optimistically and reconciles to the Ack id", async () => {
    const posts: Array<{ to: SendTarget; body: string }> = [];
    vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.includes("/api/v1/whoami")) {
        return Promise.resolve(new Response(JSON.stringify(AUTHENTICATED_HUMAN), { status: 200 }));
      }
      if (init?.method === "POST") {
        posts.push(JSON.parse(String(init.body)) as { to: SendTarget; body: string });
        return Promise.resolve(new Response(JSON.stringify({ messageId: "m_ack" }), { status: 201 }));
      }
      if (!url.includes("after=origin")) {
        return Promise.resolve(new Response(JSON.stringify([]), { status: 200 }));
      }
      return new Promise<Response>(() => {});
    }));
    const view: ConversationView = {
      key: "design",
      title: "design",
      target: { verb: "post", thread: "design" },
      composerLabel: "Message #design",
      composerPlaceholder: "Message #design",
    };

    render(
      <AppProviders>
        <IdentityProbe />
        <LiveChannelPane view={view} thread="design" />
      </AppProviders>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("identity-name")).toHaveTextContent("alice");
    });
    const box = screen.getByRole("textbox", { name: "Message #design" });
    await act(async () => {
      box.textContent = "hello from WebUI";
      screen.getByRole("button", { name: "Send" }).click();
    });

    await waitFor(() => expect(posts).toHaveLength(1));
    expect(posts[0]).toMatchObject({
      to: { verb: "post", thread: "design" },
      body: "hello from WebUI",
    });
    expect(screen.getByText("hello from WebUI")).toBeInTheDocument();
  });

  it("switching a reused pane sends to the active target", async () => {
    const posts: SendTarget[] = [];
    vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.includes("/api/v1/whoami")) {
        return Promise.resolve(new Response(JSON.stringify(AUTHENTICATED_HUMAN), { status: 200 }));
      }
      if (init?.method === "POST") {
        posts.push((JSON.parse(String(init.body)) as { to: SendTarget }).to);
        return Promise.resolve(new Response(JSON.stringify({ messageId: "m_ack" }), { status: 201 }));
      }
      return new Promise<Response>(() => {});
    }));
    const backend: ConversationView = {
      key: "backend",
      title: "backend",
      target: { verb: "post", thread: "backend" },
      composerLabel: "Message #backend",
    };
    const design: ConversationView = {
      key: "design",
      title: "design",
      target: { verb: "post", thread: "design" },
      composerLabel: "Message #design",
    };

    const { rerender } = render(
      <AppProviders>
        <IdentityProbe />
        <LiveChannelPane view={backend} thread="backend" />
      </AppProviders>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("identity-name")).toHaveTextContent("alice");
    });
    rerender(
      <AppProviders>
        <IdentityProbe />
        <LiveChannelPane view={design} thread="design" />
      </AppProviders>,
    );
    const box = screen.getByRole("textbox", { name: "Message design" });
    await act(async () => {
      box.textContent = "active target";
      screen.getByRole("button", { name: "Send" }).click();
    });

    await waitFor(() => expect(posts).toEqual([{ verb: "post", thread: "design" }]));
  });
});
