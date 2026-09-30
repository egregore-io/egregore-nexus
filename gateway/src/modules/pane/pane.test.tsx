import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, within } from "@testing-library/react";

import { AdminView } from "./AdminView";
import { Composer } from "./Composer";
import { ConversationPane } from "./ConversationPane";
import { PubView } from "./PubView";
import { AgentContext, MembersContext, PubContext } from "./ContextViews";
import type { ConversationView } from "./conversationView";
import type { AdminRow, FeedRow, Fact, MemberItem } from "./liveData";
import type { ThreadItem } from "./types";

// These tests assert the pane regions render through the real presentational
// components from explicit props (the chrome + the live-data view-models),
// including the empty states rendered when there is no data.

const backendView: ConversationView = {
  key: "backend",
  title: "backend",
  target: { verb: "post", thread: "backend" },
  topic: "store swap · auth refactor",
  glyph: "#",
  composerPlaceholder: "Message #backend",
  composerLabel: "Message #backend",
};

const backendItems: ThreadItem[] = [
  { sep: "Today" },
  {
    id: "m0",
    who: "erin",
    glyph: "E",
    chip: "you",
    isYou: true,
    time: "09:41",
    blocks: [{ b: "p", runs: [{ t: "text", v: "can someone take the auth refactor?" }] }],
  },
  {
    id: "m1",
    who: "ben",
    glyph: "B",
    chip: "agent",
    presence: "online",
    time: "09:41",
    blocks: [
      {
        b: "p",
        runs: [
          { t: "text", v: "on it — " },
          { t: "mention", v: "dylan" },
          { t: "text", v: " already touched " },
          { t: "code", v: "token.rs" },
        ],
      },
      { b: "code", lines: ["ALTER messages ADD COLUMN provenance TEXT;"] },
    ],
  },
  {
    id: "m2",
    who: "ben",
    glyph: "B",
    chip: "agent",
    presence: "online",
    streaming: true,
    blocks: [{ b: "p", runs: [{ t: "text", v: "running the migration" }] }],
  },
];

describe("ConversationPane (#backend) — thread regions", () => {
  const renderBackend = () =>
    render(<ConversationPane view={backendView} items={backendItems} />);

  it("renders the pane head with the channel title and topic", () => {
    renderBackend();
    expect(screen.getByRole("heading", { name: "backend" })).toBeInTheDocument();
    expect(screen.getByText("store swap · auth refactor")).toBeInTheDocument();
  });

  it("renders a day separator, author chips, and a mention + inline code", () => {
    renderBackend();
    const log = screen.getByRole("log");
    expect(within(log).getByText("Today")).toBeInTheDocument();
    expect(within(log).getAllByText("you").length).toBeGreaterThan(0);
    expect(within(log).getAllByText("agent").length).toBeGreaterThan(0);
    expect(within(log).getByText(/@dylan/)).toBeInTheDocument();
    expect(within(log).getByText("token.rs")).toBeInTheDocument();
  });

  it("renders a code block and the live streaming indicator", () => {
    const { container } = renderBackend();
    expect(
      screen.getByText(/ALTER messages ADD COLUMN provenance TEXT;/),
    ).toBeInTheDocument();
    expect(screen.getByText("streaming")).toBeInTheDocument();
    expect(container.querySelector('[class*="lens-blink"]')).not.toBeNull();
  });

  it("renders the composer with the channel label + Send", () => {
    renderBackend();
    expect(screen.getByRole("form", { name: "Message #backend" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Send" })).toBeInTheDocument();
    expect(screen.queryByText(/address/)).toBeNull();
    expect(screen.queryByText(/command/)).toBeNull();
  });

  it("renders thread header action controls", () => {
    render(
      <ConversationPane
        view={backendView}
        items={backendItems}
        actions={
          <>
            <button type="button">Archive thread</button>
            <button type="button">Delete thread</button>
          </>
        }
      />,
    );
    expect(screen.getByRole("button", { name: "Archive thread" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Delete thread" })).toBeInTheDocument();
  });

  it("renders an empty thread when there are no items", () => {
    render(<ConversationPane view={backendView} items={[]} />);
    expect(screen.getByRole("heading", { name: "backend" })).toBeInTheDocument();
    expect(within(screen.getByRole("log")).queryByText("Today")).toBeNull();
  });

  it("can render lane text through Streamdown markdown", async () => {
    render(
      <ConversationPane
        view={backendView}
        textRenderer="streamdown"
        items={[
          {
            id: "agent-md",
            who: "codex",
            glyph: "C",
            chip: "agent",
            blocks: [
              {
                b: "p",
                runs: [
                  {
                    t: "text",
                    v: "## Build report\n\n- shell output rendered\n\nHTML stays inert: <button>nope</button>",
                  },
                ],
              },
            ],
          },
        ]}
      />,
    );

    expect(await screen.findByRole("heading", { name: "Build report" })).toBeInTheDocument();
    expect(screen.getByRole("listitem")).toHaveTextContent("shell output rendered");
    expect(screen.queryByRole("button", { name: "nope" })).toBeNull();
  });
});

describe("Composer — send wiring", () => {
  function editor() {
    return screen.getByRole("textbox", { name: "Message ben" });
  }

  it("calls onSend with the trimmed text on Send click, then clears the editor", async () => {
    const onSend = vi.fn().mockResolvedValue(undefined);
    render(<Composer placeholder="Message ben" label="Message ben" onSend={onSend} />);
    const el = editor();
    el.textContent = "  hello ben  ";
    fireEvent.click(screen.getByRole("button", { name: "Send" }));
    await vi.waitFor(() => expect(onSend).toHaveBeenCalledWith("hello ben"));
    expect(el.textContent).toBe("");
  });

  it("sends on Enter (no Shift) and does not send on empty input", async () => {
    const onSend = vi.fn().mockResolvedValue(undefined);
    render(<Composer placeholder="Message ben" label="Message ben" onSend={onSend} />);
    const el = editor();

    // Empty → no send.
    fireEvent.keyDown(el, { key: "Enter" });
    expect(onSend).not.toHaveBeenCalled();

    // Typed → Enter sends.
    el.textContent = "ship it";
    fireEvent.keyDown(el, { key: "Enter" });
    await vi.waitFor(() => expect(onSend).toHaveBeenCalledWith("ship it"));

    // Shift+Enter does NOT send (newline).
    onSend.mockClear();
    el.textContent = "line one";
    fireEvent.keyDown(el, { key: "Enter", shiftKey: true });
    expect(onSend).not.toHaveBeenCalled();
  });

  it("is inert (Send disabled) when no onSend is provided", () => {
    render(<Composer placeholder="Message ben" label="Message ben" />);
    expect(screen.getByRole("button", { name: "Send" })).toBeDisabled();
  });

  it("does not render address or slash-command affordances", () => {
    render(<Composer placeholder="Message ben" label="Message ben" />);
    expect(screen.queryByText(/address/)).toBeNull();
    expect(screen.queryByText(/command/)).toBeNull();
  });
});

describe("PubView", () => {
  const rows: FeedRow[] = [
    { src: "github", title: "topic: ci", meta: "→ ben · 2m ago", routedTo: ["ben"] },
    { src: "sentry", title: "n_07", meta: "unrouted · 1m ago", routedTo: [] },
  ];

  it("renders feed rows with sources and route affordances", () => {
    render(<PubView rows={rows} />);
    expect(screen.getByRole("heading", { name: "Pub feed" })).toBeInTheDocument();
    expect(screen.getByText("github")).toBeInTheDocument();
    expect(screen.getByText("topic: ci")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "→ ben" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "route…" })).toBeInTheDocument();
  });

  it("renders an empty state with no notifications", () => {
    render(<PubView rows={[]} />);
    expect(screen.getByText("No notifications yet")).toBeInTheDocument();
  });
});

describe("AdminView", () => {
  const agents: AdminRow[] = [
    { name: "ben", presence: "online", harness: "claude", role: "admin agent", tier: "agent", status: "online · token.rs" },
  ];

  it("renders the admin agents table from live rows", () => {
    render(<AdminView agents={agents} />);
    expect(screen.getByRole("heading", { name: "Admin" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "+ Launch agent" })).toBeInTheDocument();
    const table = screen.getByRole("table");
    expect(within(table).getByText("admin agent")).toBeInTheDocument();
    expect(within(table).getByText("ben")).toBeInTheDocument();
  });

  it("renders an empty state with no agents", () => {
    render(<AdminView agents={[]} />);
    expect(screen.getByText("No agents yet")).toBeInTheDocument();
  });

  it("calls onAssignProject when a different project is selected", () => {
    const fn = vi.fn();
    render(
      <AdminView
        agents={[{ name: "ben", presence: "online", harness: "claude", role: "agent", tier: "agent", status: "online" }]}
        projects={["nexus", "lens"]}
        activeProject="nexus"
        onAssignProject={fn}
      />,
    );
    const select = screen.getByRole("combobox", { name: /project/i });
    fireEvent.change(select, { target: { value: "lens" } });
    expect(fn).toHaveBeenCalledWith("ben", "lens");
  });

  it("grants and revokes admin tier from the agents table", () => {
    const onGrantTier = vi.fn();
    const rows: AdminRow[] = [
      { name: "ben", presence: "online", harness: "claude", role: "agent", tier: "agent", status: "online" },
      { name: "ada", presence: "online", harness: "codex", role: "agent", tier: "admin", status: "online" },
    ];

    render(<AdminView agents={rows} onGrantTier={onGrantTier} />);

    fireEvent.click(screen.getByRole("button", { name: "Make ben admin" }));
    fireEvent.click(screen.getByRole("button", { name: "Revoke admin from ada" }));

    expect(onGrantTier).toHaveBeenNthCalledWith(1, "ben", "admin");
    expect(onGrantTier).toHaveBeenNthCalledWith(2, "ada", "agent");
    expect(screen.getByText("admin tier")).toBeInTheDocument();
  });

  it("calls onEvict with agent name when Evict is clicked", () => {
    const onEvict = vi.fn();
    render(<AdminView agents={agents} onEvict={onEvict} />);
    fireEvent.click(screen.getByRole("button", { name: "Evict ben" }));
    expect(onEvict).toHaveBeenCalledWith("ben");
  });

  it("calls onKill with agent name after Kill confirm step", () => {
    const onKill = vi.fn();
    render(<AdminView agents={agents} onKill={onKill} />);
    // First click shows confirm prompt
    fireEvent.click(screen.getByRole("button", { name: "Kill ben" }));
    expect(screen.getByRole("button", { name: "Confirm kill ben" })).toBeInTheDocument();
    // Second click (confirm) calls onKill
    fireEvent.click(screen.getByRole("button", { name: "Confirm kill ben" }));
    expect(onKill).toHaveBeenCalledWith("ben");
  });

  it("cancels the Kill confirm step without calling onKill", () => {
    const onKill = vi.fn();
    render(<AdminView agents={agents} onKill={onKill} />);
    fireEvent.click(screen.getByRole("button", { name: "Kill ben" }));
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    expect(onKill).not.toHaveBeenCalled();
    // Evict is visible again after cancel
    expect(screen.getByRole("button", { name: "Evict ben" })).toBeInTheDocument();
  });

  it("opens the Launch modal and calls onLaunch with kind and name", () => {
    const onLaunch = vi.fn();
    render(<AdminView agents={[]} onLaunch={onLaunch} />);
    fireEvent.click(screen.getByRole("button", { name: "+ Launch agent" }));
    expect(screen.getByRole("dialog")).toBeInTheDocument();
    expect(screen.getByText(/Launches a headless agent/)).toBeInTheDocument();
    // Change kind to claude
    fireEvent.change(screen.getByLabelText("Kind"), { target: { value: "claude" } });
    // Enter a name
    fireEvent.change(screen.getByLabelText(/Name/), { target: { value: "aria" } });
    // Submit
    fireEvent.submit(screen.getByRole("form", { name: "Launch agent" }));
    expect(onLaunch).toHaveBeenCalledWith("claude", "aria");
  });

  it("opens the Launch modal and passes undefined name when name is blank", () => {
    const onLaunch = vi.fn();
    render(<AdminView agents={[]} onLaunch={onLaunch} />);
    fireEvent.click(screen.getByRole("button", { name: "+ Launch agent" }));
    fireEvent.submit(screen.getByRole("form", { name: "Launch agent" }));
    expect(onLaunch).toHaveBeenCalledWith("codex", undefined);
  });
});

describe("Context views", () => {
  it("members context lists the roster members", () => {
    const members: MemberItem[] = [
      { glyph: "B", presence: "online", name: "ben", state: "online · token.rs" },
    ];
    render(<MembersContext members={members} />);
    expect(screen.getByText("Members")).toBeInTheDocument();
    expect(screen.getByText("online · token.rs")).toBeInTheDocument();
  });

  it("members context shows an empty state with no members", () => {
    render(<MembersContext members={[]} />);
    expect(screen.getByText("No members yet.")).toBeInTheDocument();
  });

  it("agent context shows the agent facts", () => {
    const facts: Fact[] = [
      { dt: "Name", dd: "ben" },
      { dt: "Harness", dd: "claude" },
    ];
    render(<AgentContext facts={facts} />);
    expect(screen.getByText("Harness")).toBeInTheDocument();
    expect(screen.getByText("claude")).toBeInTheDocument();
  });

  it("pub context shows the routing rules", () => {
    const rules: Fact[] = [{ dt: "github →", dd: "ben" }];
    render(<PubContext rules={rules} />);
    expect(screen.getByText("Routing rules")).toBeInTheDocument();
    expect(screen.getByText("github →")).toBeInTheDocument();
  });
});
