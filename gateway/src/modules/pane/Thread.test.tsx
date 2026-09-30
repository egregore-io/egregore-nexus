import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { Thread } from "./Thread";
import type { ThreadItem } from "./types";

const markdownItems: ThreadItem[] = [
  {
    id: "markdown-message",
    who: "codex",
    glyph: "C",
    chip: "agent",
    blocks: [
      {
        b: "p",
        runs: [
          {
            t: "text",
            v: "## Build report\n\n- rendered with Streamdown\n\nHTML stays inert: <button>nope</button>",
          },
        ],
      },
    ],
  },
];

function textMessage(id: string, text: string): ThreadItem {
  return {
    id,
    who: "codex",
    glyph: "C",
    chip: "agent",
    blocks: [{ b: "p", runs: [{ t: "text", v: text }] }],
  };
}

function setScrollHeight(log: HTMLElement, height: number): void {
  Object.defineProperty(log, "scrollHeight", {
    configurable: true,
    value: height,
  });
  log.scrollTop = 0;
}

function setScrollMetrics(
  log: HTMLElement,
  metrics: { scrollHeight: number; clientHeight: number; scrollTop: number },
): void {
  Object.defineProperty(log, "scrollHeight", {
    configurable: true,
    value: metrics.scrollHeight,
  });
  Object.defineProperty(log, "clientHeight", {
    configurable: true,
    value: metrics.clientHeight,
  });
  log.scrollTop = metrics.scrollTop;
}

describe("Thread markdown rendering", () => {
  it("renders paragraph blocks with Streamdown by default", () => {
    render(<Thread items={markdownItems} />);

    expect(screen.getByRole("heading", { name: "Build report" })).toBeInTheDocument();
    expect(screen.getByRole("listitem")).toHaveTextContent("rendered with Streamdown");
    expect(screen.queryByRole("button", { name: "nope" })).toBeNull();
  });

  it("keeps the inline renderer available for plain conversation runs", () => {
    render(
      <Thread
        textRenderer="inline"
        items={[
          {
            id: "inline-message",
            who: "ben",
            glyph: "B",
            chip: "agent",
            blocks: [
              {
                b: "p",
                runs: [
                  { t: "text", v: "check " },
                  { t: "mention", v: "dylan" },
                  { t: "text", v: " in " },
                  { t: "code", v: "token.rs" },
                ],
              },
            ],
          },
        ]}
      />,
    );

    expect(screen.getByText("@dylan")).toBeInTheDocument();
    expect(screen.getByText("token.rs").tagName).toBe("CODE");
  });
});

describe("Thread auto-scroll", () => {
  it("keeps the log pinned to the bottom when streamed content updates", () => {
    const { rerender } = render(<Thread items={[textMessage("m1", "hello")]} />);
    const log = screen.getByRole("log");
    setScrollHeight(log, 900);

    rerender(<Thread items={[textMessage("m1", "hello world")]} />);

    expect(log.scrollTop).toBe(900);
  });

  it("can leave scroll position alone when auto-scroll is disabled", () => {
    const { rerender } = render(
      <Thread autoScroll={false} items={[textMessage("m1", "hello")]} />,
    );
    const log = screen.getByRole("log");
    setScrollHeight(log, 900);

    rerender(
      <Thread
        autoScroll={false}
        items={[textMessage("m1", "hello"), textMessage("m2", "new")]}
      />,
    );

    expect(log.scrollTop).toBe(0);
  });

  it("does not yank the log down when the user is reading older messages", () => {
    const { rerender } = render(<Thread items={[textMessage("m1", "hello")]} />);
    const log = screen.getByRole("log");
    setScrollMetrics(log, { scrollHeight: 1000, clientHeight: 300, scrollTop: 700 });
    fireEvent.scroll(log);
    setScrollMetrics(log, { scrollHeight: 1000, clientHeight: 300, scrollTop: 650 });
    fireEvent.scroll(log);

    rerender(
      <Thread items={[textMessage("m1", "hello"), textMessage("m2", "new")]} />,
    );

    expect(log.scrollTop).toBe(650);
  });

  it("shows an unread-below arrow when new messages arrive while scrolled up", () => {
    const { rerender } = render(<Thread items={[textMessage("m1", "hello")]} />);
    const log = screen.getByRole("log");
    setScrollMetrics(log, { scrollHeight: 1000, clientHeight: 300, scrollTop: 700 });
    fireEvent.scroll(log);
    setScrollMetrics(log, { scrollHeight: 1000, clientHeight: 300, scrollTop: 640 });
    fireEvent.scroll(log);

    rerender(
      <Thread
        items={[
          textMessage("m1", "hello"),
          textMessage("m2", "new one"),
          textMessage("m3", "new two"),
        ]}
      />,
    );

    expect(log.scrollTop).toBe(640);
    expect(
      screen.getByRole("button", { name: "Jump to latest messages, 2 unread" }),
    ).toBeInTheDocument();

    setScrollMetrics(log, { scrollHeight: 1200, clientHeight: 300, scrollTop: 640 });
    fireEvent.click(screen.getByRole("button", { name: "Jump to latest messages, 2 unread" }));

    expect(log.scrollTop).toBe(1200);
    expect(screen.queryByRole("button", { name: /Jump to latest messages/ })).toBeNull();
  });

  it("resumes auto-following after the user scrolls back to the bottom", () => {
    const { rerender } = render(<Thread items={[textMessage("m1", "hello")]} />);
    const log = screen.getByRole("log");
    setScrollMetrics(log, { scrollHeight: 1000, clientHeight: 300, scrollTop: 700 });
    fireEvent.scroll(log);
    setScrollMetrics(log, { scrollHeight: 1000, clientHeight: 300, scrollTop: 650 });
    fireEvent.scroll(log);
    setScrollMetrics(log, { scrollHeight: 1000, clientHeight: 300, scrollTop: 700 });
    fireEvent.scroll(log);

    rerender(
      <Thread items={[textMessage("m1", "hello"), textMessage("m2", "new")]} />,
    );

    expect(log.scrollTop).toBe(1000);
  });
});
