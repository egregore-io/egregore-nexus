import { describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { Kind, Scope } from "@shared/types";
import type { MessageVM } from "@shared/types";

import { Button } from "./components/Button";
import { IconButton } from "./components/IconButton";
import { PresenceDot } from "./components/PresenceDot";
import { ProvenanceChip } from "./conversation/ProvenanceChip";
import { MessageTimeline } from "./conversation/MessageTimeline";

describe("Button", () => {
  it("renders an accessible button and fires onClick", async () => {
    const onClick = vi.fn();
    render(<Button onClick={onClick}>Send</Button>);
    const btn = screen.getByRole("button", { name: "Send" });
    expect(btn).toBeInTheDocument();
    await userEvent.click(btn);
    expect(onClick).toHaveBeenCalledOnce();
  });

  it("does not fire when disabled", async () => {
    const onClick = vi.fn();
    render(
      <Button disabled onClick={onClick}>
        Nope
      </Button>,
    );
    const btn = screen.getByRole("button", { name: "Nope" });
    expect(btn).toBeDisabled();
    await userEvent.click(btn);
    expect(onClick).not.toHaveBeenCalled();
  });

  it("defaults to type=button (not a form submit)", () => {
    render(<Button>X</Button>);
    expect(screen.getByRole("button", { name: "X" })).toHaveAttribute(
      "type",
      "button",
    );
  });
});

describe("IconButton", () => {
  it("uses the required label as its accessible name", () => {
    render(
      <IconButton label="Send message">
        <svg />
      </IconButton>,
    );
    expect(
      screen.getByRole("button", { name: "Send message" }),
    ).toBeInTheDocument();
  });
});

describe("PresenceDot", () => {
  it("announces presence by label, not color alone", () => {
    render(<PresenceDot presence="online" />);
    expect(screen.getByRole("img", { name: "Online" })).toBeInTheDocument();
  });
});

describe("ProvenanceChip", () => {
  it("renders 'from · via #thread' for a thread message", () => {
    render(
      <ProvenanceChip
        provenance={{ from: "ben", kind: Kind.Agent, thread: "design" }}
      />,
    );
    expect(screen.getByText("ben")).toBeInTheDocument();
    expect(screen.getByText("#design")).toBeInTheDocument();
  });

  it("omits the 'via' clause for a DM (no thread/topic)", () => {
    render(<ProvenanceChip provenance={{ from: "etan", kind: Kind.Human }} />);
    expect(screen.getByText("etan")).toBeInTheDocument();
    expect(screen.queryByText(/via/)).not.toBeInTheDocument();
  });
});

function msg(over: Partial<MessageVM> & Pick<MessageVM, "id" | "from" | "body">): MessageVM {
  return {
    project: "p",
    scope: Scope.Thread,
    thread: "design",
    createdAt: Date.now(),
    provenance: { from: over.from, kind: Kind.Agent, thread: "design" },
    ...over,
  };
}

describe("MessageTimeline", () => {
  it("groups consecutive messages by author and renders a polite live-region", () => {
    const messages: MessageVM[] = [
      msg({ id: "m1", from: "ben", body: "first" }),
      msg({ id: "m2", from: "ben", body: "second" }),
      msg({ id: "m3", from: "atlas", body: "third" }),
    ];
    render(<MessageTimeline messages={messages} />);

    const log = screen.getByRole("log");
    expect(log).toHaveAttribute("aria-live", "polite");

    // Two author groups (ben's two messages collapse into one group).
    const groups = log.querySelectorAll("[data-message-group]");
    expect(groups).toHaveLength(2);

    // All three message bodies render.
    expect(screen.getByText("first")).toBeInTheDocument();
    expect(screen.getByText("second")).toBeInTheDocument();
    expect(screen.getByText("third")).toBeInTheDocument();
  });

  it("marks a pending message and offers retry on a failed one", async () => {
    const onRetry = vi.fn();
    const messages: MessageVM[] = [
      msg({ id: "", tempId: "t1", from: "etan", body: "hello there", pending: true }),
      msg({ id: "", tempId: "t2", from: "etan", body: "broke", failed: true }),
    ];
    const { container } = render(
      <MessageTimeline messages={messages} onRetry={onRetry} />,
    );

    // The pending affordance and the data flag mark the in-flight line.
    expect(screen.getByText(/sending…/i)).toBeInTheDocument();
    expect(container.querySelector("[data-pending]")).not.toBeNull();
    expect(container.querySelector("[data-failed]")).not.toBeNull();

    const retry = screen.getByRole("button", { name: "retry" });
    await userEvent.click(retry);
    expect(onRetry).toHaveBeenCalledOnce();
  });

  it("shows the empty state when there are no messages", () => {
    render(<MessageTimeline messages={[]} />);
    const log = screen.getByRole("log");
    expect(within(log).getByText(/no messages yet/i)).toBeInTheDocument();
  });
});
