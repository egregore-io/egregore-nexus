import { describe, expect, it } from "vitest";

import { EventType } from "@ag-ui/client";
import { AguiSessionView } from "./aguiView";
import { nexusView } from "./nexusView";
import { terminalView } from "./terminalView";
import { encodeCursor, type SessionFanoutFrame } from "./sessionFanout";

describe("agent-session views", () => {
  it("keeps semantic text identical in Nexus and AG-UI and isolates terminal bytes", () => {
    const projector = new AguiSessionView("s_ada");
    const frames = [agent(1, "hello "), agent(2, "world")];
    const nexusText = frames.map((frame) => {
      const view = nexusView(frame)!;
      return String((view.event as { data: { text: string } }).data.text);
    }).join("");
    const aguiEvents = frames.flatMap((frame) => projector.project(frame)?.events ?? []);
    const aguiText = aguiEvents
      .filter((event) => event.type === EventType.TEXT_MESSAGE_CONTENT)
      .map((event) => String((event as unknown as { delta: string }).delta)).join("");
    expect(nexusText).toBe("hello world");
    expect(aguiText).toBe(nexusText);

    const raw: SessionFanoutFrame = {
      lane: "terminal", epoch: "epoch", id: 3, cursor: encodeCursor("epoch", 3),
      chunkBase64: "YW5zaQ==", encoding: "base64",
    };
    expect(nexusView(raw)).toBeNull();
    expect(projector.project(raw)).toBeNull();
    expect(terminalView(raw)).toMatchObject({ chunkBase64: "YW5zaQ==" });
  });
});

function agent(id: number, text: string): SessionFanoutFrame {
  return {
    lane: "agent", epoch: "epoch", id, cursor: encodeCursor("epoch", id),
    event: { type: "agent.update", sessionId: "s_ada", kind: "text", data: { text, itemId: "answer" } },
  };
}
