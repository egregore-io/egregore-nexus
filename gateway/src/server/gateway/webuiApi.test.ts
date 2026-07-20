import { beforeEach, describe, expect, it, vi } from "vitest";

const handlers = vi.hoisted(() => ({
  compact: vi.fn(async () => json({ route: "compact" })),
  interrupt: vi.fn(async () => json({ route: "interrupt" })),
  steer: vi.fn(async () => json({ route: "steer" })),
}));

vi.mock("../../routes/api/conversation.compact", () => ({
  handleConversationCompactPost: handlers.compact,
}));

vi.mock("../../routes/api/conversation.interrupt", () => ({
  handleConversationInterruptPost: handlers.interrupt,
}));

vi.mock("../../routes/api/conversation.steer", () => ({
  handleConversationSteerPost: handlers.steer,
}));

import { dispatchWebuiApi, isWebuiApiPath } from "./webuiApi";

describe("packaged Gateway conversation commands", () => {
  beforeEach(() => {
    handlers.compact.mockClear();
    handlers.interrupt.mockClear();
    handlers.steer.mockClear();
  });

  it.each([
    ["/api/conversation/steer", "steer", handlers.steer],
    ["/api/conversation/compact", "compact", handlers.compact],
    ["/api/conversation/interrupt", "interrupt", handlers.interrupt],
  ] as const)("mounts %s through its production handler", async (path, route, handler) => {
    expect(isWebuiApiPath(path)).toBe(true);

    const request = new Request(`http://localhost:4100${path}`, { method: "POST" });
    const response = await dispatchWebuiApi(request);

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ route });
    expect(handler).toHaveBeenCalledWith(request);
  });
});

function json(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}
