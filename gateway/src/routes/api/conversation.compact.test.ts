import { beforeEach, describe, expect, it, vi } from "vitest";

const commandIngress = vi.hoisted(() => ({
  submit: vi.fn(),
}));

vi.mock("@server/command/ingress", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@server/command/ingress")>();
  return {
    ...actual,
    submitCommandIntent: commandIngress.submit,
  };
});

import { handleConversationCompactPost } from "./conversation.compact";

describe("POST /api/conversation/compact", () => {
  beforeEach(() => {
    commandIngress.submit.mockReset();
    commandIngress.submit.mockResolvedValue({ started: true });
  });

  it("allows a native compact operation to outlive the generic daemon IPC timeout", async () => {
    const previous = process.env.NEXUS_WEB_AUTH_MODE;
    process.env.NEXUS_WEB_AUTH_MODE = "local";
    try {
      const response = await handleConversationCompactPost(
        new Request("http://localhost/api/conversation/compact", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            agentId: "a_codex",
            clientMessageId: "cc_compact_slow",
          }),
        }),
      );

      expect(response.status).toBe(201);
      expect(commandIngress.submit).toHaveBeenCalledOnce();
      expect(commandIngress.submit.mock.calls[0]?.[3]).toEqual({ timeoutMs: 120_000 });
    } finally {
      if (previous === undefined) delete process.env.NEXUS_WEB_AUTH_MODE;
      else process.env.NEXUS_WEB_AUTH_MODE = previous;
    }
  });
});
