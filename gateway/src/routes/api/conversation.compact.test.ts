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

  it.each([null, "", " ", {}])(
    "rejects malformed exact selector before ingress %j",
    async (expectedSessionId) => {
      const response = await handleConversationCompactPost(
        new Request("http://localhost/api/conversation/compact", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ agentId: "a_target", expectedSessionId }),
        }),
      );
      expect(response.status).toBe(400);
      expect(commandIngress.submit).not.toHaveBeenCalled();
    },
  );

  it.each(["s_target", "s_foreign", undefined])(
    "carries exact compact identity and checks actual session %s",
    async (sessionId) => {
      commandIngress.submit.mockResolvedValue({ started: true, sessionId });
      const response = await handleConversationCompactPost(
        new Request("http://localhost/api/conversation/compact", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            agentId: "a_target",
            expectedSessionId: "s_target",
          }),
        }),
      );
      expect(commandIngress.submit.mock.calls[0]?.[1]).toMatchObject({
        agentId: "a_target",
        expectedSessionId: "s_target",
      });
      expect(response.status).toBe(sessionId === "s_target" ? 201 : 502);
    },
  );

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
