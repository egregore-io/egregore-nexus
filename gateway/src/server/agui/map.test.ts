// the pure AG-UI mappers.
//
// `runInputToSend(input, target)` turns an AG-UI `RunAgentInput` into a
// contract `SendRequest` (latest user message → `{ to:<target>, body:<text> }`).
//
// The `acpToAguiEvents` and `messageToAguiEvents` describe blocks have been
// relocated to `mapAgentUpdate.test.ts` and `mapMessage.test.ts` respectively
//.
import { describe, it, expect } from "vitest";
import {
  runInputToSend,
} from "@server/agui/map";

describe("runInputToSend (RunAgentInput → SendRequest)", () => {
  it("maps the latest user message to { to:<target>, body:<text> }", () => {
    const send = runInputToSend(
      {
        threadId: "t1",
        runId: "r1",
        state: {},
        messages: [
          { id: "1", role: "user", content: "first" },
          { id: "2", role: "assistant", content: "hello" },
          { id: "3", role: "user", content: "hi" },
        ],
        tools: [],
        context: [],
        forwardedProps: {},
      },
      { verb: "post", thread: "design" },
    );
    expect(send).toEqual({ to: { verb: "post", thread: "design" }, body: "hi" });
  });

  it("supports a dm target", () => {
    const send = runInputToSend(
      {
        threadId: "t1",
        runId: "r1",
        state: {},
        messages: [{ id: "1", role: "user", content: "ping" }],
        tools: [],
        context: [],
        forwardedProps: {},
      },
      { verb: "dm", name: "ben" },
    );
    expect(send).toEqual({ to: { verb: "dm", name: "ben" }, body: "ping" });
  });

  it("extracts text from multimodal (array) user content", () => {
    const send = runInputToSend(
      {
        threadId: "t1",
        runId: "r1",
        state: {},
        messages: [
          {
            id: "1",
            role: "user",
            content: [
              { type: "text", text: "look " },
              { type: "text", text: "here" },
            ],
          },
        ],
        tools: [],
        context: [],
        forwardedProps: {},
      },
      { verb: "dm", name: "ben" },
    );
    expect(send.body).toBe("look here");
  });

  it("rejects empty or whitespace-only user content", () => {
    const cases: Array<string | Array<{ type: "text"; text: string }>> = [
      "",
      " \n\t ",
      [{ type: "text", text: "  " }],
    ];
    for (const content of cases) {
      expect(() =>
        runInputToSend(
          {
            threadId: "t1",
            runId: "r1",
            state: {},
            messages: [{ id: "1", role: "user", content }],
            tools: [],
            context: [],
            forwardedProps: {},
          },
          { verb: "dm", name: "ben" },
        ),
      ).toThrow(/body/i);
    }
  });

  it("throws when there is no user message", () => {
    expect(() =>
      runInputToSend(
        {
          threadId: "t1",
          runId: "r1",
          state: {},
          messages: [{ id: "1", role: "assistant", content: "hi" }],
          tools: [],
          context: [],
          forwardedProps: {},
        },
        { verb: "dm", name: "ben" },
      ),
    ).toThrow();
  });

  it("throws on empty messages", () => {
    expect(() =>
      runInputToSend(
        {
          threadId: "t1",
          runId: "r1",
          state: {},
          messages: [],
          tools: [],
          context: [],
          forwardedProps: {},
        },
        { verb: "dm", name: "ben" },
      ),
    ).toThrow();
  });
});
