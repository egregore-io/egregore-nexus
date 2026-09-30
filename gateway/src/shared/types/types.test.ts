import { describe, it, expect, expectTypeOf } from "vitest";
import type { Message, WsEvent, SendRequest, Ack } from "@shared/types";
import type { MessageVM } from "@shared/types";
// `Presence` is a contract *enum* (string-valued), so it is a value import, not a
// type import — it is re-exported through the barrel from the generated mirror.
import { isEvent, Presence } from "@shared/types";

describe("types", () => {
  it("re-exports generated contract names", () => {
    expectTypeOf<SendRequest>().toHaveProperty("to");
    expectTypeOf<Ack>().toHaveProperty("messageId");
    const p: Presence = Presence.Online;
    expect(p).toBe("online");
  });

  it("MessageVM extends generated Message with client flags", () => {
    expectTypeOf<MessageVM>().toMatchTypeOf<Message>();
    expectTypeOf<MessageVM["pending"]>().toEqualTypeOf<boolean | undefined>();
  });

  it("isEvent narrows a WsEvent by type", () => {
    const ev: WsEvent = { type: "message.created", messageId: "m_1" };
    expect(isEvent(ev, "message.created")).toBe(true);
    if (isEvent(ev, "message.created")) {
      expect(ev.messageId).toBe("m_1");
    }
  });
});
