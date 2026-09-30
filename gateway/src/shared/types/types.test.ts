import { describe, it, expect, expectTypeOf } from "vitest";
import type { Message, WsEvent, SendRequest, Ack } from "@shared/types";
import type { MessageVM } from "@shared/types";
// `Presence` is a contract *enum* (string-valued), so it is a value import, not a
// type import — it is re-exported through the barrel from the generated mirror.
import { isEvent, Presence } from "@shared/types";
import type { AgentRuntimeSummary, RuntimeTelemetryReport } from "./contracts.gen";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

describe("types", () => {
  it("shares the canonical telemetry fixture and generated optional report shape", () => {
    // This is fixture/type evidence, not a runtime validator or native collector capture.
    // Resolve from the Gateway test root; Vite rewrites asset URLs for the browser environment.
    const wire: AgentRuntimeSummary = JSON.parse(readFileSync(
      resolve("../core/crates/nexus-contracts/fixtures/runtime.telemetry.json"), "utf8"));
    expectTypeOf<NonNullable<AgentRuntimeSummary["modelReport"]>>().toHaveProperty("telemetry");
    const telemetry: RuntimeTelemetryReport | undefined = wire.modelReport?.telemetry;
    expect(telemetry?.usage.observation?.scope).toBe("sessionCumulative");
    expect(telemetry?.usage.observation?.model).toBeUndefined();
    expect(telemetry?.context.observation?.usedTokens?.provenance).toBe("estimated");
    expect(telemetry?.quota.observation?.windows.map(window => window.windowId)).toEqual(["five-hour", "week"]);
    expect(JSON.parse(JSON.stringify(wire))).toEqual(wire);
  });
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
