import { describe, expect, it } from "vitest";

import { gatewayUrl } from "./gatewayClient";

describe("Gateway-only browser client", () => {
  it("uses relative same-origin paths by default and an explicit Gateway base when configured", () => {
    expect(gatewayUrl("/api/v1/threads")).toBe("/api/v1/threads");
    expect(gatewayUrl("/api/v1/threads", "http://127.0.0.1:4100"))
      .toBe("http://127.0.0.1:4100/api/v1/threads");
  });
});
