import { afterEach, describe, expect, it, vi } from "vitest";

import { gatewayFetch, gatewayUrl, gatewayWebSocketProtocols } from "./gatewayClient";

describe("Gateway-only browser client", () => {
  afterEach(() => {
    document.cookie = "nexus_csrf=; Max-Age=0; Path=/";
    vi.unstubAllGlobals();
  });

  it("uses relative same-origin paths by default and an explicit Gateway base when configured", () => {
    expect(gatewayUrl("/api/v1/threads")).toBe("/api/v1/threads");
    expect(gatewayUrl("/api/v1/threads", "http://127.0.0.1:4100"))
      .toBe("http://127.0.0.1:4100/api/v1/threads");
  });

  it("sends the readable CSRF cookie on credentialed browser mutations", async () => {
    document.cookie = "nexus_csrf=csrf-browser; Path=/";
    const calls: Array<[RequestInfo | URL, RequestInit | undefined]> = [];
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      calls.push([input, init]);
      return new Response(null, { status: 204 });
    });
    vi.stubGlobal("fetch", fetchMock);

    await gatewayFetch("/api/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-nexus-csrf": "caller-forged",
      },
      body: "{}",
      credentials: "omit",
    });

    const init = calls[0]![1]!;
    expect(init.credentials).toBe("include");
    expect(new Headers(init.headers).get("x-nexus-csrf")).toBe("csrf-browser");
  });

  it("removes caller-forged proof when no readable CSRF cookie exists", async () => {
    const calls: Array<[RequestInfo | URL, RequestInit | undefined]> = [];
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      calls.push([input, init]);
      return new Response(null, { status: 403 });
    });
    vi.stubGlobal("fetch", fetchMock);

    await gatewayFetch("/api/v1/messages", {
      method: "POST",
      headers: { "x-nexus-csrf": "caller-forged" },
      body: "{}",
    });
    expect(fetchMock).toHaveBeenCalledOnce();
    expect(new Headers(calls[0]![1]!.headers).get("x-nexus-csrf")).toBeNull();
    expect(calls[0]![1]!.credentials).toBe("include");
  });

  it("binds the readable CSRF cookie as a WebSocket upgrade protocol", () => {
    document.cookie = "nexus_csrf=csrf-browser; Path=/";
    expect(gatewayWebSocketProtocols()).toEqual([
      "nexus-v1",
      "nexus-csrf.csrf-browser",
    ]);
  });

  it("rejects an invalid readable CSRF cookie before a WebSocket opens", () => {
    document.cookie = "nexus_csrf=not protocol safe; Path=/";
    expect(() => gatewayWebSocketProtocols()).toThrow(/protocol-safe/);
  });

  it("keeps the cookie-free local-operator WebSocket contract", () => {
    expect(gatewayWebSocketProtocols()).toEqual(["nexus-v1"]);
  });
});
