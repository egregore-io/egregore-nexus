import { describe, expect, it } from "vitest";

import {
  bindWebSocketCsrf,
  browserMutationCsrfFailure,
  csrfWebSocketProtocol,
} from "./browserMutationAuth.mjs";

describe("browser mutation authentication", () => {
  it("requires a matching double-submit token for cookie-backed mutations", async () => {
    const missing = browserMutationCsrfFailure(new Request(
      "http://gateway.test/api/conversation/prompt",
      {
        method: "POST",
        headers: { cookie: "nexus_human=human-1; nexus_csrf=csrf-1" },
      },
    ));
    expect(missing?.status).toBe(403);
    await expect(missing?.json()).resolves.toEqual({
      error: { code: "forbidden", message: "missing or invalid CSRF token" },
    });

    const mismatch = browserMutationCsrfFailure(new Request(
      "http://gateway.test/api/conversation/prompt",
      {
        method: "POST",
        headers: {
          cookie: "nexus_human=human-1; nexus_csrf=csrf-1",
          "x-nexus-csrf": "csrf-other",
        },
      },
    ));
    expect(mismatch?.status).toBe(403);

    const accepted = browserMutationCsrfFailure(new Request(
      "http://gateway.test/api/conversation/prompt",
      {
        method: "POST",
        headers: {
          cookie: "nexus_human=human-1; nexus_csrf=csrf-1",
          "x-nexus-csrf": "csrf-1",
        },
      },
    ));
    expect(accepted).toBeUndefined();
  });

  it("does not impose cookie CSRF on reads, login, bearer-only, or local-operator requests", () => {
    expect(browserMutationCsrfFailure(new Request(
      "http://gateway.test/api/conversation/prompt",
      { headers: { cookie: "nexus_human=human-1; nexus_csrf=csrf-1" } },
    ))).toBeUndefined();
    expect(browserMutationCsrfFailure(new Request(
      "http://gateway.test/api/login",
      { method: "POST" },
    ))).toBeUndefined();
    expect(browserMutationCsrfFailure(new Request(
      "http://gateway.test/api/conversation/prompt",
      { method: "POST", headers: { authorization: "Bearer bearer-1" } },
    ))).toBeUndefined();
    expect(browserMutationCsrfFailure(new Request(
      "http://gateway.test/api/conversation/prompt",
      {
        method: "POST",
        headers: { cookie: "nexus_human=stale-human; nexus_csrf=stale-csrf" },
      },
    ), { enforce: false })).toBeUndefined();
    expect(browserMutationCsrfFailure(new Request(
      "http://gateway.test/api/conversation/prompt",
      { method: "POST" },
    ))).toBeUndefined();
  });

  it("gives a presented human cookie precedence over bearer authentication", () => {
    const rejected = browserMutationCsrfFailure(new Request(
      "http://gateway.test/api/v1/messages",
      {
        method: "POST",
        headers: {
          authorization: "Bearer bearer-1",
          cookie: "nexus_human=human-1; nexus_csrf=csrf-1",
        },
      },
    ));
    expect(rejected?.status).toBe(403);
  });

  it("binds browser WebSocket CSRF only from a protocol matching the cookie", () => {
    const bound = bindWebSocketCsrf(new Request("http://gateway.test/api/agui/ws", {
      headers: {
        cookie: "nexus_human=human-1; nexus_csrf=csrf-1",
        "sec-websocket-protocol": `nexus-v1, ${csrfWebSocketProtocol("csrf-1")}`,
      },
    }));
    expect(bound.headers.get("x-nexus-csrf")).toBe("csrf-1");

    const mismatched = bindWebSocketCsrf(new Request("http://gateway.test/api/agui/ws", {
      headers: {
        cookie: "nexus_human=human-1; nexus_csrf=csrf-1",
        "sec-websocket-protocol": `nexus-v1, ${csrfWebSocketProtocol("csrf-other")}`,
        "x-nexus-csrf": "csrf-1",
      },
    }));
    expect(mismatched.headers.get("x-nexus-csrf")).toBeNull();

    const frameOrQueryCannotAssertAuth = bindWebSocketCsrf(new Request(
      "http://gateway.test/api/agui/ws?csrf=csrf-1",
      {
        headers: {
          cookie: "nexus_human=human-1; nexus_csrf=csrf-1",
          "x-nexus-csrf": "csrf-1",
        },
      },
    ));
    expect(frameOrQueryCannotAssertAuth.headers.get("x-nexus-csrf")).toBeNull();

    const overwritten = bindWebSocketCsrf(new Request("http://gateway.test/api/agui/ws", {
      headers: {
        cookie: "nexus_human=human-1; nexus_csrf=csrf-1",
        "sec-websocket-protocol": `nexus-v1, ${csrfWebSocketProtocol("csrf-1")}`,
        "x-nexus-csrf": "caller-forged",
      },
    }));
    expect(overwritten.headers.get("x-nexus-csrf")).toBe("csrf-1");
  });

  it("rejects unsafe values from the WebSocket protocol token", () => {
    expect(() => csrfWebSocketProtocol("csrf value")).toThrow(/protocol-safe/);
    expect(() => csrfWebSocketProtocol("csrf,other")).toThrow(/protocol-safe/);
  });
});
