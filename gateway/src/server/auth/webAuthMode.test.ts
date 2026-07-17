import { describe, expect, it } from "vitest";

import { AGENT_ATTACH_SCOPE } from "@server/auth/principal";
import {
  isLocalOperatorCaller,
  localOperatorCaller,
  requiresHumanLogin,
  resolveWebAuthMode,
  resolveLocalOperatorName,
  webAuthModeFromEnv,
} from "@server/auth/webAuthMode";
import { Kind, Tier } from "@shared/types";

describe("web auth mode resolver", () => {
  it("defaults to local operator zero-login mode", () => {
    expect(resolveWebAuthMode({})).toBe("local-operator");
  });

  it("accepts the legacy local/remote strings and resolves them to facets", () => {
    expect(resolveWebAuthMode({ NEXUS_WEB_AUTH_MODE: "local" })).toBe(
      "local-operator",
    );
    expect(resolveWebAuthMode({ NEXUS_WEB_AUTH_MODE: "remote" })).toBe(
      "remote-human",
    );
  });

  it("uses explicit server-configured remote-human or remote-agent facets", () => {
    expect(resolveWebAuthMode({ NEXUS_WEB_AUTH_MODE: "remote-human" })).toBe(
      "remote-human",
    );
    expect(resolveWebAuthMode({ NEXUS_WEB_AUTH_MODE: "remote-agent" })).toBe(
      "remote-agent",
    );
  });

  it("treats non-loopback binds and allow-remote as remote", () => {
    expect(resolveWebAuthMode({ NEXUS_WEB_BIND: "0.0.0.0:5173" })).toBe(
      "remote-human",
    );
    expect(resolveWebAuthMode({ NEXUS_WEB_ALLOW_REMOTE: "true" })).toBe(
      "remote-human",
    );
  });

  it("keeps loopback binds local", () => {
    expect(resolveWebAuthMode({ NEXUS_WEB_BIND: "127.0.0.1:5173" })).toBe(
      "local-operator",
    );
    expect(resolveWebAuthMode({ NEXUS_WEB_BIND: "localhost" })).toBe(
      "local-operator",
    );
  });

  it("rejects reserved peer mode as not implemented", () => {
    expect(() => resolveWebAuthMode({ NEXUS_WEB_AUTH_MODE: "peer" })).toThrow(
      /not implemented/i,
    );
  });

  it("does not infer mode from request-controlled headers", () => {
    const headers = new Headers({
      host: "localhost:5173",
      "x-forwarded-for": "127.0.0.1",
    });

    expect(webAuthModeFromEnv({ NEXUS_WEB_AUTH_MODE: "remote" }, headers)).toBe(
      "remote-human",
    );
  });

  it("exempts only POST /api/v1/notify from remote human login", () => {
    expect(requiresHumanLogin("POST", "/api/v1/notify")).toBe(false);
    expect(requiresHumanLogin("POST", "/api/v1/notify/")).toBe(false);
    expect(requiresHumanLogin("GET", "/api/v1/notify")).toBe(true);
    expect(requiresHumanLogin("POST", "/api/v1/notifications")).toBe(true);
  });
});

describe("local operator caller", () => {
  it("uses a configured local display name while preserving the stable local marker", () => {
    const caller = localOperatorCaller("default", {
      homeDir: () => "/home/tester",
      readFile: () => JSON.stringify({ name: "Alex Morgan" }),
      userInfo: () => ({ username: "exampleuser" }),
      env: {},
    });

    expect(caller).toMatchObject({
      id: "local:default:local-operator",
      name: "Alex Morgan",
      project: "default",
      sessionId: "local-operator",
      runtimeId: "local-operator",
      kind: "human",
      tier: "admin",
      credentialFacet: "local",
      scopes: expect.arrayContaining(["message:send", AGENT_ATTACH_SCOPE, "admin:*"]),
    });
    expect(caller.clientKey).toBeUndefined();
    expect(isLocalOperatorCaller(caller)).toBe(true);
  });

  it("falls back to the OS account name when no local operator config exists", () => {
    expect(
      resolveLocalOperatorName({
        homeDir: () => "/home/tester",
        readFile: () => {
          throw Object.assign(new Error("missing"), { code: "ENOENT" });
        },
        userInfo: () => ({ username: "exampleuser" }),
        env: {},
      }),
    ).toBe("exampleuser");
  });

  it("recognizes the local operator by stable markers instead of display name", () => {
    expect(
      isLocalOperatorCaller({
        id: "local:default:local-operator",
        name: "Alex Morgan",
        project: "default",
        sessionId: "local-operator",
        runtimeId: "local-operator",
        credentialFacet: "local",
        kind: Kind.Human,
        tier: Tier.Admin,
      }),
    ).toBe(true);
  });
});
