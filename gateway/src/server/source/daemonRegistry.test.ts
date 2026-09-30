import { describe, expect, it, vi } from "vitest";

import { DaemonIpcError } from "@server/daemon/ipc";
import { createDaemonSourceRegistry } from "./daemonRegistry";

describe("daemon-owned source registry", () => {
  it("lists and resolves source metadata through typed daemon queries", async () => {
    const query = vi.fn(async (method: string) => {
      if (method === "source.list") {
        return {
          sources: [{
            name: "ci",
            topic: "builds",
            enabled: true,
            createdAt: 11,
          }],
        };
      }
      return {
        name: "ci",
        topic: "builds",
        enabled: true,
        createdAt: 11,
      };
    });
    const registry = createDaemonSourceRegistry({ query });

    await expect(registry.list()).resolves.toEqual({
      sources: [expect.objectContaining({ name: "ci", enabled: true })],
    });
    await expect(registry.show("ci")).resolves.toEqual(
      expect.objectContaining({ name: "ci", topic: "builds" }),
    );
    expect(query).toHaveBeenNthCalledWith(
      1,
      "source.list",
      {},
      expect.objectContaining({ tier: "admin" }),
      expect.any(Object),
    );
    expect(query).toHaveBeenNthCalledWith(
      2,
      "source.show",
      { name: "ci" },
      expect.objectContaining({ tier: "admin" }),
      expect.any(Object),
    );
  });

  it("fetches the plaintext token only for an enabled source", async () => {
    const query = vi.fn(async (method: string) => {
      if (method === "source.show") {
        return {
          name: "ci",
          topic: "builds",
          enabled: true,
          createdAt: 11,
        };
      }
      return { name: "ci", token: "src_secret" };
    });
    const registry = createDaemonSourceRegistry({ query });

    await expect(registry.secret("ci")).resolves.toEqual({
      name: "ci",
      topic: "builds",
      enabled: true,
      createdAt: 11,
      token: "src_secret",
    });
    expect(query.mock.calls.map(([method]) => method)).toEqual([
      "source.show",
      "source.token",
    ]);
  });

  it("does not disclose a token for a disabled source", async () => {
    const query = vi.fn(async () => ({
      name: "ci",
      topic: "builds",
      enabled: false,
      createdAt: 11,
    }));
    const registry = createDaemonSourceRegistry({ query });

    await expect(registry.secret("ci")).resolves.toEqual({
      name: "ci",
      topic: "builds",
      enabled: false,
      createdAt: 11,
      token: "",
    });
    expect(query).toHaveBeenCalledTimes(1);
  });

  it("maps a missing daemon-owned source to undefined", async () => {
    const query = vi.fn(async () => {
      throw new DaemonIpcError("source not found", -32004);
    });
    const registry = createDaemonSourceRegistry({ query });

    await expect(registry.show("missing")).resolves.toBeUndefined();
    await expect(registry.secret("missing")).resolves.toBeUndefined();
  });
});
