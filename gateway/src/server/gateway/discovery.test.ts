import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import {
  DEFAULT_GATEWAY_PORT,
  FALLBACK_GATEWAY_PORTS,
  gatewayDiscoveryPath,
  gatewayInstancePath,
  readGatewayDiscovery,
  readLiveGatewayDiscovery,
  removeGatewayDiscovery,
  resolveGatewayPort,
  writeGatewayDiscovery,
} from "./discovery";

describe("gateway serve discovery", () => {
  it("uses :4100 by default and falls back through :4101-:4110", async () => {
    const occupied = new Set([4100, 4101]);

    await expect(resolveGatewayPort({}, async (port) => !occupied.has(port))).resolves.toBe(4102);
    expect(DEFAULT_GATEWAY_PORT).toBe(4100);
    expect(FALLBACK_GATEWAY_PORTS).toContain(4110);
  });

  it("honors explicit NEXUS_GATEWAY_PORT without scanning the default", async () => {
    const checked: number[] = [];

    await expect(
      resolveGatewayPort({ NEXUS_GATEWAY_PORT: "4200" }, async (port) => {
        checked.push(port);
        return true;
      }),
    ).resolves.toBe(4200);
    expect(checked).toEqual([4200]);
  });

  it("writes gateway.json atomically with a stable instance id", async () => {
    const home = await mkdtemp(join(tmpdir(), "nexus-gateway-discovery-"));
    try {
      const first = await writeGatewayDiscovery(
        { home, port: 4103, authMode: "local" },
        { pid: 123, now: () => 111, randomId: () => "inst_test" },
      );
      const second = await writeGatewayDiscovery(
        { home, port: 4104, authMode: "remote" },
        { pid: 456, now: () => 222, randomId: () => "inst_other" },
      );

      expect(first.instanceId).toBe("inst_test");
      expect(second.instanceId).toBe("inst_test");
      expect(second).toMatchObject({
        url: "http://localhost:4104",
        port: 4104,
        authMode: "remote",
        pid: 456,
        boundAt: 222,
      });
      await expect(readGatewayDiscovery(home)).resolves.toEqual(second);
      await expect(readFile(gatewayInstancePath(home), "utf8")).resolves.toContain("inst_test");
      await expect(readFile(gatewayDiscoveryPath(home), "utf8")).resolves.toContain(
        "http://localhost:4104",
      );
    } finally {
      await rm(home, { recursive: true, force: true });
    }
  });

  it("rejects stale discovery when the recorded pid or health check is dead", async () => {
    const home = await mkdtemp(join(tmpdir(), "nexus-gateway-discovery-live-"));
    try {
      const record = await writeGatewayDiscovery(
        { home, port: 4103, authMode: "local" },
        { pid: 123, now: () => 111, randomId: () => "inst_test" },
      );
      await expect(readGatewayDiscovery(home)).resolves.toEqual(record);

      await expect(
        readLiveGatewayDiscovery(home, {
          isPidAlive: () => false,
          fetch: async () => new Response("ok"),
        }),
      ).resolves.toBeNull();

      await expect(
        readLiveGatewayDiscovery(home, {
          isPidAlive: () => true,
          fetch: async () => new Response("missing", { status: 503 }),
        }),
      ).resolves.toBeNull();

      await expect(
        readLiveGatewayDiscovery(home, {
          isPidAlive: () => true,
          fetch: async () => new Response("ok"),
        }),
      ).resolves.toEqual(record);
    } finally {
      await rm(home, { recursive: true, force: true });
    }
  });

  it("removes gateway.json on clean shutdown only for the matching writer", async () => {
    const home = await mkdtemp(join(tmpdir(), "nexus-gateway-discovery-cleanup-"));
    try {
      const first = await writeGatewayDiscovery(
        { home, port: 4103, authMode: "local" },
        { pid: 123, now: () => 111, randomId: () => "inst_test" },
      );
      await removeGatewayDiscovery({ ...first, pid: 999 }, home);
      await expect(readGatewayDiscovery(home)).resolves.toEqual(first);

      await removeGatewayDiscovery(first, home);
      await expect(readFile(gatewayDiscoveryPath(home), "utf8")).rejects.toThrow();
    } finally {
      await rm(home, { recursive: true, force: true });
    }
  });
});
