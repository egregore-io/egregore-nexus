import { readFileSync } from "node:fs";

import { beforeEach, describe, expect, it, vi } from "vitest";

const daemonReadClient = vi.hoisted(() => {
  const execute = vi.fn();
  return {
    execute,
    create: vi.fn(() => ({ execute })),
  };
});

vi.mock("@server/daemon/readClient", () => ({
  createDaemonReadClient: daemonReadClient.create,
}));

import { createReadDb } from "./client";

describe("daemon-owned production read view", () => {
  beforeEach(() => {
    daemonReadClient.create.mockClear();
    daemonReadClient.execute.mockClear();
  });

  it("resolves the daemon IPC endpoint from NEXUS_HOME", () => {
    expect(() => createReadDb({ NEXUS_HOME: "/tmp/nexus-owned" })).not.toThrow();
    expect(daemonReadClient.create).toHaveBeenCalledWith({
      nexusHome: "/tmp/nexus-owned",
    });
    expect(daemonReadClient.execute).not.toHaveBeenCalled();
  });

  it("uses the default daemon home when NEXUS_HOME is absent", () => {
    expect(() => createReadDb({})).not.toThrow();
    expect(daemonReadClient.create).toHaveBeenCalledWith({});
  });

  it("cannot be redirected to a database by legacy Hrana or file variables", () => {
    expect(() => createReadDb({
      NEXUS_DB_URL: "http://attacker.invalid:8080",
      NEXUS_DB_PATH: "/tmp/not-the-daemon.db",
      NEXUS_DB_READ_TOKEN: "legacy-read-token",
      NEXUS_DB_AUTH_TOKEN: "legacy-auth-token",
    })).not.toThrow();
    expect(daemonReadClient.create).toHaveBeenCalledWith({});

    const source = readFileSync("src/drizzle/client.ts", "utf8");
    expect(source).not.toMatch(/createClient|NEXUS_DB_URL|NEXUS_DB_PATH|NEXUS_DB_READ_TOKEN|Hrana/);
  });
});
