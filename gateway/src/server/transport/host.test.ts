import { chmod, copyFile, mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createClient } from "@libsql/client";
import { afterEach, describe, expect, it, vi } from "vitest";

import { handle } from "@server/api/router";
import { bindLane } from "@server/store/repos/lane-bindings";
import { migrateGatewayStore } from "@server/store/migrations";
import { applyCanonicalProjection } from "@server/projection/apply";
import { setTransportSecret } from "./secrets";
import {
  bindIngressAuthority,
  createTransportHost,
  type TransportHost,
  type TransportIngressEvent,
} from "./host";
import { enqueueObligationsForMessage } from "./outbox";
import { gatewayTransportStates, publishGatewayTransportHost } from "./registry";

const roots: string[] = [];
afterEach(async () => {
  const { rm } = await import("node:fs/promises");
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("transport host protocol", () => {
  it("uses an allowlisted environment, resolves durable bindings after start, and dedupes ingress", async () => {
    const fixture = await hostFixture("happy");
    const db = createClient({ url: `file:${fixture.dbPath}` });
    await migrateGatewayStore(db);
    await setTransportSecret(db, "fake.token", "super-secret");
    await bindLane(db, { provider: "fake", externalChatId: "group-1", laneKind: "thread", laneName: "design" });
    const ingress = vi.fn<(event: TransportIngressEvent) => Promise<void>>(async () => undefined);
    const host = createTransportHost({
      nexusHome: fixture.home,
      db: () => db,
      env: { ...process.env, LEAK: "must-not-reach-child", PATH: process.env.PATH, HOME: fixture.home, LANG: "C" },
      onIngress: ingress,
      backoffBaseMs: 5,
    });
    await host.start();
    await eventually(() => expect(host.states()).toContainEqual({ name: "fake", state: "running" }));
    const unpublish = publishGatewayTransportHost(host);
    expect(await capabilityProviders()).toEqual([{ name: "fake", state: "running" }]);
    unpublish();
    await eventually(() => expect(ingress).toHaveBeenCalledTimes(1));
    expect(ingress.mock.calls[0]?.[0]).toMatchObject({
      ingressId: "ingress-1",
      lane: { laneKind: "thread", laneName: "design" },
      principal: { kind: "external.human" },
    });
    const childEnv = JSON.parse(await readFile(fixture.envCapture, "utf8")) as Record<string, string>;
    expect(childEnv).toEqual({
      PATH: process.env.PATH,
      HOME: fixture.home,
      LANG: "C",
      FAKE_TOKEN: "super-secret",
    });
    const logs = await db.execute("SELECT message FROM logs WHERE scope = 'transport' ORDER BY seq");
    expect(logs.rows.map((row) => String(row.message)).join("\n")).not.toContain("super-secret");
    await host.stop();
    db.close();
  });

  it("disables an unsupported hello without a restart loop", async () => {
    const fixture = await hostFixture("unsupported");
    const db = createClient({ url: `file:${fixture.dbPath}` });
    await migrateGatewayStore(db);
    await setTransportSecret(db, "fake.token", "super-secret");
    const host = createTransportHost({ nexusHome: fixture.home, db: () => db, backoffBaseMs: 5 });
    await host.start();
    await eventually(() => expect(host.states()).toContainEqual({ name: "fake", state: "disabled" }));
    const unpublish = publishGatewayTransportHost(host);
    expect(await capabilityProviders()).toEqual([{ name: "fake", state: "disabled" }]);
    unpublish();
    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(Number(await readFile(fixture.startCount, "utf8"))).toBe(1);
    await host.stop();
    db.close();
  });

  it("resolves a bridge-created lane from the durable table after restart", async () => {
    const fixture = await hostFixture("bind-restart");
    const db = createClient({ url: `file:${fixture.dbPath}` });
    await migrateGatewayStore(db);
    await setTransportSecret(db, "fake.token", "secret");
    const ingress = vi.fn<(event: TransportIngressEvent) => Promise<void>>(async () => undefined);
    const host = createTransportHost({
      nexusHome: fixture.home,
      db: () => db,
      onIngress: ingress,
      backoffBaseMs: 5,
    });
    await host.start();
    await eventually(() => expect(ingress).toHaveBeenCalledTimes(1), 4_000);
    expect(ingress.mock.calls[0]?.[0]).toMatchObject({
      chatId: "restart-chat",
      lane: { laneKind: "thread", laneName: "restart-thread" },
    });
    expect(Number(await readFile(fixture.startCount, "utf8"))).toBeGreaterThanOrEqual(2);
    await host.stop();
    db.close();
  });

  it("binds first private contact to the external principal DM and routes the reply to that chat", async () => {
    const fixture = await hostFixture("dm-first-contact");
    const db = createClient({ url: `file:${fixture.dbPath}` });
    await migrateGatewayStore(db);
    await setTransportSecret(db, "fake.token", "secret");
    const ingress = vi.fn<(event: TransportIngressEvent) => Promise<void>>(async () => undefined);
    const host = createTransportHost({
      nexusHome: fixture.home,
      db: () => db,
      onIngress: ingress,
      backoffBaseMs: 5,
    });
    await host.start();
    await eventually(() => expect(ingress).toHaveBeenCalledTimes(1));
    const event = ingress.mock.calls[0]![0];
    expect(event.lane).toEqual({
      provider: "fake",
      externalChatId: "private-chat-1",
      laneKind: "dm",
      laneName: event.principal.principalId,
    });

    await applyCanonicalProjection(db, {
      eventId: "message:m_private_reply",
      daemonEpoch: "boot-private",
      seq: 1,
      occurredAt: 1,
      kind: "message.accepted",
      version: 1,
      payload: {
        messageId: "m_private_reply",
        scope: "dm",
        toName: event.principal.principalId,
        body: "private reply",
      },
    });
    await eventually(async () => {
      const result = await db.execute(
        "SELECT external_chat_id, lane_kind, lane_name, state FROM transport_outbox " +
        "WHERE message_id = 'm_private_reply'",
      );
      expect(result.rows[0]).toMatchObject({
        external_chat_id: "private-chat-1",
        lane_kind: "dm",
        lane_name: event.principal.principalId,
        state: "delivered",
      });
    });
    await host.stop();
    db.close();
  });

  it("rolls back the subject when first-contact lane binding fails", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.execute(`CREATE TRIGGER refuse_first_contact_lane
      BEFORE INSERT ON transport_lane_bindings
      BEGIN SELECT RAISE(ABORT, 'injected lane failure'); END`);

    await expect(bindIngressAuthority(db, {
      provider: "fake",
      externalUserId: "atomic-user",
      displayName: "Atomic User",
      externalChatId: "atomic-chat",
    })).rejects.toThrow(/injected lane failure/);

    for (const table of ["principals", "subject_bindings", "transport_lane_bindings"]) {
      const rows = await db.execute(`SELECT COUNT(*) AS count FROM ${table}`);
      expect(Number(rows.rows[0]?.count), table).toBe(0);
    }
    db.close();
  });

  it("caps unsettled delivery at 256, resumes, and ignores duplicate receipts", async () => {
    const fixture = await hostFixture("slow");
    const db = createClient({ url: `file:${fixture.dbPath}` });
    await migrateGatewayStore(db);
    await setTransportSecret(db, "fake.token", "secret");
    await bindLane(db, { provider: "fake", externalChatId: "capacity-chat", laneKind: "thread", laneName: "capacity" });
    for (let index = 0; index < 260; index += 1) {
      await enqueueObligationsForMessage(db, {
        messageId: `m_capacity_${index.toString().padStart(3, "0")}`,
        kind: "thread",
        toName: "capacity",
        body: `message ${index}`,
        createdAt: index,
      });
    }
    const host = createTransportHost({ nexusHome: fixture.home, db: () => db, backoffBaseMs: 5 });
    await host.start();
    await eventually(async () => {
      expect(await readFile(join(fixture.stateRoot, "first-batch.txt"), "utf8")).toBe("256");
    }, 4_000);
    await eventually(async () => {
      const result = await db.execute("SELECT COUNT(*) AS count FROM transport_outbox WHERE state = 'delivered'");
      expect(Number(result.rows[0]?.count)).toBe(260);
    }, 10_000);
    await host.stop();
    db.close();
  }, 20_000);

  it("redelivers from a bridge journal after provider success without a duplicate provider call", async () => {
    const fixture = await hostFixture("journal-crash");
    const db = createClient({ url: `file:${fixture.dbPath}` });
    await migrateGatewayStore(db);
    await setTransportSecret(db, "fake.token", "secret");
    await bindLane(db, { provider: "fake", externalChatId: "journal-chat", laneKind: "thread", laneName: "journal" });
    await enqueueObligationsForMessage(db, {
      messageId: "m_journal",
      kind: "thread",
      toName: "journal",
      body: "once",
      createdAt: 1,
    });
    const host = createTransportHost({ nexusHome: fixture.home, db: () => db, backoffBaseMs: 5 });
    await host.start();
    await eventually(async () => {
      const result = await db.execute("SELECT state FROM transport_outbox WHERE message_id = 'm_journal'");
      expect(result.rows[0]?.state).toBe("delivered");
    }, 4_000);
    expect(await readFile(join(fixture.stateRoot, "provider-calls.txt"), "utf8")).toBe("1");
    expect(Number(await readFile(fixture.startCount, "utf8"))).toBeGreaterThanOrEqual(2);
    await host.stop();
    db.close();
  });

  it("wakes a running bridge only after the canonical message transaction commits", async () => {
    const fixture = await hostFixture("idle");
    const db = createClient({ url: `file:${fixture.dbPath}` });
    await migrateGatewayStore(db);
    await setTransportSecret(db, "fake.token", "secret");
    await bindLane(db, { provider: "fake", externalChatId: "wake-chat", laneKind: "thread", laneName: "wake" });
    const host = createTransportHost({ nexusHome: fixture.home, db: () => db, backoffBaseMs: 5 });
    await host.start();
    await eventually(() => expect(host.states()).toContainEqual({ name: "fake", state: "running" }));

    await applyCanonicalProjection(db, {
      eventId: "message:m_wake",
      daemonEpoch: "boot-wake",
      seq: 1,
      occurredAt: 1,
      kind: "message.accepted",
      version: 1,
      payload: {
        messageId: "m_wake",
        scope: "thread",
        toName: "wake",
        body: "wake the bridge",
      },
    });
    await eventually(async () => {
      const result = await db.execute("SELECT state FROM transport_outbox WHERE message_id = 'm_wake'");
      expect(result.rows[0]?.state).toBe("delivered");
    });
    await host.stop();
    db.close();
  });

  it.each(["crash-loop", "overflow"])(
    "opens the circuit after ten %s failures",
    async (scenario) => {
      const fixture = await hostFixture(scenario);
      const db = createClient({ url: `file:${fixture.dbPath}` });
      await migrateGatewayStore(db);
      await setTransportSecret(db, "fake.token", "secret");
      const host = createTransportHost({ nexusHome: fixture.home, db: () => db, backoffBaseMs: 1 });
      await host.start();
      await eventually(() => expect(host.states()).toContainEqual({ name: "fake", state: "disabled" }), 6_000);
      expect(Number(await readFile(fixture.startCount, "utf8"))).toBe(10);
      await host.stop();
      db.close();
    },
    10_000,
  );
});

async function capabilityProviders() {
  const response = await handle(
    {
      method: "GET",
      path: "/api/v1/capabilities",
      query: {},
      headers: {},
      caller: { name: "operator", project: "default" },
    },
    {
      db: () => { throw new Error("capability read must not open the read DB"); },
      transportStates: gatewayTransportStates,
    },
  );
  return ((response.body as Record<string, unknown>).protocol as {
    surfaces: { transports: { providers: ReturnType<TransportHost["states"]> } };
  }).surfaces.transports.providers;
}

async function hostFixture(scenario: string) {
  const root = await mkdtemp(join(tmpdir(), "nexus-transport-host-"));
  roots.push(root);
  const home = join(root, "home");
  const dir = join(home, "gateway", "transports.d");
  await mkdir(dir, { recursive: true, mode: 0o700 });
  await chmod(home, 0o700);
  await chmod(join(home, "gateway"), 0o700);
  await chmod(dir, 0o700);
  const entry = join(dir, "fake-bridge.mjs");
  await copyFile(join(process.cwd(), "test-fixtures", "fake-bridge.mjs"), entry);
  await chmod(entry, 0o700);
  const envCapture = join(root, "env.json");
  const startCount = join(root, "starts.txt");
  const dbPath = join(root, "gateway.db");
  const stateRoot = join(root, "state");
  await writeFile(join(dir, "fake.toml"), [
    'name = "fake"',
    'provider = "fake"',
    'entry = "fake-bridge.mjs"',
    `args = [${JSON.stringify(scenario)}, ${JSON.stringify(envCapture)}, ${JSON.stringify(startCount)}, ${JSON.stringify(stateRoot)}]`,
    '[config]',
    `scenario = ${JSON.stringify(scenario)}`,
    `envCapture = ${JSON.stringify(envCapture)}`,
    `startCount = ${JSON.stringify(startCount)}`,
    '[secretRefs]',
    'FAKE_TOKEN = "fake.token"',
    "",
  ].join("\n"), { mode: 0o600 });
  return { root, home, dir, entry, envCapture, startCount, stateRoot, dbPath };
}

async function eventually(assertion: () => void | Promise<void>, timeoutMs = 2000) {
  const deadline = Date.now() + timeoutMs;
  let error: unknown;
  while (Date.now() < deadline) {
    try {
      await assertion();
      return;
    } catch (caught) {
      error = caught;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }
  throw error;
}
