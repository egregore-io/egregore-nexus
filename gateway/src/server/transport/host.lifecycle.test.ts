import { chmod, copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setImmediate } from "node:timers/promises";

import { createClient, type Client, type InArgs, type InStatement } from "@libsql/client";
import { afterEach, describe, expect, it, vi } from "vitest";

import { windowsFixtureAcl } from "../../../test-fixtures/windowsTransportAuthority";
import { migrateGatewayStore } from "@server/store/migrations";
import { createTransportHost, type TransportHost } from "./host";
import * as manifests from "./manifest";

const fixtures: Array<{ root: string; db: Client; host: TransportHost }> = [];
const releases: Array<() => void> = [];
const hooks = new Set<Promise<unknown>>();

afterEach(async () => {
  for (const release of releases.splice(0)) release();
  await Promise.allSettled([...hooks]);
  await setImmediate();
  for (const fixture of fixtures.splice(0)) {
    await fixture.host.stop();
    await setImmediate();
    fixture.db.close();
    await rm(fixture.root, { recursive: true, force: true });
  }
  vi.restoreAllMocks();
});

describe("transport host owned shutdown", { timeout: process.platform === "win32" ? 60_000 : 5_000 }, () => {
  it.each(["resolve", "reject"] as const)("joins a held restart authority %s before stop returns", async (outcome) => {
    const fixture = await hostFixture();
    const gate = holdRestartAuthority(outcome);
    await fixture.host.start();
    await gate.entered;
    expect(await fixture.starts()).toBe(1);

    let stopped = false;
    const stopping = fixture.host.stop().then(() => { stopped = true; });
    // An event-loop checkpoint, not a latency assertion: the entered operation is still held.
    await setImmediate();
    expect(stopped, "stop returned while restart authority was still owned and pending").toBe(false);
    gate.release();
    await stopping;

    const calls = fixture.dbCalls();
    fixture.db.close();
    await setImmediate();
    expect(fixture.dbCalls()).toBe(calls);
    expect(await fixture.starts()).toBe(1);
    expect(fixture.host.states()).toEqual([]);
    expect(gate.calls()).toBe(2);
  });

  it.each(["crash-loop", "overflow"])("joins a held %s log without scheduling a post-stop restart", async (scenario) => {
    const fixture = await hostFixture(scenario);
    const gate = holdCrashLog(fixture.db);
    const timers = vi.spyOn(globalThis, "setTimeout");
    await fixture.host.start();
    await gate.entered;

    let stopped = false;
    const stopping = fixture.host.stop().then(() => { stopped = true; });
    await setImmediate();
    expect(stopped, "stop returned while the crash log was still owned and pending").toBe(false);
    const timerCount = timers.mock.calls.length;
    gate.release();
    await stopping;
    expect(timers.mock.calls).toHaveLength(timerCount);
    expect(await fixture.starts()).toBe(1);
    expect(fixture.host.states()).toEqual([]);
    const result = await fixture.db.execute("SELECT COUNT(*) AS count FROM logs WHERE scope = 'transport'");
    expect(Number(result.rows[0]?.count)).toBe(1);
  });

  it("restarts after the same held authority resolves when not stopped", async () => {
    const fixture = await hostFixture();
    const gate = holdRestartAuthority("resolve");
    await fixture.host.start();
    await gate.entered;
    expect(await fixture.starts()).toBe(1);
    gate.release();
    await vi.waitFor(async () => expect(await fixture.starts()).toBeGreaterThanOrEqual(2), waitOptions());
    await fixture.host.stop();
    expect(gate.calls()).toBeGreaterThanOrEqual(2);
  });

  it("disables and durably reports the same held authority rejection when not stopped", async () => {
    const fixture = await hostFixture();
    const gate = holdRestartAuthority("reject");
    await fixture.host.start();
    await gate.entered;
    gate.release();
    await vi.waitFor(async () => {
      expect(fixture.host.states()).toEqual([{ name: "fake", state: "disabled" }]);
      const result = await fixture.db.execute("SELECT message FROM logs WHERE message = 'held authority rejected'");
      expect(result.rows).toHaveLength(1);
    }, waitOptions());
    expect(await fixture.starts()).toBe(1);
    expect(gate.calls()).toBe(2);
  });

  it("restarts after the same held crash log completes when not stopped", async () => {
    const fixture = await hostFixture();
    const gate = holdCrashLog(fixture.db);
    await fixture.host.start();
    await gate.entered;
    expect(await fixture.starts()).toBe(1);
    gate.release();
    await vi.waitFor(async () => expect(await fixture.starts()).toBeGreaterThanOrEqual(2), waitOptions());
    await fixture.host.stop();
  });

  it.each(["crash-loop", "overflow"])("surfaces an owned %s logging failure from stop", async (scenario) => {
    const fixture = await hostFixture(scenario);
    const failure = new Error("held log write failed");
    const gate = holdCrashLog(fixture.db, failure);
    await fixture.host.start();
    await gate.entered;
    gate.release();
    await Promise.allSettled([...hooks]);
    await setImmediate();
    await expect(fixture.host.stop()).rejects.toBe(failure);
    expect(await fixture.starts()).toBe(1);
  });

  it("does not suppress a held database acquisition failure during stop", async () => {
    const fixture = await hostFixture();
    const gate = barrier();
    const failure = new Error("held database acquisition failed");
    fixture.nextDb(async () => {
      gate.enter();
      await gate.released;
      throw failure;
    });
    const starting = fixture.host.start().then(() => undefined, (error: unknown) => error);
    await gate.entered;
    const stopping = fixture.host.stop().then(() => undefined, (error: unknown) => error);
    gate.release();
    expect(await starting).toBe(failure);
    expect(await stopping).toBe(failure);
  });

  it.each(["resolve", "reject"] as const)("serializes a new start behind stop while the old start will %s", async (outcome) => {
    const fixture = await hostFixture("idle");
    const gate = barrier();
    const failure = new Error("held initial start failed");
    const loads = vi.spyOn(manifests, "loadTransportManifest");
    fixture.nextDb(async () => {
      gate.enter();
      await gate.released;
      if (outcome === "reject") throw failure;
      return fixture.db;
    });
    const capture = (work: Promise<void>) => work.then(() => undefined, (error: unknown) => error);
    const first = capture(fixture.host.start());
    await gate.entered;
    const stopping = capture(fixture.host.stop());
    const next = capture(fixture.host.start());
    gate.release();

    const expected = outcome === "reject" ? failure : undefined;
    expect(await first).toBe(expected);
    expect(await stopping).toBe(expected);
    expect(await next).toBe(expected);
    if (outcome === "resolve") {
      expect(loads).toHaveBeenCalledTimes(2);
      await vi.waitFor(() => expect(fixture.host.states()).toEqual([{ name: "fake", state: "running" }]), waitOptions());
      expect(await fixture.starts()).toBe(1);
    } else {
      // Failed shutdown is not permission to silently launch a replacement.
      expect(loads).toHaveBeenCalledTimes(1);
    }
    await fixture.host.stop();
    expect(fixture.host.states()).toEqual([]);
  });
});

function waitOptions() {
  return { timeout: process.platform === "win32" ? 30_000 : 1_000 };
}

function barrier() {
  let enter!: () => void;
  let release!: () => void;
  const entered = new Promise<void>((resolve) => { enter = resolve; });
  const released = new Promise<void>((resolve) => { release = resolve; });
  releases.push(release);
  return { entered, released, enter, release };
}

function observe<T>(work: Promise<T>): Promise<T> {
  hooks.add(work);
  void work.then(() => hooks.delete(work), () => hooks.delete(work));
  return work;
}

function holdRestartAuthority(outcome: "resolve" | "reject") {
  const gate = barrier();
  const original = manifests.revalidateTransportManifest;
  let calls = 0;
  vi.spyOn(manifests, "revalidateTransportManifest").mockImplementation((manifest) => observe((async () => {
    calls += 1;
    if (calls === 2) {
      gate.enter();
      await gate.released;
      if (outcome === "reject") throw new Error("held authority rejected");
    }
    await original(manifest);
  })()));
  return { ...gate, calls: () => calls };
}

function holdCrashLog(db: Client, failure?: Error) {
  const gate = barrier();
  const execute = db.execute.bind(db);
  let held = false;
  vi.spyOn(db, "execute").mockImplementation((statement: InStatement, args?: InArgs) => observe((async () => {
    const sql = typeof statement === "string" ? statement : statement.sql;
    if (!held && sql.includes("INSERT INTO logs")) {
      held = true;
      gate.enter();
      await gate.released;
      if (failure) throw failure;
    }
    return typeof statement === "string" ? execute(statement, args) : execute(statement);
  })()));
  return gate;
}

async function hostFixture(scenario = "crash-loop") {
  const root = await mkdtemp(join(tmpdir(), "nexus-transport-lifecycle-"));
  const home = join(root, "home");
  const dir = join(home, "gateway", "transports.d");
  await mkdir(dir, { recursive: true, mode: 0o700 });
  for (const path of [home, join(home, "gateway"), dir]) await chmod(path, 0o700);
  const entry = join(dir, "fake-bridge.mjs");
  await copyFile(join(process.cwd(), "test-fixtures", "fake-bridge.mjs"), entry);
  await chmod(entry, 0o700);
  const startCount = join(root, "starts.txt");
  await writeFile(join(dir, "fake.toml"), [
    'name = "fake"', 'provider = "fake"', 'entry = "fake-bridge.mjs"',
    `args = [${JSON.stringify(scenario)}, "", ${JSON.stringify(startCount)}]`, "",
  ].join("\n"), { mode: 0o600 });
  if (process.platform === "win32") await windowsFixtureAcl(home, "secure-tree");
  const db = createClient({ url: ":memory:" });
  await migrateGatewayStore(db);
  let calls = 0;
  let nextDb: (() => Promise<Client>) | undefined;
  const host = createTransportHost({ nexusHome: home, db: () => {
    calls += 1;
    const override = nextDb;
    nextDb = undefined;
    return override ? override() : db;
  }, backoffBaseMs: 1 });
  fixtures.push({ root, db, host });
  return { root, db, host, dbCalls: () => calls, nextDb: (override: () => Promise<Client>) => { nextDb = override; },
    starts: async () => Number(await readFile(startCount, "utf8")) };
}
