import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, mkdir, readFile, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { removeOwnedDiscovery, writeDiscovery } from "../lib/lifecycle.mjs";

test("writes bounded Webconsole discovery atomically", async () => {
  const home = await mkdtemp(join(tmpdir(), "nexus-webconsole-"));
  const path = join(home, "webconsole.json");
  const record = {
    pid: 42,
    host: "127.0.0.1",
    port: 4200,
    url: "http://127.0.0.1:4200",
    gatewayUrl: "http://127.0.0.1:4100",
    startedAtMs: 123,
    executable: "/opt/nexus-webui.mjs",
  };

  await writeDiscovery(path, record);

  assert.deepEqual(JSON.parse(await readFile(path, "utf8")), record);
});

test("cleanup never removes a successor discovery record", async () => {
  const home = await mkdtemp(join(tmpdir(), "nexus-webconsole-owner-"));
  const path = join(home, "webconsole.json");
  await writeFile(path, JSON.stringify({ pid: 43 }));

  await removeOwnedDiscovery(path, 42);
  assert.equal(JSON.parse(await readFile(path, "utf8")).pid, 43);

  await removeOwnedDiscovery(path, 43);
  await assert.rejects(readFile(path, "utf8"), { code: "ENOENT" });
});

test("server publishes its bound endpoint and removes only its own record", async () => {
  const home = await mkdtemp(join(tmpdir(), "nexus-webconsole-live-"));
  const dist = join(home, "dist");
  const discovery = join(home, "webconsole.json");
  await mkdir(dist);
  await writeFile(join(dist, "index.html"), "<!doctype html><title>Nexus</title>");
  const port = await reservePort();
  const child = spawn(
    process.execPath,
    [
      new URL("../bin/nexus-webui.mjs", import.meta.url).pathname,
      "--host",
      "127.0.0.1",
      "--port",
      String(port),
      "--gateway-url",
      "http://127.0.0.1:4100",
      "--discovery",
      discovery,
    ],
    { env: { ...process.env, NEXUS_WEBUI_DIST: dist }, stdio: "ignore" },
  );
  try {
    const record = await waitForDiscovery(discovery);
    assert.equal(record.pid, child.pid);
    assert.equal(record.url, `http://127.0.0.1:${port}`);
    const healthResponse = await fetch(`${record.url}/health`);
    assert.equal(healthResponse.status, 200);
    assert.deepEqual(await healthResponse.json(), {
      ok: true,
      service: "nexus-webui",
      pid: child.pid,
      host: "127.0.0.1",
      port,
      url: record.url,
      gateway: "http://127.0.0.1:4100",
      executable: record.executable,
    });
    assert.equal((await fetch(record.url)).status, 200);
  } finally {
    child.kill("SIGTERM");
    await new Promise((resolve) => child.once("exit", resolve));
  }
  await assert.rejects(readFile(discovery, "utf8"), { code: "ENOENT" });
});

async function reservePort() {
  const server = createServer();
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();
  await new Promise((resolve) => server.close(resolve));
  return port;
}

async function waitForDiscovery(path) {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    try {
      return JSON.parse(await readFile(path, "utf8"));
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
  }
  throw new Error("Webconsole discovery did not appear");
}
