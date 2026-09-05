import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, mkdir, readFile, realpath, symlink, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { createServer as createHttpServer } from "node:http";
import { once } from "node:events";
import { WebSocket, WebSocketServer } from "ws";
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

test("umbrella package launcher records the executable that Core spawned", async () => {
  const home = await mkdtemp(join(tmpdir(), "nexus-webconsole-umbrella-"));
  const dist = join(home, "dist");
  const discovery = join(home, "webconsole.json");
  const modules = join(home, "node_modules");
  const umbrella = join(modules, "@egregore", "nexus");
  const gateway = join(modules, "@egregore", "nexus-gateway");
  const launcher = join(umbrella, "bin", "nexus-webui.mjs");
  const installedBin = join(modules, ".bin", "nexus-webui");
  await mkdir(dist, { recursive: true });
  await mkdir(join(umbrella, "bin"), { recursive: true });
  await mkdir(join(gateway, "webconsole", "bin"), { recursive: true });
  await mkdir(join(gateway, "webconsole", "lib"), { recursive: true });
  await mkdir(join(modules, ".bin"), { recursive: true });
  await writeFile(join(dist, "index.html"), "<!doctype html><title>Nexus</title>");
  await writeFile(join(umbrella, "package.json"), '{"type":"module"}\n');
  await writeFile(join(gateway, "package.json"), '{"type":"module"}\n');
  await writeFile(
    launcher,
    await readFile(new URL("../../../packages/nexus/bin/nexus-webui.mjs", import.meta.url)),
  );
  await writeFile(
    join(gateway, "webconsole", "bin", "nexus-webui.mjs"),
    await readFile(new URL("../bin/nexus-webui.mjs", import.meta.url)),
  );
  await writeFile(
    join(gateway, "webconsole", "lib", "lifecycle.mjs"),
    await readFile(new URL("../lib/lifecycle.mjs", import.meta.url)),
  );
  await symlink(launcher, installedBin);
  const spawnedExecutable = await realpath(installedBin);
  const port = await reservePort();
  const child = spawn(
    process.execPath,
    [
      spawnedExecutable,
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
    assert.equal(record.executable, spawnedExecutable);
  } finally {
    child.kill("SIGTERM");
    await new Promise((resolve) => child.once("exit", resolve));
  }
  await assert.rejects(readFile(discovery, "utf8"), { code: "ENOENT" });
});

test("packaged WebUI proxies bidirectional Gateway WebSocket upgrades", { timeout: 10000 }, async () => {
  const home = await mkdtemp(join(tmpdir(), "nexus-webconsole-ws-"));
  const dist = join(home, "dist");
  const discovery = join(home, "webconsole.json");
  await mkdir(dist);
  await writeFile(join(dist, "index.html"), "<!doctype html><title>Nexus</title>");
  const upstream = createHttpServer((_req, res) => { res.writeHead(426); res.end(); });
  const wss = new WebSocketServer({ server: upstream });
  const paths = [];
  wss.on("connection", (ws, req) => {
    paths.push(req.url);
    ws.on("message", (data) => ws.send(`ack:${data}`));
  });
  await new Promise((resolve) => upstream.listen(0, "127.0.0.1", resolve));
  const port = await reservePort();
  const child = spawn(process.execPath, [
    new URL("../bin/nexus-webui.mjs", import.meta.url).pathname,
    "--port", String(port), "--gateway-url", `http://127.0.0.1:${upstream.address().port}`,
    "--discovery", discovery,
  ], { env: { ...process.env, NEXUS_WEBUI_DIST: dist }, stdio: "ignore" });
  let socket;
  try {
    await waitForDiscovery(discovery);
    socket = new WebSocket(`ws://127.0.0.1:${port}/api/agui/ws?agentId=a_test`);
    await once(socket, "open");
    const reply = once(socket, "message");
    socket.send("browser-message");
    assert.equal(String((await reply)[0]), "ack:browser-message");
    assert.deepEqual(paths, ["/api/agui/ws?agentId=a_test"]);
    socket.close();
    await once(socket, "close");
  } finally {
    socket?.terminate();
    for (const client of wss.clients) client.terminate();
    wss.close();
    await new Promise((resolve) => upstream.close(resolve));
    const exit = once(child, "exit");
    child.kill("SIGTERM");
    await exit;
  }
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
