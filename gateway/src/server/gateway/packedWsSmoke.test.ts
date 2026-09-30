import { execFile, spawn, type SpawnOptionsWithoutStdio } from "node:child_process";
import { createHash } from "node:crypto";
import { once } from "node:events";
import { cp, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import {
  createServer as createNetServer,
  Socket,
  type Server as NetServer,
} from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { promisify } from "node:util";

import { createClient } from "@libsql/client";
import WebSocket, { type RawData } from "ws";
import { afterEach, describe, expect, it } from "vitest";
import { collectClosedSqliteHandles } from "../../../test-fixtures/closedSqliteHandles";

const execFileAsync = promisify(execFile);
const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
const temporaryRoots: string[] = [];
const npmProgram = process.platform === "win32" ? process.execPath : "npm";
const npmArgs = process.platform === "win32"
  ? [join(dirname(process.execPath), "node_modules", "npm", "bin", "npm-cli.js")] : [];

afterEach(async () => {
  await collectClosedSqliteHandles();
  await Promise.all(
    temporaryRoots.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

describe("packed public Gateway WebSocket closure", () => {
  it("runs both WS lanes through the installed launcher from a foreign cwd", async () => {
    const temporaryRoot = await mkdtemp(join(tmpdir(), "nexus-packed-ws-"));
    temporaryRoots.push(temporaryRoot);
    const installed = await installPackedGateway(temporaryRoot);
    const serveSource = await readFile(
      join(installed.packageRoot, "scripts", "gateway-serve-impl.mjs"),
      "utf8",
    );
    expect(serveSource).not.toContain("../src/server/");
    expect(serveSource).not.toContain("src/server");
    expect(serveSource).not.toContain("tsx/cli");

    await exerciseConcurrentPackedInitialization(installed, temporaryRoot);
    await exerciseLocalFacet(installed, temporaryRoot);
    await exerciseRemotePackedLanes(installed, temporaryRoot);
  }, 240_000);
});

interface PackedInstall {
  foreignCwd: string;
  gatewayBin: string;
  packageRoot: string;
}

async function installPackedGateway(temporaryRoot: string): Promise<PackedInstall> {
  const stage = join(temporaryRoot, "stage");
  const packs = join(temporaryRoot, "packs");
  const installRoot = join(temporaryRoot, "install");
  const foreignCwd = join(temporaryRoot, "foreign-cwd");
  await mkdir(join(stage, "dist-gateway"), { recursive: true });
  await mkdir(join(stage, "scripts"), { recursive: true });
  await mkdir(join(stage, "webconsole"), { recursive: true });
  await mkdir(packs, { recursive: true });
  await mkdir(installRoot, { recursive: true });
  await mkdir(foreignCwd, { recursive: true });
  await cp(join(root, "package.json"), join(stage, "package.json"));
  for (const script of [
    "gateway-serve-impl.mjs",
    "gateway-serve.mjs",
    "nexus-gateway.mjs",
    "nexus.mjs",
  ]) {
    await cp(join(root, "scripts", script), join(stage, "scripts", script));
  }
  for (const asset of ["bin", "lib", "dist"]) {
    await cp(
      join(root, "webconsole", asset),
      join(stage, "webconsole", asset),
      { recursive: true },
    );
  }
  await execFileAsync(
    process.execPath,
    [
      "--eval", 'require("esbuild").buildSync(JSON.parse(process.argv[1]))',
      JSON.stringify({ entryPoints: ["src/server/gateway/headless.ts"], bundle: true,
        platform: "node", format: "esm", target: "node20", packages: "external",
        outfile: join(stage, "dist-gateway", "headless.mjs") }),
    ],
    { cwd: root, timeout: 60_000 },
  );
  const packed = await execFileAsync(
    npmProgram,
    [...npmArgs, "pack", "--ignore-scripts", "--json", "--pack-destination", packs],
    { cwd: stage, timeout: 60_000 },
  );
  const [{ filename }] = JSON.parse(packed.stdout) as [{ filename: string }];
  const cliPacked = await execFileAsync(
    npmProgram,
    [...npmArgs, "pack", "--ignore-scripts", "--json", "--pack-destination", packs],
    { cwd: resolve(root, "../packages/nexus-cli"), timeout: 60_000 },
  );
  const [{ filename: cliFilename }] = JSON.parse(cliPacked.stdout) as [{ filename: string }];
  await execFileAsync(npmProgram, [...npmArgs, "init", "-y"], { cwd: installRoot, timeout: 30_000 });
  await execFileAsync(
    npmProgram,
    [
      ...npmArgs,
      "install",
      "--ignore-scripts",
      "--no-audit",
      "--no-fund",
      "--no-package-lock",
      join(packs, cliFilename),
      join(packs, filename),
    ],
    { cwd: installRoot, timeout: 180_000 },
  );
  const packageRoot = join(installRoot, "node_modules", "@egregore", "nexus-gateway");
  expect(resolve(packageRoot)).not.toBe(resolve(root));
  await readFile(join(packageRoot, "dist-gateway", "headless.mjs"));
  await readFile(join(packageRoot, "webconsole", "dist", "index.html"));
  return {
    foreignCwd,
    packageRoot,
    gatewayBin: join(packageRoot, "scripts", "nexus-gateway.mjs"),
  };
}

async function exerciseConcurrentPackedInitialization(
  installed: PackedInstall,
  temporaryRoot: string,
): Promise<void> {
  const nexusHome = join(temporaryRoot, "concurrent-home", ".nexus");
  const url = `file:${join(nexusHome, "gateway.db")}`;
  await mkdir(nexusHome, { recursive: true });
  const lock = await holdExclusiveStoreLock(url);
  const startedAt = Date.now();
  try {
    // Load the installed native addon in an owned child. Windows holds a loaded
    // .node DLL until process exit, even after every DB/statement is closed.
    const result = await execFileAsync(process.execPath, [
      "--input-type=module", "--eval",
      'const { migrateHeadlessGatewayStore } = await import(process.argv[1]); ' +
      'console.log(JSON.stringify(await migrateHeadlessGatewayStore()));',
      pathToFileURL(join(installed.packageRoot, "dist-gateway", "headless.mjs")).href,
    ], { env: isolatedEnv({ NEXUS_GATEWAY_DB: url }), timeout: 30_000 });
    expect(JSON.parse(result.stdout)).toEqual({ schemaVersion: expect.any(Number) });
    expect(Date.now() - startedAt).toBeGreaterThanOrEqual(250);
  } finally {
    await lock.exit;
  }
}

async function holdExclusiveStoreLock(url: string): Promise<{ exit: Promise<void> }> {
  const child = spawn(
    process.execPath,
    [
      "--input-type=module",
      "-e",
      `import { createClient } from "@libsql/client";
       const db = createClient({ url: process.env.LOCK_URL });
       await db.execute("BEGIN EXCLUSIVE");
       await db.execute("PRAGMA user_version = 1");
       process.stdout.write("LOCKED\\n");
       await new Promise((resolve) => setTimeout(resolve, 750));
       await db.execute("ROLLBACK");
       db.close();`,
    ],
    {
      cwd: root,
      env: { ...process.env, LOCK_URL: url },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk: string) => {
    stderr += chunk;
  });
  const exit = new Promise<void>((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (code === 0) resolve();
      else reject(new Error(`SQLite lock owner exited ${code ?? signal}: ${stderr}`));
    });
  });
  await new Promise<void>((resolve, reject) => {
    let stdout = "";
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      stdout += chunk;
      if (stdout.includes("LOCKED\n")) resolve();
    });
    void exit.catch(reject);
  });
  return { exit };
}

async function exerciseLocalFacet(
  installed: PackedInstall,
  temporaryRoot: string,
): Promise<void> {
  const home = join(temporaryRoot, "local-home");
  const nexusHome = join(home, ".nexus");
  await mkdir(nexusHome, { recursive: true });
  const port = await reservePort();
  const discovery = join(nexusHome, "gateway.json");
  const ipc = await startDaemonIpcFixture(nexusHome);
  const child = spawnGateway(installed.gatewayBin, {
    cwd: installed.foreignCwd,
    env: isolatedEnv({
      HOME: home,
      NEXUS_HOME: nexusHome,
      NEXUS_GATEWAY_DB: `file:${join(nexusHome, "gateway.db")}`,
      NEXUS_GATEWAY_BIND: "127.0.0.1",
      NEXUS_GATEWAY_PORT: String(port),
      NEXUS_GATEWAY_CLOSE_TIMEOUT_MS: "1000",
      NEXUS_WEB_AUTH_MODE: "local-operator",
    }),
  });
  const sockets: WebSocket[] = [];
  try {
    await waitForHealth(child, port);
    expect(await waitForJson(discovery)).toMatchObject({
      url: `http://127.0.0.1:${port}`,
      port,
      authMode: "local",
    });
    const socket = new WebSocket(`ws://127.0.0.1:${port}/api/agui/ws`);
    sockets.push(socket);
    const frames = await collectFrames(
      socket,
      () => socket.send(JSON.stringify({ t: "ping" })),
      (seen) => seen.some((frame) => frame.t === "pong"),
    );
    expect(frames).toContainEqual({ t: "pong" });
    const mutation = await fetch(`http://127.0.0.1:${port}/api/conversation/prompt`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        name: "packed-agent",
        text: "cookie-free local packed mutation",
        clientMessageId: "cm_packed_local",
      }),
    });
    expect(mutation.status).toBe(201);
    expect(ipc.count("harness.prompt")).toBe(1);
  } finally {
    await terminateChildGracefully(child);
    await closeWebSockets(sockets);
    await ipc.close();
    await assertPortClosed(port);
  }
  await expect(readFile(discovery, "utf8")).rejects.toMatchObject({ code: "ENOENT" });
}

async function exerciseRemotePackedLanes(
  installed: PackedInstall,
  temporaryRoot: string,
): Promise<void> {
  const home = join(temporaryRoot, "remote-home");
  const nexusHome = join(home, ".nexus");
  await mkdir(nexusHome, { recursive: true });
  const gatewayPath = join(nexusHome, "gateway.db");
  const gatewayDb = `file:${gatewayPath}`;
  const port = await reservePort();
  const discovery = join(nexusHome, "gateway.json");
  const ipc = await startDaemonIpcFixture(nexusHome);
  const push = await startDaemonPushFixture(nexusHome);
  const env = isolatedEnv({
    HOME: home,
    NEXUS_HOME: nexusHome,
    NEXUS_GATEWAY_DB: gatewayDb,
    NEXUS_GATEWAY_BIND: "127.0.0.1",
    NEXUS_GATEWAY_PORT: String(port),
    NEXUS_GATEWAY_CLOSE_TIMEOUT_MS: "1000",
    NEXUS_WEB_AUTH_MODE: "remote-human",
  });
  let child: ReturnType<typeof spawn> | undefined;
  const sockets: WebSocket[] = [];
  const csrfToken = "csrf-packed";
  const cookie = `nexus_human=human-packed; nexus_csrf=${csrfToken}`;
  const bearerToken = "nx_at_packed_bearer";
  try {
    await execFileAsync(process.execPath, [installed.gatewayBin, "--migrate-only"], {
      cwd: installed.foreignCwd,
      env,
      timeout: 30_000,
    });
    const seed = createClient({ url: gatewayDb });
    try {
      await seed.batch([
        {
          sql: `INSERT INTO human_user
                (name_key, name, password_hash, client_key, project, daemon_session_id,
                 created_at, updated_at, human_user_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          args: [
            "packed-human",
            "packed-human",
            "unused",
            "nexus_ck_packed_human",
            "metadata-only",
            "s_previous_boot",
            1_780_000_000_000,
            1_780_000_000_000,
            "hu_aaaaaaaaaaaaaaaaaaaaaaaa",
          ],
        },
        {
          sql: `INSERT INTO principals
                (principal_id, kind, access, created_at)
                VALUES (?, ?, ?, ?)`,
          args: [
            "h_aaaaaaaaaaaaaaaaaaaaaaaa",
            "local.human",
            "admin",
            1_780_000_000_000,
          ],
        },
        {
          sql: `INSERT INTO human_session
                (cookie_token, name, client_key, project, daemon_session_id, created_at,
                 human_user_id, principal_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
          args: [
            "human-packed",
            "packed-human",
            "nexus_ck_packed_human",
            "metadata-only",
            "s_previous_boot",
            1_780_000_000_000,
            "hu_aaaaaaaaaaaaaaaaaaaaaaaa",
            "h_aaaaaaaaaaaaaaaaaaaaaaaa",
          ],
        },
        {
          sql: `INSERT INTO identities
                (agent_id, name, owner, role, tier, metadata_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)`,
          args: ["a_packed", "packed-agent", null, "agent", "agent", "{}", 1],
        },
        {
          sql: `INSERT INTO rest_bearer_token
                (token_id, family_id, actor_name, actor_project, actor_kind, actor_tier,
                 actor_session_id, actor_agent_id, actor_runtime_id, actor_client_key,
                 scopes_json, access_hash, refresh_hash, expires_at, refresh_expires_at,
                 revoked_at, last_used_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)`,
          args: [
            "bt_packed",
            "bf_packed",
            "packed-machine",
            "metadata-only",
            "agent",
            "agent",
            "s_packed_machine",
            "a_packed_machine",
            "r_packed_machine",
            "nexus_ck_packed_machine",
            JSON.stringify(["message:send"]),
            sha256(bearerToken),
            sha256("nx_rt_packed_refresh"),
            2_000_000_000_000,
            2_000_000_100_000,
            1_780_000_000_000,
          ],
        },
        {
          sql: `INSERT INTO runtime_descriptors
                (runtime_id, agent_id, session_id, harness, mode, backend, cwd,
                 native_resume_key, status, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          args: [
            "r_packed",
            "a_packed",
            "s_packed",
            "codex",
            "headless",
            "acp",
            "/work",
            null,
            "online",
            2,
          ],
        },
        {
          sql: `INSERT INTO bus_messages
                (message_id, kind, from_name, from_agent_id, to_name, to_agent_id,
                 thread_id, topic, summary, body, provenance_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          args: [
            "m_replay_packed",
            "thread",
            "Fable",
            "a_fable",
            "packed-smoke",
            null,
            "packed-smoke",
            null,
            null,
            "replay body stays out of the event envelope",
            "{}",
            1_780_000_000_050,
          ],
        },
      ], "write");
    } finally {
      seed.close();
    }

    child = spawnGateway(installed.gatewayBin, {
      cwd: installed.foreignCwd,
      env,
    });
    try {
      // Health is the unauthenticated startup/readiness contract even when the
      // installed Gateway runs in remote-human auth mode.
      await waitForHealth(child, port);
    } catch (error) {
      throw new Error(`${String(error)}; push=${JSON.stringify(push.frames)}`);
    }
    await push.waitForFrame((frame) => frame.t === "hello");
    expect(await waitForJson(discovery)).toMatchObject({
      url: `http://127.0.0.1:${port}`,
      port,
      authMode: "remote",
    });

    const httpBase = `http://127.0.0.1:${port}`;
    const wsBase = `ws://127.0.0.1:${port}`;
    const promptBody = JSON.stringify({
      name: "packed-agent",
      text: "packed direct HTTP auth",
      clientMessageId: "cm_packed_http",
    });
    const rejectedHttp = await fetch(`${httpBase}/api/conversation/prompt`, {
      method: "POST",
      headers: { cookie, "content-type": "application/json" },
      body: promptBody,
    });
    expect(rejectedHttp.status).toBe(403);
    expect(ipc.count("harness.prompt")).toBe(0);

    const acceptedHttp = await fetch(`${httpBase}/api/conversation/prompt`, {
      method: "POST",
      headers: {
        cookie,
        "content-type": "application/json",
        "x-nexus-csrf": csrfToken,
      },
      body: promptBody,
    });
    expect(acceptedHttp.status).toBe(201);
    expect(ipc.count("identity.register")).toBe(1);
    expect(ipc.count("harness.prompt")).toBe(1);
    expect(ipc.requests.find((request) => commandKind(request) === "harness.prompt"))
      .toMatchObject({ caller: { name: "packed-human", sessionId: "s_packed_human" } });

    const missingProtocol = new WebSocket(
      `${wsBase}/api/v1/agent-sessions/s_packed/events?nexus_csrf=${csrfToken}`,
      ["nexus-v1"],
      { headers: { cookie, "x-nexus-csrf": csrfToken } },
    );
    sockets.push(missingProtocol);
    const missingFrames = await collectFrames(
      missingProtocol,
      () => missingProtocol.send(sessionInput("cm_ws_missing", {
        csrf: csrfToken,
        "x-nexus-csrf": csrfToken,
      })),
      (frames) => frames.some((frame) => frame.t === "input.err"),
    );
    expect(missingFrames).toContainEqual(expect.objectContaining({
      t: "input.err",
      clientMessageId: "cm_ws_missing",
      status: 403,
    }));
    expect(ipc.count("harness.prompt")).toBe(1);

    const duplicateProtocol = new WebSocket(
      `${wsBase}/api/v1/agent-sessions/s_packed/events`,
      ["nexus-v1", `nexus-csrf.${csrfToken}`, "nexus-csrf.second-proof"],
      { headers: { cookie } },
    );
    sockets.push(duplicateProtocol);
    const duplicateFrames = await collectFrames(
      duplicateProtocol,
      () => duplicateProtocol.send(sessionInput("cm_ws_duplicate")),
      (frames) => frames.some((frame) => frame.t === "input.err"),
    );
    expect(duplicateFrames).toContainEqual(expect.objectContaining({
      t: "input.err",
      clientMessageId: "cm_ws_duplicate",
      status: 403,
    }));
    expect(ipc.count("harness.prompt")).toBe(1);

    const mismatchedProtocol = new WebSocket(
      `${wsBase}/api/v1/agent-sessions/s_packed/events`,
      ["nexus-v1", "nexus-csrf.wrong"],
      { headers: { cookie, "x-nexus-csrf": csrfToken } },
    );
    sockets.push(mismatchedProtocol);
    const mismatchedFrames = await collectFrames(
      mismatchedProtocol,
      () => mismatchedProtocol.send(sessionInput("cm_ws_mismatched")),
      (frames) => frames.some((frame) => frame.t === "input.err"),
    );
    expect(mismatchedFrames).toContainEqual(expect.objectContaining({
      t: "input.err",
      clientMessageId: "cm_ws_mismatched",
      status: 403,
    }));
    expect(ipc.count("harness.prompt")).toBe(1);

    const socket = new WebSocket(
      `${wsBase}/api/v1/agent-sessions/s_packed/events`,
      ["nexus-v1", `nexus-csrf.${csrfToken}`],
      { headers: { cookie } },
    );
    sockets.push(socket);
    const acceptedFrames = await collectFrames(
      socket,
      () => socket.send(sessionInput("cm_ws_accepted")),
      (frames) => frames.some((frame) => frame.t === "input.ack"),
    );
    expect(acceptedFrames).toContainEqual(expect.objectContaining({
      t: "input.ack",
      clientMessageId: "cm_ws_accepted",
      sessionId: "s_packed",
      }));
    expect(ipc.count("harness.prompt")).toBe(2);

    const bearerMutation = await fetch(`${httpBase}/api/v1/messages`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${bearerToken}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({
        to: { verb: "post", thread: "packed-bearer" },
        body: "cookie-free bearer packed mutation",
        idempotencyKey: "cm_packed_bearer",
      }),
    });
    expect(bearerMutation.status).toBe(201);
    expect(ipc.count("harness.prompt")).toBe(2);
    expect(ipc.count("message.post.send")).toBe(1);

    const topic = "sys.message.thread.packed-smoke";
    const subscribeFrames = await collectFrames(
      socket,
      () => socket.send(JSON.stringify({ t: "subscribe", topic, afterSeq: 0 })),
      (frames) => frames.some((frame) =>
        frame.type === "developer.event" &&
        (frame.event as { messageId?: unknown } | undefined)?.messageId === "m_replay_packed"
      ),
    );
    expect(subscribeFrames).toContainEqual({ t: "subscribe.ack", topic, afterSeq: 0 });
    expect(subscribeFrames).toContainEqual(expect.objectContaining({
      type: "developer.event",
      event: expect.objectContaining({
        topic,
        messageId: "m_replay_packed",
      }),
    }));
    expect(JSON.stringify(subscribeFrames)).not.toContain("replay body stays out");

    const liveThreadEvent = collectFrames(
      socket,
      () => undefined,
      (frames) => frames.some((frame) =>
        frame.type === "developer.event" &&
        (frame.event as { messageId?: unknown } | undefined)?.messageId === "m_live_packed"
      ),
    );
    const postResponse = await fetch(`${httpBase}/api/v1/messages`, {
      method: "POST",
      headers: {
        cookie,
        "content-type": "application/json",
        "x-nexus-csrf": csrfToken,
      },
      body: JSON.stringify({
        to: { verb: "post", thread: "packed-smoke" },
        body: "distinctive packed live wake",
        idempotencyKey: "cm_live_packed",
      }),
    });
    expect(postResponse.status).toBe(201);
    expect(ipc.count("message.post.send")).toBe(2);
    push.send({
      t: "projection",
      event: {
        eventId: "message:m_live_packed",
        daemonEpoch: "boot-packed",
        seq: 1,
        occurredAt: 1_780_000_000_100,
        kind: "message.accepted",
        version: 1,
        payload: {
          messageId: "m_live_packed",
          scope: "thread",
          fromName: "packed-human",
          threadId: "packed-smoke",
          threadName: "packed-smoke",
          toName: "packed-smoke",
          body: "distinctive packed live wake",
          createdAt: 1_780_000_000_100,
          provenance: { caller: "human:packed-human" },
        },
      },
    });
    const liveFrames = await liveThreadEvent;
    expect(liveFrames).toContainEqual(expect.objectContaining({
      type: "developer.event",
      event: expect.objectContaining({
        topic,
        messageId: "m_live_packed",
      }),
    }));
    await push.waitForFrame((frame) => frame.t === "projection.ack");

    const sessionSocket = new WebSocket(
      `${wsBase}/api/v1/agent-sessions/s_packed/events`,
      ["nexus-v1", `nexus-csrf.${csrfToken}`],
      { headers: { cookie } },
    );
    sockets.push(sessionSocket);
    const sessionEvent = collectFrames(
      sessionSocket,
      () => undefined,
      (frames) => frames.some((frame) =>
        (frame.event as { data?: { text?: unknown } } | undefined)?.data?.text ===
          "distinctive-packed-session-relay"
      ),
    );
    await push.waitForFrame((frame) =>
      frame.t === "subscribe" && frame.lane === "agent" && frame.sessionId === "s_packed"
    );
    push.send({
      t: "agent.update",
      sessionId: "s_packed",
      streamEventId: 1,
      kind: "text",
      data: { text: "distinctive-packed-session-relay", streamEventId: 1 },
    });
    const sessionFrames = await sessionEvent;
    expect(sessionFrames).toContainEqual(expect.objectContaining({
      epoch: "boot-packed",
      event: expect.objectContaining({
        type: "agent.update",
        sessionId: "s_packed",
        kind: "text",
        data: expect.objectContaining({
          text: "distinctive-packed-session-relay",
          streamEventId: 1,
        }),
      }),
    }));
    const relayedSessionFrame = sessionFrames.find((frame) =>
      (frame.event as { data?: { text?: unknown } } | undefined)?.data?.text ===
        "distinctive-packed-session-relay"
    );
    expect(relayedSessionFrame).toBeDefined();
    expect(
      JSON.parse(
        Buffer.from(String(relayedSessionFrame?.cursor), "base64url").toString("utf8"),
      ),
    ).toEqual({ v: 1, daemonBootId: "boot-packed", id: 1 });
  } finally {
    if (child) {
      await terminateChildGracefully(child);
    }
    await closeWebSockets(sockets);
    await Promise.all([ipc.close(), push.close()]);
    if (child) await assertPortClosed(port);
  }
  await expect(readFile(discovery, "utf8")).rejects.toMatchObject({ code: "ENOENT" });
}

interface DaemonIpcFixture {
  requests: Array<Record<string, unknown>>;
  count(kind: string): number;
  close(): Promise<void>;
}

async function startDaemonIpcFixture(nexusHome: string): Promise<DaemonIpcFixture> {
  const socketPath = fixtureSocketPath(nexusHome, "daemon-ipc");
  const requests: Array<Record<string, unknown>> = [];
  const sockets = new Set<Socket>();
  const server = createNetServer((socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
    readFramedJson(socket, (request) => {
      requests.push(request);
      const result = daemonResult(request, requests.length);
      writeFramedJson(socket, {
        version: 1,
        requestId: request.requestId,
        result,
      });
      socket.end();
    });
  });
  await listenUnix(server, socketPath);
  await writeFile(join(nexusHome, "daemon-ipc-endpoint.json"), JSON.stringify({
    version: 1,
    path: socketPath,
    token: "packed-ipc-token",
    daemonBootId: "boot-packed",
    createdAt: 1,
  }));
  return {
    requests,
    count(kind) {
      return requests.filter((request) => commandKind(request) === kind).length;
    },
    close: () => closeNetFixture(server, sockets, socketPath),
  };
}

function daemonResult(request: Record<string, unknown>, sequence: number): unknown {
  const call = request.call as Record<string, unknown> | undefined;
  const kind = typeof call?.kind === "string" ? call.kind : undefined;
  if (kind === "identity.register") {
    return { sessionId: "s_packed_human", agentId: "a_packed_human" };
  }
  if (kind === "harness.prompt") {
    return {
      commandId: String(call?.commandId ?? "cmd_packed_prompt"),
      status: "pending",
      createdAt: 1_780_000_000_000 + sequence,
      revision: 1,
      sessionId: "s_packed",
      seq: sequence,
    };
  }
  if (kind === "message.post.send") {
    return { messageId: "m_live_packed", delivered: 1 };
  }
  if (call?.mode === "query" && call.method === "local.sessionQueue.read") {
    return {
      target: "packed-agent",
      sessionId: "s_packed",
      turnActive: false,
      steerCapability: "interrupt_and_send",
      commands: [],
      seq: 0,
    };
  }
  return { ok: true };
}

function commandKind(request: Record<string, unknown>): string | undefined {
  const call = request.call;
  if (!call || typeof call !== "object") return undefined;
  return typeof (call as Record<string, unknown>).kind === "string"
    ? String((call as Record<string, unknown>).kind)
    : undefined;
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

interface DaemonPushFixture {
  frames: Array<Record<string, unknown>>;
  send(frame: Record<string, unknown>): void;
  waitForFrame(predicate: (frame: Record<string, unknown>) => boolean): Promise<void>;
  close(): Promise<void>;
}

async function startDaemonPushFixture(nexusHome: string): Promise<DaemonPushFixture> {
  const socketPath = fixtureSocketPath(nexusHome, "daemon-push");
  const frames: Array<Record<string, unknown>> = [];
  const sockets = new Set<Socket>();
  let current: Socket | undefined;
  const server = createNetServer((socket) => {
    sockets.add(socket);
    current = socket;
    socket.once("close", () => {
      sockets.delete(socket);
      if (current === socket) current = undefined;
    });
    readFramedJson(socket, (frame) => {
      frames.push(frame);
      if (frame.t === "hello") {
        writeFramedJson(socket, {
          t: "ready",
          version: 1,
          daemonBootId: "boot-packed",
          resume: "store",
        });
      }
    });
  });
  await listenUnix(server, socketPath);
  await writeFile(join(nexusHome, "gateway-stream-endpoint.json"), JSON.stringify({
    version: 1,
    path: socketPath,
    token: "packed-push-token",
    daemonBootId: "boot-packed",
    createdAt: 1,
  }));
  return {
    frames,
    send(frame) {
      if (!current || current.destroyed) throw new Error("packed daemon push is not connected");
      writeFramedJson(current, frame);
    },
    waitForFrame: (predicate) => waitFor(() => frames.some(predicate), 10_000),
    close: () => closeNetFixture(server, sockets, socketPath),
  };
}

function readFramedJson(
  socket: Socket,
  onFrame: (frame: Record<string, unknown>) => void,
): void {
  let pending = Buffer.alloc(0);
  socket.on("data", (chunk) => {
    pending = Buffer.concat([pending, chunk]);
    while (pending.length >= 4) {
      const length = pending.readUInt32BE(0);
      if (pending.length < length + 4) return;
      const payload = pending.subarray(4, length + 4);
      pending = pending.subarray(length + 4);
      onFrame(JSON.parse(payload.toString("utf8")) as Record<string, unknown>);
    }
  });
}

function writeFramedJson(socket: Socket, value: unknown): void {
  const payload = Buffer.from(JSON.stringify(value), "utf8");
  const frame = Buffer.allocUnsafe(payload.length + 4);
  frame.writeUInt32BE(payload.length, 0);
  payload.copy(frame, 4);
  socket.write(frame);
}

async function listenUnix(server: NetServer, socketPath: string): Promise<void> {
  await new Promise<void>((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(socketPath, resolveListen);
  });
}

async function closeNetFixture(
  server: NetServer,
  sockets: Set<Socket>,
  socketPath: string,
): Promise<void> {
  for (const socket of sockets) socket.destroy();
  await new Promise<void>((resolveClose) => server.close(() => resolveClose()));
  if (process.platform !== "win32") await rm(socketPath, { force: true });
}

function fixtureSocketPath(rootPath: string, label: string): string {
  return process.platform === "win32"
    ? `\\\\.\\pipe\\nexus-packed-${process.pid}-${label}`
    : join(rootPath, `${label}.sock`);
}

function sessionInput(
  clientMessageId: string,
  extra: Record<string, unknown> = {},
): string {
  return JSON.stringify({
    t: "input",
    mode: "session",
    agentId: "a_packed",
    expectedSessionId: "s_packed",
    target: { name: "packed-agent" },
    text: "packed WebSocket auth",
    clientMessageId,
    ...extra,
  });
}

async function collectFrames(
  socket: WebSocket,
  send: () => void,
  done: (frames: Array<Record<string, unknown>>) => boolean,
): Promise<Array<Record<string, unknown>>> {
  return new Promise((resolveFrames, reject) => {
    const frames: Array<Record<string, unknown>> = [];
    const timer = setTimeout(() => reject(new Error("packed WS frame wait timed out")), 10_000);
    const finish = () => {
      clearTimeout(timer);
      socket.off("error", onError);
      socket.off("message", onMessage);
      resolveFrames(frames);
    };
    const onError = (error: Error) => {
      clearTimeout(timer);
      reject(error);
    };
    const onMessage = (data: RawData) => {
      const frame = JSON.parse(String(data)) as Record<string, unknown>;
      frames.push(frame);
      if (done(frames)) finish();
    };
    socket.on("error", onError);
    socket.on("message", onMessage);
    if (socket.readyState === WebSocket.OPEN) send();
    else socket.once("open", send);
  });
}

async function closeWebSockets(sockets: WebSocket[]): Promise<void> {
  await Promise.all(sockets.map(async (socket) => {
    if (socket.readyState === WebSocket.CLOSED) return;
    const closed = once(socket, "close");
    socket.terminate();
    await settledWithin(closed, 1_000);
  }));
}

async function waitForHealth(
  child: ReturnType<typeof spawn>,
  port: number,
  headers?: HeadersInit,
): Promise<void> {
  const deadline = Date.now() + 15_000;
  let stderr = "";
  let lastStatus: number | undefined;
  let lastBody = "";
  child.stderr?.on("data", (data: Buffer) => {
    stderr += String(data);
  });
  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      throw new Error(`packed public Gateway exited before healthy (${child.exitCode}): ${stderr}`);
    }
    try {
      const response = await fetch(`http://127.0.0.1:${port}/api/v1/health`, {
        headers,
        signal: AbortSignal.timeout(500),
      });
      lastStatus = response.status;
      lastBody = await response.text();
      if (response.ok) return;
    } catch {
      // Startup races are expected until the installed listener is ready.
    }
    await delay(50);
  }
  throw new Error(
    `packed public Gateway did not become healthy ` +
    `(status=${String(lastStatus)} body=${lastBody}): ${stderr}`,
  );
}

async function waitForJson(path: string): Promise<Record<string, unknown>> {
  let parsed: Record<string, unknown> | undefined;
  await waitFor(async () => {
    try {
      parsed = JSON.parse(await readFile(path, "utf8")) as Record<string, unknown>;
      return true;
    } catch {
      return false;
    }
  }, 5_000);
  return parsed!;
}

async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await delay(20);
  }
  throw new Error("timed out waiting for packed fixture condition");
}

async function reservePort(): Promise<number> {
  const server = createNetServer();
  await new Promise<void>((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolveListen);
  });
  const address = server.address();
  if (!address || typeof address === "string") {
    server.close();
    throw new Error("failed to reserve a packed Gateway port");
  }
  await new Promise<void>((resolveClose, reject) => {
    server.close((error) => (error ? reject(error) : resolveClose()));
  });
  return address.port;
}

async function assertPortClosed(port: number): Promise<void> {
  try {
    await waitFor(async () => !(await canConnect(port)), 8_000);
  } catch {
    throw new Error(`packed Gateway port ${port} remained open after shutdown`);
  }
}

async function canConnect(port: number): Promise<boolean> {
  const socket = new Socket();
  return new Promise<boolean>((resolveConnect) => {
    const finish = (connected: boolean) => {
      socket.destroy();
      resolveConnect(connected);
    };
    socket.setTimeout(250, () => finish(false));
    socket.once("error", () => finish(false));
    socket.once("connect", () => finish(true));
    socket.connect(port, "127.0.0.1");
  });
}

function isolatedEnv(overrides: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const env = { ...process.env };
  for (const key of Object.keys(env)) {
    if (key.startsWith("NEXUS_")) delete env[key];
  }
  delete env.NODE_PATH;
  delete env.INIT_CWD;
  return { ...env, ...overrides };
}

function spawnGateway(entry: string, options: SpawnOptionsWithoutStdio) {
  // Windows child.kill(SIGTERM) uses TerminateProcess: it cannot exercise a JS
  // signal handler. This fixture-only parent IPC invokes that same handler in the
  // actual installed launcher. Native CLI/task termination is a separate gate.
  const args = process.platform === "win32"
    ? ["--input-type=module", "--eval",
      'import { pathToFileURL } from "node:url"; ' +
      'process.on("message", (message) => { if (message === "fixture-sigterm") process.emit("SIGTERM"); }); ' +
      'await import(pathToFileURL(process.argv[1]).href);', entry]
    : [entry];
  return spawn(process.execPath, args, {
    ...options,
    stdio: process.platform === "win32" ? ["ignore", "pipe", "pipe", "ipc"] : ["ignore", "pipe", "pipe"],
  });
}

async function terminateChildGracefully(child: ReturnType<typeof spawn>): Promise<void> {
  if (child.exitCode !== null) return;
  const gracefulExit = once(child, "exit");
  if (process.platform === "win32") child.send!("fixture-sigterm");
  else child.kill("SIGTERM");
  if (await settledWithin(gracefulExit, 2_500)) return;
  const forcedExit = once(child, "exit");
  child.kill("SIGKILL");
  const forceSettled = await settledWithin(forcedExit, 2_000);
  if (!forceSettled && child.exitCode === null) {
    throw new Error("packed Gateway did not terminate after SIGKILL");
  }
  throw new Error("packed Gateway missed the SIGTERM shutdown deadline");
}

async function settledWithin(promise: Promise<unknown>, timeoutMs: number): Promise<boolean> {
  return Promise.race([
    promise.then(() => true),
    delay(timeoutMs).then(() => false),
  ]);
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, milliseconds));
}
