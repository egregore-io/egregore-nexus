import { mkdtemp, rm, writeFile } from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";

import { callDaemonQuery } from "./ipcQuery.mjs";

it("uses the boot-scoped daemon endpoint for a bounded plain-Node query frame", async () => {
  const home = await mkdtemp(path.join(os.tmpdir(), "nexus-plain-ipc-"));
  const socketPath = process.platform === "win32"
    ? `\\\\.\\pipe\\nexus-plain-ipc-${process.pid}-${Date.now()}`
    : path.join(home, "daemon-ipc.sock");
  let request: Record<string, unknown> | undefined;
  const sockets = new Set<net.Socket>();
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
    let pending = Buffer.alloc(0);
    socket.on("data", (chunk) => {
      pending = Buffer.concat([pending, chunk]);
      if (pending.length < 4) return;
      const length = pending.readUInt32BE(0);
      if (pending.length < length + 4) return;
      request = JSON.parse(pending.subarray(4, length + 4).toString("utf8"));
      const body = Buffer.from(JSON.stringify({
        version: 1,
        requestId: "rpc_plain_query",
        result: { columns: ["value"], rows: [[7]], rowsAffected: 0 },
      }));
      const frame = Buffer.allocUnsafe(body.length + 4);
      frame.writeUInt32BE(body.length, 0);
      body.copy(frame, 4);
      socket.end(frame);
    });
  });

  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(socketPath, resolve);
    });
    await writeFile(path.join(home, "daemon-ipc-endpoint.json"), JSON.stringify({
      version: 1,
      path: socketPath,
      token: "plain-boot-token",
      daemonBootId: "plain-boot",
      createdAt: 1,
    }));

    await expect(callDaemonQuery(
      "local.store.read",
      { sql: "SELECT ? AS value", args: [7] },
      { nexusHome: home, requestId: "rpc_plain_query", timeoutMs: 1_000 },
    )).resolves.toEqual({ columns: ["value"], rows: [[7]], rowsAffected: 0 });
    expect(request).toEqual({
      version: 1,
      token: "plain-boot-token",
      requestId: "rpc_plain_query",
      caller: {
        name: "Nexus Gateway",
        project: "default",
        sessionId: "local-operator",
        runtimeId: "local-operator",
        kind: "human",
        tier: "admin",
      },
      call: {
        mode: "query",
        method: "local.store.read",
        params: { sql: "SELECT ? AS value", args: [7] },
      },
    });
  } finally {
    for (const socket of sockets) socket.destroy();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(home, { recursive: true, force: true });
  }
});
