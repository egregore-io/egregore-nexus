import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";

import {
  callDaemonCommand,
  callDaemonEnqueue,
  callDaemonQuery,
  readDaemonBootId,
  type DaemonIpcRequest,
} from "./ipc";

interface Fixture {
  home: string;
  requests: DaemonIpcRequest[];
  close: () => Promise<void>;
}

async function fixture(result: unknown): Promise<Fixture> {
  const home = await mkdtemp(path.join(os.tmpdir(), "nexus-gateway-ipc-"));
  await mkdir(home, { recursive: true });
  const socketPath = process.platform === "win32"
    ? `\\\\.\\pipe\\nexus-gateway-ipc-${process.pid}-${Date.now()}`
    : path.join(home, "daemon-ipc.sock");
  const requests: DaemonIpcRequest[] = [];
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
      const request = JSON.parse(
        pending.subarray(4, length + 4).toString("utf8"),
      ) as DaemonIpcRequest;
      requests.push(request);
      const payload = Buffer.from(JSON.stringify({
        version: 1,
        requestId: request.requestId,
        result,
      }));
      const frame = Buffer.allocUnsafe(payload.length + 4);
      frame.writeUInt32BE(payload.length, 0);
      payload.copy(frame, 4);
      socket.end(frame);
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, resolve);
  });
  await writeFile(
    path.join(home, "daemon-ipc-endpoint.json"),
    JSON.stringify({
      version: 1,
      path: socketPath,
      token: "boot-token",
      daemonBootId: "boot-test",
      createdAt: 1,
    }),
  );
  return {
    home,
    requests,
    close: async () => {
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await rm(home, { recursive: true, force: true });
    },
  };
}

const caller = {
  name: "Alex",
  project: "metadata-only",
  sessionId: "local-operator",
  runtimeId: "local-operator",
  kind: "human" as const,
  tier: "admin" as const,
};

describe("gateway daemon IPC", () => {
  it("reads the daemon boot epoch from the selected Nexus home", async () => {
    const fx = await fixture(null);
    try {
      await expect(readDaemonBootId({ nexusHome: fx.home })).resolves.toBe("boot-test");
      expect(fx.requests).toEqual([]);
    } finally {
      await fx.close();
    }
  });

  it("sends one bounded command frame and returns the held daemon result", async () => {
    const fx = await fixture({ messageId: "m_1", delivered: 1 });
    try {
      await expect(callDaemonCommand(
        "message.post.send",
        { to: { verb: "dm", name: "blake" }, body: "hello" },
        caller,
        {
          nexusHome: fx.home,
          commandId: "cmd_gateway_1",
          requestId: "rpc_gateway_1",
          timeoutMs: 1_000,
        },
      )).resolves.toEqual({ messageId: "m_1", delivered: 1 });
      expect(fx.requests).toEqual([{
        version: 1,
        token: "boot-token",
        requestId: "rpc_gateway_1",
        caller,
        call: {
          mode: "command",
          commandId: "cmd_gateway_1",
          kind: "message.post.send",
          params: { to: { verb: "dm", name: "blake" }, body: "hello" },
        },
      }]);
    } finally {
      await fx.close();
    }
  });

  it("routes read calls through the same endpoint without command fields", async () => {
    const fx = await fixture({ members: [] });
    try {
      await expect(callDaemonQuery("members", {}, caller, {
        nexusHome: fx.home,
        requestId: "rpc_gateway_members",
        timeoutMs: 1_000,
      })).resolves.toEqual({ members: [] });
      expect(fx.requests[0]?.call).toEqual({
        mode: "query",
        method: "members",
        params: {},
      });
    } finally {
      await fx.close();
    }
  });

  it("requests a durable enqueue receipt without waiting for execution", async () => {
    const receipt = {
      commandId: "cmd_prompt_1",
      status: "pending",
      createdAt: 1,
      revision: 1,
      sessionId: "s_target",
      seq: 4,
    };
    const fx = await fixture(receipt);
    try {
      await expect(callDaemonEnqueue(
        "harness.prompt",
        { name: "blake", text: "next" },
        caller,
        {
          nexusHome: fx.home,
          commandId: "cmd_prompt_1",
          idempotencyKey: "client-prompt-1",
          requestId: "rpc_prompt_1",
          timeoutMs: 1_000,
        },
      )).resolves.toEqual(receipt);
      expect(fx.requests[0]?.call).toEqual({
        mode: "enqueue",
        commandId: "cmd_prompt_1",
        kind: "harness.prompt",
        params: { name: "blake", text: "next" },
        idempotencyKey: "client-prompt-1",
      });
    } finally {
      await fx.close();
    }
  });
});
