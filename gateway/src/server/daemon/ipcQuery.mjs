// Minimal plain-Node twin of the TypeScript daemon IPC client. The packaged gateway's WebSocket
// upgrade graph loads under Node without a TS loader, so durable replay still needs this bounded
// query-only adapter.
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { createConnection } from "node:net";

const VERSION = 1;
const MAX_FRAME_BYTES = 16 * 1024 * 1024;
const DEFAULT_TIMEOUT_MS = 10_000;

export async function callDaemonQuery(method, params, options = {}) {
  const nexusHome = options.nexusHome ?? process.env.NEXUS_HOME?.trim() ?? join(homedir(), ".nexus");
  const manifestPath = join(nexusHome, "daemon-ipc-endpoint.json");
  let endpoint;
  try {
    endpoint = JSON.parse(await readFile(manifestPath, "utf8"));
  } catch (error) {
    throw new Error(`Nexus daemon IPC endpoint is unavailable; start the daemon (${errorMessage(error)})`);
  }
  if (endpoint.version !== VERSION || !endpoint.path || !endpoint.token) {
    throw new Error(`unsupported or incomplete daemon IPC endpoint manifest at ${manifestPath}`);
  }
  const requestId = options.requestId ?? `rpc_${randomUUID()}`;
  const response = await exchange(endpoint, {
    version: VERSION,
    token: endpoint.token,
    requestId,
    caller: options.caller ?? {
      name: "Nexus Gateway",
      project: "default",
      sessionId: "local-operator",
      runtimeId: "local-operator",
      kind: "human",
      tier: "admin",
    },
    call: { mode: "query", method, params },
  }, options.timeoutMs ?? DEFAULT_TIMEOUT_MS);
  if (response.version !== VERSION) {
    throw new Error(`unsupported daemon IPC response version ${response.version}`);
  }
  if (response.requestId !== requestId) {
    throw new Error(`daemon IPC response id mismatch: expected ${requestId}, received ${response.requestId}`);
  }
  if (response.error) {
    const error = new Error(response.error.message);
    error.code = response.error.code;
    error.data = response.error.data;
    throw error;
  }
  return response.result;
}

function exchange(endpoint, request, timeoutMs) {
  const payload = Buffer.from(JSON.stringify(request), "utf8");
  if (payload.length > MAX_FRAME_BYTES) {
    throw new Error(`daemon IPC request frame too large: ${payload.length}`);
  }
  const frame = Buffer.allocUnsafe(payload.length + 4);
  frame.writeUInt32BE(payload.length, 0);
  payload.copy(frame, 4);

  return new Promise((resolve, reject) => {
    const socket = createConnection({ path: endpoint.path });
    let settled = false;
    let pending = Buffer.alloc(0);
    const finish = (error, response) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      if (error) reject(error);
      else resolve(response);
    };
    socket.setTimeout(timeoutMs, () => {
      finish(new Error(`daemon IPC request timed out after ${timeoutMs}ms: ${request.requestId}`));
    });
    socket.once("error", (error) => finish(new Error(`daemon IPC connection failed: ${error.message}`)));
    socket.once("connect", () => socket.write(frame));
    socket.on("data", (chunk) => {
      pending = Buffer.concat([pending, chunk]);
      if (pending.length < 4) return;
      const length = pending.readUInt32BE(0);
      if (length > MAX_FRAME_BYTES) {
        finish(new Error(`daemon IPC response frame too large: ${length}`));
        return;
      }
      if (pending.length < length + 4) return;
      try {
        finish(undefined, JSON.parse(pending.subarray(4, length + 4).toString("utf8")));
      } catch (error) {
        finish(new Error(`invalid daemon IPC response JSON: ${errorMessage(error)}`));
      }
    });
    socket.once("end", () => {
      if (!settled) finish(new Error("daemon IPC response ended before one complete frame"));
    });
  });
}

function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}
