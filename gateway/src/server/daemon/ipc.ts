import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import net from "node:net";

const PROTOCOL_VERSION = 1;
const MAX_FRAME_BYTES = 16 * 1024 * 1024;
const DEFAULT_TIMEOUT_MS = 10_000;
const ENDPOINT_MANIFEST = "daemon-ipc-endpoint.json";

export type DaemonCallerKind = "agent" | "human" | "app" | "notification";
export type DaemonCallerLocality = "local" | "external" | "trusted";
export type DaemonCallerTier = "agent" | "admin";

export interface DaemonIpcCaller {
  name?: string;
  project: string;
  sessionId?: string;
  agentId?: string;
  runtimeId?: string;
  clientKey?: string;
  kind: DaemonCallerKind;
  locality?: DaemonCallerLocality;
  access?: string;
  principalId?: string;
  tier: DaemonCallerTier;
}

export type DaemonIpcCall =
  | {
      mode: "command";
      commandId: string;
      kind: string;
      params: unknown;
      idempotencyKey?: string;
    }
  | {
      mode: "enqueue";
      commandId: string;
      kind: string;
      params: unknown;
      idempotencyKey?: string;
    }
  | {
      mode: "query";
      method: string;
      params: unknown;
    };

export interface DaemonIpcRequest {
  version: number;
  token: string;
  requestId: string;
  caller?: DaemonIpcCaller;
  call: DaemonIpcCall;
}

export interface DaemonIpcErrorBody {
  code: number;
  message: string;
  data?: unknown;
}

interface DaemonIpcResponse {
  version: number;
  requestId: string;
  result?: unknown;
  error?: DaemonIpcErrorBody;
}

interface DaemonIpcEndpoint {
  version: number;
  path: string;
  token: string;
  daemonBootId: string;
  createdAt: number;
}

export interface DaemonIpcCallOptions {
  nexusHome?: string;
  requestId?: string;
  timeoutMs?: number;
}

export interface DaemonIpcCommandOptions extends DaemonIpcCallOptions {
  commandId?: string;
  idempotencyKey?: string;
}

/** Return the boot epoch advertised by the daemon's current local IPC endpoint. */
export async function readDaemonBootId(options: DaemonIpcCallOptions = {}): Promise<string> {
  const nexusHome = options.nexusHome ?? resolveNexusHome();
  const endpoint = await readEndpoint(nexusHome);
  if (typeof endpoint.daemonBootId !== "string" || !endpoint.daemonBootId) {
    throw new DaemonIpcError("daemon IPC endpoint manifest is missing daemonBootId");
  }
  return endpoint.daemonBootId;
}

export class DaemonIpcError extends Error {
  readonly code?: number;
  readonly data?: unknown;

  constructor(message: string, code?: number, data?: unknown) {
    super(message);
    this.name = "DaemonIpcError";
    this.code = code;
    this.data = data;
  }
}

export function resolveNexusHome(
  env: NodeJS.ProcessEnv = process.env,
  homeDir = os.homedir(),
): string {
  const configured = env.NEXUS_HOME?.trim();
  return configured || path.join(homeDir, ".nexus");
}

export async function callDaemonCommand<T = unknown>(
  kind: string,
  params: unknown,
  caller: DaemonIpcCaller,
  options: DaemonIpcCommandOptions = {},
): Promise<T> {
  const commandId = options.commandId ?? `cmd_${randomUUID()}`;
  return callDaemon<T>({
    mode: "command",
    commandId,
    kind,
    params,
    ...(options.idempotencyKey
      ? { idempotencyKey: options.idempotencyKey }
      : {}),
  }, caller, options);
}

export async function callDaemonEnqueue<T = unknown>(
  kind: string,
  params: unknown,
  caller: DaemonIpcCaller,
  options: DaemonIpcCommandOptions = {},
): Promise<T> {
  const commandId = options.commandId ?? `cmd_${randomUUID()}`;
  return callDaemon<T>({
    mode: "enqueue",
    commandId,
    kind,
    params,
    ...(options.idempotencyKey
      ? { idempotencyKey: options.idempotencyKey }
      : {}),
  }, caller, options);
}

export async function callDaemonQuery<T = unknown>(
  method: string,
  params: unknown,
  caller: DaemonIpcCaller,
  options: DaemonIpcCallOptions = {},
): Promise<T> {
  return callDaemon<T>({ mode: "query", method, params }, caller, options);
}

async function callDaemon<T>(
  call: DaemonIpcCall,
  caller: DaemonIpcCaller,
  options: DaemonIpcCallOptions,
): Promise<T> {
  const nexusHome = options.nexusHome ?? resolveNexusHome();
  const endpoint = await readEndpoint(nexusHome);
  const requestId = options.requestId ?? `rpc_${randomUUID()}`;
  const response = await exchange(endpoint, {
    version: PROTOCOL_VERSION,
    token: endpoint.token,
    requestId,
    caller,
    call,
  }, options.timeoutMs ?? DEFAULT_TIMEOUT_MS);
  if (response.version !== PROTOCOL_VERSION) {
    throw new DaemonIpcError(
      `unsupported daemon IPC response version ${response.version}; expected ${PROTOCOL_VERSION}`,
    );
  }
  if (response.requestId !== requestId) {
    throw new DaemonIpcError(
      `daemon IPC response id mismatch: expected ${requestId}, received ${response.requestId}`,
    );
  }
  if (response.error) {
    throw new DaemonIpcError(
      response.error.message,
      response.error.code,
      response.error.data,
    );
  }
  return response.result as T;
}

async function readEndpoint(nexusHome: string): Promise<DaemonIpcEndpoint> {
  const manifest = path.join(nexusHome, ENDPOINT_MANIFEST);
  let raw: string;
  try {
    raw = await readFile(manifest, "utf8");
  } catch (error) {
    throw new DaemonIpcError(
      `Nexus daemon IPC endpoint is unavailable; start the daemon (${errorMessage(error)})`,
    );
  }
  let endpoint: DaemonIpcEndpoint;
  try {
    endpoint = JSON.parse(raw) as DaemonIpcEndpoint;
  } catch (error) {
    throw new DaemonIpcError(`invalid daemon IPC endpoint manifest: ${errorMessage(error)}`);
  }
  if (
    endpoint.version !== PROTOCOL_VERSION ||
    typeof endpoint.path !== "string" ||
    !endpoint.path ||
    typeof endpoint.token !== "string" ||
    !endpoint.token
  ) {
    throw new DaemonIpcError(
      `unsupported or incomplete daemon IPC endpoint manifest at ${manifest}`,
    );
  }
  return endpoint;
}

async function exchange(
  endpoint: DaemonIpcEndpoint,
  request: DaemonIpcRequest,
  timeoutMs: number,
): Promise<DaemonIpcResponse> {
  const payload = Buffer.from(JSON.stringify(request), "utf8");
  if (payload.length > MAX_FRAME_BYTES) {
    throw new DaemonIpcError(`daemon IPC request frame too large: ${payload.length}`);
  }
  const frame = Buffer.allocUnsafe(payload.length + 4);
  frame.writeUInt32BE(payload.length, 0);
  payload.copy(frame, 4);

  return new Promise<DaemonIpcResponse>((resolve, reject) => {
    const socket = net.createConnection({ path: endpoint.path });
    let settled = false;
    let pending = Buffer.alloc(0);
    const finish = (
      error?: Error,
      response?: DaemonIpcResponse,
    ) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      if (error) reject(error);
      else resolve(response as DaemonIpcResponse);
    };
    socket.setTimeout(timeoutMs, () => {
      finish(new DaemonIpcError(
        `daemon IPC request timed out after ${timeoutMs}ms: ${request.requestId}`,
      ));
    });
    socket.once("error", (error) => {
      finish(new DaemonIpcError(`daemon IPC connection failed: ${error.message}`));
    });
    socket.once("connect", () => socket.write(frame));
    socket.on("data", (chunk) => {
      pending = Buffer.concat([pending, chunk]);
      if (pending.length < 4) return;
      const length = pending.readUInt32BE(0);
      if (length > MAX_FRAME_BYTES) {
        finish(new DaemonIpcError(`daemon IPC response frame too large: ${length}`));
        return;
      }
      if (pending.length < length + 4) return;
      try {
        finish(undefined, JSON.parse(
          pending.subarray(4, length + 4).toString("utf8"),
        ) as DaemonIpcResponse);
      } catch (error) {
        finish(new DaemonIpcError(
          `invalid daemon IPC response JSON: ${errorMessage(error)}`,
        ));
      }
    });
    socket.once("end", () => {
      if (!settled) {
        finish(new DaemonIpcError("daemon IPC response ended before one complete frame"));
      }
    });
  });
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
