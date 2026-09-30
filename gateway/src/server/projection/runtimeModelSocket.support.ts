import type { Client } from "@libsql/client";
import { execFile } from "node:child_process";
import { EventEmitter, once } from "node:events";
import { createServer } from "node:http";
import { createRequire } from "node:module";
import type { AddressInfo } from "node:net";
import { promisify } from "node:util";
import { expect, vi } from "vitest";
import { Kind, Locality, Tier } from "@shared/types";
import { makeDispatch } from "../../routes/api/v1/$";
import { createRuntimeSnapshotSource, type RuntimeSnapshotFrame } from "../agui/runtimeSnapshots";
// The production Node upgrade adapter intentionally remains plain JavaScript.
import { attachAguiWsUpgrade } from "../agui/ws.mjs";
import { issueBearerToken, revokeBearerToken } from "../identity/bearer";
import type { GatewayChangeBus } from "../store/changeBus";

interface Socket extends EventEmitter {
  send(data: string): void;
  close(): void;
  terminate(): void;
}
const { WebSocket } = createRequire(import.meta.url)("ws") as {
  WebSocket: new (url: string, options: {
    headers: Record<string, string>; handshakeTimeout: number;
  }) => Socket;
};

/** A real loopback socket with real REST bearer/scope checks. Only identity setup is a
 * fixture; runtime bodies arrive unchanged from the actual Rust publisher artifact.
 * No daemon ingress, provider, operator credentials, or process startup lifecycle is used. */
export async function openRuntimeModelSocket(db: Client, bus: GatewayChangeBus, agentId: string) {
  const legacyRead = vi.fn(() => { throw new Error("daemon read view must remain unopened"); });
  const commands = { submit: vi.fn(async () => { throw new Error("no daemon command is allowed"); }) };
  const dispatch = makeDispatch({
    db: async () => db, canonicalDb: () => db, readDb: legacyRead,
    authMode: "remote-human", commands,
  });
  const source = createRuntimeSnapshotSource({ changeBus: bus, fetchHandler: dispatch });
  const server = createServer((_request, response) => { response.writeHead(404).end(); });
  const wss = await attachAguiWsUpgrade(server, { runtimeSnapshots: source }) as {
    close(callback: () => void): void;
  };
  const sockets = new Set<Socket>();
  const bearerDeps = { db, now: Date.now };
  const actor = {
    name: "disposable-model-reader", project: "fixture", kind: Kind.Human,
    locality: Locality.External, tier: Tier.Agent,
    scopes: ["agent:read", "message:read"] as const,
  };
  const issue = (scope: "agent:read" | "message:read") => issueBearerToken({
    actor: { ...actor, scopes: [...actor.scopes] }, scopes: [scope],
  }, bearerDeps);
  const close = async () => {
    source.close();
    for (const socket of sockets) socket.terminate();
    await new Promise<void>(resolve => wss.close(() => resolve()));
    if (server.listening) await new Promise<void>((resolve, reject) =>
      server.close(error => error ? reject(error) : resolve()));
  };
  try {
    await db.execute({
      sql: "INSERT INTO identities (agent_id,name,role,tier,metadata_json,updated_at) VALUES (?,?,'agent','agent','{}',1)",
      args: [agentId, `fixture-${agentId}`],
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const port = (server.address() as AddressInfo).port;
    const allowed = await issue("agent:read");
    const denied = await issue("message:read");
    let generation = 0;
    const connect = async (token?: string) => {
      const socket = new WebSocket(`ws://127.0.0.1:${port}/api/agui/ws`, {
        headers: token ? { authorization: `Bearer ${token}` } : {}, handshakeTimeout: 5000,
      });
      sockets.add(socket);
      const frames: RuntimeSnapshotFrame[] = [];
      let failure: Error | undefined;
      let sequence = 0;
      socket.on("error", error => { failure = error; });
      socket.on("message", data => {
        try {
          const frame = JSON.parse(String(data));
          if (frame.t !== "runtime.snapshot" && frame.t !== "runtime.unavailable") {
            throw new Error(`unexpected socket frame: ${frame.t}`);
          }
          frames.push(frame);
        } catch (error) { failure = error as Error; }
      });
      await once(socket, "open");
      const subscriptionId = `actual-socket-${++generation}`;
      socket.send(JSON.stringify({ t: "runtime.subscribe", subscriptionId, agentId }));
      return {
        subscriptionId,
        async next() {
          await vi.waitFor(() => {
            if (failure) throw failure;
            expect(frames.length, "actual socket must deliver a runtime frame").toBeGreaterThan(0);
          }, { timeout: 5000 });
          const frame = frames.shift()!;
          expect(frame.subscriptionId).toBe(subscriptionId);
          expect(frame.agentId).toBe(agentId);
          expect(frame.sequence).toBeGreaterThan(sequence);
          sequence = frame.sequence;
          return frame;
        },
        async close() {
          const closed = once(socket, "close");
          socket.close();
          await closed;
          sockets.delete(socket);
        },
      };
    };
    let current = await connect(allowed.accessToken);
    return {
      next: () => current.next(),
      async probeConsumer(artifact: string, phase: "newer" | "stopped") {
        // Optional cross-repository acceptance extension. The base gate has no Lens checkout
        // dependency; an explicitly requested probe must never silently skip or fabricate output.
        const python = process.env.NEXUS_MODEL_CONSUMER_PYTHON;
        const probe = process.env.NEXUS_MODEL_CONSUMER_PROBE;
        if (!python && !probe) return;
        if (!python || !probe) throw new Error("consumer probe requires both executable and script");
        const result = await promisify(execFile)(python, [probe], {
          env: {
            ...process.env, NEXUS_PROBE_GATEWAY_URL: `http://127.0.0.1:${port}`,
            NEXUS_PROBE_BEARER: allowed.accessToken, NEXUS_PROBE_AGENT_ID: agentId,
            NEXUS_PROBE_ARTIFACT: artifact, NEXUS_PROBE_EXPECTED_PHASE: phase,
          }, timeout: 35000, maxBuffer: 256 * 1024,
        });
        const receipt = JSON.parse(result.stdout);
        expect(receipt).toMatchObject({result:"pass",connections:2,exactReport:true});
        process.stdout.write(`Actual portable SDK socket acceptance (${phase}): ${result.stdout.trim()}\n`);
      },
      async reconnect() {
        const previousId = current.subscriptionId;
        await current.close();
        current = await connect(allowed.accessToken);
        expect(current.subscriptionId).not.toBe(previousId);
        const frame = await current.next();
        expect(frame.sequence).toBe(1);
        return frame;
      },
      async assertAuthorization() {
        for (const token of [undefined, "invalid-disposable-token", denied.accessToken]) {
          const reader = await connect(token);
          expect(await reader.next()).toMatchObject({
            t: "runtime.unavailable", reason: "unauthorized", sequence: 1,
          });
          await reader.close();
        }
        expect(await revokeBearerToken(allowed.tokenId, bearerDeps)).toBe(true);
        // Revocation is checked on the next canonical read, not claimed to push by itself.
        bus.publish("runtime-snapshots");
        expect(await current.next()).toMatchObject({ t: "runtime.unavailable", reason: "unauthorized" });
        const reader = await connect(allowed.accessToken);
        expect(await reader.next()).toMatchObject({
          t: "runtime.unavailable", reason: "unauthorized", sequence: 1,
        });
        await reader.close();
        expect(legacyRead).not.toHaveBeenCalled();
        expect(commands.submit).not.toHaveBeenCalled();
      },
      close,
    };
  } catch (error) {
    await close();
    throw error;
  }
}
