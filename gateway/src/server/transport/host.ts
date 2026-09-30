import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { readdir } from "node:fs/promises";
import { join } from "node:path";

import type { Client } from "@libsql/client";

import { Kind, Locality, Tier } from "@shared/types";
import { sendViaCommandIngress } from "@server/messagePost/commandIngress";
import { bindLane, laneForChat } from "@server/store/repos/lane-bindings";
import {
  externalPrincipalIdForSubject,
  upsertSubjectBinding,
  type PrincipalRow,
} from "@server/store/repos/principals";
import {
  loadTransportManifest,
  revalidateTransportManifest,
  type LoadedTransportManifest,
} from "./manifest";
import {
  listPendingObligations,
  onTransportOutboxWake,
  settleObligation,
  type TransportObligation,
} from "./outbox";
import {
  TRANSPORT_MAX_FRAME_BYTES,
  TRANSPORT_MAX_UNSETTLED,
  TRANSPORT_PROTOCOL_VERSION,
  type BridgeTransportFrame,
  type HostTransportFrame,
} from "./protocol";
import { resolveTransportSecretRefs } from "./secrets";
import { windowsEntryCommand } from "./windowsAuthority";

const MAX_WRITE_BUFFER_BYTES = TRANSPORT_MAX_FRAME_BYTES * 16;
const MAX_CRASHES = 10;
const MAX_BACKOFF_MS = 30_000;

export type TransportState = "starting" | "running" | "backoff" | "disabled" | "stopped";

export interface TransportIngressEvent {
  provider: string;
  ingressId: string;
  chatId: string;
  text: string;
  displayName?: string;
  principal: PrincipalRow;
  lane: {
    provider: string;
    externalChatId: string;
    laneKind: "thread" | "dm";
    laneName: string;
  };
}

export interface TransportHostOptions {
  nexusHome: string;
  db: () => Promise<Client> | Client;
  env?: NodeJS.ProcessEnv;
  onIngress?: (event: TransportIngressEvent) => Promise<void>;
  backoffBaseMs?: number;
}

export interface TransportHost {
  start(): Promise<void>;
  stop(): Promise<void>;
  states(): Array<{ name: string; state: TransportState }>;
}

export function createTransportHost(options: TransportHostOptions): TransportHost {
  const controllers = new Map<string, TransportController>();
  let stopped = false;
  let closeWake: (() => void) | undefined;
  let starting: Promise<void> | undefined;
  let stopping: Promise<void> | undefined;
  return {
    start() {
      // A new start must not reopen admission while the previous host is draining.
      if (stopping) return stopping.then(() => this.start());
      if (starting) return starting;
      stopped = false;
      const work = (async () => {
        closeWake ??= onTransportOutboxWake(() => {
          for (const controller of controllers.values()) void controller.wake();
        });
        const directory = join(options.nexusHome, "gateway", "transports.d");
        const entries = await readdir(directory, { withFileTypes: true }).catch((error: NodeJS.ErrnoException) => {
          if (error.code === "ENOENT") return [];
          throw error;
        });
        for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
          if (stopped) return;
          if (!entry.isFile() || !entry.name.endsWith(".toml")) continue;
          const name = entry.name.slice(0, -5);
          const manifest = await loadTransportManifest(name, { nexusHome: options.nexusHome });
          if (stopped) return;
          const controller = new TransportController(manifest, options);
          controllers.set(name, controller);
          await controller.start();
        }
      })();
      starting = work;
      void work.then(() => { starting = undefined; }, () => { starting = undefined; });
      return work;
    },
    stop() {
      if (stopping) return stopping;
      stopped = true;
      closeWake?.();
      closeWake = undefined;
      const work = (async () => {
        // Stop each controller synchronously before joining startup. Startup cannot add
        // another controller after its next await because admission is now closed.
        const results = await Promise.allSettled([
          ...[...controllers.values()].map((controller) => controller.stop()),
          ...(starting ? [starting] : []),
        ]);
        controllers.clear();
        const failure = results.find((result) => result.status === "rejected");
        if (failure?.status === "rejected") throw failure.reason;
      })();
      stopping = work;
      void work.then(() => { stopping = undefined; }, () => { stopping = undefined; });
      return work;
    },
    states() {
      if (stopped && controllers.size === 0) return [];
      return [...controllers.values()]
        .map((controller) => ({ name: controller.name, state: controller.state }))
        .sort((a, b) => a.name.localeCompare(b.name));
    },
  };
}

class TransportController {
  readonly name: string;
  state: TransportState = "stopped";
  private child?: ChildProcessWithoutNullStreams;
  private generation = 0;
  private crashes = 0;
  private failedGeneration = 0;
  private stopping = false;
  private restartTimer?: NodeJS.Timeout;
  private helloTimer?: NodeJS.Timeout;
  private stdoutBuffer = "";
  private frameQueue: Promise<void> = Promise.resolve();
  private readonly work = new Set<Promise<void>>();
  private readonly children = new Map<ChildProcessWithoutNullStreams, Promise<void>>();
  private workFailure?: { error: unknown };
  private stopWork?: Promise<void>;
  private readonly sent = new Set<string>();
  private secretValues: string[] = [];

  constructor(
    private readonly manifest: LoadedTransportManifest,
    private readonly options: TransportHostOptions,
  ) {
    this.name = manifest.name;
  }

  async start(): Promise<void> {
    this.stopping = false;
    await this.own(this.spawnGeneration());
  }

  stop(): Promise<void> {
    if (this.stopWork) return this.stopWork;
    this.stopping = true;
    if (this.restartTimer) clearTimeout(this.restartTimer);
    if (this.helloTimer) clearTimeout(this.helloTimer);
    this.restartTimer = undefined;
    this.helloTimer = undefined;
    const child = this.child;
    this.child = undefined;
    this.state = "stopped";
    this.sent.clear();
    if (child && child.exitCode === null && !child.killed) {
      this.writeFrame({ t: "transport/shutdown" }, child);
      const timeout = setTimeout(() => child.kill("SIGKILL"), 1_000);
      child.once("close", () => clearTimeout(timeout));
    }
    // Only external entry points enter `work`; shutdown is never one of its own
    // dependencies. Keep ownership of killed children until their actual close.
    this.stopWork = (async () => {
      while (this.work.size > 0) await Promise.allSettled([...this.work]);
      await Promise.all([...this.children.values()]);
      if (this.workFailure) throw this.workFailure.error;
    })();
    return this.stopWork;
  }

  wake(): Promise<void> {
    if (this.stopping) return Promise.resolve();
    return this.own(this.drain(this.generation));
  }

  private own(work: Promise<void>): Promise<void> {
    this.work.add(work);
    void work.then(() => this.work.delete(work), (error) => {
      this.work.delete(work);
      // Detached event handlers cannot reject to their caller. Retain their first
      // failure for stop(), fail closed, and still drain all other admitted work.
      this.workFailure ??= { error };
      if (!this.stopping) {
        this.state = "disabled";
        if (this.restartTimer) clearTimeout(this.restartTimer);
        if (this.helloTimer) clearTimeout(this.helloTimer);
        this.restartTimer = undefined;
        this.helloTimer = undefined;
        if (this.child && this.child.exitCode === null && !this.child.killed) this.child.kill("SIGKILL");
      }
    });
    return work;
  }

  private acceptsWork(): boolean {
    return !this.stopping && this.state !== "disabled";
  }

  private async spawnGeneration(): Promise<void> {
    if (!this.acceptsWork()) return;
    try {
      const db = await this.options.db();
      if (!this.acceptsWork()) return;
      const resolvedSecrets = await resolveTransportSecretRefs(db, this.manifest.secretRefs);
      if (!this.acceptsWork()) return;
      this.secretValues = Object.values(resolvedSecrets).filter(Boolean);
      const sourceEnv = this.options.env ?? process.env;
      const environment: NodeJS.ProcessEnv = {
        PATH: sourceEnv.PATH ?? "/usr/local/bin:/usr/bin:/bin",
        HOME: this.options.nexusHome,
        LANG: sourceEnv.LANG ?? "C.UTF-8",
        ...resolvedSecrets,
      };
      if (process.platform === "win32") {
        const root = sourceEnv.SystemRoot ?? sourceEnv.SYSTEMROOT;
        if (!root) throw new Error("Windows transport requires SystemRoot");
        environment.SystemRoot = root;
      }
      try {
        await revalidateTransportManifest(this.manifest);
      } catch (error) {
        // Only this now-revoked launch authorization is discarded on shutdown.
        // Database/logging failures still propagate to the owner below.
        if (this.stopping) return;
        throw error;
      }
      if (!this.acceptsWork()) return;
      this.generation += 1;
      const generation = this.generation;
      this.state = "starting";
      this.stdoutBuffer = "";
      this.frameQueue = Promise.resolve();
      this.sent.clear();
      const launch = process.platform === "win32"
        ? windowsEntryCommand(this.manifest.entryPath, this.manifest.args)
        : { command: this.manifest.entryPath, args: [...this.manifest.args] };
      const child = spawn(launch.command, launch.args, {
        env: environment,
        stdio: ["pipe", "pipe", "pipe"],
        shell: false,
      });
      this.child = child;
      const closed = new Promise<void>((resolve) => child.once("close", () => {
        this.children.delete(child);
        resolve();
      }));
      this.children.set(child, closed);
      child.stdout.setEncoding("utf8");
      child.stderr.setEncoding("utf8");
      child.stdout.on("data", (chunk: string) => {
        if (this.stopping) return;
        this.frameQueue = this.own(this.frameQueue
          .then(() => this.onStdout(generation, chunk))
          .catch((error) => {
            // A failed crash/disable log is not a second bridge failure. Preserve
            // it for the owner instead of losing it to crash's duplicate fence.
            if (this.stopping || generation === this.failedGeneration || this.state === "disabled") throw error;
            return this.crash(generation, error);
          }));
      });
      child.stderr.on("data", (chunk: string) => {
        if (!this.stopping) this.own(this.log("warn", chunk.trim(), generation));
      });
      child.once("error", (error) => {
        if (!this.stopping) this.own(this.crash(generation, error));
      });
      child.once("close", (code, signal) => {
        if (this.stopping || this.state === "disabled" || generation !== this.generation) return;
        this.own(this.crash(generation, new Error(`bridge exited code=${code} signal=${signal}`)));
      });
      this.helloTimer = setTimeout(() => {
        if (generation === this.generation && this.state === "starting") {
          this.own(this.crash(generation, new Error("bridge hello timeout")));
        }
      }, 5_000);
    } catch (error) {
      if (this.stopping) throw error;
      await this.disable(error);
    }
  }

  private async onStdout(generation: number, chunk: string): Promise<void> {
    if (generation !== this.generation || this.stopping || this.state === "disabled") return;
    this.stdoutBuffer += chunk;
    if (Buffer.byteLength(this.stdoutBuffer, "utf8") > TRANSPORT_MAX_FRAME_BYTES) {
      await this.crash(generation, new Error("transport frame exceeds 64 KiB"));
      return;
    }
    while (true) {
      const newline = this.stdoutBuffer.indexOf("\n");
      if (newline < 0) return;
      const line = this.stdoutBuffer.slice(0, newline);
      this.stdoutBuffer = this.stdoutBuffer.slice(newline + 1);
      if (Buffer.byteLength(line, "utf8") > TRANSPORT_MAX_FRAME_BYTES) {
        await this.crash(generation, new Error("transport frame exceeds 64 KiB"));
        return;
      }
      if (!line.trim()) continue;
      let frame: BridgeTransportFrame;
      try {
        frame = JSON.parse(line) as BridgeTransportFrame;
      } catch {
        await this.crash(generation, new Error("bridge emitted invalid JSON"));
        return;
      }
      await this.onFrame(generation, frame);
    }
  }

  private async onFrame(generation: number, frame: BridgeTransportFrame): Promise<void> {
    if (generation !== this.generation || this.stopping) return;
    if (this.state === "starting") {
      if (frame.t !== "transport/hello") {
        await this.disable(new Error("transport/hello must be the first bridge frame"));
        return;
      }
      if (frame.protocolVersion !== TRANSPORT_PROTOCOL_VERSION) {
        await this.disable(new Error(`unsupported transport protocol ${frame.protocolVersion}`));
        return;
      }
      if (this.helloTimer) clearTimeout(this.helloTimer);
      this.helloTimer = undefined;
      this.state = "running";
      this.writeFrame({
        t: "transport/init",
        protocolVersions: [TRANSPORT_PROTOCOL_VERSION],
        config: this.manifest.config,
        generation,
      });
      await this.drain(generation);
      return;
    }
    if (this.state !== "running") return;
    const db = await this.options.db();
    if (generation !== this.generation || this.stopping) return;
    switch (frame.t) {
      case "transport/hello":
        await this.crash(generation, new Error("duplicate transport/hello"));
        return;
      case "transport/bind":
        await upsertSubjectBinding(db, {
          provider: this.manifest.provider,
          externalUserId: required(frame.external?.userId, "external.userId"),
          displayName: frame.external?.displayName,
          kind: "external.human",
        });
        return;
      case "transport/bindLane":
        await bindLane(db, {
          provider: this.manifest.provider,
          externalChatId: required(frame.external?.chatId, "external.chatId"),
          laneKind: laneKind(frame.lane?.kind),
          laneName: required(frame.lane?.name, "lane.name"),
        });
        return;
      case "transport/ingress":
        await this.ingress(frame);
        return;
      case "transport/receipt": {
        const obligationId = required(frame.obligationId, "obligationId");
        if (!this.sent.has(obligationId)) return;
        await settleObligation(
          db,
          obligationId,
          required(frame.externalMessageId, "externalMessageId"),
        );
        this.sent.delete(obligationId);
        await this.drain(generation);
        return;
      }
      case "transport/log":
        await this.log(frame.level, frame.message, generation);
        return;
      default:
        await this.crash(generation, new Error("unsupported bridge frame"));
    }
  }

  private async ingress(frame: Extract<BridgeTransportFrame, { t: "transport/ingress" }>) {
    const db = await this.options.db();
    const ingressId = required(frame.ingressId, "ingressId");
    const inserted = await db.execute({
      sql: `INSERT OR IGNORE INTO transport_ingress (provider, ingress_id, received_at)
            VALUES (?, ?, ?)`,
      args: [this.manifest.provider, ingressId, Date.now()],
    });
    if (inserted.rowsAffected === 0) return;
    try {
      const { principal, lane } = await bindIngressAuthority(db, {
        provider: this.manifest.provider,
        externalUserId: required(frame.external?.userId, "external.userId"),
        displayName: frame.external?.displayName,
        externalChatId: required(frame.chatId, "chatId"),
      });
      const event: TransportIngressEvent = {
        provider: this.manifest.provider,
        ingressId,
        chatId: frame.chatId,
        text: required(frame.text, "text"),
        displayName: frame.external.displayName,
        principal,
        lane,
      };
      await (this.options.onIngress ?? defaultIngress)(event);
    } catch (error) {
      await db.execute({
        sql: "DELETE FROM transport_ingress WHERE provider = ? AND ingress_id = ?",
        args: [this.manifest.provider, ingressId],
      });
      throw error;
    }
  }

  private async drain(generation: number): Promise<void> {
    if (generation !== this.generation || this.stopping || this.state !== "running") return;
    const available = TRANSPORT_MAX_UNSETTLED - this.sent.size;
    if (available <= 0) return;
    const pending = await listPendingObligations(
      await this.options.db(),
      this.manifest.provider,
      available,
    );
    if (generation !== this.generation || this.stopping || this.state !== "running") return;
    for (const obligation of pending) {
      if (this.sent.has(obligation.obligationId)) continue;
      this.writeDelivery(obligation);
      this.sent.add(obligation.obligationId);
    }
  }

  private writeDelivery(obligation: TransportObligation): void {
    this.writeFrame({
      t: "transport/deliver",
      obligationId: obligation.obligationId,
      externalChatId: obligation.externalChatId,
      lane: { kind: obligation.laneKind, name: obligation.laneName },
      text: obligation.text,
    });
  }

  private writeFrame(
    frame: HostTransportFrame,
    target: ChildProcessWithoutNullStreams | undefined = this.child,
  ): void {
    if (!target || target.exitCode !== null || target.killed) return;
    const encoded = `${JSON.stringify(frame)}\n`;
    const bytes = Buffer.byteLength(encoded, "utf8");
    if (bytes > TRANSPORT_MAX_FRAME_BYTES || target.stdin.writableLength + bytes > MAX_WRITE_BUFFER_BYTES) {
      this.own(this.crash(this.generation, new Error("transport write buffer overflow")));
      return;
    }
    target.stdin.write(encoded);
  }

  private async log(level: string, message: string, generation: number): Promise<void> {
    if (generation !== this.generation) return;
    let redacted = String(message);
    for (const secret of this.secretValues) redacted = redacted.replaceAll(secret, "[REDACTED]");
    await (await this.options.db()).execute({
      sql: `INSERT INTO logs (ts, level, scope, conversation_id, message, data)
            VALUES (?, ?, 'transport', ?, ?, ?)`,
      args: [Date.now(), normalizedLevel(level), this.manifest.name, redacted, null],
    });
  }

  private async disable(error: unknown): Promise<void> {
    if (this.stopping) return;
    this.state = "disabled";
    if (this.helloTimer) clearTimeout(this.helloTimer);
    this.helloTimer = undefined;
    const child = this.child;
    this.child = undefined;
    if (child && child.exitCode === null && !child.killed) child.kill("SIGKILL");
    await this.log("error", errorMessage(error), this.generation);
  }

  private async crash(generation: number, error: unknown): Promise<void> {
    if (
      generation !== this.generation ||
      generation === this.failedGeneration ||
      this.stopping ||
      this.state === "disabled"
    ) return;
    this.failedGeneration = generation;
    if (this.helloTimer) clearTimeout(this.helloTimer);
    this.helloTimer = undefined;
    const child = this.child;
    this.child = undefined;
    if (child && child.exitCode === null && !child.killed) child.kill("SIGKILL");
    this.sent.clear();
    this.crashes += 1;
    await this.log("error", errorMessage(error), generation);
    if (!this.acceptsWork() || generation !== this.generation) return;
    if (this.crashes >= MAX_CRASHES) {
      this.state = "disabled";
      return;
    }
    this.state = "backoff";
    const delay = Math.min(
      MAX_BACKOFF_MS,
      (this.options.backoffBaseMs ?? 250) * 2 ** Math.max(0, this.crashes - 1),
    );
    this.restartTimer = setTimeout(() => {
      this.restartTimer = undefined;
      if (!this.stopping) this.own(this.spawnGeneration());
    }, delay);
  }
}

interface IngressAuthorityInput {
  provider: string;
  externalUserId: string;
  displayName?: string;
  externalChatId: string;
}

/** Atomically mint first-contact subject and DM-lane authority. */
export async function bindIngressAuthority(db: Client, input: IngressAuthorityInput) {
  const principalId = externalPrincipalIdForSubject(input.provider, input.externalUserId);
  const createdAt = Date.now();
  // One libsql write batch is the atomic boundary: neither the external subject nor its default
  // private-chat lane can become visible alone. An explicit group-chat binding already present
  // wins because the lane insert is idempotent and is validated after the commit.
  await db.batch([
    {
      sql: `INSERT OR IGNORE INTO principals
              (principal_id, kind, access, created_at)
            VALUES (?, 'external.human', 'guest', ?)`,
      args: [principalId, createdAt],
    },
    {
      sql: `INSERT OR IGNORE INTO subject_bindings
              (provider, external_user_id, principal_id, display_name, created_at)
            VALUES (?, ?, ?, ?, ?)`,
      args: [input.provider, input.externalUserId, principalId, input.displayName ?? null, createdAt],
    },
    {
      sql: `INSERT OR IGNORE INTO transport_lane_bindings
              (provider, external_chat_id, lane_kind, lane_name, created_at)
            VALUES (?, ?, 'dm', ?, ?)`,
      args: [input.provider, input.externalChatId, principalId, createdAt],
    },
  ], "write");
  const principal = await upsertSubjectBinding(db, {
    provider: input.provider,
    externalUserId: input.externalUserId,
    displayName: input.displayName,
    kind: "external.human",
  });
  const lane = await laneForChat(db, input.provider, input.externalChatId);
  if (!lane) throw new Error(`failed to bind chat ${input.provider}/${input.externalChatId}`);
  return { principal, lane };
}

async function defaultIngress(event: TransportIngressEvent): Promise<void> {
  await sendViaCommandIngress(
    {
      to: event.lane.laneKind === "thread"
        ? { verb: "post", thread: event.lane.laneName }
        : { verb: "dm", name: event.lane.laneName },
      body: event.text,
    },
    {
      name: event.displayName ?? event.principal.principalId,
      project: "default",
      kind: Kind.Human,
      locality: Locality.External,
      access: event.principal.access,
      principalId: event.principal.principalId,
      tier: Tier.Agent,
      sessionId: `transport:${event.provider}`,
      credentialFacet: "source",
    },
    {},
    { idempotencyKey: `transport:${event.provider}:${event.ingressId}` },
  );
}

function required(value: unknown, field: string): string {
  if (typeof value !== "string" || !value.trim()) throw new Error(`${field} is required`);
  return value.trim();
}

function laneKind(value: unknown): "thread" | "dm" {
  if (value !== "thread" && value !== "dm") throw new Error("lane.kind must be thread or dm");
  return value;
}

function normalizedLevel(value: string): string {
  return ["debug", "info", "warn", "error"].includes(value) ? value : "info";
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
