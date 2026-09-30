import { createClient } from "@libsql/client";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it, vi } from "vitest";
import { z } from "zod";
import { removeTempPath } from "../../test/removeTempPath";
import { canonicalRuntimes } from "../read/canonical";
import { GatewayChangeBus } from "../store/changeBus";
import { migrateGatewayStore } from "../store/migrations";
import { parseRuntimeModelReport } from "./modelReport";
import type { ProjectionFrame } from "./consumer";
import { GatewayProjectionService } from "./service";
import { openRuntimeModelSocket } from "./runtimeModelSocket.support";

const payload = z.object({
  runtimeId: z.string().min(1), agentId: z.string().min(1), harness: z.string().min(1),
  active: z.boolean(), presence: z.string(), modelReport: z.unknown().transform(parseRuntimeModelReport),
}).passthrough();
const artifactSchema = z.object({
  runId: z.string().min(1), harness: z.string(), mode: z.enum(["headless", "headed"]),
  frames: z.array(z.object({t:z.literal("projection"),event:z.object({
    version:z.literal(1),kind:z.literal("runtime.upserted"),daemonEpoch:z.string().min(1),
    eventId:z.string().min(1),seq:z.number().int().positive(),occurredAt:z.number().int(),payload,
  }).strict()}).strict()).length(4),
  expected:z.object({fresh:payload,newer:payload,stopped:payload}).strict(),
}).strict();

// Every case requires a current actual-adapter artifact. Missing/malformed files are failures,
// never skipped or replaced with a hand-authored frame. Native payloads are captured fixtures,
// not live provider execution; the production publisher assigned these envelope sequences.
for (const mode of ["headless", "headed"] as const) for (const harness of ["claude", "codex", "opencode", "hermes"]) {
  it(`${harness}.${mode}: actual Rust publisher -> Gateway canonical snapshot fences delayed OLD`, async () => {
    const directory = process.env.NEXUS_MODEL_PROJECTION_FIXTURE_DIR;
    const runId = process.env.NEXUS_MODEL_PROJECTION_RUN_ID;
    expect(directory, "required fresh Rust artifact directory").toBeTruthy();
    expect(runId, "required fresh run identity").toBeTruthy();
    const artifactPath = join(directory!, `${harness}.${mode}.json`);
    const raw = readFileSync(artifactPath, "utf8");
    expect(raw.length).toBeGreaterThan(0);
    const artifact = artifactSchema.parse(JSON.parse(raw));
    expect(artifact.runId).toBe(runId);
    expect(artifact.harness).toBe(harness);
    expect(artifact.mode).toBe(mode);
    const { fresh, newer, stopped } = artifact.expected;
    const headed = {claude:"claude.transcript",codex:"codex.appserver",opencode:"opencode.plugin",hermes:"hermes.gateway"};
    const backend = mode === "headless" ? `${harness}.acp` : headed[harness as keyof typeof headed];
    expect(fresh.modelReport.backend).toBe(backend);
    expect(newer.modelReport.backend).toBe(backend);
    expect(newer.modelReport.reportRevision).toBeGreaterThan(fresh.modelReport.reportRevision);
    expect(stopped.modelReport.reportRevision).toBeGreaterThan(newer.modelReport.reportRevision);
    expect(fresh.modelReport.observerActive).toBe(true);
    expect(newer.modelReport.observerActive).toBe(true);
    expect(stopped.modelReport.observerActive).toBe(false);
    expect(stopped.active).toBe(false);
    expect(stopped.presence).toBe("offline");
    if (harness === "codex" && mode === "headed") {
      for (const body of [fresh, newer]) {
        expect(body.modelReport.telemetry?.usage.observation).toMatchObject({
          scope: "sessionCumulative", counterId: "codex.thread.tokenUsage.total",
          inputTokens: 900000, outputTokens: 30000, totalTokens: 930000,
          cacheReadTokens: 700000, cacheWriteTokens: 300, reasoningTokens: 9000,
          nativeTurnId: "t1",
        });
        expect(body.modelReport.telemetry?.usage.observation).not.toHaveProperty("resetId");
        expect(body.modelReport.telemetry?.context.observation).toMatchObject({
          effectiveCapacityTokens: { value: 200000, provenance: "native" },
          usedTokens: { value: 42000, provenance: "estimated" },
          remainingTokens: { value: 158000, provenance: "estimated" },
          remainingPercent: { value: 84, provenance: "estimated",
            basis: "codex.0.154.display:baseline12000:last-reported-estimate" },
        });
        expect(body.modelReport.telemetry?.quota.observation).toMatchObject({
          providerId: "openai", windows: [
            {windowId:"codex/primary",units:"percent",usedPercent:42,windowSeconds:18000,resetsAt:1800000000000},
            {windowId:"codex/secondary",units:"percent",usedPercent:7,windowSeconds:604800,resetsAt:1800600000000},
          ],
        });
        expect(body.modelReport.telemetry?.quota.observation).not.toHaveProperty("accountId");
      }
    }
    if (harness === "opencode" && mode === "headed") {
      for (const body of [fresh, newer, stopped]) {
        expect(body.modelReport.telemetry?.usage.observation).toMatchObject({
          metadata: {source: "opencode.plugin.assistant.usage", nativeSessionId: "ses_exact"},
          scope: "lastResponse", counterId: "opencode.plugin.assistant.usage",
          inputTokens: 40000, outputTokens: 1000, reasoningTokens: 100, cacheReadTokens: 800, cacheWriteTokens: 100,
        });
        for (const key of ["totalTokens", "nativeTurnId", "resetId", "model"]) expect(body.modelReport.telemetry?.usage.observation).not.toHaveProperty(key);
        expect(body.modelReport.telemetry?.context.observation).toMatchObject({
          metadata: {source: "opencode.plugin.assistant.context"},
          effectiveCapacityTokens: {value: 200000, provenance: "native"},
          usedTokens: {value: 42000, provenance: "estimated"},
          remainingTokens: {value: 158000, provenance: "estimated"},
          remainingPercent: {value: 79, provenance: "estimated", basis: "opencode.1.17.17:live-tui-last-assistant-token-sum"},
        });
        expect(body.modelReport.telemetry?.quota.capability).toBe("unsupported");
      }
    }
    if (harness === "claude" && mode === "headed") {
      for (const body of [fresh, newer, stopped]) {
        expect(body.modelReport.telemetry?.usage.observation).toMatchObject({
          metadata: {source: "claude.transcript.assistant.usage", nativeSessionId: "native"},
          scope: "lastResponse", counterId: "claude.transcript.assistant.usage",
          inputTokens: 120, outputTokens: 30, cacheReadTokens: 80, cacheWriteTokens: 20, reasoningTokens: 12,
        });
        for (const key of ["totalTokens", "nativeTurnId", "resetId", "model"]) expect(body.modelReport.telemetry?.usage.observation).not.toHaveProperty(key);
        expect(body.modelReport.telemetry?.usage.observation?.metadata).not.toHaveProperty("nativeReportedAt");
        expect(body.modelReport.telemetry?.context.capability).toBe("unverified");
        expect(body.modelReport.telemetry?.quota.capability).toBe("unverified");
      }
    }
    if (harness === "hermes" && mode === "headed") {
      for (const body of [fresh, newer, stopped]) {
        expect(body.modelReport.telemetry?.usage.observation).toMatchObject({
          metadata: {source: "hermes.gateway.session.usage", nativeSessionId: "native-root"},
          scope: "sessionCumulative", counterId: "hermes.gateway.session.usage",
          inputTokens: 120, outputTokens: 30, cacheReadTokens: 80, cacheWriteTokens: 20, reasoningTokens: 12,
        });
        for (const field of ["totalTokens", "resetId", "nativeTurnId", "model"]) {
          expect(body.modelReport.telemetry?.usage.observation).not.toHaveProperty(field);
        }
        expect(body.modelReport.telemetry?.context.capability).toBe("unsupported");
        expect(body.modelReport.telemetry?.quota.capability).toBe("unsupported");
      }
    }
    if (mode === "headless") {
      for (const body of [fresh, newer]) {
        expect(body.modelReport.telemetry?.context.observation).toMatchObject({
          metadata: {source: `${harness}.acp.usage_update`, nativeSessionId: "fixture-root"},
          effectiveCapacityTokens: {value: 200000, provenance: harness === "claude" ? "estimated" : "native"},
          usedTokens: {value: 42000, provenance: "estimated"},
          remainingTokens: {value: 158000, provenance: "estimated"},
          remainingPercent: {value: 79, provenance: "estimated",
            basis: ({claude: "claude.acp.0.58.1:assistant-token-proxy-or-compaction-fallback",
              codex: "acp.usage_update:last-reported-context-ratio",
              opencode: "opencode.acp.1.17.17:last-assistant-input-and-cache-read",
              hermes: "hermes.acp.0.17.0:rough-request-or-last-prompt-fallback"} as Record<string,string>)[harness]},
        });
        expect(body.modelReport.telemetry?.context.observation).not.toHaveProperty("resetId");
        expect(body.modelReport.telemetry?.usage.observation).toMatchObject({
          metadata: {source: `${harness}.acp.prompt.usage`, nativeSessionId: "fixture-root"},
          scope: harness === "claude" ? "lastPrompt" : harness === "hermes" ? "sessionCumulative" : "lastResponse",
          counterId: `${harness}.acp.prompt.usage`,
          inputTokens: harness === "hermes" ? 200 : 120, outputTokens: 30, cacheReadTokens: 80,
          totalTokens: harness === "opencode" ? 262 : 230,
        });
        for (const key of ["nativeTurnId", "resetId", "model"]) {
          expect(body.modelReport.telemetry?.usage.observation).not.toHaveProperty(key);
        }
        const usage = body.modelReport.telemetry?.usage.observation;
        if (harness === "claude") {
          expect(usage).toMatchObject({cacheWriteTokens: 0});
          expect(usage).not.toHaveProperty("reasoningTokens");
          expect(body.modelReport.telemetry?.quota.observation).toMatchObject({
            providerId: "anthropic", metadata: {source: "claude.acp.rate_limit.selected"},
            windows: [{windowId: "claude/five_hour", units: "percent", usedPercent: 75,
              windowSeconds: 18000, resetsAt: 1800000000000}],
          });
          expect(body.modelReport.telemetry?.quota.observation).not.toHaveProperty("accountId");
        } else {
          expect(usage).toMatchObject({reasoningTokens: 12});
          if (harness === "opencode") expect(usage).toMatchObject({cacheWriteTokens: 20});
          else expect(usage).not.toHaveProperty("cacheWriteTokens");
        }
        if (harness !== "claude") expect(body.modelReport.telemetry?.quota.observation).toBeUndefined();
      }
    }
    const bodies = [newer, fresh, stopped, fresh];
    expect(artifact.frames[3]!.event.occurredAt).toBeGreaterThan(artifact.frames[2]!.event.occurredAt);
    for (const [index, frame] of artifact.frames.entries()) {
      expect(frame.event.payload).toEqual(bodies[index]);
      expect(frame.event.seq).toBe(index + 1);
      expect(frame.event.daemonEpoch).toBe(artifact.frames[0]!.event.daemonEpoch);
      expect(frame.event.payload.runtimeId).toBe(fresh.runtimeId);
      expect(frame.event.payload.agentId).toBe(fresh.agentId);
      expect(frame.event.payload.harness).toBe(harness);
      expect(frame.event.payload).not.toHaveProperty("modelObserverToken");
    }

    const root = mkdtempSync(join(tmpdir(), "nexus-model-integration-"));
    const db = createClient({url:`file:${join(root, "gateway.db")}`});
    const bus = new GatewayChangeBus();
    let push!: (frame:ProjectionFrame)=>void;
    const ack = vi.fn(async()=>undefined);
    const service = new GatewayProjectionService(db, {
      ready:Promise.resolve(), subscribeProjections(handlers){push=handlers.onFrame;return()=>{};},
      ackProjection:ack, close(){},
    }, bus);
    let socket: Awaited<ReturnType<typeof openRuntimeModelSocket>> | undefined;
    try {
      await migrateGatewayStore(db);
      await service.start();
      socket = await openRuntimeModelSocket(db, bus, fresh.agentId);
      expect(await socket.next()).toMatchObject({t:"runtime.snapshot",sequence:1,runtimes:[]});
      for (const [index, frame] of artifact.frames.entries()) {
        push(frame);
        await vi.waitFor(()=>expect(ack).toHaveBeenCalledTimes(index+1));
        const expected = index < 2 ? newer : stopped;
        const last = await socket.next();
        expect(last.t).toBe("runtime.snapshot");
        if (last.t !== "runtime.snapshot") throw new Error("canonical snapshot unavailable");
        expect(last.runtimes).toHaveLength(1);
        expect(last.runtimes[0]).toMatchObject({
          runtimeId:expected.runtimeId,agentId:expected.agentId,harness,
          modelReport:expected.modelReport, active:expected.active, presence:expected.presence,
        });
        // Canonical descriptors expose the accepted projection timestamp, not the daemon's
        // original stop timestamp. A later OLD envelope must not advance that timestamp.
        if (!expected.active) expect(last.runtimes[0]?.stoppedAt).toBe(artifact.frames[2]!.event.occurredAt);
        if (index === 0) {
          const reconnected = await socket.reconnect();
          expect(reconnected).toMatchObject({t:"runtime.snapshot",sequence:1,runtimes:last.runtimes});
          await socket.probeConsumer(artifactPath, "newer");
        }
        if (index === 2) await socket.probeConsumer(artifactPath, "stopped");
      }
      await socket.assertAuthorization();
      expect((await canonicalRuntimes(db))).toHaveLength(0);
      expect((await db.execute("SELECT through_seq FROM projection_cursors")).rows).toMatchObject([{through_seq:4}]);
      expect((await db.execute("SELECT * FROM projection_gaps")).rows).toHaveLength(0);
      expect((await db.execute("SELECT * FROM projection_quarantine")).rows).toHaveLength(0);
    } finally {
      await socket?.close(); await service.close(); db.close(); await removeTempPath(root, {recursive:true});
    }
  }, process.env.NEXUS_MODEL_CONSUMER_PROBE ? 90000 : 10000);
}
