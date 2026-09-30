import { createClient } from "@libsql/client";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it, vi } from "vitest";
import { z } from "zod";
import { removeTempPath } from "../../test/removeTempPath";
import { createRuntimeSnapshotSource, type RuntimeSnapshotFrame } from "../agui/runtimeSnapshots";
import { canonicalRuntimes } from "../read/canonical";
import { GatewayChangeBus } from "../store/changeBus";
import { migrateGatewayStore } from "../store/migrations";
import { parseRuntimeModelReport } from "./modelReport";
import type { ProjectionFrame } from "./consumer";
import { GatewayProjectionService } from "./service";

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
    const raw = readFileSync(join(directory!, `${harness}.${mode}.json`), "utf8");
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
    const source = createRuntimeSnapshotSource({changeBus:bus,fetchHandler:async()=>
      Response.json({agentId:fresh.agentId,runtimes:await canonicalRuntimes(db,{includeStopped:true})})});
    const snapshots: RuntimeSnapshotFrame[] = [];
    try {
      await migrateGatewayStore(db);
      await service.start();
      await source.subscribe(new Request("http://localhost/api/agui/ws"), `model-${harness}`,
        fresh.agentId, frame=>{snapshots.push(frame);}).ready;
      expect(snapshots[0]).toMatchObject({t:"runtime.snapshot",runtimes:[]});
      for (const [index, frame] of artifact.frames.entries()) {
        const count = snapshots.length;
        push(frame);
        await vi.waitFor(()=>expect(ack).toHaveBeenCalledTimes(index+1));
        await vi.waitFor(()=>expect(snapshots.length).toBeGreaterThan(count));
        const expected = index < 2 ? newer : stopped;
        const last = snapshots.at(-1)!;
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
      }
      expect((await canonicalRuntimes(db))).toHaveLength(0);
      expect((await db.execute("SELECT through_seq FROM projection_cursors")).rows).toMatchObject([{through_seq:4}]);
      expect((await db.execute("SELECT * FROM projection_gaps")).rows).toHaveLength(0);
      expect((await db.execute("SELECT * FROM projection_quarantine")).rows).toHaveLength(0);
    } finally {
      source.close(); await service.close(); db.close(); await removeTempPath(root, {recursive:true});
    }
  });
}
