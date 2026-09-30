import { createClient } from "@libsql/client";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { removeTempPath } from "../../test/removeTempPath";
import { migrateGatewayStore } from "../store/migrations";
import { canonicalRuntimes } from "../read/canonical";
import { GatewayChangeBus } from "../store/changeBus";
import { GatewayProjectionConsumer, type ProjectionFrame } from "./consumer";
import { GatewayProjectionService } from "./service";
import { createRuntimeSnapshotSource } from "../agui/runtimeSnapshots";

const runtime = () => JSON.parse(readFileSync(resolve("../core/crates/nexus-contracts/fixtures/runtime.telemetry.json"), "utf8"));
const paths: string[] = [];
const testDb = () => {
  const path = join(tmpdir(), `nexus-model-projection-${randomUUID()}.db`);
  paths.push(path);
  return createClient({ url: `file:${path}` });
};
afterEach(async () => { await Promise.all(paths.splice(0).map((path) => removeTempPath(path))); });
function frame(seq: number, revision: number | undefined, agent = "a_telemetry_fixture"): Extract<ProjectionFrame,{t:"projection"}> {
  const payload = runtime();
  payload.agentId = agent;
  if (revision === undefined) delete payload.modelReport;
  else payload.modelReport.reportRevision = revision;
  return { t: "projection", event: { version: 1, kind: "runtime.upserted", daemonEpoch: "model-epoch",
    eventId: `runtime:${seq}`, seq, occurredAt: seq, payload } };
}

describe("canonical runtime model delivery", () => {
  it("wakes complete runtime snapshots for a non-runtime new epoch and never fans out obsolete report payloads", async () => {
    const db=testDb();
    const bus=new GatewayChangeBus();
    let push!:(frame:ProjectionFrame)=>void;
    const ack=vi.fn(async()=>undefined);
    const service=new GatewayProjectionService(db,{ready:Promise.resolve(),subscribeProjections(handlers){push=handlers.onFrame;return()=>{};},ackProjection:ack,close(){}},bus);
    const source=createRuntimeSnapshotSource({changeBus:bus,fetchHandler:async()=>Response.json({agentId:"a_telemetry_fixture",runtimes:await canonicalRuntimes(db,{includeStopped:true})})});
    const snapshots:any[]=[];
    try {
      await migrateGatewayStore(db);await service.start();
      const subscription=source.subscribe(new Request("http://localhost/api/agui/ws"),"live1","a_telemetry_fixture",value=>{snapshots.push(value);});
      await subscription.ready;
      push(frame(1,20));
      await vi.waitFor(()=>expect(snapshots.at(-1)?.runtimes[0]?.modelReport.reportRevision).toBe(20));
      push(frame(2,10));
      await vi.waitFor(()=>expect(ack).toHaveBeenCalledTimes(2));
      push({t:"projection",event:{...frame(3,undefined).event,daemonEpoch:"next-epoch",seq:1,eventId:"next-epoch-identity",kind:"identity.upserted",payload:{agentId:"a_telemetry_fixture",name:"current"}}});
      await vi.waitFor(()=>expect(snapshots.at(-1)?.runtimes[0]?.active).toBe(false));
      for(const snapshot of snapshots) for(const row of snapshot.runtimes ?? []) expect(row.modelReport.reportRevision).toBe(20);
    } finally {source.close();await service.close();db.close();}
  });
  it("retains NEW report when OLD arrives at a higher stream sequence, while ACK/cursor settle", async () => {
    const db = testDb();
    try {
      await migrateGatewayStore(db);
      const ack = vi.fn(async () => undefined);
      const consumer = new GatewayProjectionConsumer(db, { ack });
      for (const f of [frame(1, 20), frame(2, 10), frame(3, 20), frame(4, undefined)]) await consumer.handleFrame(f);
      const rows = await canonicalRuntimes(db, { includeStopped: true });
      expect(rows[0]).toMatchObject({ modelReport: { ...runtime().modelReport, reportRevision: 20 } });
      expect((await db.execute("SELECT through_seq FROM projection_cursors")).rows).toMatchObject([{ through_seq: 4 }]);
      expect(ack.mock.calls).toHaveLength(4);
      expect((await db.execute("SELECT * FROM projection_gaps")).rows).toHaveLength(0);
    } finally { db.close(); }
  });

  it("does not relabel a newer report with an obsolete agent binding", async () => {
    const db = testDb();
    try {
      await migrateGatewayStore(db);
      const consumer = new GatewayProjectionConsumer(db, { ack: async () => undefined });
      await consumer.handleFrame(frame(1, 20, "a_new"));
      await consumer.handleFrame(frame(2, 10, "a_old"));
      await consumer.handleFrame(frame(3, undefined, "a_old"));
      expect((await canonicalRuntimes(db, { includeStopped: true }))[0]).toMatchObject({ agentId: "a_new", modelReport: { reportRevision: 20 } });
      await consumer.handleFrame(frame(4, 21, "a_next"));
      expect((await canonicalRuntimes(db, { includeStopped: true }))[0]).toMatchObject({ agentId: "a_next", modelReport: { reportRevision: 21 } });
    } finally { db.close(); }
  });

  it("preserves equal-revision same-owner status transitions and legacy no-report updates", async () => {
    const db = testDb();
    try {
      await migrateGatewayStore(db);
      const consumer = new GatewayProjectionConsumer(db, { ack: async () => undefined });
      const stopped = frame(1, 20);
      stopped.event.payload = { ...(stopped.event.payload as Record<string, unknown>), active: false, presence: "offline" };
      await consumer.handleFrame(stopped);
      expect((await canonicalRuntimes(db, { includeStopped: true }))[0]?.active).toBe(false);
      const online = frame(2, 20);
      online.event.payload = { ...(online.event.payload as Record<string, unknown>), active: true, presence: "online" };
      await consumer.handleFrame(online);
      expect((await canonicalRuntimes(db))[0]).toMatchObject({ active: true, presence: "online", modelReport: { reportRevision: 20 } });
      const legacy = frame(3, undefined);
      legacy.event.payload = { ...(legacy.event.payload as Record<string, unknown>), active: false, presence: "offline" };
      await consumer.handleFrame(legacy);
      expect((await canonicalRuntimes(db, { includeStopped: true }))[0]).toMatchObject({ active: false, presence: "offline", modelReport: { reportRevision: 20 } });
    } finally { db.close(); }
  });

  it("publishes runtime change keys after commit for canonical rereads, never malformed success", async () => {
    const db = testDb();
    try {
      await migrateGatewayStore(db);
      const bus = new GatewayChangeBus();
      const reads: Array<ReturnType<typeof canonicalRuntimes>> = [];
      bus.subscribe("runtime:s_telemetry_fixture", () => { reads.push(canonicalRuntimes(db, { includeStopped: true })); });
      let push!: (value: ProjectionFrame) => void;
      const ack = vi.fn(async () => undefined);
      const service = new GatewayProjectionService(db, { ready: Promise.resolve(), subscribeProjections(h) { push = h.onFrame; return () => {}; }, ackProjection: ack, close() {} }, bus);
      await service.start();
      push(frame(1, 20));
      push(frame(2, 10));
      push(frame(3, 0));
      await service.close();
      expect(reads).toHaveLength(2);
      for (const rows of await Promise.all(reads)) expect(rows[0]).toMatchObject({ modelReport: { reportRevision: 20 } });
      expect((await db.execute("SELECT * FROM projection_quarantine")).rows).toHaveLength(1);
      expect(ack.mock.calls.at(-1)).toEqual([{ daemonEpoch: "model-epoch", throughSeq: 3 }]);
    } finally { db.close(); }
  });
});
