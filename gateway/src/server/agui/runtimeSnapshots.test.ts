import { describe, expect, it, vi } from "vitest";
import { GatewayChangeBus } from "../store/changeBus";
import { createRuntimeSnapshotSource } from "./runtimeSnapshots";

const request = new Request("http://localhost/api/agui/ws", { headers: {authorization:"Bearer test-only"} });
const row = (runtimeId = "s_current") => ({runtimeId,agentId:"a_current",harness:"codex",presence:"online",active:true,startedAt:1});
const response = (rows = [row()]) => Response.json({agentId:"a_current",runtimes:rows});

describe("canonical runtime snapshot source", () => {
  it.each(["Codex","bad/harness","a".repeat(65)])("rejects malformed public harness ID %s",async harness=> {
    const source=createRuntimeSnapshotSource({changeBus:new GatewayChangeBus(),fetchHandler:async()=>response([{...row(),harness}])});
    const frames:any[]=[];
    try {
      await source.subscribe(request,"grammar","a_current",frame=>{frames.push(frame);}).ready;
      expect(frames).toEqual([{t:"runtime.unavailable",subscriptionId:"grammar",agentId:"a_current",sequence:1,reason:"invalidSnapshot"}]);
    } finally {source.close();}
  });
  it("marks a stalled read unavailable without admitting parallel replacement reads", async () => {
    vi.useFakeTimers();
    const bus=new GatewayChangeBus();
    let release!: (value:Response)=>void;
    const read=vi.fn(() => new Promise<Response>(done=>{release=done;}));
    const source=createRuntimeSnapshotSource({fetchHandler:read,changeBus:bus});
    const frames:any[]=[];
    const sub=source.subscribe(request,"slow","a_current",frame=>{frames.push(frame);});
    try {
      await vi.advanceTimersByTimeAsync(5000);
      expect(frames).toEqual([{t:"runtime.unavailable",subscriptionId:"slow",agentId:"a_current",sequence:1,reason:"unavailable"}]);
      await sub.ready;
      bus.publish("runtime-snapshots");
      await vi.advanceTimersByTimeAsync(1);
      expect(read).toHaveBeenCalledTimes(1);
      release(response([row("s_timed_out")]));
      await vi.advanceTimersByTimeAsync(0);
      expect(frames).toHaveLength(1);
      expect(read).toHaveBeenCalledTimes(2);
    } finally {source.close();vi.useRealTimers();}
  });
  it("hydrates and rereads through the authorized REST path, including authoritative empty state", async () => {
    const bus = new GatewayChangeBus();
    let rows = [row()];
    const read = vi.fn(async (req: Request) => {
      expect(new URL(req.url).pathname).toBe("/api/v1/agents/a_current/runtimes");
      expect(new URL(req.url).searchParams.get("includeStopped")).toBe("true");
      expect(req.headers.get("authorization")).toBe("Bearer test-only");
      return response(rows);
    });
    const source = createRuntimeSnapshotSource({fetchHandler:read,changeBus:bus});
    const frames: any[] = [];
    const sub = source.subscribe(request,"sub1","a_current",frame => {frames.push(frame);});
    try {
      await sub.ready;
      expect(frames).toEqual([{t:"runtime.snapshot",subscriptionId:"sub1",agentId:"a_current",sequence:1,runtimes:[row()]}]);
      rows = [];
      bus.publish("runtime-snapshots");
      await vi.waitFor(() => expect(frames).toHaveLength(2));
      expect(frames[1]).toMatchObject({t:"runtime.snapshot",sequence:2,runtimes:[]});
    } finally {source.close();}
  });

  it("subscribes before reading and discards a captured OLD read when canonical state changes", async () => {
    const bus = new GatewayChangeBus();
    let release!: (value:Response) => void;
    let entered!: () => void;
    const started = new Promise<void>(done => {entered = done;});
    let reads = 0;
    const source = createRuntimeSnapshotSource({changeBus:bus,fetchHandler:async () => {
      if (++reads === 1) {entered(); return new Promise<Response>(done => {release=done;});}
      return response([row("s_new")]);
    }});
    const frames: any[] = [];
    const sub = source.subscribe(request,"sub1","a_current",frame => {frames.push(frame);});
    try {
      await started;
      bus.publish("runtime-snapshots");
      release(response([row("s_old")]));
      await sub.ready;
      expect(frames).toHaveLength(1);
      expect(frames[0]).toMatchObject({sequence:1,runtimes:[{runtimeId:"s_new"}]});
    } finally {source.close();}
  });

  it("uses ordered unavailable frames for auth/read/validation failures, then allows canonical recovery", async () => {
    const bus = new GatewayChangeBus();
    let read = () => new Response("secret internal detail",{status:401});
    const source = createRuntimeSnapshotSource({changeBus:bus,fetchHandler:async () => read()});
    const frames: any[] = [];
    const sub = source.subscribe(request,"sub1","a_current",frame => {frames.push(frame);});
    try {
      await sub.ready;
      expect(frames).toEqual([{t:"runtime.unavailable",subscriptionId:"sub1",agentId:"a_current",sequence:1,reason:"unauthorized"}]);
      read=() => response(); bus.publish("runtime-snapshots");
      await vi.waitFor(() => expect(frames).toHaveLength(2));
      expect(frames[1]).toMatchObject({t:"runtime.snapshot",sequence:2});
      read=() => Response.json({agentId:"a_foreign",runtimes:[row()]}); bus.publish("runtime-snapshots");
      await vi.waitFor(() => expect(frames).toHaveLength(3));
      expect(frames[2]).toMatchObject({t:"runtime.unavailable",sequence:3,reason:"invalidSnapshot"});
      expect(JSON.stringify(frames)).not.toContain("secret");
    } finally {source.close();}
  });

  it("fences a canceled reader and always hydrates a fresh subscription", async () => {
    let release!: (value:Response) => void;
    let entered!: () => void;
    const started = new Promise<void>(done => {entered=done;});
    let reads=0;
    const source = createRuntimeSnapshotSource({changeBus:new GatewayChangeBus(),fetchHandler:async () => {
      if (++reads===1) {entered();return new Promise<Response>(done => {release=done;});}
      return response();
    }});
    const old:any[]=[]; const current:any[]=[];
    const first=source.subscribe(request,"old","a_current",frame => {old.push(frame);});
    try {
      await started; first.close();
      const second=source.subscribe(request,"new","a_current",frame => {current.push(frame);});
      await second.ready; release(response([row("s_obsolete")]));
      await first.ready;
      expect(old).toEqual([]);
      expect(current).toHaveLength(1);
      expect(current[0]).toMatchObject({subscriptionId:"new",sequence:1,runtimes:[row()]});
    } finally {source.close();}
  });
});
