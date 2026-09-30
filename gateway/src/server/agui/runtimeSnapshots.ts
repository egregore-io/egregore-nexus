import { z } from "zod";
import { Presence, type AgentRuntimeSummary, type RuntimeSnapshotFrame as Snapshot,
  type RuntimeUnavailableFrame as Unavailable, type RuntimeUnavailableReason } from "@shared/types";
import { gatewayChangeBus, type GatewayChangeBus } from "../store/changeBus";
import { parseRuntimeModelReport } from "../projection/modelReport";

// Subscription-local transport order is separate from each runtime's durable reportRevision.
export type RuntimeSnapshotFrame = (Omit<Snapshot,"t"> & {t:"runtime.snapshot"})
  | (Omit<Unavailable,"t"|"reason"> & {t:"runtime.unavailable";reason:`${RuntimeUnavailableReason}`});
export interface RuntimeSnapshotSubscription {ready:Promise<void>;close():void;}
export interface RuntimeSnapshotSource {
  subscribe(request:Request,subscriptionId:string,agentId:string,onFrame:(frame:RuntimeSnapshotFrame)=>boolean|void):RuntimeSnapshotSubscription;
  close():void;
}

const identifier = z.string().min(1).refine(value => !/^\p{White_Space}*$/u.test(value)
  && !/[\uD800-\uDFFF\p{Cc}]/u.test(value) && new TextEncoder().encode(value).length<=1024);
const runtimeRow = z.object({
  runtimeId:identifier,agentId:identifier,harness:z.string().regex(/^[a-z][a-z0-9_-]{0,63}$/),cwd:z.string().optional(),transport:z.string().optional(),
  presence:z.enum(Presence),active:z.boolean(),startedAt:z.number(),stoppedAt:z.number().optional(),lastHeartbeat:z.number().optional(),
  modelReport:z.unknown().optional(),
});
const rowsResponse = z.object({agentId:identifier,runtimes:z.array(runtimeRow)});

function parseRows(value:unknown,agentId:string):AgentRuntimeSummary[] {
  const parsed=rowsResponse.parse(value);
  if(parsed.agentId!==agentId) throw new Error("canonical agent mismatch");
  const seen=new Set<string>();
  return parsed.runtimes.map(row => {
    if(row.agentId!==agentId || seen.has(row.runtimeId)) throw new Error("canonical runtime ownership mismatch");
    seen.add(row.runtimeId);
    return {...row,modelReport:row.modelReport==null ? undefined : parseRuntimeModelReport(row.modelReport)};
  });
}

/** Snapshot-only source on the existing canonical change bus. Every read passes through the
 * authenticated REST handler. No timer, raw daemon feed, durable replay cursor or inferred owner.
 * Subscribe before reading; a change during an awaited read invalidates that entire read. */
export function createRuntimeSnapshotSource(options:{fetchHandler(request:Request):Promise<Response>;changeBus?:GatewayChangeBus}):RuntimeSnapshotSource {
  const bus=options.changeBus ?? gatewayChangeBus;
  const readers=new Set<RuntimeSnapshotSubscription>();
  let closed=false;
  return {
    subscribe(request,subscriptionId,agentId,onFrame) {
      if(closed) throw new Error("runtime snapshot source is closed");
      identifier.parse(agentId);
      if(!subscriptionId || /^\p{White_Space}*$/u.test(subscriptionId) || new TextEncoder().encode(subscriptionId).length>128 || /[\uD800-\uDFFF\p{Cc}]/u.test(subscriptionId)) throw new Error("invalid subscription id");
      let stopped=false,reading=false,scheduled=false,rerun=false,sequence=0;
      let resolveReady!:()=>void;
      const ready=new Promise<void>(done => {resolveReady=done;});
      let currentAbort:AbortController|undefined;
      let currentTimeout:ReturnType<typeof setTimeout>|undefined;
      const emit=(payload:{runtimes:AgentRuntimeSummary[]}|{reason:Extract<RuntimeSnapshotFrame,{t:"runtime.unavailable"}>["reason"]}) => {
        if(stopped) return;
        if(sequence===Number.MAX_SAFE_INTEGER) {sub.close();return;}
        const common={subscriptionId,agentId,sequence:++sequence};
        const frame:RuntimeSnapshotFrame="runtimes" in payload
          ? {t:"runtime.snapshot",...common,...payload} : {t:"runtime.unavailable",...common,...payload};
        try {if(onFrame(frame)===false) sub.close();} catch {sub.close();}
        resolveReady();
      };
      const requestRead=() => {
        if(stopped) return;
        if(reading) {rerun=true;return;}
        if(scheduled) return;
        scheduled=true;
        queueMicrotask(() => {scheduled=false;void read();});
      };
      const read=async () => {
        if(stopped || reading) return;
        reading=true;
        const abort=new AbortController();
        currentAbort=abort;
        let expired=false;
        const timeout=setTimeout(() => {
          expired=true;emit({reason:"unavailable"});abort.abort();
        },5000);
        currentTimeout=timeout;
        const revision=bus.revision("runtime-snapshots");
        try {
          const url=new URL(`/api/v1/agents/${encodeURIComponent(agentId)}/runtimes?includeStopped=true`,request.url);
          const response=await options.fetchHandler(new Request(url,{method:"GET",headers:request.headers,signal:abort.signal}));
          if(stopped || expired) return;
          if(revision!==bus.revision("runtime-snapshots")) {rerun=true;return;}
          if(!response.ok) {
            emit({reason:response.status===401 || response.status===403 ? "unauthorized" : response.status===404 ? "notFound" : "unavailable"});
            return;
          }
          try {
            const rows=parseRows(await response.json(),agentId);
            if(expired) return;
            if(revision!==bus.revision("runtime-snapshots")) {rerun=true;return;}
            emit({runtimes:rows});
          } catch {
            if(expired) return;
            if(revision!==bus.revision("runtime-snapshots")) {rerun=true;return;}
            emit({reason:"invalidSnapshot"});
          }
        } catch {
          if(expired) return;
          if(revision!==bus.revision("runtime-snapshots")) rerun=true;
          else emit({reason:"unavailable"});
        } finally {
          clearTimeout(timeout);
          if(currentTimeout===timeout) currentTimeout=undefined;
          if(currentAbort===abort) currentAbort=undefined;
          reading=false;
          if(rerun && !stopped) {rerun=false;requestRead();}
        }
      };
      const unsubscribe=bus.subscribe("runtime-snapshots",requestRead);
      const sub:RuntimeSnapshotSubscription={ready,close() {
        if(stopped) return;
        stopped=true;currentAbort?.abort();clearTimeout(currentTimeout);unsubscribe();readers.delete(sub);resolveReady();
      }};
      readers.add(sub);requestRead();
      return sub;
    },
    close() {if(closed) return;closed=true;for(const reader of [...readers]) reader.close();},
  };
}
