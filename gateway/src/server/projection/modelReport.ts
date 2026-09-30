import { z } from "zod";
import {
  ModelEvidenceCapability, ModelInvalidReason, ModelUnknownReason,
  TelemetryAvailability, TelemetryProvenance, TokenUsageScope,
  type RuntimeModelReport,
} from "@shared/types";

// Rust str::trim uses Unicode White_Space, not JavaScript trim's FEFF handling. Reject lone
// UTF-16 surrogates that Rust's ordinary JSON string decoder cannot represent. Never normalize.
const nonblank = z.string().refine((v) => !/^\p{White_Space}*$/u.test(v) && !/[\uD800-\uDFFF]/u.test(v));
const boundedId = (limit: number) => nonblank.refine((v) => !/\p{Cc}/u.test(v) && new TextEncoder().encode(v).length <= limit);
const source = boundedId(128);
const id = boundedId(1024);
const timestamp = z.number().int().min(-Number.MAX_SAFE_INTEGER).max(Number.MAX_SAFE_INTEGER);
const counter = z.number().int().min(0).max(Number.MAX_SAFE_INTEGER);
const quantity = z.number().min(0).max(Number.MAX_SAFE_INTEGER);
const capability = z.enum(ModelEvidenceCapability);
const optional = <T extends z.ZodType>(type: T) => z.preprocess((v) => v === null ? undefined : v, type.optional());
const metadata = z.object({
  nativeSessionId: id, source, observedAt: timestamp, nativeReportedAt: optional(timestamp),
});
const model = z.object({ modelId: id, providerId: optional(id) });
const modelObservation = z.object({
  modelId: nonblank, providerId: optional(nonblank), source, observedAt: timestamp,
  nativeSessionId: optional(nonblank), nativeTurnId: optional(nonblank),
  nativeMessageId: optional(nonblank), nativeReportedAt: optional(timestamp),
});
// Unlike ordinary report/observation DTOs, Rust's status union denies unknown fields.
const modelSlot = z.discriminatedUnion("status", [
  z.object({ status: z.literal("observed"), capability: z.literal(ModelEvidenceCapability.Supported), observation: modelObservation }).strict(),
  z.object({ status: z.literal("unknown"), capability, reason: optional(z.enum(ModelUnknownReason)) }).strict(),
  z.object({ status: z.literal("invalid"), capability, reason: z.enum(ModelInvalidReason) }).strict(),
]);
const usage = z.object({
  metadata, scope: z.enum(TokenUsageScope), counterId: id, resetId: optional(id),
  nativeTurnId: optional(id), model: optional(model),
  inputTokens: optional(counter), outputTokens: optional(counter), cacheReadTokens: optional(counter),
  cacheWriteTokens: optional(counter), reasoningTokens: optional(counter), totalTokens: optional(counter),
}).refine((v) => v.scope !== TokenUsageScope.Turn || v.nativeTurnId !== undefined)
  .refine((v) => [v.inputTokens, v.outputTokens, v.cacheReadTokens, v.cacheWriteTokens, v.reasoningTokens, v.totalTokens].some((n) => n !== undefined));
const contextValue = <T extends z.ZodType>(value: T) => z.object({
  value, provenance: z.enum(TelemetryProvenance), basis: optional(id),
}).refine((v) => v.provenance === TelemetryProvenance.Native || v.basis !== undefined);
const tokenValue = contextValue(counter);
const percentageValue = contextValue(quantity);
const context = z.object({
  metadata, model: optional(model), effectiveCapacityTokens: optional(tokenValue),
  usedTokens: optional(tokenValue), remainingTokens: optional(tokenValue),
  usedPercent: optional(percentageValue), remainingPercent: optional(percentageValue),
  outputReserveTokens: optional(tokenValue), compactionCount: optional(counter), resetId: optional(id),
}).refine((v) => [v.effectiveCapacityTokens, v.usedTokens, v.remainingTokens, v.usedPercent, v.remainingPercent].some((n) => n !== undefined))
  .refine((v) => v.effectiveCapacityTokens === undefined || v.effectiveCapacityTokens.value > 0)
  .refine((v) => v.remainingPercent === undefined || v.remainingPercent.value <= 100);
const window = z.object({
  windowId: id, units: id, used: optional(quantity), remaining: optional(quantity),
  limit: optional(quantity), usedPercent: optional(quantity), remainingPercent: optional(quantity),
  resetsAt: optional(timestamp), windowSeconds: optional(counter),
}).refine((v) => [v.used, v.remaining, v.limit, v.usedPercent, v.remainingPercent].some((n) => n !== undefined))
  .refine((v) => v.remainingPercent === undefined || v.remainingPercent <= 100)
  .refine((v) => v.windowSeconds === undefined || v.windowSeconds > 0);
const quota = z.object({
  metadata, providerId: id, accountId: optional(id), windows: z.array(window).min(1).max(16),
}).refine((v) => new Set(v.windows.map((w) => w.windowId)).size === v.windows.length);
const telemetrySlot = <T extends z.ZodType>(observation: T) => z.object({
  status: z.enum(TelemetryAvailability), capability, observation: optional(observation),
}).refine((v) => v.status === TelemetryAvailability.Observed
  ? v.capability === ModelEvidenceCapability.Supported && v.observation !== undefined
  : v.observation === undefined);
const telemetry = z.object({ usage: telemetrySlot(usage), context: telemetrySlot(context), quota: telemetrySlot(quota) });
const report = z.object({
  backend: source, observerActive: z.boolean(), reportRevision: counter.min(1),
  configured: modelSlot, turnSelected: modelSlot, responseReported: modelSlot,
  telemetry: optional(telemetry),
}).refine((v) => v.backend !== "unknown" || (!v.observerActive && v.telemetry === undefined
  && [v.configured, v.turnSelected, v.responseReported].every((s) => s.status === "invalid"
    && s.capability === ModelEvidenceCapability.Unverified && s.reason === ModelInvalidReason.CorruptStoredMetadata)));

/** Ordinary validation and canonicalization only; no writers, service calls or harness catalog. */
export function parseRuntimeModelReport(value: unknown): RuntimeModelReport {
  return report.parse(value);
}

/** A revision and JSON image form one authoritative stored value. Corruption is not absence. */
export function readStoredModelReport(revision: unknown, json: unknown): RuntimeModelReport | undefined {
  if (revision === 0 && json === null) return undefined;
  if (!Number.isSafeInteger(revision) || typeof revision !== "number" || revision < 1 || typeof json !== "string") {
    throw new Error("invalid stored model report/revision pair");
  }
  const value = parseRuntimeModelReport(JSON.parse(json));
  if (value.reportRevision !== revision) throw new Error("stored model report revision mismatch");
  return value;
}
