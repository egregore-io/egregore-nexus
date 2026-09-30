import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { parseRuntimeModelReport, readStoredModelReport } from "./modelReport";

const wireFixture = (name: string) => JSON.parse(readFileSync(resolve(`../core/crates/nexus-contracts/fixtures/${name}`), "utf8")).modelReport;
const fixture = () => ({ ...wireFixture("runtime.telemetry.json"), configured: wireFixture("runtime.model_report.json").configured });

describe("ordinary canonical model/telemetry validation", () => {
  it("agrees with Rust on shared raw report revisions, slots and tombstones", () => {
    const cases = JSON.parse(readFileSync(resolve("../core/crates/nexus-contracts/fixtures/model-report-validation.json"), "utf8")).reports;
    for (const entry of cases) {
      const parse = () => parseRuntimeModelReport(JSON.parse(entry.raw));
      if (!entry.valid) expect(parse, entry.name).toThrow();
      else expect(parse(), entry.name).toEqual(JSON.parse(entry.raw));
    }
  });
  it("agrees with Rust on shared raw observation JSON without Unicode preprocessing", () => {
    const cases = JSON.parse(readFileSync(resolve("../core/crates/nexus-contracts/fixtures/model-report-validation.json"), "utf8")).observations;
    for (const entry of cases) {
      const parse = () => {
        const report = fixture();
        report.configured.observation = JSON.parse(entry.raw);
        return parseRuntimeModelReport(report).configured;
      };
      if (!entry.valid) {
        expect(parse, entry.name).toThrow();
      } else {
        expect(parse(), entry.name).toMatchObject({ observation: entry.canonical ?? JSON.parse(entry.raw) });
      }
    }
  });
  it("roundtrips the same Rust telemetry fixture without inventing totals or model identity", () => {
    for (const name of ["runtime.telemetry.json", "runtime.model_report.json"]) {
      expect(parseRuntimeModelReport(wireFixture(name))).toEqual(wireFixture(name));
    }
    expect(parseRuntimeModelReport(fixture())).toEqual(fixture());
    expect(readStoredModelReport(0, null)).toBeUndefined();
    const report = fixture();
    expect(readStoredModelReport(report.reportRevision, JSON.stringify(report))).toEqual(report);
  });

  it.each([
    ["unsafe revision", (r: any) => { r.reportRevision = 9007199254740992; }],
    ["zero revision", (r: any) => { r.reportRevision = 0; }],
    ["unsafe model time", (r: any) => { r.configured.observation.observedAt = 9007199254740992; }],
    ["fractional counter", (r: any) => { r.telemetry.usage.observation.inputTokens = 0.5; }],
    ["negative counter", (r: any) => { r.telemetry.usage.observation.inputTokens = -1; }],
    ["unsupported observed", (r: any) => { r.telemetry.context.capability = "unsupported"; }],
    ["unknown with payload", (r: any) => { r.telemetry.usage.status = "unknown"; }],
    ["missing payload", (r: any) => { delete r.telemetry.usage.observation; }],
    ["turn without ID", (r: any) => { r.telemetry.usage.observation.scope = "turn"; delete r.telemetry.usage.observation.nativeTurnId; }],
    ["no counter", (r: any) => { const u = r.telemetry.usage.observation; for (const k of ["inputTokens", "outputTokens", "cacheReadTokens", "cacheWriteTokens", "reasoningTokens", "totalTokens"]) delete u[k]; }],
    ["estimate without basis", (r: any) => { delete r.telemetry.context.observation.usedTokens.basis; }],
    ["zero capacity", (r: any) => { r.telemetry.context.observation.effectiveCapacityTokens.value = 0; }],
    ["over remaining", (r: any) => { r.telemetry.context.observation.remainingPercent.value = 101; }],
    ["duplicate quota window", (r: any) => { r.telemetry.quota.observation.windows.push(r.telemetry.quota.observation.windows[0]); }],
    ["control source", (r: any) => { r.configured.observation.source = "x\u0085y"; }],
    ["long source bytes", (r: any) => { r.configured.observation.source = "é".repeat(65); }],
    ["lone surrogate", (r: any) => { r.configured.observation.modelId = "\ud800"; }],
    ["slot extras", (r: any) => { r.configured.extra = true; }],
    ["false tombstone", (r: any) => { r.backend = "unknown"; }],
  ])("rejects %s", (_, change) => {
    const report = fixture();
    change(report);
    expect(() => parseRuntimeModelReport(report)).toThrow();
  });

  it("preserves native overage, FEFF, supplementary scalars and negative timestamps", () => {
    const report = fixture();
    report.configured.observation.modelId = "\ufeff";
    report.configured.observation.source = "native/😀";
    report.configured.observation.observedAt = -Number.MAX_SAFE_INTEGER;
    report.telemetry.context.observation.usedPercent = { value: 125, provenance: "native" };
    report.telemetry.context.observation.usedTokens = { value: 1250, provenance: "native" };
    expect(parseRuntimeModelReport(report)).toEqual(report);
  });

  it("canonicalizes ignored extras but not status-union fields", () => {
    const report = fixture();
    report.extra = "ignored";
    report.configured.observation.extra = "ignored";
    report.telemetry.usage.extra = "ignored";
    expect(parseRuntimeModelReport(report)).toEqual(fixture());
  });

  it("validates the stored JSON/revision as one pair", () => {
    const report = fixture();
    for (const [revision, json] of [[0, JSON.stringify(report)], [2, null], [report.reportRevision + 1, JSON.stringify(report)], [1, "broken"]]) {
      expect(() => readStoredModelReport(revision, json)).toThrow();
    }
  });
});
