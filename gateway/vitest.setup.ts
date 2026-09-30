// Vitest + jsdom global setup.
// Brings in @testing-library/jest-dom matchers (toBeInTheDocument, etc.),
// guarantees the DOM is reset between tests, and keeps gateway tests detached
// from any live Nexus daemon/sqld environment inherited from the agent shell.
import "@testing-library/jest-dom/vitest";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, afterEach, beforeEach } from "vitest";
import { cleanup } from "@testing-library/react";

type EnvSnapshot = Map<string, string>;

function snapshotNexusEnv(): EnvSnapshot {
  const snapshot: EnvSnapshot = new Map();
  for (const [key, value] of Object.entries(process.env)) {
    if (key.startsWith("NEXUS_") && value !== undefined) {
      snapshot.set(key, value);
    }
  }
  return snapshot;
}

function clearNexusEnv(): void {
  for (const key of Object.keys(process.env)) {
    if (key.startsWith("NEXUS_")) {
      delete process.env[key];
    }
  }
}

function restoreNexusEnv(snapshot: EnvSnapshot): void {
  clearNexusEnv();
  for (const [key, value] of snapshot.entries()) {
    process.env[key] = value;
  }
}

const inheritedNexusEnv = snapshotNexusEnv();
clearNexusEnv();

// Tests must never observe the operator's live ~/.nexus. Anything that resolves
// NEXUS_HOME (daemon push manifest, gateway discovery, …) gets an empty sandbox.
// A live push-endpoint manifest must not change the transport lane under test.
process.env.NEXUS_HOME = mkdtempSync(join(tmpdir(), "nexus-vitest-home-"));
const testNexusEnv = snapshotNexusEnv();

beforeEach(() => {
  restoreNexusEnv(testNexusEnv);
});

afterEach(() => {
  cleanup();
  restoreNexusEnv(testNexusEnv);
});

afterAll(() => {
  restoreNexusEnv(inheritedNexusEnv);
});
