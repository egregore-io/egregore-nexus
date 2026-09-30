import { describe, expect, it } from "vitest";

function nexusEnvKeys(): string[] {
  return Object.keys(process.env)
    .filter((key) => key.startsWith("NEXUS_"))
    .sort();
}

describe.sequential("gateway Vitest Nexus env isolation", () => {
  it("scrubs inherited Nexus env while keeping the test-owned home sandbox", () => {
    expect(process.env.NEXUS_HOME).toMatch(/nexus-vitest-home-/);
    expect(process.env.NEXUS_DB_URL).toBeUndefined();
    expect(process.env.NEXUS_DB_READ_TOKEN).toBeUndefined();
    expect(process.env.NEXUS_DB_WRITE_TOKEN).toBeUndefined();
    expect(process.env.NEXUS_CLIENT_KEY).toBeUndefined();
    expect(nexusEnvKeys()).toEqual(["NEXUS_HOME"]);
  });

  it("allows a test to set Nexus env explicitly", () => {
    process.env.NEXUS_DB_URL = "http://127.0.0.1:4141";
    process.env.NEXUS_CLIENT_KEY = "test-client-key";

    expect(process.env.NEXUS_DB_URL).toBe("http://127.0.0.1:4141");
    expect(process.env.NEXUS_CLIENT_KEY).toBe("test-client-key");
  });

  it("restores the clean Nexus env baseline before the next test", () => {
    expect(process.env.NEXUS_HOME).toMatch(/nexus-vitest-home-/);
    expect(process.env.NEXUS_DB_URL).toBeUndefined();
    expect(process.env.NEXUS_CLIENT_KEY).toBeUndefined();
    expect(nexusEnvKeys()).toEqual(["NEXUS_HOME"]);
  });
});
