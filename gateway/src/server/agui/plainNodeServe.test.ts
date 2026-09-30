import { execFile } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { promisify } from "node:util";

import { describe, expect, it } from "vitest";

const execFileAsync = promisify(execFile);
const HERE = dirname(fileURLToPath(import.meta.url));

// scripts/gateway-serve.mjs imports the WS upgrade stack with PLAIN Node — no vitest
// transform, no vite bundling, no TS, no path aliases. A TypeScript import reachable from
// ws.mjs at module load can crash the packaged gateway at boot. This pin
// runs the same import the serve script does, in a real node child process.
describe("plain-node serve path", () => {
  it("imports ws.mjs (and its transitive graph) under plain node", async () => {
    const wsUrl = pathToFileURL(join(HERE, "ws.mjs")).href;
    const script = `
      const mod = await import(${JSON.stringify(wsUrl)});
      if (typeof mod.attachAguiWsUpgrade !== "function") {
        throw new Error("attachAguiWsUpgrade export missing");
      }
    `;
    await expect(
      execFileAsync(process.execPath, ["--input-type=module", "-e", script], {
        timeout: 30_000,
      }),
    ).resolves.toBeTruthy();
  });
});
