import { execFile } from "node:child_process";
import { mkdtemp, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { promisify } from "node:util";

import { afterEach, describe, expect, it } from "vitest";

const execFileAsync = promisify(execFile);
const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(HERE, "../../..");
const temporaryRoots: string[] = [];

afterEach(async () => {
  await Promise.all(
    temporaryRoots.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

// scripts/gateway-serve.mjs imports the bundled headless entry with plain Node — no Vitest
// transform, Vite source fallback, TypeScript loader, or path aliases. This pin rebuilds that
// artifact and imports its complete WS graph in a clean plain-Node child process.
describe("plain-node serve path", () => {
  it("imports the bundled headless WS graph under plain node", async () => {
    const temporaryRoot = await mkdtemp(join(tmpdir(), "nexus-plain-node-ws-"));
    temporaryRoots.push(temporaryRoot);
    const bundle = join(temporaryRoot, "headless.mjs");
    await symlink(join(ROOT, "node_modules"), join(temporaryRoot, "node_modules"), "dir");
    await execFileAsync(
      join(ROOT, "node_modules", ".bin", "esbuild"),
      [
        "src/server/gateway/headless.ts",
        "--bundle",
        "--platform=node",
        "--format=esm",
        "--target=node20",
        "--packages=external",
        `--outfile=${bundle}`,
      ],
      { cwd: ROOT, timeout: 60_000 },
    );
    const bundleUrl = pathToFileURL(bundle).href;
    const script = `
      const mod = await import(${JSON.stringify(bundleUrl)});
      if (typeof mod.attachHeadlessGatewayWs !== "function") {
        throw new Error("attachHeadlessGatewayWs export missing");
      }
      if (typeof mod.guardGatewayBrowserRequest !== "function") {
        throw new Error("guardGatewayBrowserRequest export missing");
      }
    `;
    await expect(
      execFileAsync(process.execPath, ["--input-type=module", "-e", script], {
        timeout: 30_000,
      }),
    ).resolves.toBeTruthy();
  });
});
