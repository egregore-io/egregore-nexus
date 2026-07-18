import { constants } from "node:fs";
import { access, readFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, test } from "vitest";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");

describe("public gateway package contract", () => {
  test("publishes Gateway plus its bundled WebUI and installs the native CLI", async () => {
    const manifest = JSON.parse(await readFile(join(root, "package.json"), "utf8"));

    expect(manifest.name).toBe("@egregore/nexus-gateway");
    expect(manifest.version).toBe("0.1.3");
    expect(manifest.private).toBe(false);
    expect(manifest.license).toBe("Apache-2.0");
    expect(manifest.engines?.node).toBe(">=20");
    expect(manifest.bin?.nexus).toBe("scripts/nexus.mjs");
    expect(manifest.bin?.["nexus-gateway"]).toBe("scripts/nexus-gateway.mjs");
    expect(manifest.bin?.["nexus-webui"]).toBe("webconsole/bin/nexus-webui.mjs");
    expect(manifest.dependencies?.["@egregore/nexus-cli"]).toBe("0.1.3");
    expect(manifest.files).toEqual([
      "dist-gateway/headless.mjs",
      "scripts/gateway-serve-impl.mjs",
      "scripts/gateway-serve.mjs",
      "scripts/nexus-gateway.mjs",
      "scripts/nexus.mjs",
      "webconsole/bin/nexus-webui.mjs",
      "webconsole/lib/lifecycle.mjs",
      "webconsole/dist",
    ]);
    expect(manifest.scripts?.prepack).toContain("webconsole:build");
    expect(manifest.scripts?.["build:gateway-package"]).toContain("esbuild");

    const bin = join(root, manifest.bin["nexus-gateway"]);
    await access(bin, constants.X_OK);
    const source = await readFile(bin, "utf8");
    expect(source).toContain('"--api-only"');
    expect(source).toContain('"--discovery=write"');

    const serveSource = await readFile(join(root, "scripts/gateway-serve-impl.mjs"), "utf8");
    expect(serveSource).not.toContain('import { attachAguiWsUpgrade } from "../src/server/agui/ws.mjs"');
    expect(serveSource).toContain('await import("../src/server/agui/ws.mjs")');
    expect(serveSource).toContain('import.meta.resolve("tsx/cli")');
    expect(serveSource).toContain('"--tsconfig", join(FRONTEND_DIR, "tsconfig.json")');
    expect(serveSource).toContain('join(FRONTEND_DIR, "dist-gateway/headless.mjs")');
    expect(serveSource).toContain("writeGatewayDiscovery(port, process.pid)");
    expect(serveSource).not.toContain("writeGatewayDiscovery(port, runner.pid)");
  });
});
