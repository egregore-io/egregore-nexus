import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const root = process.cwd();

function readJson<T>(path: string): T {
  return JSON.parse(readFileSync(join(root, path), "utf8")) as T;
}

type PackageJson = {
  name: string;
  scripts: Record<string, string>;
};

describe("gateway/webconsole package split", () => {
  it("declares and delegates separate gateway and webconsole package surfaces", () => {
    const workspace = readFileSync(join(root, "pnpm-workspace.yaml"), "utf8");
    expect(workspace).toContain("- gateway");
    expect(workspace).toContain("- webconsole");
    expect(workspace).toContain("allowBuilds:");
    expect(workspace).toContain("esbuild: true");

    const publishedGateway = readJson<PackageJson>("package.json");
    expect(publishedGateway.name).toBe("@egregore/nexus-gateway");
    expect(publishedGateway.scripts.start).toBe("pnpm --dir gateway start");
    expect(publishedGateway.scripts["start:api"]).toBe("pnpm --dir gateway start:api");
    expect(publishedGateway.scripts.dev).toBe("pnpm --dir gateway start:api");
    expect(publishedGateway.scripts.build).toBe("npm run build:gateway-package");
    expect(publishedGateway.scripts["webconsole:dev"]).toBe("pnpm --dir webconsole dev");
    expect(publishedGateway.scripts["webconsole:build"]).toBe(
      "node ../scripts/nexus-version check && node webconsole/build-webconsole.mjs",
    );
  });

  it("keeps the gateway headless entrypoint independent of the webconsole package", () => {
    const gateway = readJson<PackageJson>("gateway/package.json");
    expect(gateway.name).toBe("@egregore/nexus-gateway-workspace");
    expect(gateway.scripts.start).toBe("node ../scripts/gateway-serve.mjs");
    expect(gateway.scripts["start:api"]).toBe(
      "node ../scripts/gateway-serve.mjs --api-only --discovery=none",
    );
    expect(gateway.scripts.start).not.toContain("vite");
    expect(gateway.scripts["start:api"]).not.toContain("vite");
  });

  it("keeps the bundled webconsole workspace on the Vite/TanStack surface", () => {
    const webconsole = readJson<PackageJson>("webconsole/package.json");
    expect(webconsole.name).toBe("@egregore/nexus-webui-workspace");
    expect(webconsole.scripts.dev).toBe("vite --config vite.config.ts");
    expect(webconsole.scripts.build).toBe(
      "node ../../scripts/nexus-version check && node build-webconsole.mjs",
    );
    expect(webconsole.scripts.dev).not.toContain("gateway-serve");
    expect(webconsole.scripts.build).not.toContain("gateway-serve");
  });

  it("loads the bundled client and server entries relative to the packaged script", () => {
    const source = readFileSync(join(root, "scripts/gateway-serve-impl.mjs"), "utf8");
    expect(source).toContain(
      "const FRONTEND_DIR = dirname(dirname(fileURLToPath(import.meta.url)))",
    );
    expect(source).toContain('const DIST_CLIENT_DIR = join(FRONTEND_DIR, "dist/client")');
    expect(source).toContain(
      'const DIST_SERVER_ENTRY = join(FRONTEND_DIR, "dist/server/server.js")',
    );
    expect(source).toContain('await cp(join(FRONTEND_DIR, "dist"), dir, { recursive: true })');
  });

  it("binds the packaged webconsole server instead of executing the generated handler", () => {
    const source = readFileSync(join(root, "scripts/gateway-serve-impl.mjs"), "utf8");
    expect(source).toContain("startPackagedGateway(port)");
    // The handler loads from the run-scoped dist SNAPSHOT (deploy hygiene: an in-place
    // rebuild of dist/ must not yank modules from under the running server).
    expect(source).toContain('loadPackagedServerHandler(join(distDir, "server/server.js"))');
    expect(source).toContain("snapshotDistForRun()");
    expect(source).toContain("createHttpServer");
    expect(source).toContain("attachHeadlessGatewayWs,");
    expect(source).toContain(
      "await attachHeadlessGatewayWs(server, { fetchHandler: guardedHandler })",
    );
    expect(source).toContain("startGatewayProjectionService({");
    expect(source).toContain("stopGatewayProjectionService()");
    expect(source).toContain("startGatewayHookService()");
    expect(source).toContain("stopGatewayHookService()");
    expect(source).toContain("closeSharedDaemonPushConnector()");
    expect(source).toContain("pathToFileURL(serverEntry).href");
    expect(source).not.toContain("spawn(process.execPath, [join(FRONTEND_DIR, \"dist/server/server.js\")]");
  });

  it("bounds gateway stop on live sockets", () => {
    const source = readFileSync(join(root, "scripts/gateway-serve-impl.mjs"), "utf8");
    expect(source).toContain("NEXUS_GATEWAY_CLOSE_TIMEOUT_MS");
    expect(source).toContain("closeHttpServerWithDeadline(server, sockets");
    expect(source).toContain("server.on(\"connection\", (socket) => trackSocket(sockets, socket))");
    expect(source).toContain("server.on(\"upgrade\", (_req, socket) => trackSocket(sockets, socket))");
    expect(source).toContain("destroyTrackedSockets(sockets)");
    expect(source).toContain(
      "await closeHttpServerWithDeadline(server, sockets, GATEWAY_CLOSE_TIMEOUT_MS)",
    );
  });

  it("preserves duplicate Set-Cookie headers from registration responses", () => {
    const source = readFileSync(join(root, "scripts/gateway-serve-impl.mjs"), "utf8");
    expect(source).toContain("response.headers.getSetCookie()");
    expect(source).toContain('res.setHeader("set-cookie", setCookies)');
    expect(source).toContain('if (key.toLowerCase() === "set-cookie") return');
  });
});
