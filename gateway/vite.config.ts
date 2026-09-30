/// <reference types="vitest/config" />
import { fileURLToPath } from "node:url";
import { defineConfig, type Plugin } from "vite";
import tailwindcss from "@tailwindcss/vite";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import viteReact from "@vitejs/plugin-react";

import {
  resolveGatewayAuthMode,
  resolveGatewayPort,
  removeGatewayDiscovery,
  type GatewayDiscovery,
  writeGatewayDiscovery,
} from "./src/server/gateway/discovery";

const alias = {
  "@app": fileURLToPath(new URL("./src/app", import.meta.url)),
  "@modules": fileURLToPath(new URL("./src/modules", import.meta.url)),
  "@server": fileURLToPath(new URL("./src/server", import.meta.url)),
  "@shared": fileURLToPath(new URL("./src/shared", import.meta.url)),
  "@drizzle": fileURLToPath(new URL("./src/drizzle", import.meta.url)),
};

// Vitest runs the config too. The TanStack Start plugin owns SSR/dev/build and
// applies route-file transforms (code-splitting route modules) that we don't want
// — and don't need — during unit tests. Gate it to non-test runs; tests render
// components in isolation with just React + Tailwind resolved.
const isTest = process.env.VITEST === "true";

export default defineConfig(async ({ command }) => {
  const isServe = command === "serve" && !isTest;
  const gatewayPort = isServe ? await resolveGatewayPort(process.env) : undefined;
  return {
    resolve: { alias },
    define: {
      "import.meta.env.NEXUS_WEB_TRANSPORT": JSON.stringify(process.env.NEXUS_WEB_TRANSPORT ?? "sse"),
      "import.meta.env.NEXUS_GATEWAY_URL": JSON.stringify(process.env.NEXUS_GATEWAY_URL ?? ""),
    },
    server: gatewayPort
      ? {
          host: "127.0.0.1",
          port: gatewayPort,
          strictPort: true,
        }
      : undefined,
    plugins: [
      // Tailwind v4 Vite plugin — CSS-first; tokens live in src/shared/ui/tokens.css.
      tailwindcss(),
      // TanStack Start (Vite plugin era; NOT vinxi/app.config.ts). Generates the
      // route tree, wires the server runtime + SSR. The React plugin MUST follow it.
      // Colocated *.test.tsx live under src/routes but are NOT routes — exclude them
      // from the generated route tree.
      ...(isServe ? [gatewayDiscoveryPlugin(gatewayPort!)] : []),
      ...(isTest
        ? []
        : [
            tanstackStart({
              router: { routeFileIgnorePattern: ".*\\.(test|spec)\\.[tj]sx?$" },
            }),
          ]),
      viteReact(),
    ],
    test: {
      environment: "jsdom",
      setupFiles: ["./vitest.setup.ts"],
      globals: true,
      // Vitest resolves its own module graph; mirror the aliases so tests can use them.
      alias,
      include: ["src/**/*.{test,spec}.{ts,tsx}"],
    },
  };
});

function gatewayDiscoveryPlugin(port: number): Plugin {
  let record: GatewayDiscovery | undefined;
  return {
    name: "nexus-gateway-discovery",
    configureServer(server) {
      server.httpServer?.once("listening", async () => {
        try {
          record = await writeGatewayDiscovery({
            port,
            authMode: resolveGatewayAuthMode(process.env),
          });
        } catch (err) {
          server.config.logger.error(
            `failed to write Nexus gateway discovery: ${
              err instanceof Error ? err.message : String(err)
            }`,
          );
        }
      });
      server.httpServer?.once("close", () => {
        if (record) void removeGatewayDiscovery(record);
      });
    },
  };
}
