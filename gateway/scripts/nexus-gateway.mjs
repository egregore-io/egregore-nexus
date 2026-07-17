#!/usr/bin/env node

// Public headless gateway executable. The WebUI is a separate installation, so this entrypoint
// always selects the REST/WebSocket server and writes discovery for `nexus gateway status`.
if (process.argv.includes("--help")) {
  process.stdout.write("Usage: nexus-gateway [--migrate-only] [--api-only]\n");
  process.exit(0);
}
if (process.argv.includes("--migrate-only")) {
  const { migrateHeadlessGatewayStore } = await import("../dist-gateway/headless.mjs");
  const result = await migrateHeadlessGatewayStore();
  process.stdout.write(`${JSON.stringify(result)}\n`);
  process.exit(0);
}
if (!process.argv.includes("--api-only")) {
  process.argv.push("--api-only");
}
if (!process.argv.some((argument) => argument.startsWith("--discovery="))) {
  process.argv.push("--discovery=write");
}

await import("./gateway-serve.mjs");
