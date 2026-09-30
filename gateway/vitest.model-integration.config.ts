import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

// Explicit composed gate, not an optionally skipped unit test or a stored Rust-frame fixture.
export default defineConfig({
  resolve: { alias: {
    "@shared": fileURLToPath(new URL("./src/shared", import.meta.url)),
    "@server": fileURLToPath(new URL("./src/server", import.meta.url)),
    "@drizzle": fileURLToPath(new URL("./src/drizzle", import.meta.url)),
  } },
  test: { environment: "node", include: ["src/server/projection/runtimeModelIntegration.case.ts"], maxWorkers: 1 },
});
