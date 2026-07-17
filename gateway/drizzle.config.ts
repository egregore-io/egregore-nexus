import { defineConfig } from "drizzle-kit";

/*
 * drizzle-kit config — INTROSPECT ONLY (`drizzle-kit pull`).
 *
 * Development tooling only. The production gateway never opens the Nexus store;
 * it reads daemon-owned views over IPC. `db:pull` may introspect an explicitly
 * supplied disposable schema fixture and must never inherit runtime DB settings.
 */
export default defineConfig({
  dialect: "turso",
  schema: "./src/drizzle/schema.gen.ts",
  out: "./src/drizzle",
  dbCredentials: {
    url: process.env.NEXUS_SCHEMA_DB_URL ?? "file:./.nexus-dev.db",
    authToken: process.env.NEXUS_SCHEMA_DB_TOKEN,
  },
});
