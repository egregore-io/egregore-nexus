import { writeFile } from "node:fs/promises";
import { join } from "node:path";

import { createClient } from "@libsql/client";

import { localOperatorCaller } from "../src/server/auth/webAuthMode";
import { createHeadlessGatewayServer } from "../src/server/gateway/headless";
import { issueBearerToken } from "../src/server/identity/bearer";
import { makeDispatch } from "../src/routes/api/v1/$";
import { migrateGatewayStore } from "../src/server/store/migrations";

const nexusHome = process.argv[2];
if (!nexusHome) throw new Error("usage: transport-secret-server.ts NEXUS_HOME");
const db = createClient({ url: `file:${join(nexusHome, "gateway-test.db")}` });
await migrateGatewayStore(db);
let id = 0;
const issued = await issueBearerToken({
  actor: localOperatorCaller(),
  scopes: ["admin:*"],
}, {
  db,
  now: () => Date.now(),
  genId: () => `fixture_${++id}`,
  randomSecret: (prefix) => `${prefix}_fixture_${++id}`,
});
await writeFile(join(nexusHome, "transport-test-token"), issued.accessToken, { mode: 0o600 });

process.env.NEXUS_WEB_AUTH_MODE = "remote-human";
const apiV1 = makeDispatch({
  db: () => db,
  canonicalDb: () => db,
  authMode: "remote-human",
});
const unavailable = async () => new Response("not found", { status: 404 });
const lifecycle = {
  async startHooks() { return undefined; },
  async startProjection() {},
  async stopProjection() {},
  async stopHooks() {},
};
const server = await createHeadlessGatewayServer({
  apiV1,
  aguiObserve: unavailable,
  networkMcp: unavailable,
  webuiApi: unavailable,
}, lifecycle);
await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
const address = server.address();
if (!address || typeof address === "string") throw new Error("missing listener address");
await writeFile(join(nexusHome, "gateway.json"), JSON.stringify({
  pid: process.pid,
  url: `http://127.0.0.1:${address.port}`,
}), { mode: 0o600 });
process.stdout.write("ready\n");

async function shutdown() {
  await server.shutdown();
  db.close();
  process.exit(0);
}
process.once("SIGTERM", () => void shutdown());
process.once("SIGINT", () => void shutdown());
