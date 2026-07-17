#!/usr/bin/env node
// Custom-app source (remote, Node): emit your app's events via the same signed HTTP contract.
//
//   Setup (once):  nexus source register myapp --topic deploys   # copy the printed token
//   A listener:    (in an agent's shell)  nexus subscribe deploys
//   Use:           NEXUS_URL=… NEXUS_SOURCE_TOKEN=… node custom_app_remote.mjs
//
// Identical contract to custom_app_remote.py and to a GitHub/CI webhook — only the language differs.
import crypto from "node:crypto";

const NEXUS_URL = process.env.NEXUS_URL ?? "https://nexus.example";
const SOURCE = "myapp";
const TOKEN = process.env.NEXUS_SOURCE_TOKEN; // from `nexus source register myapp`

export async function notify(summary, body, meta = {}, topic) {
  const raw = JSON.stringify({ summary, body, meta });
  const ts = String(Math.floor(Date.now() / 1000));
  // HMAC-SHA256(token, "<timestamp>." + rawBody) — timestamp signed for replay protection.
  const sig = crypto.createHmac("sha256", TOKEN).update(`${ts}.${raw}`).digest("hex");
  const url = `${NEXUS_URL}/api/v1/sources/${SOURCE}/push${topic ? `?topic=${encodeURIComponent(topic)}` : ""}`;
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "X-Nexus-Timestamp": ts,
      "X-Nexus-Signature": `sha256=${sig}`,
      "content-type": "application/json",
    },
    body: raw,
  });
  if (!response.ok) {
    throw new Error(`Nexus source push failed: ${response.status} ${await response.text()}`);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  await notify("Deploy started", "web v1.4.2 → prod", { service: "web", version: "1.4.2" });
}
