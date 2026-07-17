#!/usr/bin/env node
/**
 * Desktop-toast sink: route Nexus messages onto your desktop's notification tray.
 *
 * The mirror image of `examples/sources/desktop_notifications_*.py` — those tap the
 * desktop and push INTO the bus; this drains the bus and toasts OUT to the desktop.
 * One file, all three OSes (node-notifier: libnotify on Linux, toast on Windows,
 * terminal-notifier on macOS).
 *
 *     Setup (once):   npm install node-notifier
 *                     nexus subscribe builds        # whatever topics you care about
 *     Run:            node desktop_toast.mjs
 *
 * Nexus stays generic: it never learns this is "a desktop". The TAP is one long-lived
 * `nexus listen --json`; the SINK and the FILTER are entirely local. Swap node-notifier
 * for Apprise/ntfy/Slack-webhook here and Nexus never notices.
 */
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";

import notifier from "node-notifier"; // npm install node-notifier

// ── THE FILTER ── your policy, not Nexus's. Everything drained toasts by default;
// tighten freely (per-topic allow-list, sender match, quiet hours…).
const keep = (msg) => true;

const toast = (msg) => {
  // ── THE SINK ── one desktop notification per surviving message.
  const scope = msg.thread ?? msg.topic ?? "dm";
  notifier.notify({
    title: `nexus · ${scope} · ${msg.from}`,
    message: msg.truncated ? `${msg.body}…` : msg.body,
  });
};

// ── THE TAP ── the only Nexus touchpoint: CLI-native, trusted-local (no token, no
// HTTP). `listen` is the same drain loop agents run: held-receive, then split-ack.
// Each stdout line under --json is one NexusBatch (camelCase):
//   { counts: {dms, thread, total}, dms: [BatchMessage], threads: [BatchMessage], … }
//   BatchMessage: { id, from, kind, scope, thread?, topic?, body, truncated }
const tap = spawn("nexus", ["listen", "--json"], {
  stdio: ["ignore", "pipe", "inherit"],
});

createInterface({ input: tap.stdout }).on("line", (line) => {
  let batch;
  try {
    batch = JSON.parse(line);
  } catch {
    return; // non-batch chatter (startup notes etc.) — not ours to interpret
  }
  for (const msg of [...(batch.dms ?? []), ...(batch.threads ?? [])]) {
    if (keep(msg)) toast(msg);
  }
});

tap.on("exit", (code) => process.exit(code ?? 1));
