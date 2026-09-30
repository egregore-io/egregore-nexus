#!/usr/bin/env node

import assert from "node:assert/strict";
import { extname, resolve } from "node:path";
import { spawnSync } from "node:child_process";

const launcher = process.argv[2];
if (!launcher) {
  process.stderr.write("usage: scripts/nexus-cli-command-surface-smoke.mjs <nexus-launcher>\n");
  process.exit(2);
}

const resolvedLauncher = resolve(launcher);
const version = run(["--version"]);
assert.match(version, /^nexus 0\.1\.[01] \(revision [^)]+\)/);

const queue = [[]];
const visited = new Set();
while (queue.length > 0) {
  const path = queue.shift();
  const key = path.join(" ");
  if (visited.has(key)) continue;
  visited.add(key);

  const help = run([...path, "--help"]);
  assert.match(help, /Usage:/, `missing Usage line for nexus ${key}`);
  for (const child of visibleSubcommands(help)) {
    if (child !== "help") queue.push([...path, child]);
  }
  if (path.length === 0) queue.push(["daemon"]);
}

for (const required of [
  "launch",
  "whoami",
  "attach",
  "notify",
  "thread new",
  "source register",
  "agents list",
  "update",
  "gateway install",
  "gateway start",
  "gateway delivery-mode set",
  "webconsole launch",
  "webconsole status",
  "daemon install",
  "daemon status",
]) {
  assert(visited.has(required), `command surface omitted: nexus ${required}`);
}

process.stdout.write(`nexus CLI command surface: PASS (${visited.size} help endpoints)\n`);

function run(args) {
  const command = extname(resolvedLauncher) === ".mjs" ? process.execPath : resolvedLauncher;
  const commandArgs = command === process.execPath ? [resolvedLauncher, ...args] : args;
  const result = spawnSync(command, commandArgs, {
    encoding: "utf8",
    env: { ...process.env, NEXUS_NO_AUTOSTART: "1" },
    windowsHide: true,
  });
  assert.equal(
    result.status,
    0,
    `nexus ${args.join(" ")} failed (${result.status})\nstdout=${result.stdout}\nstderr=${result.stderr}`,
  );
  return result.stdout;
}

function visibleSubcommands(help) {
  const commands = [];
  let inCommands = false;
  for (const line of help.split(/\r?\n/)) {
    if (line === "Commands:") {
      inCommands = true;
      continue;
    }
    if (!inCommands) continue;
    if (/^[A-Z][A-Za-z ]+:$/.test(line)) break;
    const match = line.match(/^  ([a-z][a-z0-9-]*)\s{2,}/);
    if (match) commands.push(match[1]);
  }
  return commands;
}
