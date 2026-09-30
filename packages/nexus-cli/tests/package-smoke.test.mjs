import assert from "node:assert/strict";
import {
  chmodSync,
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { platformTarget } from "../lib/platform-target.mjs";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
const target = platformTarget({
  platform: process.platform,
  arch: process.arch,
  glibcVersionRuntime:
    process.platform === "linux" ? process.report.getReport().header.glibcVersionRuntime : undefined,
});

test(
  "packed CLI, Gateway, and complete installs expose the lifecycle command surface",
  { skip: !target || process.platform === "win32" },
  () => {
    const root = mkdtempSync(join(tmpdir(), "nexus-package-smoke-"));
    try {
      const packs = join(root, "packs");
      mkdirSync(packs);
      const cliTarball = packCliAssembly(root, packs);
      const gatewayTarball = packAssembly({
        root,
        packs,
        directory: "gateway",
        source: join(repositoryRoot, "gateway"),
        paths: [
          "package.json",
          "scripts/nexus.mjs",
          "scripts/nexus-gateway.mjs",
          "scripts/gateway-serve-impl.mjs",
          "scripts/gateway-serve.mjs",
          "webconsole/bin/nexus-webui.mjs",
          "webconsole/lib/lifecycle.mjs",
        ],
      });
      const nexusTarball = packAssembly({
        root,
        packs,
        directory: "nexus",
        source: join(repositoryRoot, "packages/nexus"),
        paths: ["package.json", "bin/nexus.mjs", "bin/nexus-gateway.mjs", "bin/nexus-webui.mjs"],
      });

      const topologies = [
        {
          name: "@egregore/nexus-cli",
          tarballs: [cliTarball],
          launcher: "node_modules/@egregore/nexus-cli/bin/nexus.mjs",
          absent: ["node_modules/@egregore/nexus-gateway"],
        },
        {
          name: "@egregore/nexus-gateway",
          tarballs: [cliTarball, gatewayTarball],
          launcher: "node_modules/@egregore/nexus-gateway/scripts/nexus.mjs",
          absent: [],
        },
        {
          name: "@egregore/nexus",
          tarballs: [cliTarball, gatewayTarball, nexusTarball],
          launcher: "node_modules/@egregore/nexus/bin/nexus.mjs",
          absent: [],
        },
      ];

      for (const topology of topologies) {
        verifyTopology(root, topology);
      }
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  },
);

function packCliAssembly(root, packs) {
  const source = join(repositoryRoot, "packages/nexus-cli");
  const assembly = join(root, "cli");
  copyPaths(source, assembly, [
    "package.json",
    "bin/nexus.mjs",
    "lib/install-context.mjs",
    "lib/path-guidance.mjs",
    "lib/platform-target.mjs",
    "scripts/postinstall.mjs",
  ]);
  const executable = join(assembly, "native", target, "nexus");
  mkdirSync(dirname(executable), { recursive: true });
  writeFileSync(
    executable,
    `#!/usr/bin/env node
import fs from "node:fs";
fs.appendFileSync(process.env.NEXUS_PACKAGE_SMOKE_LOG, JSON.stringify({
  args: process.argv.slice(2),
  method: process.env.NEXUS_INSTALL_METHOD,
  packageName: process.env.NEXUS_MANAGED_PACKAGE,
  packageRoot: process.env.NEXUS_MANAGED_PACKAGE_ROOT,
  launcher: process.env.NEXUS_LAUNCHER_PATH,
}) + "\\n");
`,
  );
  chmodSync(executable, 0o755);
  return npmPack(assembly, packs);
}

function packAssembly({ root, packs, directory, source, paths }) {
  const assembly = join(root, directory);
  copyPaths(source, assembly, paths);
  return npmPack(assembly, packs);
}

function copyPaths(source, destination, paths) {
  for (const relative of paths) {
    const from = join(source, relative);
    const to = join(destination, relative);
    mkdirSync(dirname(to), { recursive: true });
    cpSync(from, to, { recursive: true });
  }
}

function npmPack(directory, packs) {
  const result = spawnSync("npm", ["pack", "--ignore-scripts", "--pack-destination", packs, "--json"], {
    cwd: directory,
    encoding: "utf8",
  });
  assert.equal(result.status, 0, result.stderr || result.stdout);
  const report = JSON.parse(result.stdout);
  return join(packs, report[0].filename);
}

function verifyTopology(root, topology) {
  const install = join(root, "installs", topology.name.split("/").at(-1));
  mkdirSync(install, { recursive: true });
  writeFileSync(join(install, "package.json"), '{"private":true}');
  const installed = spawnSync(
    "npm",
    ["install", "--ignore-scripts", "--no-audit", "--no-fund", ...topology.tarballs],
    { cwd: install, encoding: "utf8" },
  );
  assert.equal(installed.status, 0, installed.stderr || installed.stdout);
  for (const path of topology.absent) {
    assert.equal(existsSync(join(install, path)), false, `${topology.name} unexpectedly installed ${path}`);
  }

  const log = join(install, "commands.jsonl");
  const launcher = join(install, topology.launcher);
  const commands = [
    ["update", "--help"],
    ["daemon", "install", "--help"],
    ["gateway", "install", "--help"],
    ["webconsole", "launch", "--help"],
  ];
  for (const args of commands) {
    const result = spawnSync(process.execPath, [launcher, ...args], {
      cwd: install,
      encoding: "utf8",
      env: { ...process.env, NEXUS_PACKAGE_SMOKE_LOG: log },
    });
    assert.equal(result.status, 0, result.stderr || result.stdout);
  }

  const reports = readFileSync(log, "utf8")
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line));
  assert.equal(reports.length, commands.length);
  for (const [index, report] of reports.entries()) {
    assert.deepEqual(report.args, commands[index]);
    assert.equal(report.method, "npm");
    assert.equal(report.packageName, topology.name);
    assert.equal(report.packageRoot, resolve(install, `node_modules/${topology.name}`));
    assert.equal(report.launcher, resolve(launcher));
  }
}
