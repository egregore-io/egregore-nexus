#!/usr/bin/env node

import { existsSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const repositoryRoot = resolve(process.env.NEXUS_VERSION_ROOT || join(packageRoot, "..", ".."));
const checker = join(repositoryRoot, "scripts", "nexus-version");
const versionFile = join(repositoryRoot, "VERSION");

if (!existsSync(checker) || !existsSync(versionFile)) {
  console.error(
    "Cannot verify the Nexus package version. Pack from the repository or set NEXUS_VERSION_ROOT.",
  );
  process.exit(2);
}

const expected = readFileSync(versionFile, "utf8").trim();
const manifest = JSON.parse(readFileSync(join(packageRoot, "package.json"), "utf8"));
if (manifest.version !== expected) {
  console.error(`Package version drift: expected ${expected}, found ${manifest.version}`);
  process.exit(1);
}

const result = spawnSync(process.execPath, [checker, "check"], {
  cwd: repositoryRoot,
  env: { ...process.env, NEXUS_VERSION_ROOT: repositoryRoot },
  stdio: ["ignore", "ignore", "inherit"],
});
if (result.error) {
  console.error(`Could not run the Nexus version check: ${result.error.message}`);
  process.exit(2);
}
process.exit(result.status ?? 2);
