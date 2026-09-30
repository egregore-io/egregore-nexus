#!/usr/bin/env node

import { accessSync, constants } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { platformTarget } from "../lib/platform-target.mjs";

const executableName = process.platform === "win32" ? "nexus.exe" : "nexus";
const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const candidates = [
  process.env.NEXUS_NATIVE_BIN,
  packagedNativeBinary(),
  process.env.CARGO_HOME ? join(process.env.CARGO_HOME, "bin", executableName) : undefined,
  join(homedir(), ".cargo", "bin", executableName),
].filter(Boolean).map((candidate) => resolve(candidate));

const executable = candidates.find(isExecutable);
if (!executable) {
  process.stderr.write(
    "Nexus native CLI is not installed for this platform. "
      + "Reinstall `@egregore/nexus` or `@egregore/nexus-cli`, "
      + "run `cargo install egregore-nexus`, "
      + "or set NEXUS_NATIVE_BIN.\n",
  );
  process.exit(127);
}

const child = spawnSync(executable, process.argv.slice(2), {
  stdio: "inherit",
  env: process.env,
  windowsHide: false,
});
if (child.error) throw child.error;
if (child.signal) {
  process.kill(process.pid, child.signal);
} else {
  process.exit(child.status ?? 1);
}

function packagedNativeBinary() {
  const report = process.platform === "linux" ? process.report?.getReport?.() : undefined;
  const target = platformTarget({
    platform: process.platform,
    arch: process.arch,
    glibcVersionRuntime: report?.header?.glibcVersionRuntime,
  });
  return target ? join(packageRoot, "native", target, executableName) : undefined;
}

function isExecutable(path) {
  try {
    accessSync(path, process.platform === "win32" ? constants.F_OK : constants.X_OK);
    return true;
  } catch {
    return false;
  }
}
