#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { pathGuidance } from "../lib/path-guidance.mjs";

try {
  if (isGlobalInstall(process.env)) {
    const prefix = globalPrefix(process.env);
    const guidance = pathGuidance({
      platform: process.platform,
      shell: process.env.SHELL,
      pathValue: process.env.PATH,
      prefix,
      home: process.env.HOME ?? process.env.USERPROFILE,
    });
    if (guidance) process.stdout.write(`${guidance}\n`);
  }
} catch {
  // PATH guidance must never turn a successful package extraction into a failed installation.
}

function isGlobalInstall(env) {
  return env.npm_config_global === "true" || env.npm_config_location === "global";
}

function globalPrefix(env) {
  if (env.npm_config_prefix) return env.npm_config_prefix;
  if (!env.npm_execpath) return undefined;

  const result = spawnSync(process.execPath, [env.npm_execpath, "prefix", "--global"], {
    encoding: "utf8",
    env,
    windowsHide: true,
  });
  return result.status === 0 ? result.stdout.trim() : undefined;
}
