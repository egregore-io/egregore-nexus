import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { pathGuidance } from "../lib/path-guidance.mjs";

const postinstall = fileURLToPath(new URL("../scripts/postinstall.mjs", import.meta.url));

test("stays silent when the Unix global npm bin directory is already on PATH", () => {
  assert.equal(pathGuidance({
    platform: "linux",
    shell: "/usr/bin/zsh",
    pathValue: "/usr/local/bin:/opt/egregore/bin:/usr/bin",
    prefix: "/opt/egregore",
    home: "/home/ada",
  }), null);
});

test("prints an exact Bash profile command when the global bin directory is missing", () => {
  const guidance = pathGuidance({
    platform: "linux",
    shell: "/bin/bash",
    pathValue: "/usr/bin:/bin",
    prefix: "/home/ada/.npm-global",
    home: "/home/ada",
  });

  assert.match(guidance, /not on PATH/);
  assert.match(guidance, /\.bashrc/);
  assert.match(guidance, /\/home\/ada\/\.npm-global\/bin/);
  assert.match(guidance, /source "\$HOME\/\.bashrc"/);
});

test("uses the native Zsh and Fish profile commands", () => {
  const zsh = pathGuidance({
    platform: "darwin",
    shell: "/bin/zsh",
    pathValue: "/usr/bin",
    prefix: "/Users/ada/.npm-global",
    home: "/Users/ada",
  });
  const fish = pathGuidance({
    platform: "linux",
    shell: "/usr/bin/fish",
    pathValue: "/usr/bin",
    prefix: "/home/ada/.npm-global",
    home: "/home/ada",
  });

  assert.match(zsh, /\.zshrc/);
  assert.match(zsh, /source "\$HOME\/\.zshrc"/);
  assert.match(fish, /fish_add_path --universal/);
  assert.doesNotMatch(fish, /\.bashrc|\.zshrc/);
});

test("prints a user-scoped PowerShell PATH command on Windows", () => {
  const guidance = pathGuidance({
    platform: "win32",
    shell: undefined,
    pathValue: String.raw`C:\Windows\System32;C:\Program Files\nodejs`,
    prefix: String.raw`C:\Users\Ada\AppData\Roaming\npm`,
    home: String.raw`C:\Users\Ada`,
  });

  assert.match(guidance, /PowerShell/);
  assert.match(guidance, /SetEnvironmentVariable/);
  assert.match(guidance, /'User'/);
  assert.match(guidance, /C:\\Users\\Ada\\AppData\\Roaming\\npm/);
  assert.doesNotMatch(guidance, /\.TrimEnd/, "an empty Windows user PATH must not throw");
});

test("Windows PATH comparison is case-insensitive", () => {
  assert.equal(pathGuidance({
    platform: "win32",
    shell: undefined,
    pathValue: String.raw`C:\WINDOWS;C:\USERS\ADA\APPDATA\ROAMING\NPM`,
    prefix: String.raw`C:\Users\Ada\AppData\Roaming\npm`,
    home: String.raw`C:\Users\Ada`,
  }), null);
});

test("postinstall only emits guidance for a global install with a missing bin directory", () => {
  const globalMissing = runPostinstall({
    npm_config_global: "true",
    npm_config_prefix: "/home/ada/.npm-global",
    PATH: "/usr/bin:/bin",
    SHELL: "/bin/bash",
    HOME: "/home/ada",
  });
  const localMissing = runPostinstall({
    npm_config_global: "false",
    npm_config_prefix: "/home/ada/.npm-global",
    PATH: "/usr/bin:/bin",
    SHELL: "/bin/bash",
    HOME: "/home/ada",
  });
  const globalPresent = runPostinstall({
    npm_config_global: "true",
    npm_config_prefix: "/home/ada/.npm-global",
    PATH: "/home/ada/.npm-global/bin:/usr/bin",
    SHELL: "/bin/bash",
    HOME: "/home/ada",
  });

  assert.equal(globalMissing.status, 0);
  assert.match(globalMissing.stdout, /\.npm-global\/bin/);
  assert.equal(localMissing.status, 0);
  assert.equal(localMissing.stdout, "");
  assert.equal(globalPresent.status, 0);
  assert.equal(globalPresent.stdout, "");
});

function runPostinstall(overrides) {
  return spawnSync(process.execPath, [postinstall], {
    encoding: "utf8",
    env: { ...process.env, ...overrides },
  });
}
