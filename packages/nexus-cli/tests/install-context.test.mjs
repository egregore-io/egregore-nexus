import assert from "node:assert/strict";
import {
  chmodSync,
  copyFileSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import test from "node:test";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
const launchers = [
  {
    name: "@egregore/nexus-cli",
    path: join(repositoryRoot, "packages/nexus-cli/bin/nexus.mjs"),
    root: join(repositoryRoot, "packages/nexus-cli"),
  },
  {
    name: "@egregore/nexus-gateway",
    path: join(repositoryRoot, "gateway/scripts/nexus.mjs"),
    root: join(repositoryRoot, "gateway"),
  },
  {
    name: "@egregore/nexus",
    path: join(repositoryRoot, "packages/nexus/bin/nexus.mjs"),
    root: join(repositoryRoot, "packages/nexus"),
  },
];

for (const launcher of launchers) {
  test(`${launcher.name} stamps its top-level managed install context`, () => {
    const { report, executedLauncher, managedRoot } = runLauncher(launcher);

    assert.equal(report.NEXUS_INSTALL_METHOD, "npm");
    assert.equal(report.NEXUS_MANAGED_PACKAGE, launcher.name);
    assert.equal(report.NEXUS_MANAGED_PACKAGE_ROOT, managedRoot);
    assert.equal(report.NEXUS_LAUNCHER_PATH, executedLauncher);
    assert.match(report.NEXUS_NATIVE_BIN, /fake-nexus$/);
  });
}

test("direct CLI launcher ignores spoofed managed package environment", () => {
  const launcher = launchers[0];
  const { report } = runLauncher(launcher, {
    NEXUS_INSTALL_METHOD: "cargo",
    NEXUS_MANAGED_PACKAGE: "@egregore/nexus",
    NEXUS_MANAGED_PACKAGE_ROOT: "/tmp/not-the-running-package",
    NEXUS_LAUNCHER_PATH: "/tmp/not-the-running-launcher",
  });

  assert.equal(report.NEXUS_INSTALL_METHOD, "npm");
  assert.equal(report.NEXUS_MANAGED_PACKAGE, "@egregore/nexus-cli");
  assert.equal(report.NEXUS_MANAGED_PACKAGE_ROOT, resolve(launcher.root));
  assert.equal(report.NEXUS_LAUNCHER_PATH, resolve(launcher.path));
});

function runLauncher(launcher, overrides = {}) {
  const directory = mkdtempSync(join(tmpdir(), "nexus-install-context-"));
  const executable = join(directory, "fake-nexus");
  const output = join(directory, "env.json");
  let executedLauncher = resolve(launcher.path);
  let managedRoot = resolve(launcher.root);
  if (launcher.name === "@egregore/nexus") {
    managedRoot = join(directory, "nexus-package");
    executedLauncher = join(managedRoot, "bin/nexus.mjs");
    mkdirSync(join(managedRoot, "bin"), { recursive: true });
    mkdirSync(join(managedRoot, "node_modules/@egregore"), { recursive: true });
    copyFileSync(launcher.path, executedLauncher);
    symlinkSync(
      join(repositoryRoot, "packages/nexus-cli"),
      join(managedRoot, "node_modules/@egregore/nexus-cli"),
      "dir",
    );
  }
  writeFileSync(
    executable,
    `#!/bin/sh\nnode -e 'const fs=require("node:fs"); const keys=["NEXUS_INSTALL_METHOD","NEXUS_MANAGED_PACKAGE","NEXUS_MANAGED_PACKAGE_ROOT","NEXUS_LAUNCHER_PATH","NEXUS_NATIVE_BIN"]; fs.writeFileSync(process.argv[1], JSON.stringify(Object.fromEntries(keys.map((key)=>[key,process.env[key]]))))' "${output}"\n`,
  );
  chmodSync(executable, 0o755);
  try {
    const result = spawnSync(process.execPath, [executedLauncher, "--version"], {
      encoding: "utf8",
      env: {
        ...process.env,
        ...overrides,
        NEXUS_NATIVE_BIN: executable,
      },
    });
    assert.equal(result.status, 0, result.stderr || result.stdout);
    return {
      report: JSON.parse(readFileSync(output, "utf8")),
      executedLauncher,
      managedRoot,
    };
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}
