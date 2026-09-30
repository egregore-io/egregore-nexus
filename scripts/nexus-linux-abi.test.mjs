import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { verifyGlibcRequirements } from "./nexus-linux-abi.mjs";

const needs = (...versions) =>
  "Version needs section '.gnu.version_r' contains 1 entry:\n" +
  versions.map((v) => `  Name: ${v}  Flags: none  Version: 2`).join("\n");

test("Linux requirements at or below glibc 2.35 pass numeric comparison", () => {
  assert.doesNotThrow(() => verifyGlibcRequirements(needs("GLIBC_2.2.5", "GLIBC_2.9", "GLIBC_2.17", "GLIBC_2.35")));
});

test("newer glibc requirements fail even among supported requirements", () => {
  for (const version of ["2.36", "2.39", "2.100", "3.0", "2.35.1"]) {
    assert.throws(() => verifyGlibcRequirements(needs("GLIBC_2.17", `GLIBC_${version}`)), /exceeds glibc 2\.35/);
  }
});

test("missing or unrecognized requirements fail closed", () => {
  for (const output of ["", "No version information found", needs("GCC_3.0"), needs("GLIBC_PRIVATE"), needs("GLIBC_2.bad")]) {
    assert.throws(() => verifyGlibcRequirements(output));
  }
});

test("version definitions alone do not stand in for imported requirements", () => {
  assert.throws(() => verifyGlibcRequirements("Version definition section '.gnu.version_d':\n Name: GLIBC_2.35"));
});

test("CLI help is side-effect free and missing readelf or invalid ELF fails closed", () => {
  const script = fileURLToPath(new URL("./nexus-linux-abi.mjs", import.meta.url));
  const invoke = (args, env = process.env) => spawnSync(process.execPath, [script, ...args], { env, encoding: "utf8" });
  assert.equal(invoke(["--help"], { PATH: "" }).status, 0);
  const missingTool = invoke([script], { PATH: "" });
  assert.equal(missingTool.status, 1);
  assert.match(missingTool.stderr, /ENOENT/);
  const invalidElf = invoke([script]);
  assert.equal(invalidElf.status, 1);
  assert.match(invalidElf.stderr, /Linux ABI verification failed/);
});

test("both Linux build runners stay on the documented baseline with artifact verification", () => {
  const workflow = readFileSync(new URL("../.github/workflows/npm-native-artifacts.yml", import.meta.url), "utf8");
  assert.match(workflow, /target: linux-x64-gnu\s+support: release baseline\s+runner: ubuntu-22\.04\s/);
  assert.match(workflow, /target: linux-arm64-gnu\s+support: release baseline\s+runner: ubuntu-22\.04-arm\s/);
  assert.match(workflow, /node --test scripts\/nexus-linux-abi\.test\.mjs/);
  assert.ok(workflow.includes('test "$(getconf GNU_LIBC_VERSION)" = "glibc 2.35"'));
  assert.match(workflow, /if: startsWith\(matrix\.target, 'linux-'\)[\s\S]*?node scripts\/nexus-linux-abi\.mjs packages\/nexus-cli\/native\//);
  assert.ok(workflow.indexOf("name: Verify packaged Linux glibc baseline") < workflow.indexOf("uses: actions/upload-artifact"));
});
