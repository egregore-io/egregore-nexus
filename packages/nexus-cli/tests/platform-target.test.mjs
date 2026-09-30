import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";

import { platformTarget } from "../lib/platform-target.mjs";

const environments = [
  ["Linux x64", { platform: "linux", arch: "x64", glibcVersionRuntime: "2.39" }, "linux-x64-gnu"],
  ["Linux arm64", { platform: "linux", arch: "arm64", glibcVersionRuntime: "2.39" }, "linux-arm64-gnu"],
  ["macOS x64", { platform: "darwin", arch: "x64" }, "darwin-x64"],
  ["macOS arm64", { platform: "darwin", arch: "arm64" }, "darwin-arm64"],
  ["Windows x64", { platform: "win32", arch: "x64" }, "win32-x64-msvc"],
  ["WSL2 x64", { platform: "linux", arch: "x64", glibcVersionRuntime: "2.35" }, "linux-x64-gnu"],
];

for (const [name, environment, expected] of environments) {
  test(`${name} selects ${expected}`, () => {
    assert.equal(platformTarget(environment), expected);
  });
}

test("unsupported libc and architectures fail closed", () => {
  assert.equal(platformTarget({ platform: "linux", arch: "x64" }), undefined);
  assert.equal(platformTarget({ platform: "win32", arch: "arm64" }), undefined);
});

test("beta npm manifests explicitly limit installation to Linux and Windows", () => {
  for (const path of ["../package.json", "../../nexus/package.json"]) {
    const manifest = JSON.parse(readFileSync(new URL(path, import.meta.url), "utf8"));
    assert.deepEqual(manifest.os, ["linux", "win32"]);
  }
});
