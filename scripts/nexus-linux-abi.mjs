import { execFileSync } from "node:child_process";
import { pathToFileURL } from "node:url";

// This is an ELF import-version ceiling, not a substitute for execution on the baseline.
export function verifyGlibcRequirements(output) {
  const section = output.match(/Version needs section[^\n]*\n([\s\S]*)/);
  if (!section) throw new Error("Missing ELF version needs section");
  const names = [...section[1].matchAll(/\bName:\s+(GLIBC_\S+)/g)].map((match) => match[1]);
  if (names.length === 0) throw new Error("Missing glibc import requirements");
  const ceiling = [2, 35, 0];
  for (const name of names) {
    const match = /^GLIBC_(\d+)\.(\d+)(?:\.(\d+))?$/.exec(name);
    if (!match) throw new Error(`Unrecognized glibc requirement: ${name}`);
    const parts = match.slice(1).map((part) => Number(part ?? 0));
    if (parts.some((part) => !Number.isSafeInteger(part))) {
      throw new Error(`Invalid glibc requirement: ${name}`);
    }
    for (let i = 0; i < ceiling.length; i++) {
      if (parts[i] > ceiling[i]) throw new Error(`${name} exceeds glibc 2.35 release baseline`);
      if (parts[i] < ceiling[i]) break;
    }
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const args = process.argv.slice(2);
  if (args.length === 1 && ["--help", "-h"].includes(args[0])) {
    console.log("Usage: node scripts/nexus-linux-abi.mjs BINARY\nCheck ELF glibc import requirements against the 2.35 release ceiling. Requires readelf; does not execute the binary.");
  } else {
    try {
      if (args.length !== 1 || args[0].startsWith("-")) throw new Error("Expected one binary path; use --help");
      const output = execFileSync("readelf", ["--wide", "--version-info", "--", args[0]], {
        encoding: "utf8",
        env: { ...process.env, LC_ALL: "C" },
        maxBuffer: 4 * 1024 * 1024,
        timeout: 30_000,
      });
      verifyGlibcRequirements(output);
      console.log(`${args[0]}: glibc requirements <= 2.35`);
    } catch (error) {
      console.error(`Linux ABI verification failed: ${error.message}`);
      process.exitCode = 1;
    }
  }
}
