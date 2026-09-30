import { access, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { afterEach, describe, expect, it } from "vitest";

import { HOOK_PROTOCOL, type HookInvocation } from "./eventAdapter";
import {
  HookCommandError,
  LocalCommandRunner,
} from "./localCommandRunner";
import type { HookManifest } from "./types";

const FIXTURES = join(dirname(fileURLToPath(import.meta.url)), "test-fixtures");
const PYTHON_COMMAND = process.platform === "win32" ? "python.exe" : "python3";
const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(temporaryDirectories.splice(0).map((path) => rm(path, { recursive: true })));
});

function invocation(id = "hi_deterministic"): HookInvocation {
  return {
    protocol: HOOK_PROTOCOL,
    invocationId: id,
    event: "before_send",
    handler: {
      hookId: "test-hook",
      entrypoint: "main",
      runtime: "test",
    },
    executedBy: [],
    message: {
      sender: { agentId: "a_fixture_sender", name: "fixture-sender" },
      target: { verb: "post", thread: "release" },
      body: "exact input",
      mention: [],
      metadata: {},
    },
  };
}

function manifest(run: string[], options: Partial<HookManifest> = {}): HookManifest {
  return {
    version: 1,
    id: "test-hook",
    event: "before_send",
    order: 0,
    timeoutMs: 1_000,
    onFailure: "reject",
    enabled: true,
    manifestPath: join(FIXTURES, "test-hook.toml"),
    handler: {
      kind: "local",
      entry: run.at(-1) ?? process.execPath,
      entrypoint: "main",
      run,
      passEnv: [],
    },
    ...options,
  };
}

function nodeHook(source: string, options: Partial<HookManifest> = {}): HookManifest {
  return manifest([process.execPath, "-e", source], options);
}

describe("local hook command runner", () => {
  it("passes exact versioned JSON and only the allowlisted environment", async () => {
    const entry = join(FIXTURES, "echo-hook.mjs");
    const hook = manifest([process.execPath, entry]);
    hook.handler.passEnv = ["HOOK_ALLOWED"];
    const runner = new LocalCommandRunner({
      environment: {
        PATH: process.env.PATH,
        HOME: process.env.HOME,
        LANG: "C.UTF-8",
        ...(process.platform === "win32" ? { SystemRoot: process.env.SystemRoot } : {}),
        HOOK_ALLOWED: "yes",
        HOOK_SECRET: "must-not-leak",
      },
    });

    const result = await runner.run(hook, invocation());

    expect(result).toEqual({
      action: "continue",
      metadata: {
        runtime: "javascript",
        invocationId: "hi_deterministic",
        body: "exact input",
        environment: expect.arrayContaining([
          "HOME",
          "HOOK_ALLOWED",
          "LANG",
          "NEXUS_HOOK_ENTRYPOINT",
          "NEXUS_HOOK_EVENT",
          "NEXUS_HOOK_ID",
          "NEXUS_HOOK_INVOCATION_ID",
          "PATH",
        ]),
        allowedValue: "yes",
      },
    });
    expect((result as { metadata: { environment: string[] } }).metadata.environment).not.toContain(
      "HOOK_SECRET",
    );
  });

  it.each([
    ["javascript", [process.execPath, join(FIXTURES, "echo-hook.mjs")]],
    ["python", [PYTHON_COMMAND, join(FIXTURES, "echo-hook.py")]],
    ["shell", ["sh", join(FIXTURES, "echo-hook.sh")]],
  ])("executes %s hooks through the same JSON boundary", async (runtime, run) => {
    const result = await new LocalCommandRunner({
      environment: { PATH: process.env.PATH, HOME: process.env.HOME, HOOK_ALLOWED: "yes",
        ...(process.platform === "win32" ? { SystemRoot: process.env.SystemRoot } : {}) },
    }).run(manifest(run), invocation());

    expect(result).toMatchObject({ action: "continue", metadata: { runtime } });
  });

  it("passes argv literally without shell interpolation", async () => {
    const marker = join(await temporaryDirectory(), "must-not-exist");
    const literal = `$(touch ${marker})`;
    const hook = manifest([
      process.execPath,
      "-e",
      "process.stdin.resume(); process.stdin.on('end', () => process.stdout.write(JSON.stringify({argument: process.argv[1]})))",
      literal,
    ]);

    await expect(new LocalCommandRunner().run(hook, invocation())).resolves.toEqual({
      argument: literal,
    });
    await expect(access(marker)).rejects.toThrow();
  });

  it("reports non-zero exits with bounded stderr", async () => {
    const hook = nodeHook(
      "process.stdin.resume(); process.stdin.on('end', () => { process.stderr.write('intentional failure'); process.exit(7); })",
    );

    await expect(new LocalCommandRunner().run(hook, invocation())).rejects.toMatchObject({
      name: "HookCommandError",
      kind: "non_zero_exit",
      exitCode: 7,
      stderr: "intentional failure",
    });
  });

  it("rejects malformed JSON and non-object JSON", async () => {
    const malformed = nodeHook(
      "process.stdin.resume(); process.stdin.on('end', () => process.stdout.write('{not json'))",
    );
    const array = nodeHook(
      "process.stdin.resume(); process.stdin.on('end', () => process.stdout.write('[]'))",
    );
    const runner = new LocalCommandRunner();

    await expect(runner.run(malformed, invocation())).rejects.toMatchObject({
      kind: "invalid_output",
    });
    await expect(runner.run(array, invocation())).rejects.toMatchObject({
      kind: "invalid_output",
    });
  });

  it.each([
    ["stdout", "process.stdout.write('x'.repeat(1024))"],
    ["stderr", "process.stderr.write('x'.repeat(1024))"],
  ])("terminates hooks that exceed the %s byte limit", async (stream, write) => {
    const hook = nodeHook(`process.stdin.resume(); process.stdin.on('end', () => { ${write}; setInterval(() => {}, 1000); })`);
    const runner = new LocalCommandRunner({ maxStdoutBytes: 64, maxStderrBytes: 64 });

    await expect(runner.run(hook, invocation())).rejects.toMatchObject({
      kind: `${stream}_limit`,
    });
  });

  it("times out and terminates the process group", async () => {
    const directory = await temporaryDirectory();
    const pidPath = join(directory, "child.pid");
    const hook = nodeHook(
      [
        "const {spawn}=require('node:child_process');",
        "const {writeFileSync}=require('node:fs');",
        "process.stdin.resume();",
        `const child=spawn(process.execPath,['-e','setInterval(() => {}, 1000)'],{stdio:'ignore'}); writeFileSync(${JSON.stringify(pidPath)}, String(child.pid));`,
        "setInterval(() => {}, 1000);",
      ].join(" "),
      { timeoutMs: 80 },
    );

    await expect(new LocalCommandRunner({ killGraceMs: 20 }).run(hook, invocation())).rejects.toMatchObject({
      kind: "timeout",
    });
    if (process.platform !== "win32") {
      const pid = Number.parseInt(await readFile(pidPath, "utf8"), 10);
      await expect(waitForProcessExit(pid, 1_000)).resolves.toBe(true);
    }
  });

  it("bounds concurrent child processes", async () => {
    const hook = nodeHook(
      "let input=''; process.stdin.on('data', chunk => input += chunk); process.stdin.on('end', () => { const start=Date.now(); setTimeout(() => process.stdout.write(JSON.stringify({start,finish:Date.now(),id:JSON.parse(input).invocationId})), 80); })",
    );
    const runner = new LocalCommandRunner({ maxConcurrent: 1 });

    const [first, second] = (await Promise.all([
      runner.run(hook, invocation("hi_first")),
      runner.run(hook, invocation("hi_second")),
    ])) as Array<{ start: number; finish: number; id: string }>;
    if (!first || !second) throw new Error("expected two hook results");

    expect(first.id).toBe("hi_first");
    expect(second.id).toBe("hi_second");
    expect(second.start).toBeGreaterThanOrEqual(first.finish);
  });

  it("uses a stable error type for spawn failures", async () => {
    const hook = manifest(["definitely-not-a-hook-command"]);
    await expect(new LocalCommandRunner().run(hook, invocation())).rejects.toBeInstanceOf(
      HookCommandError,
    );
  });
});

async function temporaryDirectory(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "nexus-hook-runner-"));
  temporaryDirectories.push(directory);
  return directory;
}

async function waitForProcessExit(pid: number, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!processExists(pid)) return true;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  return !processExists(pid);
}

function processExists(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}
