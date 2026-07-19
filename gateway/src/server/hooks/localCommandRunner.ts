import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { dirname } from "node:path";

import type { HookInvocation } from "./eventAdapter";
import type { HookRunner } from "./engine";
import { isPlainObject } from "./merge";
import type { HookManifest } from "./types";

const DEFAULT_MAX_STDOUT_BYTES = 4 * 1024 * 1024;
const DEFAULT_MAX_STDERR_BYTES = 64 * 1024;
const DEFAULT_MAX_CONCURRENT = 4;
const DEFAULT_KILL_GRACE_MS = 100;
const BASE_ENVIRONMENT = [
  "PATH",
  "HOME",
  "LANG",
  "LANGUAGE",
  "LC_ALL",
  "LC_CTYPE",
] as const;
const WINDOWS_ENVIRONMENT = ["SystemRoot", "SYSTEMROOT", "ComSpec", "COMSPEC", "PATHEXT"] as const;

export type HookCommandErrorKind =
  | "spawn"
  | "timeout"
  | "stdout_limit"
  | "stderr_limit"
  | "non_zero_exit"
  | "invalid_output";

export class HookCommandError extends Error {
  readonly kind: HookCommandErrorKind;
  readonly exitCode?: number;
  readonly signal?: NodeJS.Signals;
  readonly stderr?: string;

  constructor(
    kind: HookCommandErrorKind,
    message: string,
    details: { exitCode?: number; signal?: NodeJS.Signals; stderr?: string } = {},
  ) {
    super(message);
    this.name = "HookCommandError";
    this.kind = kind;
    this.exitCode = details.exitCode;
    this.signal = details.signal;
    this.stderr = details.stderr;
  }
}

export interface LocalCommandRunnerOptions {
  environment?: NodeJS.ProcessEnv;
  maxStdoutBytes?: number;
  maxStderrBytes?: number;
  maxConcurrent?: number;
  killGraceMs?: number;
  platform?: NodeJS.Platform;
}

export class LocalCommandRunner implements HookRunner {
  readonly #environment: NodeJS.ProcessEnv;
  readonly #maxStdoutBytes: number;
  readonly #maxStderrBytes: number;
  readonly #killGraceMs: number;
  readonly #platform: NodeJS.Platform;
  readonly #semaphore: Semaphore;

  constructor(options: LocalCommandRunnerOptions = {}) {
    this.#environment = options.environment ?? process.env;
    this.#maxStdoutBytes = positiveInteger(
      options.maxStdoutBytes ?? DEFAULT_MAX_STDOUT_BYTES,
      "maxStdoutBytes",
    );
    this.#maxStderrBytes = positiveInteger(
      options.maxStderrBytes ?? DEFAULT_MAX_STDERR_BYTES,
      "maxStderrBytes",
    );
    this.#killGraceMs = nonNegativeInteger(
      options.killGraceMs ?? DEFAULT_KILL_GRACE_MS,
      "killGraceMs",
    );
    this.#platform = options.platform ?? process.platform;
    this.#semaphore = new Semaphore(
      positiveInteger(options.maxConcurrent ?? DEFAULT_MAX_CONCURRENT, "maxConcurrent"),
    );
  }

  async run(hook: HookManifest, invocation: HookInvocation): Promise<unknown> {
    const release = await this.#semaphore.acquire();
    try {
      return await this.#execute(hook, invocation);
    } finally {
      release();
    }
  }

  async #execute(hook: HookManifest, invocation: HookInvocation): Promise<unknown> {
    const [command, ...arguments_] = hook.handler.run;
    if (!command) throw new HookCommandError("spawn", `hook ${hook.id} has no command`);
    const serialized = JSON.stringify(invocation);

    return await new Promise((resolve, reject) => {
      let child: ChildProcessWithoutNullStreams;
      try {
        child = spawn(command, arguments_, {
          cwd: dirname(hook.manifestPath),
          detached: this.#platform !== "win32",
          env: hookEnvironment(this.#environment, hook, invocation, this.#platform),
          shell: false,
          stdio: ["pipe", "pipe", "pipe"],
          windowsHide: true,
        });
      } catch (error) {
        reject(spawnError(hook, error));
        return;
      }

      const stdout: Buffer[] = [];
      const stderr: Buffer[] = [];
      let stdoutBytes = 0;
      let stderrBytes = 0;
      let terminalError: HookCommandError | undefined;
      let settled = false;
      let hardKill: NodeJS.Timeout | undefined;

      const terminate = (error: HookCommandError): void => {
        if (terminalError) return;
        terminalError = error;
        signalProcessTree(child, "SIGTERM", this.#platform);
        hardKill = setTimeout(
          () => signalProcessTree(child, "SIGKILL", this.#platform),
          this.#killGraceMs,
        );
        hardKill.unref();
      };

      const timeout = setTimeout(() => {
        terminate(
          new HookCommandError(
            "timeout",
            `hook ${hook.id} exceeded its ${hook.timeoutMs}ms timeout`,
          ),
        );
      }, hook.timeoutMs);
      timeout.unref();

      child.stdout.on("data", (chunk: Buffer) => {
        const nextBytes = stdoutBytes + chunk.length;
        appendWithinLimit(stdout, chunk, stdoutBytes, this.#maxStdoutBytes);
        stdoutBytes = nextBytes;
        if (stdoutBytes > this.#maxStdoutBytes) {
          terminate(
            new HookCommandError(
              "stdout_limit",
              `hook ${hook.id} stdout exceeded ${this.#maxStdoutBytes} bytes`,
            ),
          );
        }
      });
      child.stderr.on("data", (chunk: Buffer) => {
        const nextBytes = stderrBytes + chunk.length;
        appendWithinLimit(stderr, chunk, stderrBytes, this.#maxStderrBytes);
        stderrBytes = nextBytes;
        if (stderrBytes > this.#maxStderrBytes) {
          terminate(
            new HookCommandError(
              "stderr_limit",
              `hook ${hook.id} stderr exceeded ${this.#maxStderrBytes} bytes`,
              { stderr: Buffer.concat(stderr).toString("utf8") },
            ),
          );
        }
      });
      child.stdin.on("error", () => undefined);
      child.on("error", (error) => {
        if (!terminalError) terminalError = spawnError(hook, error);
      });
      child.on("close", (exitCode, signal) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        if (hardKill) clearTimeout(hardKill);
        if (terminalError) {
          reject(terminalError);
          return;
        }
        const stderrText = Buffer.concat(stderr).toString("utf8");
        if (exitCode !== 0) {
          reject(
            new HookCommandError(
              "non_zero_exit",
              `hook ${hook.id} exited with ${exitCode ?? signal ?? "unknown status"}`,
              {
                ...(exitCode !== null ? { exitCode } : {}),
                ...(signal ? { signal } : {}),
                ...(stderrText ? { stderr: stderrText } : {}),
              },
            ),
          );
          return;
        }
        try {
          const result: unknown = JSON.parse(Buffer.concat(stdout).toString("utf8"));
          if (!isPlainObject(result)) throw new Error("result must be a JSON object");
          resolve(result);
        } catch (error) {
          reject(
            new HookCommandError(
              "invalid_output",
              `hook ${hook.id} did not emit exactly one JSON object: ${errorMessage(error)}`,
              { ...(stderrText ? { stderr: stderrText } : {}) },
            ),
          );
        }
      });

      child.stdin.end(serialized);
    });
  }
}

class Semaphore {
  readonly #limit: number;
  #active = 0;
  readonly #waiters: Array<() => void> = [];

  constructor(limit: number) {
    this.#limit = limit;
  }

  async acquire(): Promise<() => void> {
    if (this.#active >= this.#limit) {
      await new Promise<void>((resolve) => this.#waiters.push(resolve));
    }
    this.#active += 1;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.#active -= 1;
      this.#waiters.shift()?.();
    };
  }
}

function hookEnvironment(
  source: NodeJS.ProcessEnv,
  hook: HookManifest,
  invocation: HookInvocation,
  platform: NodeJS.Platform,
): NodeJS.ProcessEnv {
  const environment: NodeJS.ProcessEnv = {};
  for (const name of BASE_ENVIRONMENT) copyEnvironment(source, environment, name);
  if (platform === "win32") {
    for (const name of WINDOWS_ENVIRONMENT) copyEnvironment(source, environment, name);
  }
  for (const name of hook.handler.passEnv) copyEnvironment(source, environment, name);
  environment.NEXUS_HOOK_PROTOCOL = invocation.protocol;
  environment.NEXUS_HOOK_ID = hook.id;
  environment.NEXUS_HOOK_EVENT = invocation.event;
  environment.NEXUS_HOOK_INVOCATION_ID = invocation.invocationId;
  environment.NEXUS_HOOK_ENTRYPOINT = hook.handler.entrypoint;
  environment.NEXUS_HOOK_MANIFEST = hook.manifestPath;
  return environment;
}

function copyEnvironment(
  source: NodeJS.ProcessEnv,
  target: NodeJS.ProcessEnv,
  name: string,
): void {
  const value = source[name];
  if (value !== undefined) target[name] = value;
}

function appendWithinLimit(
  chunks: Buffer[],
  chunk: Buffer,
  currentBytes: number,
  limit: number,
): void {
  const remaining = limit - currentBytes;
  if (remaining <= 0) return;
  chunks.push(chunk.subarray(0, remaining));
}

function signalProcessTree(
  child: ChildProcessWithoutNullStreams,
  signal: NodeJS.Signals,
  platform: NodeJS.Platform,
): void {
  const pid = child.pid;
  if (!pid) return;
  if (platform === "win32") {
    try {
      spawn("taskkill", ["/PID", String(pid), "/T", "/F"], {
        detached: false,
        stdio: "ignore",
        windowsHide: true,
      }).unref();
    } catch {
      child.kill(signal);
    }
    return;
  }
  try {
    process.kill(-pid, signal);
  } catch {
    try {
      child.kill(signal);
    } catch {
      // The child already exited between detection and termination.
    }
  }
}

function spawnError(hook: HookManifest, error: unknown): HookCommandError {
  return new HookCommandError("spawn", `hook ${hook.id} could not start: ${errorMessage(error)}`);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function positiveInteger(value: number, field: string): number {
  if (!Number.isSafeInteger(value) || value < 1) throw new Error(`${field} must be positive`);
  return value;
}

function nonNegativeInteger(value: number, field: string): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`${field} must not be negative`);
  return value;
}
