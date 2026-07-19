import { describe, expect, it } from "vitest";

import type { HookMessage } from "@shared/types";
import { createBeforeSendAdapter } from "./eventAdapter";
import {
  HookEngine,
  HookRunnerFailure,
  type HookPipelineObserver,
  type HookRunner,
} from "./engine";
import type { HookManifest, HookRegistrySnapshot } from "./types";

function message(): HookMessage {
  return {
    sender: { agentId: "a_paul", name: "paul" },
    target: { verb: "post", thread: "release" },
    body: "original",
    mention: [],
    metadata: { policy: { original: true } },
  };
}

function manifest(id: string, order: number): HookManifest {
  return {
    version: 1,
    id,
    event: "before_send",
    order,
    timeoutMs: 1000,
    onFailure: "reject",
    enabled: true,
    manifestPath: `/hooks/${id}.toml`,
    handler: {
      kind: "local",
      entry: `/hooks/${id}.py`,
      entrypoint: "main",
      run: ["python3", `/hooks/${id}.py`],
      passEnv: [],
    },
  };
}

function snapshot(hooks: HookManifest[]): HookRegistrySnapshot {
  return { generation: "sha256:generation", hooks, createdAt: 1 };
}

describe("generic hook engine", () => {
  it("passes each transformed message to the next ordered handler", async () => {
    const seen: Array<{ hook: string; body: string; metadata: unknown }> = [];
    const runner: HookRunner = {
      async run(hook, invocation) {
        seen.push({
          hook: hook.id,
          body: invocation.message.body,
          metadata: invocation.message.metadata,
        });
        return hook.id === "first"
          ? {
              action: "continue",
              message: { body: "from-first", mention: ["fable"] },
              metadata: { policy: { first: true }, tags: ["first"], explicit: null },
            }
          : {
              action: "continue",
              message: { summary: "from-second" },
              metadata: { policy: { second: true }, tags: ["second"] },
              timing: "after_tool_loop",
            };
      },
    };
    const engine = new HookEngine({
      snapshot: () => snapshot([manifest("first", 10), manifest("second", 20)]),
      runner,
      invocationId: (_generation, hook, index) => `hi_${index}_${hook.id}`,
    });

    const result = await engine.run(createBeforeSendAdapter(), message());

    expect(seen).toEqual([
      { hook: "first", body: "original", metadata: { policy: { original: true } } },
      {
        hook: "second",
        body: "from-first",
        metadata: {
          policy: { original: true, first: true },
          tags: ["first"],
          explicit: null,
        },
      },
    ]);
    expect(result).toMatchObject({
      action: "continue",
      timing: "after_tool_loop",
      message: {
        body: "from-first",
        summary: "from-second",
        mention: ["fable"],
        metadata: {
          policy: { original: true, first: true, second: true },
          tags: ["second"],
          explicit: null,
        },
      },
    });
  });

  it("passes through unchanged when no hooks match", async () => {
    const runner: HookRunner = {
      run: async () => {
        throw new Error("must not run");
      },
    };
    const engine = new HookEngine({ snapshot: () => snapshot([]), runner });
    await expect(engine.run(createBeforeSendAdapter(), message())).resolves.toEqual({
      action: "continue",
      message: message(),
      executedBy: [],
    });
  });

  it("stops the pipeline after a rejection", async () => {
    const called: string[] = [];
    const runner: HookRunner = {
      async run(hook) {
        called.push(hook.id);
        return { action: "reject", metadata: { policy: "blocked" } };
      },
    };
    const engine = new HookEngine({
      snapshot: () => snapshot([manifest("reject", 10), manifest("never", 20)]),
      runner,
    });

    const result = await engine.run(createBeforeSendAdapter(), message());
    expect(result.action).toBe("reject");
    expect(called).toEqual(["reject"]);
  });

  it("attaches signed provenance when a continued handler fails", async () => {
    const failedExecution = {
      hookId: "continued",
      entrypoint: "main",
      runtime: "node",
      artifactDigest: "sha256:failed",
      invocationId: "hi_failed",
      outcome: "continued_failure",
      attestation: {
        algorithm: "ed25519",
        keyId: "sha256:key",
        startedAt: 10,
        completedAt: 11,
        signature: "signed",
      },
    };
    const continued = { ...manifest("continued", 10), onFailure: "continue" as const };
    const engine = new HookEngine({
      snapshot: () => snapshot([continued]),
      runner: {
        run: async () => {
          throw new HookRunnerFailure("continued hook failed", failedExecution);
        },
      },
      invocationId: () => "hi_failed",
    });

    await expect(engine.run(createBeforeSendAdapter(), message())).resolves.toMatchObject({
      action: "continue",
      executedBy: [failedExecution],
    });
  });

  it("deduplicates concurrent evaluations and reuses a durably completed result", async () => {
    let calls = 0;
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    let completed: unknown;
    const observer: HookPipelineObserver = {
      begin: async () => completed === undefined ? undefined : { completedOutput: completed },
      complete: async (_context, _completedAt, output) => { completed = output; },
      fail: async () => undefined,
    };
    const engine = new HookEngine({
      snapshot: () => snapshot([manifest("once", 10)]),
      runner: {
        async run() {
          calls += 1;
          await gate;
          return { metadata: { once: true } };
        },
      },
      observer,
    });
    const context = { evaluationId: "he_once", messageId: "m_once" };
    const first = engine.run(createBeforeSendAdapter(), message(), context);
    const concurrent = engine.run(createBeforeSendAdapter(), message(), context);
    release?.();

    const [firstResult, concurrentResult] = await Promise.all([first, concurrent]);
    const replayed = await engine.run(createBeforeSendAdapter(), message(), context);
    expect(firstResult).toEqual(concurrentResult);
    expect(replayed).toEqual(firstResult);
    expect(calls).toBe(1);
  });

  it("fails closed instead of applying a newer generation to an unfinished evaluation", async () => {
    const runner = { run: async () => ({ metadata: { mustNotRun: true } }) };
    const engine = new HookEngine({
      snapshot: () => snapshot([manifest("new-generation", 10)]),
      runner,
      observer: {
        begin: async () => ({ generation: "sha256:previous-generation" }),
        complete: async () => undefined,
        fail: async () => undefined,
      },
    });

    await expect(engine.run(
      createBeforeSendAdapter(),
      message(),
      { evaluationId: "he_pinned", messageId: "m_pinned" },
    )).rejects.toThrow(/pinned hook generation is unavailable/i);
  });

  it.each([
    ["target mutation", { action: "continue", message: { target: { verb: "reply" } } }],
    ["sender mutation", { action: "continue", message: { sender: { name: "other" } } }],
    ["reserved metadata", { action: "continue", metadata: { _nexus: { forged: true } } }],
    ["unknown field", { action: "continue", extra: true }],
  ])("rejects %s", async (_label, output) => {
    const engine = new HookEngine({
      snapshot: () => snapshot([manifest("invalid", 10)]),
      runner: { run: async () => output },
    });
    await expect(engine.run(createBeforeSendAdapter(), message())).rejects.toThrow(
      /not allowed|reserved|unknown/,
    );
  });
});
