import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { removeTempPath } from "../../test/removeTempPath";
import { createGatewayStore } from "../store/client";
import type {
  GatewayHookCapabilities,
  HookEvaluationRequest,
  HookEvaluationResponse,
} from "@shared/types";
import { createGatewayHookService, type GatewayHookConnection } from "./service";

describe("Gateway hook service", () => {
  it("starts one signed pipeline, retains the last good generation, and closes cleanly", async () => {
    const root = await mkdtemp(join(tmpdir(), "nexus-hook-service-"));
    const hooks = join(root, "gateway", "hooks.d");
    const db = await createGatewayStore({ url: `file:${join(root, "gateway.db")}` });
    let capabilities: GatewayHookCapabilities | undefined;
    let evaluate: ((request: HookEvaluationRequest) => Promise<HookEvaluationResponse>) | undefined;
    let bridgeClosed = false;
    let connectionClosed = false;
    let registrations = 0;
    const connection: GatewayHookConnection = {
      ready: Promise.resolve(),
      registerHooks(next, handler) {
        registrations += 1;
        capabilities = next;
        evaluate = handler;
        return () => { bridgeClosed = true; };
      },
      close() { connectionClosed = true; },
    };

    try {
      await mkdir(hooks, { recursive: true });
      await writeFile(join(hooks, "redact.mjs"), [
        "let body = '';",
        "for await (const chunk of process.stdin) body += chunk;",
        "const input = JSON.parse(body);",
        "process.stdout.write(JSON.stringify({ metadata: { tested: input.handler.hookId } }));",
      ].join("\n"));
      await writeFile(join(hooks, "redact.toml"), [
        "version = 1",
        "id = \"redact\"",
        "event = \"before_send\"",
        "order = 10",
        "[handler]",
        "kind = \"local\"",
        "entry = \"redact.mjs\"",
        "run = [\"node\", \"{entry}\"]",
      ].join("\n"));

      const service = await createGatewayHookService({
        db,
        nexusHome: root,
        connection,
        now: (() => { let value = 100; return () => ++value; })(),
      });
      expect(capabilities).toMatchObject({ events: ["before_send"] });

      const response = await evaluate?.({
        event: "before_send",
        request: {
          evaluationId: "he_message",
          message: {
            sender: { name: "fixture-sender" },
            target: { verb: "post", thread: "release" },
            body: "hello",
            mention: [],
            metadata: {},
          },
        },
      });
      expect(response?.event).toBe("before_send");
      if (response?.event !== "before_send") throw new Error("missing before_send response");
      expect(response.result.message.metadata).toEqual({ tested: "redact" });
      const executedBy = response.result.executedBy ?? [];
      expect(executedBy).toHaveLength(1);
      expect(executedBy[0]).toMatchObject({
        hookId: "redact",
        invocationId: expect.stringMatching(/^hi_/),
        outcome: "completed",
        attestation: { algorithm: "ed25519" },
      });

      const publicView = await service.list(false);
      expect(JSON.stringify(publicView)).not.toContain(root);
      expect(JSON.stringify(publicView)).not.toContain('"run"');
      expect(JSON.stringify(publicView)).not.toContain('"entrypoint"');
      const adminView = await service.list(true);
      const adminHooks = adminView.hooks as Array<{
        manifestPath: string;
        handler: { run: string[] };
      }>;
      expect(adminHooks[0]?.manifestPath).toBe(join(hooks, "redact.toml"));
      expect(adminHooks[0]?.handler.run).toEqual(["node", join(hooks, "redact.mjs")]);

      const audit = await service.audit(20) as { executions: Array<Record<string, unknown>> };
      expect(audit.executions).toHaveLength(1);
      expect(audit.executions[0]).toMatchObject({ hookId: "redact", verified: true });
      const publicAudit = await service.audit(20, false) as {
        executions: Array<Record<string, unknown>>;
      };
      expect(publicAudit.executions[0]).toMatchObject({
        executedBy: { hookId: "redact" },
        verified: true,
      });
      expect(JSON.stringify(publicAudit)).not.toContain(root);
      expect((await service.publicKey()).publicKey).toContain("BEGIN PUBLIC KEY");

      const generation = service.generation();
      await writeFile(join(hooks, "redact.toml"), "version = 1\nid = false\n");
      const reload = await service.reload();
      expect(reload.activated).toBe(false);
      expect(service.generation()).toBe(generation);
      expect(service.errors()).not.toEqual([]);
      expect(registrations).toBe(1);
      expect(JSON.stringify(await service.list(false))).not.toContain(root);

      await writeFile(join(hooks, "redact.toml"), [
        "version = 1",
        "id = \"redact\"",
        "event = \"before_send\"",
        "order = 11",
        "[handler]",
        "kind = \"local\"",
        "entry = \"redact.mjs\"",
        "run = [\"node\", \"{entry}\"]",
      ].join("\n"));
      expect((await service.reload()).activated).toBe(true);
      expect(service.generation()).not.toBe(generation);
      expect(registrations).toBe(2);

      await service.close();
      expect(bridgeClosed).toBe(true);
      expect(connectionClosed).toBe(false);
    } finally {
      db.close();
      await removeTempPath(root, { recursive: true });
    }
  });

  it("audits an artifact-read failure and continues with signed provenance", async () => {
    const root = await mkdtemp(join(tmpdir(), "nexus-hook-digest-failure-"));
    const hooks = join(root, "gateway", "hooks.d");
    const entry = join(hooks, "gone.mjs");
    const db = await createGatewayStore({ url: `file:${join(root, "gateway.db")}` });
    let evaluate: ((request: HookEvaluationRequest) => Promise<HookEvaluationResponse>) | undefined;
    const connection: GatewayHookConnection = {
      ready: Promise.resolve(),
      registerHooks(_capabilities, handler) {
        evaluate = handler;
        return () => undefined;
      },
      close() {},
    };
    try {
      await mkdir(hooks, { recursive: true });
      await writeFile(entry, "process.stdout.write('{}');\n");
      await writeFile(join(hooks, "gone.toml"), [
        "version = 1",
        "id = \"gone\"",
        "event = \"before_send\"",
        "on_failure = \"continue\"",
        "[handler]",
        "kind = \"local\"",
        "entry = \"gone.mjs\"",
        "run = [\"node\", \"{entry}\"]",
      ].join("\n"));
      const service = await createGatewayHookService({ db, nexusHome: root, connection });
      await rm(entry);

      const response = await evaluate?.({
        event: "before_send",
        request: {
          evaluationId: "he_digest_failure",
          message: {
            sender: { name: "fixture-sender" },
            target: { verb: "post", thread: "release" },
            body: "hello",
            mention: [],
            metadata: {},
          },
        },
      });

      if (response?.event !== "before_send") throw new Error("missing result");
      expect(response.result.executedBy).toMatchObject([
        { hookId: "gone", artifactDigest: "unavailable", outcome: "continued_failure" },
      ]);
      expect((await service.auditStore.executions())[0]).toMatchObject({
        artifactDigest: "unavailable",
        outcome: "continued_failure",
      });
      await service.close();
    } finally {
      db.close();
      await removeTempPath(root, { recursive: true });
    }
  });

  it("resumes after completed handlers and replays the completed pipeline", async () => {
    const root = await mkdtemp(join(tmpdir(), "nexus-hook-resume-"));
    const hooks = join(root, "gateway", "hooks.d");
    const db = await createGatewayStore({ url: `file:${join(root, "gateway.db")}` });
    let evaluate: ((request: HookEvaluationRequest) => Promise<HookEvaluationResponse>) | undefined;
    const calls = new Map<string, number>();
    const connection: GatewayHookConnection = {
      ready: Promise.resolve(),
      registerHooks(_capabilities, handler) {
        evaluate = handler;
        return () => undefined;
      },
      close() {},
    };
    try {
      await mkdir(hooks, { recursive: true });
      for (const [id, order, failure] of [
        ["first", 10, "continue"],
        ["second", 20, "reject"],
      ] as const) {
        await writeFile(join(hooks, `${id}.mjs`), "export default {};\n");
        await writeFile(join(hooks, `${id}.toml`), [
          "version = 1",
          `id = "${id}"`,
          "event = \"before_send\"",
          `order = ${order}`,
          `on_failure = "${failure}"`,
          "[handler]",
          "kind = \"local\"",
          `entry = "${id}.mjs"`,
          "run = [\"node\", \"{entry}\"]",
        ].join("\n"));
      }
      const service = await createGatewayHookService({
        db,
        nexusHome: root,
        connection,
        runner: {
          async run(hook) {
            const count = (calls.get(hook.id) ?? 0) + 1;
            calls.set(hook.id, count);
            if (hook.id === "second" && count === 1) throw new Error("retry me");
            return { metadata: { [hook.id]: count } };
          },
        },
      });
      const request: HookEvaluationRequest = {
        event: "before_send",
        request: {
          evaluationId: "he_resume",
          message: {
            sender: { name: "fixture-sender" },
            target: { verb: "post", thread: "release" },
            body: "hello",
            mention: [],
            metadata: {},
          },
        },
      };

      await expect(evaluate?.(request)).rejects.toThrow("retry me");
      const completed = await evaluate?.(request);
      const replayed = await evaluate?.(request);
      expect(completed).toEqual(replayed);
      expect(calls).toEqual(new Map([["first", 1], ["second", 2]]));
      if (completed?.event !== "before_send") throw new Error("missing result");
      expect(completed.result.message.metadata).toEqual({ first: 1, second: 2 });
      expect(completed.result.executedBy).toHaveLength(2);
      await service.close();
    } finally {
      db.close();
      await removeTempPath(root, { recursive: true });
    }
  });
});
