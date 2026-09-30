import { describe, expect, it } from "vitest";

import { spawnSchema } from "./schemas";

describe("spawnSchema harness ids", () => {
  it("accepts an unknown valid harness id", () => {
    expect(spawnSchema.parse({ kind: "acme-agent" })).toEqual({
      kind: "acme-agent",
    });
  });

  it.each(["", "Codex", "1agent", "agent.plugin", `a${"b".repeat(64)}`])(
    "rejects invalid harness id %j",
    (kind) => {
      expect(spawnSchema.safeParse({ kind }).success).toBe(false);
    },
  );

  it.each(["project", "role", "unexpected"])(
    "rejects the unsupported field %s",
    (field) => {
      expect(spawnSchema.safeParse({ kind: "codex", [field]: "review" }).success)
        .toBe(false);
    },
  );
});

describe("spawnSchema launch options", () => {
  it.each(["claude", "codex", "opencode", "hermes"])(
    "keeps omitted launch options absent for %s",
    (kind) => {
      const body = { kind, name: "worker-1", cwd: "/repo" };
      const parsed = spawnSchema.parse(body);

      expect(parsed).toStrictEqual(body);
      expect(parsed).not.toHaveProperty("headless");
      expect(parsed).not.toHaveProperty("initialPrompt");
    },
  );

  it.each([false, true])("preserves explicit headless=%s without adding a prompt", (headless) => {
    const body = { kind: "codex", headless };

    expect(spawnSchema.parse(body)).toStrictEqual(body);
  });

  it.each(["", " \t\n ", "  Review <var.cwd>\nKeep this spacing.\t "])(
    "preserves initialPrompt %j exactly without adding a launch mode",
    (initialPrompt) => {
      const body = { kind: "codex", initialPrompt };

      expect(spawnSchema.parse(body)).toStrictEqual(body);
    },
  );

  it.each(
    ["claude", "codex", "opencode", "hermes"].flatMap((kind) =>
      [false, true].map((headless) => ({ kind, headless })),
    ),
  )("preserves the full $kind launch request with headless=$headless", ({ kind, headless }) => {
    const body = {
      kind,
      name: "worker-1",
      cwd: "/repo",
      headless,
      initialPrompt: "  Review <var.cwd>\nKeep this spacing.\t ",
    };

    expect(spawnSchema.parse(body)).toStrictEqual(body);
  });

  it.each([null, "false", "true", 0, 1, [], {}])("rejects non-boolean headless %j", (headless) => {
    expect(spawnSchema.safeParse({ kind: "codex", headless }).success).toBe(false);
  });

  it.each([null, false, 42, [], {}])("rejects non-string initialPrompt %j", (initialPrompt) => {
    expect(spawnSchema.safeParse({ kind: "codex", initialPrompt }).success).toBe(false);
  });
});
