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

  it.each(["project", "role"])(
    "rejects the Lens-owned affordance field %s",
    (field) => {
      expect(spawnSchema.safeParse({ kind: "codex", [field]: "review" }).success)
        .toBe(false);
    },
  );
});
