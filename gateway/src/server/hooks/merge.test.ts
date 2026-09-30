import { describe, expect, it } from "vitest";

import { mergeHookMetadata } from "./merge";

describe("hook metadata merge", () => {
  it("recursively merges objects while replacing arrays and scalars", () => {
    const original = {
      policy: { first: true, shared: "old" },
      tags: ["old"],
      score: 1,
    };

    expect(
      mergeHookMetadata(original, {
        policy: { second: true, shared: "new" },
        tags: ["new"],
        score: 2,
      }),
    ).toEqual({
      policy: { first: true, second: true, shared: "new" },
      tags: ["new"],
      score: 2,
    });
    expect(original).toEqual({
      policy: { first: true, shared: "old" },
      tags: ["old"],
      score: 1,
    });
  });

  it("preserves explicit null as a stored value", () => {
    expect(mergeHookMetadata({ state: "present" }, { state: null })).toEqual({ state: null });
  });

  it("rejects writes to the Gateway-owned _nexus namespace", () => {
    expect(() => mergeHookMetadata({}, { _nexus: { forged: true } })).toThrow(
      "metadata._nexus is reserved",
    );
  });
});
