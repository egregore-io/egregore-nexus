import { describe, expect, it, vi } from "vitest";

import { removeTempPath } from "./removeTempPath";

describe("removeTempPath", () => {
  it("removes an unlocked fixture immediately", async () => {
    const remove = vi.fn(async () => undefined);
    const defer = vi.fn();

    await removeTempPath("fixture", { recursive: true }, {
      platform: "win32",
      remove,
      defer,
    });

    expect(remove).toHaveBeenCalledWith("fixture", { recursive: true, force: true });
    expect(defer).not.toHaveBeenCalled();
  });

  it("defers Windows cleanup when the native SQLite handle outlives the test", async () => {
    const error = Object.assign(new Error("busy"), { code: "EBUSY" });
    const remove = vi.fn(async () => Promise.reject(error));
    const defer = vi.fn();

    await removeTempPath("fixture", {}, {
      platform: "win32",
      remove,
      defer,
    });

    expect(defer).toHaveBeenCalledWith("fixture", false);
  });

  it("does not hide cleanup errors on non-Windows hosts", async () => {
    const error = Object.assign(new Error("busy"), { code: "EBUSY" });

    await expect(removeTempPath("fixture", {}, {
      platform: "linux",
      remove: async () => Promise.reject(error),
      defer: vi.fn(),
    })).rejects.toBe(error);
  });
});
