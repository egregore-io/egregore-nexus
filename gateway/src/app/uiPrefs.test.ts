import { beforeEach, describe, expect, it } from "vitest";
import { useUiPrefsStore } from "./uiPrefs";

describe("uiPrefs store", () => {
  beforeEach(() => {
    useUiPrefsStore.setState({
      collapsed: { rail: false, ctx: false, search: true },
      accent: "mono",
      backdropDim: 0.66,
      pinnedProjects: [],
    });
  });

  it("toggles a panel collapse flag", () => {
    useUiPrefsStore.getState().togglePanel("rail");
    expect(useUiPrefsStore.getState().collapsed.rail).toBe(true);
    useUiPrefsStore.getState().togglePanel("rail");
    expect(useUiPrefsStore.getState().collapsed.rail).toBe(false);
  });

  it("pins and unpins a project by name (idempotent, order preserved)", () => {
    const s = () => useUiPrefsStore.getState();
    s().togglePin("nexus");
    s().togglePin("lens");
    expect(s().pinnedProjects).toEqual(["nexus", "lens"]);
    expect(s().isPinned("nexus")).toBe(true);
    s().togglePin("nexus");
    expect(s().pinnedProjects).toEqual(["lens"]);
  });

  it("sets appearance", () => {
    useUiPrefsStore.getState().setAccent("green");
    useUiPrefsStore.getState().setBackdropDim(0.85);
    expect(useUiPrefsStore.getState().accent).toBe("green");
    expect(useUiPrefsStore.getState().backdropDim).toBe(0.85);
  });
});
