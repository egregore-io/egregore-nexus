import { beforeEach, describe, expect, it } from "vitest";
import { useIdentityStore } from "./identity";

describe("identity store", () => {
  beforeEach(() => useIdentityStore.setState({ name: null }));
  it("sets and forgets the operator name", () => {
    useIdentityStore.getState().setName("etan");
    expect(useIdentityStore.getState().name).toBe("etan");
    useIdentityStore.getState().forget();
    expect(useIdentityStore.getState().name).toBeNull();
  });
});
