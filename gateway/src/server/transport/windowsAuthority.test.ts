import { describe, expect, it } from "vitest";
import { parseWindowsCapture, validateWindowsDescriptor, windowsEntryCommand } from "./windowsAuthority";

const user = "S-1-5-21-1-2-3-1001";
const descriptor = () => ({
  owner: user,
  daclPresent: true,
  aces: [{ type: 0, flags: 0, sid: user, mask: 0x1f01ff }],
});

describe("Windows transport authority policy", () => {
  it("accepts only the actual owner and trusted mutation principals", () => {
    expect(() => validateWindowsDescriptor(descriptor(), user)).not.toThrow();
    for (const sid of ["S-1-5-18", "S-1-5-32-544"]) {
      const value = descriptor(); value.aces.push({ type: 0, flags: 0, sid, mask: 0x1f01ff });
      expect(() => validateWindowsDescriptor(value, user)).not.toThrow();
    }
    expect(() => validateWindowsDescriptor(descriptor(), "S-1-5-21-9")).toThrow(/owner/);
  });

  it("rejects unsafe mutation ACEs, including inherited and generic access", () => {
    for (const sid of ["S-1-1-0", "S-1-5-32-545", "S-1-5-21-9"]) {
      for (const mask of [2, 4, 16, 64, 256, 0x10000, 0x40000, 0x80000, 0x40000000, 0x10000000]) {
        const value = descriptor(); value.aces.push({ type: 0, flags: 16, sid, mask });
        expect(() => validateWindowsDescriptor(value, user)).toThrow(/mutation/);
      }
    }
  });

  it("permits foreign read access but fails closed on absent or unsupported DACL data", () => {
    const read = descriptor(); read.aces.push({ type: 0, flags: 0, sid: "S-1-1-0", mask: 0x120089 });
    expect(() => validateWindowsDescriptor(read, user)).not.toThrow();
    expect(() => validateWindowsDescriptor({ ...descriptor(), daclPresent: false }, user)).toThrow(/DACL/);
    const unsupported = descriptor(); unsupported.aces[0]!.type = 9;
    expect(() => validateWindowsDescriptor(unsupported, user)).toThrow(/ACE/);
    expect(() => validateWindowsDescriptor({ ...descriptor(), aces: null }, user)).toThrow();
  });

  it("never uses a shell or file association for Windows transport entries", () => {
    const args = ["a b", "& echo unsafe", "$(touch nope)"];
    expect(windowsEntryCommand("C:\\safe\\bridge.mjs", args)).toEqual({
      command: process.execPath, args: ["C:\\safe\\bridge.mjs", ...args],
    });
    expect(windowsEntryCommand("C:\\safe\\bridge.exe", args)).toEqual({ command: "C:\\safe\\bridge.exe", args });
    for (const entry of ["bridge.cmd", "bridge.bat", "bridge.ps1", "bridge", "bridge.mjs:evil"])
      expect(() => windowsEntryCommand(entry, args)).toThrow(/entry/);
  });

  it("requires exact requested identities and valid native descriptor/hash output", () => {
    const request = { path: "C:\\fixture\\entry.mjs", directory: false, includeBytes: true };
    const item = { ...request, identity: "00000001:0000000000000002", size: 0,
      sha256: "0".repeat(64), bytes: "", security: { ...descriptor(), digest: "1".repeat(64) } };
    const encode = (value: unknown) => JSON.stringify({ user, items: [value] });
    expect(parseWindowsCapture(encode(item), [request])).toHaveLength(1);
    for (const changed of [
      { ...item, path: "C:\\foreign" }, { ...item, directory: true },
      { ...item, identity: "rounded-id" }, { ...item, size: -1 },
      { ...item, sha256: "missing" }, { ...item, bytes: null },
      { ...item, security: { ...item.security, owner: "S-1-5-21-9" } },
    ]) expect(() => parseWindowsCapture(encode(changed), [request])).toThrow();
    for (const output of ["not JSON", "null", JSON.stringify({ user, items: [] })])
      expect(() => parseWindowsCapture(output, [request])).toThrow();
  });
});
