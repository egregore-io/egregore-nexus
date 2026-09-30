import { win32 } from "node:path";
import { spawn } from "node:child_process";
import { WINDOWS_AUTHORITY_SCRIPT } from "./windowsAuthorityScript";

const SID = /^S-1-(?:\d+-)+\d+$/;
const MUTATION = 0x10000000 | 0x40000000 | 0x000d0156;

export interface WindowsCaptureRequest { path: string; directory: boolean; includeBytes?: boolean }
export interface WindowsCapture {
  path: string;
  directory: boolean;
  identity: string;
  size: number;
  sha256: string | null;
  bytes: string | null;
  security: { owner: string; digest: string; daclPresent: boolean; aces: unknown[] };
}

/** One bounded native invocation for the entire requested chain, without cached ACL decisions. */
export async function captureWindowsAuthority(requests: WindowsCaptureRequest[]): Promise<WindowsCapture[]> {
  if (process.platform !== "win32") throw new Error("native Windows authority is unavailable on this platform");
  if (requests.length === 0 || requests.length > 128) throw new Error("invalid Windows authority request count");
  for (const request of requests) {
    if (!/^[A-Za-z]:\\/.test(request.path) || /[\x00-\x1f]/.test(request.path)
      || request.path.slice(2).includes(":")) throw new Error("invalid Windows authority path");
  }
  const systemRoot = process.env.SystemRoot ?? process.env.SYSTEMROOT ?? "C:\\Windows";
  if (!/^[A-Za-z]:\\/.test(systemRoot)) throw new Error("invalid Windows system directory");
  const command = win32.join(systemRoot, "System32", "WindowsPowerShell", "v1.0", "powershell.exe");
  const output = await new Promise<string>((resolve, reject) => {
    const child = spawn(command, ["-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand",
      Buffer.from(WINDOWS_AUTHORITY_SCRIPT, "utf16le").toString("base64")], {
      shell: false, windowsHide: true, stdio: ["pipe", "pipe", "pipe"],
      env: { SystemRoot: systemRoot },
    });
    let stdout = "", stderr = "", failure: Error | undefined;
    const fail = (error: Error) => { failure ??= error; child.kill(); };
    const timer = setTimeout(() => fail(new Error("Windows authority verifier timed out")), 15_000);
    child.stdout.setEncoding("utf8"); child.stderr.setEncoding("utf8");
    child.stdout.on("data", (data: string) => {
      stdout += data;
      if (Buffer.byteLength(stdout) > 2 * 1024 * 1024) fail(new Error("Windows authority response too large"));
    });
    child.stderr.on("data", (data: string) => {
      stderr += data;
      if (Buffer.byteLength(stderr) > 65536) fail(new Error("Windows authority diagnostic too large"));
    });
    child.on("error", (error) => { clearTimeout(timer); reject(error); });
    child.stdin.on("error", (error) => fail(error));
    child.on("close", (code) => {
      clearTimeout(timer);
      if (failure) reject(failure);
      else if (code !== 0) reject(new Error(`Windows authority verifier failed (${code}): ${stderr.slice(0, 2048)}`));
      else resolve(stdout);
    });
    child.stdin.end(JSON.stringify(requests));
  });
  return parseWindowsCapture(output, requests);
}

export function parseWindowsCapture(output: string, requests: WindowsCaptureRequest[]): WindowsCapture[] {
  const result = JSON.parse(output) as { user?: unknown; items?: unknown };
  if (!result || typeof result.user !== "string" || !SID.test(result.user)
    || !Array.isArray(result.items) || result.items.length !== requests.length)
    throw new Error("invalid Windows authority response");
  return result.items.map((raw: unknown, index: number) => {
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw new Error("invalid Windows authority item");
    const item = raw as WindowsCapture;
    const request = requests[index]!;
    if (item.path !== request.path || item.directory !== request.directory
      || typeof item.identity !== "string" || !/^[a-f0-9]{8}:[a-f0-9]{16}$/.test(item.identity)
      || !Number.isSafeInteger(item.size) || item.size < 0)
      throw new Error("Windows authority identity mismatch");
    validateWindowsDescriptor(item.security, result.user as string);
    if (typeof item.security.digest !== "string" || !/^[a-f0-9]{64}$/.test(item.security.digest)
      || (!item.directory && (typeof item.sha256 !== "string" || !/^[a-f0-9]{64}$/.test(item.sha256)))
      || (request.includeBytes && typeof item.bytes !== "string"))
      throw new Error("invalid Windows authority digest");
    return item;
  });
}

/** Conservative allow-list, not a general Windows effective-access evaluator. */
export function validateWindowsDescriptor(value: unknown, user: string): void {
  if (!value || typeof value !== "object" || Array.isArray(value) || !SID.test(user))
    throw new Error("invalid Windows security descriptor");
  const descriptor = value as Record<string, unknown>;
  if (descriptor.owner !== user) throw new Error("Windows authority has the wrong owner SID");
  if (descriptor.daclPresent !== true || !Array.isArray(descriptor.aces))
    throw new Error("Windows authority requires a present non-null DACL");
  if (descriptor.aces.length > 4096) throw new Error("Windows DACL is too large");
  for (const raw of descriptor.aces) {
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw new Error("invalid Windows ACE");
    const ace = raw as Record<string, unknown>;
    if ((ace.type !== 0 && ace.type !== 1) || typeof ace.sid !== "string" || !SID.test(ace.sid)
      || !Number.isInteger(ace.mask) || (ace.mask as number) < 0 || (ace.mask as number) > 0xffffffff
      || !Number.isInteger(ace.flags) || (ace.flags as number) < 0 || (ace.flags as number) > 255)
      throw new Error("unsupported Windows ACE");
    // INHERIT_ONLY does not grant access to this object. Denies cannot make an unsafe allow safe.
    if (ace.type === 1 || ((ace.flags as number) & 8) !== 0) continue;
    if (((ace.mask as number) & MUTATION) !== 0
      && ![user, "S-1-5-18", "S-1-5-32-544"].includes(ace.sid))
      throw new Error("Windows DACL grants mutation to an untrusted SID");
  }
}

export function windowsEntryCommand(entry: string, args: readonly string[]) {
  const extension = win32.extname(entry).toLowerCase();
  if ([".mjs", ".cjs", ".js"].includes(extension))
    return { command: process.execPath, args: [entry, ...args] };
  if (extension === ".exe") return { command: entry, args: [...args] };
  throw new Error("Windows transport entry must be an .exe or explicit JavaScript file");
}
