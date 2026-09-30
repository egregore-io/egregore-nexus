import { spawn } from "node:child_process";
import { rm } from "node:fs/promises";

interface RemoveOptions {
  recursive?: boolean;
}

interface RemoveTempPathDeps {
  platform?: NodeJS.Platform;
  remove?: (
    path: string,
    options: { recursive?: boolean; force: boolean },
  ) => Promise<void>;
  defer?: (path: string, recursive: boolean) => void;
}

const DEFERRED_REMOVE_SCRIPT = String.raw`
const { rmSync } = require("node:fs");
const target = process.argv[1];
const recursive = process.argv[2] === "1";
const deadline = Date.now() + 120000;
function remove() {
  try {
    rmSync(target, { recursive, force: true, maxRetries: 3, retryDelay: 100 });
  } catch (error) {
    if (Date.now() < deadline && ["EBUSY", "EPERM", "ENOTEMPTY"].includes(error?.code)) {
      setTimeout(remove, 250);
      return;
    }
    process.exitCode = 1;
  }
}
remove();
`;

function deferRemove(path: string, recursive: boolean): void {
  const child = spawn(
    process.execPath,
    ["-e", DEFERRED_REMOVE_SCRIPT, path, recursive ? "1" : "0"],
    {
      detached: true,
      stdio: "ignore",
      windowsHide: true,
    },
  );
  child.unref();
}

/**
 * Remove a test fixture without treating libSQL's Windows close/GC boundary as
 * an application failure. The detached cleanup retries after the Vitest worker
 * exits, when native transaction handles are guaranteed to be released.
 */
export async function removeTempPath(
  path: string,
  options: RemoveOptions = {},
  deps: RemoveTempPathDeps = {},
): Promise<void> {
  const remove = deps.remove ?? ((target, removeOptions) => rm(target, removeOptions));
  const recursive = options.recursive === true;
  try {
    await remove(path, { recursive, force: true });
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    const platform = deps.platform ?? process.platform;
    if (platform !== "win32" || !["EBUSY", "EPERM", "ENOTEMPTY"].includes(code ?? "")) {
      throw error;
    }
    (deps.defer ?? deferRemove)(path, recursive);
  }
}
