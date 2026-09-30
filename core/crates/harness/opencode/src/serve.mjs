
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { once } from "node:events";
import { randomBytes } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { delimiter, join } from "node:path";

function resolveOpencodeBin() {
  const override = process.env.NEXUS_OPENCODE_BIN?.trim();
  if (override) return override;
  const platform = { linux: "linux", darwin: "darwin", win32: "windows" }[process.platform];
  const arch = { x64: "x64", arm64: "arm64" }[process.arch];
  if (!platform || !arch) return process.platform === "win32" ? "opencode.exe" : "opencode";
  const binary = platform === "windows" ? "opencode.exe" : "opencode";
  const packageBase = `opencode-${platform}-${arch}`;
  const provider = packageBase.slice(0, packageBase.indexOf("-"));
  const providerPackage = `${provider}-ai`;
  const stagedBinary = `${provider}.exe`;
  const packages = [packageBase, `${packageBase}-baseline`];
  for (const dir of (process.env.PATH ?? "").split(delimiter)) {
    if (!dir) continue;
    const moduleRoots = [join(dir, "node_modules"), join(dir, "..", "lib", "node_modules")];
    const candidates = [join(dir, binary)];
    for (const modules of moduleRoots) {
      candidates.push(join(modules, providerPackage, "bin", stagedBinary));
      for (const packageName of packages) {
        candidates.push(join(modules, packageName, "bin", binary));
        candidates.push(join(modules, "opencode-ai", "node_modules", packageName, "bin", binary));
      }
    }
    for (const exe of candidates) {
      if (existsSync(exe)) return exe;
    }
  }
  return binary;
}

async function freePort() {
  const server = createServer();
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const port = server.address().port;
  await new Promise((resolve) => server.close(resolve));
  return port;
}

async function killServe(child) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  child.kill("SIGTERM");
  const dead = await Promise.race([
    once(child, "exit").then(() => true),
    new Promise((resolve) => setTimeout(() => resolve(false), 3000)),
  ]);
  if (!dead) {
    child.kill("SIGKILL");
    await once(child, "exit");
  }
}

function splitNativeArgs(args) {
  const out = { model: undefined, agent: undefined, session: undefined, attachArgs: [] };
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    const next = args[i + 1];
    if ((arg === "--model" || arg === "-m") && next) {
      out.model = next;
      i += 1;
      continue;
    }
    if (arg.startsWith("--model=")) {
      out.model = arg.slice("--model=".length);
      continue;
    }
    if (arg === "--agent" && next) {
      out.agent = next;
      i += 1;
      continue;
    }
    if (arg.startsWith("--agent=")) {
      out.agent = arg.slice("--agent=".length);
      continue;
    }
    if ((arg === "--session" || arg === "-s") && next) {
      out.session = next;
      i += 1;
      continue;
    }
    if (arg.startsWith("--session=")) {
      out.session = arg.slice("--session=".length);
      continue;
    }
    // The attached TUI is only a viewer for the plugin-owned session. Keep UI-native flags that
    // attach supports; do not pass session/model/prompt flags that apply to a normal launch and
    // would either duplicate `--session` or be rejected by the attach subcommand.
    if (arg === "--mini" || arg === "--no-replay") {
      out.attachArgs.push(arg);
      continue;
    }
    if (arg === "--replay-limit" && next) {
      out.attachArgs.push(arg, next);
      i += 1;
      continue;
    }
    if (arg.startsWith("--replay-limit=")) {
      out.attachArgs.push(arg);
    }
  }
  return out;
}

async function main() {
  const nativeArgs = process.argv.slice(2);
  const native = splitNativeArgs(nativeArgs);
  const pluginPath = process.env.NEXUS_OPENCODE_PLUGIN_PATH?.trim();
  const dataRoot = process.env.NEXUS_OPENCODE_HOME?.trim();
  const readyPath = process.env.NEXUS_OPENCODE_READY_PATH?.trim();
  if (!pluginPath) throw new Error("NEXUS_OPENCODE_PLUGIN_PATH is required");
  if (!dataRoot) throw new Error("NEXUS_OPENCODE_HOME is required");

  const bin = resolveOpencodeBin();
  const port = String(await freePort());
  const url = `http://127.0.0.1:${port}`;
  const secret = randomBytes(24).toString("hex");
  const username = "opencode";
  mkdirSync(dataRoot, { recursive: true });
  const resumeIsolated = process.env.NEXUS_OPENCODE_RESUME_ISOLATED === "1";
  const isolatedStore = !native.session || resumeIsolated;
  const dbPath = join(dataRoot, "opencode.db");
  const pidFile = join(dataRoot, "serve.pid");
  const baseEnv = { ...process.env };
  if (!isolatedStore) delete baseEnv.OPENCODE_DB;

  if (existsSync(pidFile)) {
    const pid = Number(readFileSync(pidFile, "utf8"));
    try {
      process.kill(pid, 0);
      throw new Error(`opencode serve already running for this Nexus session (pid ${pid})`);
    } catch (error) {
      if (error.code !== "ESRCH") throw error;
      rmSync(pidFile, { force: true });
    }
  }

  const config = {
    $schema: "https://opencode.ai/config.json",
    permission: "allow",
    plugin: [pluginPath],
  };
  // The project config is shared by all launches in a cwd. Never let its stale
  // caller key override the daemon-captured identity used by this server child.
  if (baseEnv.NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL !== "1" && baseEnv.NEXUS_SKIP_AGENT_HOOK_INSTALL !== "1") {
    const cli = baseEnv.NEXUS_CLI?.trim();
    const name = baseEnv.NEXUS_NAME?.trim() || baseEnv.NEXUS_AGENT_ID?.trim();
    const project = baseEnv.NEXUS_PROJECT?.trim();
    const key = baseEnv.NEXUS_CLIENT_KEY?.trim();
    const agent = baseEnv.NEXUS_AGENT?.trim();
    if (!cli || !name || !project || !key || !agent) {
      throw new Error("captured Nexus MCP identity is incomplete");
    }
    config.mcp = {
      "nexus-bus": {
        type: "local", enabled: true,
        command: [cli, "mcp", "--as", name, "--project", project, "--client-key", key, "--agent", agent],
      },
    };
  }
  if (native.model) {
    config.model = native.model;
    config.agent = { nexus: { mode: "primary", model: native.model } };
    config.default_agent = "nexus";
  }
  if (native.agent) config.default_agent = native.agent;

  const serve = spawn(bin, ["serve", "--hostname", "127.0.0.1", "--port", port], {
    env: {
      ...baseEnv,
      NEXUS_OPENCODE_SERVER_URL: url,
      NEXUS_OPENCODE_SESSION_ID: native.session ?? "",
      OPENCODE_SERVER_USERNAME: username,
      OPENCODE_SERVER_PASSWORD: secret,
      NEXUS_OPENCODE_PROMPT_MODEL: native.model ?? "",
      NEXUS_OPENCODE_PROMPT_AGENT: native.agent ?? (native.model ? "nexus" : ""),
      ...(isolatedStore ? { OPENCODE_DB: dbPath } : {}),
      OPENCODE_CONFIG_CONTENT: JSON.stringify(config),
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  writeFileSync(pidFile, String(serve.pid));
  serve.on("exit", () => rmSync(pidFile, { force: true }));

  let sessionId;
  let attached = false;
  let onSession;
  const scan = (data) => {
    if (!attached) process.stderr.write(data);
    if (!sessionId) {
      const match = data.toString().match(/\[nexus-opencode-session\] (\S+)/);
      if (match) {
        sessionId = match[1];
        onSession?.(sessionId);
      }
    }
  };
  serve.stdout?.on("data", scan);
  serve.stderr?.on("data", scan);
  serve.on("exit", (code, signal) => {
    if (!attached) process.exit(code ?? (signal ? 1 : 0));
  });

  const auth = `Basic ${Buffer.from(`${username}:${secret}`).toString("base64")}`;
  void (async () => {
    for (let i = 0; i < 300 && !sessionId; i += 1) {
      try {
        await fetch(`${url}/session`, { headers: { authorization: auth }, signal: AbortSignal.timeout(1500) });
      } catch {
        // Serve not ready yet, or an early request hung during boot.
      }
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
  })();

  const id = await new Promise((resolve) => {
    if (sessionId) return resolve(sessionId);
    onSession = resolve;
    setTimeout(() => resolve(sessionId), 60_000);
  });
  if (!id) {
    process.stderr.write(`[nexus-opencode-serve] session never came up; aborting\n`);
    await killServe(serve);
    process.exit(1);
  }
  const tuiEnv = {
    ...baseEnv,
    OPENCODE_SERVER_USERNAME: username,
    OPENCODE_SERVER_PASSWORD: secret,
    ...(isolatedStore ? { OPENCODE_DB: dbPath } : {}),
  };
  delete tuiEnv.OPENCODE_CONFIG_CONTENT;
  delete tuiEnv.NEXUS_OPENCODE_BRIDGE_URL;
  delete tuiEnv.NEXUS_OPENCODE_BRIDGE_TOKEN;
  delete tuiEnv.NEXUS_OPENCODE_PLUGIN_PATH;
  delete tuiEnv.NEXUS_OPENCODE_SERVER_URL;

  attached = true;
  const tui = spawn(bin, ["attach", url, "--session", id, "--password", secret, ...native.attachArgs], {
    env: tuiEnv,
    stdio: "inherit",
  });

  for (const sig of ["SIGINT", "SIGTERM"]) {
    process.on(sig, () => {
      tui.kill(sig);
      serve.kill(sig);
    });
  }
  tui.on("exit", (code, signal) => {
    void killServe(serve).then(() => process.exit(code ?? (signal ? 1 : 0)));
  });
  await new Promise((resolve) => setTimeout(resolve, 1000));
  if (
    serve.exitCode !== null ||
    serve.signalCode !== null ||
    tui.exitCode !== null ||
    tui.signalCode !== null
  ) {
    await killServe(serve);
    process.exit(tui.exitCode ?? serve.exitCode ?? 1);
  }
  if (readyPath) {
    const readyTempPath = `${readyPath}.tmp-${process.pid}`;
    writeFileSync(readyTempPath, JSON.stringify({ sessionId: id, url, pid: serve.pid, readyOwner: process.env.NEXUS_NATIVE_READY_OWNER }) + "\n");
    renameSync(readyTempPath, readyPath);
  }
}

void main();
