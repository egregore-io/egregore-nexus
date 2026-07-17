import { readdir, readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { build } from "vite";

const root = resolve(import.meta.dirname);
await build({ configFile: resolve(root, "vite.config.ts") });

const files = await walk(resolve(root, "dist"));
const forbidden = /(@libsql\/client|daemon[_/-]ipc|NEXUS_(?:HOME|DB_PATH|DB_URL)|\/api\/agui\/observe\?(?:thread|dm)=)/i;
for (const file of files.filter((path) => /\.(?:js|css|html)$/.test(path))) {
  const source = await readFile(file, "utf8");
  if (forbidden.test(source)) throw new Error(`WebUI artifact contains a server/daemon dependency: ${file}`);
}

const css = (await Promise.all(
  files.filter((path) => path.endsWith(".css")).map((file) => readFile(file, "utf8")),
)).join("\n");
for (const utility of [".h-screen{", ".grid{", ".bg-bg-primary{", ".text-text-normal"]) {
  if (!css.includes(utility)) {
    throw new Error(`WebUI artifact is missing application Tailwind utility ${utility}`);
  }
}

async function walk(directory) {
  const output = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = resolve(directory, entry.name);
    if (entry.isDirectory()) output.push(...await walk(path));
    else output.push(path);
  }
  return output;
}
