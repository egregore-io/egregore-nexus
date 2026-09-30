#!/usr/bin/env node

const usage = () => {
  console.log(`Usage: scripts/nexus-webui-screenshot.mjs \\
  --browser-executable=PATH --url=URL --screenshot=PATH --user-data-dir=PATH

Captures one Nexus WebUI screenshot with a disposable browser profile.`);
};

const argumentsList = process.argv.slice(2);
if (argumentsList.includes("--help") || argumentsList.includes("-h")) {
  usage();
  process.exit(0);
}

const options = new Map(
  argumentsList.map((argument) => {
    const separator = argument.indexOf("=");
    if (!argument.startsWith("--") || separator < 0) {
      throw new Error(`invalid argument: ${argument}`);
    }
    return [argument.slice(2, separator), argument.slice(separator + 1)];
  }),
);

const required = (name) => {
  const value = options.get(name);
  if (!value) throw new Error(`missing --${name}`);
  return value;
};

const executablePath = required("browser-executable");
const url = required("url");
const screenshotPath = required("screenshot");
const userDataDir = required("user-data-dir");
const { default: puppeteer } = await import("puppeteer-core");
const browser = await puppeteer.launch({
  executablePath,
  headless: true,
  userDataDir,
  protocolTimeout: 20_000,
  args: [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--no-first-run",
    "--no-default-browser-check",
  ],
});

try {
  const page = await browser.newPage();
  await page.setViewport({ width: 1600, height: 1000 });
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 15_000 });
  await new Promise((resolve) => setTimeout(resolve, 5_000));
  await page.screenshot({ path: screenshotPath });
  console.log(`captured ${url} -> ${screenshotPath}`);
} finally {
  const closed = await Promise.race([
    browser.close().then(() => true),
    new Promise((resolve) => setTimeout(() => resolve(false), 5_000)),
  ]);
  if (!closed) browser.process()?.kill("SIGKILL");
}
