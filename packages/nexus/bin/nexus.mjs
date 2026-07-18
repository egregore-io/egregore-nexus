#!/usr/bin/env node

import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { setNpmLauncherContext } from "@egregore/nexus-cli/lib/install-context.mjs";

const launcherPath = fileURLToPath(import.meta.url);
setNpmLauncherContext({
  packageName: "@egregore/nexus",
  packageRoot: dirname(dirname(launcherPath)),
  launcherPath,
});
await import("@egregore/nexus-cli/bin/nexus.mjs");
