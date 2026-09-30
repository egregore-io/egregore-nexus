import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

describe("WebUI package boundary", () => {
  it("keeps WebUI private and bundles its artifact into the public Gateway package", () => {
    const pkg = JSON.parse(readFileSync(resolve("webconsole/package.json"), "utf8")) as {
      name?: string;
      private?: boolean;
      files?: string[];
      bin?: Record<string, string>;
      scripts?: Record<string, string>;
    };
    expect(pkg.name).toBe("@egregore/nexus-webui-workspace");
    expect(pkg.private).toBe(true);
    expect(pkg.files).toEqual(["bin/nexus-webui.mjs", "dist"]);
    expect(pkg.bin).toEqual({ "nexus-webui": "bin/nexus-webui.mjs" });
    expect(pkg.scripts?.dev).toBe("vite --config vite.config.ts");
    expect(pkg.scripts?.preview).toBe("vite preview --config vite.config.ts");
    expect(pkg.scripts?.build).toContain("build-webconsole.mjs");

    const gateway = JSON.parse(readFileSync(resolve("package.json"), "utf8")) as {
      files?: string[];
      bin?: Record<string, string>;
    };
    expect(gateway.files).toContain("webconsole/dist");
    expect(gateway.bin?.["nexus-webui"]).toBe("webconsole/bin/nexus-webui.mjs");
  });
});
