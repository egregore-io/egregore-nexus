import { fileURLToPath } from "node:url";
import tailwindcss from "@tailwindcss/vite";
import viteReact from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const gatewayRoot = new URL("../", import.meta.url);
const alias = {
  "@app": fileURLToPath(new URL("src/app", gatewayRoot)),
  "@modules": fileURLToPath(new URL("src/modules", gatewayRoot)),
  "@shared": fileURLToPath(new URL("src/shared", gatewayRoot)),
};
const proxy = {
  "/api": {
    target: process.env.NEXUS_GATEWAY_PROXY_TARGET ?? "http://127.0.0.1:4101",
    ws: true,
  },
};

export default defineConfig({
  root: fileURLToPath(new URL("./", import.meta.url)),
  publicDir: fileURLToPath(new URL("public", gatewayRoot)),
  resolve: { alias },
  define: {
    "import.meta.env.NEXUS_WEBUI_STANDALONE": JSON.stringify("true"),
    "import.meta.env.NEXUS_WEB_TRANSPORT": JSON.stringify(process.env.NEXUS_WEB_TRANSPORT ?? "sse"),
    "import.meta.env.NEXUS_GATEWAY_URL": JSON.stringify(process.env.NEXUS_GATEWAY_URL ?? ""),
  },
  plugins: [tailwindcss(), viteReact()],
  server: {
    host: "0.0.0.0",
    port: 4100,
    proxy,
  },
  preview: { host: "0.0.0.0", port: 4100, proxy },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
});
