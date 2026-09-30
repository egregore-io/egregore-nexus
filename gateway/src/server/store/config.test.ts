import { join } from "node:path";
import { describe, expect, it } from "vitest";

import { GatewayStoreConfigError, gatewayStoreConfig } from "./config";

describe("Gateway canonical store configuration", () => {
  it("defaults to a local Gateway-owned database", () => {
    expect(gatewayStoreConfig({}, "/home/test")).toEqual({
      url: `file:${join("/home/test", ".nexus", "gateway.db")}`,
    });
  });

  it("prefers NEXUS_GATEWAY_DB over the deprecated webconsole alias", () => {
    expect(
      gatewayStoreConfig(
        {
          NEXUS_GATEWAY_DB: "file:/tmp/gateway.db",
          NEXUS_WEBCONSOLE_DB: "file:/tmp/legacy.db",
        },
        "/home/test",
      ),
    ).toEqual({ url: "file:/tmp/gateway.db" });
  });

  it("accepts the deprecated local alias only when the canonical variable is unset", () => {
    expect(
      gatewayStoreConfig({ NEXUS_WEBCONSOLE_DB: "file:~/.nexus/legacy.db" }, "/home/test"),
    ).toEqual({ url: `file:${join("/home/test", ".nexus", "legacy.db")}` });
  });

  it.each(["libsql://remote.invalid", "http://127.0.0.1:4141", "https://db.invalid"])(
    "rejects non-local database URL %s",
    (url) => {
      expect(() => gatewayStoreConfig({ NEXUS_GATEWAY_DB: url }, "/home/test")).toThrow(
        GatewayStoreConfigError,
      );
    },
  );

  it("does not let daemon database variables select the Gateway store", () => {
    expect(
      gatewayStoreConfig(
        {
          NEXUS_DB_URL: "http://attacker.invalid:4141",
          NEXUS_DB_PATH: "/tmp/nexus.db",
        },
        "/home/test",
      ),
    ).toEqual({ url: `file:${join("/home/test", ".nexus", "gateway.db")}` });
  });
});
