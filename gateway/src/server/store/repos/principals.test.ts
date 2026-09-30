import { createClient, type Client } from "@libsql/client";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { migrateGatewayStore } from "../migrations";
import {
  bindHumanPrincipal,
  principalByAlias,
  principalById,
  upsertSubjectBinding,
} from "./principals";

describe("Gateway principal repository", () => {
  let db: Client;

  beforeEach(async () => {
    db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
  });

  afterEach(() => db.close());

  it("binds one immutable local-human principal per human_user_id", async () => {
    const humanUserId = `hu_${"a".repeat(24)}`;
    const first = await bindHumanPrincipal(db, { humanUserId });
    const second = await bindHumanPrincipal(db, { humanUserId });

    expect(first).toEqual(second);
    expect(first).toMatchObject({ kind: "local.human", access: "admin" });
    expect(first.principalId).toMatch(/^h_[a-f0-9]{24}$/);
    await expect(principalById(db, first.principalId)).resolves.toEqual(first);
    await expect(principalByAlias(db, `human:${humanUserId}`)).resolves.toEqual(first);
    const count = await db.execute("SELECT COUNT(*) AS n FROM principals");
    expect(Number(count.rows[0]!.n)).toBe(1);
  });

  it("upserts one external principal per provider subject, independent of display name", async () => {
    const first = await upsertSubjectBinding(db, {
      provider: "telegram",
      externalUserId: "100",
      displayName: "Shared",
      kind: "external.human",
    });
    const repeat = await upsertSubjectBinding(db, {
      provider: "telegram",
      externalUserId: "100",
      displayName: "Renamed",
      kind: "external.human",
    });
    const other = await upsertSubjectBinding(db, {
      provider: "telegram",
      externalUserId: "200",
      displayName: "Shared",
      kind: "external.human",
    });

    expect(repeat.principalId).toBe(first.principalId);
    expect(other.principalId).not.toBe(first.principalId);
    expect(first.kind).toBe("external.human");
    const rows = await db.execute(
      "SELECT external_user_id, display_name FROM subject_bindings ORDER BY external_user_id",
    );
    expect(rows.rows).toMatchObject([
      { external_user_id: "100", display_name: "Renamed" },
      { external_user_id: "200", display_name: "Shared" },
    ]);
  });

  it("concurrent subject upserts converge on one binding and one principal", async () => {
    const inputs = Array.from({ length: 8 }, (_, index) => upsertSubjectBinding(db, {
      provider: "telegram",
      externalUserId: "concurrent",
      displayName: `Name ${index}`,
      kind: "external.human",
    }));
    const rows = await Promise.all(inputs);
    expect(new Set(rows.map((row) => row.principalId)).size).toBe(1);
    const bindings = await db.execute(
      "SELECT COUNT(*) AS n FROM subject_bindings WHERE provider = 'telegram' AND external_user_id = 'concurrent'",
    );
    expect(Number(bindings.rows[0]!.n)).toBe(1);
  });
});
