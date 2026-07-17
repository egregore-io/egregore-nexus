import { createDaemonReadClient } from "./readClient";

describe("daemon-owned gateway read client", () => {
  it("adapts daemon SQL rows to the libSQL row shape used by Drizzle", async () => {
    const calls: unknown[] = [];
    const client = createDaemonReadClient({
      query: async (params) => {
        calls.push(params);
        return {
          columns: ["answer", "label", "payload"],
          rows: [[42, "daemon-owned", { $blob: [1, 2, 3] }]],
          rowsAffected: 0,
        };
      },
    });

    const result = await client.execute({
      sql: "SELECT ? AS answer, ? AS label, ? AS payload",
      args: [42, "daemon-owned", new Uint8Array([1, 2, 3])],
    });
    expect(calls).toEqual([{
      sql: "SELECT ? AS answer, ? AS label, ? AS payload",
      args: [42, "daemon-owned", { $blob: [1, 2, 3] }],
    }]);
    expect(Array.from(result.rows[0] ?? [])).toEqual([
      42,
      "daemon-owned",
      new Uint8Array([1, 2, 3]).buffer,
    ]);
    expect(result.rows[0]).toMatchObject({
      answer: 42,
      label: "daemon-owned",
    });
  });

  it("batches read statements through the same daemon endpoint", async () => {
    const calls: unknown[] = [];
    const client = createDaemonReadClient({
      query: async (params) => {
        calls.push(params);
        return { columns: ["value"], rows: [[calls.length]], rowsAffected: 0 };
      },
    });
    const results = await client.batch([
      { sql: "SELECT 1 AS value", args: [] },
      { sql: "SELECT 2 AS value", args: [] },
    ]);
    expect(results.map((result) => result.rows[0]?.value)).toEqual([1, 2]);
  });

  it("supports the two-argument and tuple client forms but rejects named binds", async () => {
    const calls: unknown[] = [];
    const client = createDaemonReadClient({
      query: async (params) => {
        calls.push(params);
        return { columns: [], rows: [], rowsAffected: 0 };
      },
    });

    await client.execute("SELECT ?", [7]);
    await client.batch([["SELECT ?", [8]]]);
    expect(calls).toEqual([
      { sql: "SELECT ?", args: [7] },
      { sql: "SELECT ?", args: [8] },
    ]);
    await expect(client.execute({ sql: "SELECT $value", args: { value: 9 } }))
      .rejects.toThrow(/positional/);
  });
});
