/**
 * The one check: boot the library under Bun.serve on a random port and drive it with the REAL
 * `@streamsforge/client` over both plain transports -- push -> LATEST BY supersession ->
 * remove -> auth refusal. If the wire contract drifts from plain-transport.ts, this fails.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { connect, type Client } from "@streamsforge/client";
import { createStreamsForge } from "../src/index.js";

const sf = createStreamsForge({
  auth: {
    login: (u, p) => (u === "admin" && p === "pw" ? { token: "t-admin", role: "Admin" } : null),
    verify: (t) => t === "t-admin",
  },
});
const trades = sf.table("trades", { keyFields: ["trade_id"] });
const log = sf.table("trade_log"); // whole-row identity: a plain multiset
sf.source("trade_feed", (rows) => {
  for (const r of rows) {
    if (r.deleted) trades.remove(r);
    else {
      trades.upsert(r);
      log.upsert(r);
    }
  }
});

let server: ReturnType<typeof Bun.serve>;
beforeAll(() => {
  server = Bun.serve({ port: 0, fetch: sf.fetch, websocket: sf.websocket });
});
afterAll(() => server.stop(true));

for (const transport of ["ws", "sse"] as const) {
  describe(transport, () => {
    let client: Client;
    beforeAll(async () => {
      client = await connect({ url: `http://localhost:${server.port}`, user: "admin", password: "pw", transport });
    });
    afterAll(() => client.close());

    test("push -> live upsert -> supersession -> remove", async () => {
      const id = `${transport}-T1`;
      const [, t] = await Promise.all([
        client.push("trade_feed", [{ trade_id: id, notional: 100 }]),
        client.table("trades", { timeoutMs: 5_000 }),
      ]);
      try {
        await t.waitFor((rows) => rows.some((r) => r.trade_id === id), 5_000);
        expect(t.value("notional", { trade_id: id })).toBe(100);

        await client.push("trade_feed", [{ trade_id: id, notional: 250 }]);
        await t.waitFor((rows) => rows.find((r) => r.trade_id === id)?.notional === 250, 5_000);
        expect(t.rows.filter((r) => r.trade_id === id)).toHaveLength(1); // superseded, not duplicated

        await client.push("trade_feed", [{ trade_id: id, deleted: true }]);
        await t.waitFor((rows) => !rows.some((r) => r.trade_id === id), 5_000);
      } finally {
        t.close();
      }
    });

    test("snapshot seeds a late subscriber", async () => {
      const t = await client.table("trade_log", { timeoutMs: 5_000 });
      try {
        expect(t.rows.length).toBeGreaterThan(0);
      } finally {
        t.close();
      }
    });
  });
}

test("auth: bad password is refused, missing token is 401", async () => {
  await expect(connect({ url: `http://localhost:${server.port}`, user: "admin", password: "nope", transport: "sse" }).then((c) => c.tables())).rejects.toThrow(/401/);
  const res = await fetch(`http://localhost:${server.port}/api/tables`);
  expect(res.status).toBe(401);
});
