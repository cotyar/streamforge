/**
 * Contract tests against a real, isolated StreamsForge engine (see engine-fixture.ts), run once
 * per transport -- the whole justification for the Transport interface (design doc §3.6/§8):
 * implementations that agree on every assertion are interchangeable, and one that drifts fails on
 * the same line the others pass.
 *
 * Owns: connect() + auth, subscribe -> snapshot -> replay, LATEST BY supersession over the wire,
 * GROUP BY aggregation over a derived LATEST BY table, and (once, transport-agnostic) validate +
 * ad-hoc SQL create/drop.
 */

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { connect, type Client, type TransportName } from "../src/index.js";
import { bootEngine, preflightSkipReason, type Engine } from "./engine-fixture.js";

const skipReason = preflightSkipReason();
const describeOrSkip = skipReason ? describe.skip : describe;
if (skipReason) console.warn(`streamsforge contract tests: SKIPPED -- ${skipReason}`);

describeOrSkip("contract: isolated engine", () => {
  let engine: Engine;

  beforeAll(async () => {
    engine = await bootEngine();
  }, 180_000);

  afterAll(async () => {
    await engine?.stop();
  }, 20_000);

  const transports: TransportName[] = ["grpc", "signalr:ws", "signalr:sse", "signalr:lp"];

  for (const transportName of transports) {
    describe(transportName, () => {
      let client: Client;

      beforeAll(async () => {
        client = await connect({
          url: engine.baseUrl,
          grpc: engine.grpcTarget,
          user: engine.user,
          password: engine.password,
          transport: transportName,
        });
      }, 30_000);

      afterAll(async () => {
        await client?.close();
      });

      test("connects via the requested transport", () => {
        expect(client.transportName).toBe(transportName);
      });

      test("no delta lost across subscribe establishment (regression: subscribe must register before snapshot)", async () => {
        // Regression coverage for a bug found in this project's Kotlin client (cold Flow -- the
        // subscription didn't actually connect until collection began) and independently in a
        // narrower form in Python (socket eager, hub handshake lazy): Transport.subscribe() must
        // fully register with the server -- not just return an unstarted async iterable -- before
        // LiveTable's snapshot read fires, or a delta landing in that window is gone for good (a
        // LATEST BY row asserted once and never touched again gets no backfill to recover it).
        //
        // Firing the push and the table() open in the same tick exercises exactly that window:
        // under the old (buggy) `async function* subscribe()` shape, the generator's body --
        // including the real handshake -- didn't run until live-table.ts's first `.next()`, so
        // the snapshot read could fire (and for SignalR, reliably WOULD fire, since establishing
        // a fresh connection is far slower than one REST round trip) while the server still had
        // no idea we existed. This LATEST BY row is asserted exactly once, so there is nothing
        // downstream to self-heal a lost delta -- it either shows up, or it is gone.
        const tag = `race_${transportName.replace(/[^a-z0-9]/g, "_")}`;
        const [, table] = await Promise.all([
          client.push(engine.source, [{ trade_id: `${tag}-T1`, desk: "Rates", notional: 111 }]),
          client.table(engine.latestTable, { key: ["trade_id"], timeoutMs: 30_000 }),
        ]);
        try {
          await table.waitFor((rows) => rows.some((r) => r.trade_id === `${tag}-T1`), 10_000);
          expect(table.value("notional", { trade_id: `${tag}-T1` })).toBe(111);
        } finally {
          table.close();
        }
      }, 30_000);

      test("subscribe -> snapshot -> replay, LATEST BY supersession, GROUP BY aggregation", async () => {
        const tag = transportName.replace(/[^a-z0-9]/g, "_");

        // Subscribe to BOTH tables before pushing anything -- desk_totals is a table reading
        // ANOTHER table (GROUP BY over the LATEST BY view), and this engine's Rows/snapshot read
        // for that shape lags its own live dataflow by up to its persistence flush interval
        // (~2s, CLAUDE.md's "Flush interval in ms for Batched/FireAndForget" default): a snapshot
        // taken between the push and that flush can read stale/empty even though the live delta
        // stream has already emitted the update. Subscribing first sidesteps the race entirely
        // and is also the realistic usage pattern (subscribe once, watch data flow in) --
        // confirmed empirically that a live subscriber sees the update in ~150ms.
        await using latest = await client.table(engine.latestTable, { key: ["trade_id"], timeoutMs: 30_000 });
        await using agg = await client.table(engine.aggTable, { key: ["desk"], timeoutMs: 30_000 });

        await client.push(engine.source, [
          { trade_id: `${tag}-T1`, desk: "Rates", notional: 100 },
          { trade_id: `${tag}-T2`, desk: "Credit", notional: 40 },
        ]);
        await latest.waitFor((rows) => rows.filter((r) => String(r.trade_id).startsWith(tag)).length >= 2, 15_000);

        const t1 = latest.value("notional", { trade_id: `${tag}-T1` });
        expect(t1).toBe(100);

        // A LATEST BY update over the wire: the old row for T1 must be superseded, not duplicated.
        await client.push(engine.source, [{ trade_id: `${tag}-T1`, desk: "Rates", notional: 250 }]);
        await latest.waitFor((rows) => rows.some((r) => r.trade_id === `${tag}-T1` && r.notional === 250), 15_000);

        const mine = latest.rows.filter((r) => String(r.trade_id).startsWith(tag));
        expect(mine.length).toBe(2); // still exactly one row per trade_id -- no duplicate from the supersession

        // GROUP BY over the derived LATEST BY table must reflect the superseded value, not both.
        await agg.waitFor((rows) => {
          const rates = rows.find((r) => r.desk === "Rates");
          return rates !== undefined && Number(rates.total) >= 250;
        }, 15_000);
      }, 30_000);
    });
  }

  test("keyFields resolved from the engine (transport-agnostic, wishlist #18)", async () => {
    // client.table() with no `key` must read the table's own `keyFields` (GET /api/tables)
    // instead of the deleted hand-maintained map -- correct supersession (never a duplicate row)
    // is the observable proof the resolved key was actually used, not just that *a* key was, for
    // all three wire states: non-empty (LATEST BY), [] (global aggregate) and, implicitly, the
    // GROUP BY case shared with the existing suite above.
    const client = await connect({
      url: engine.baseUrl,
      grpc: engine.grpcTarget,
      user: engine.user,
      password: engine.password,
      transport: "signalr:ws",
    });
    try {
      const tag = "keyfields_engine";

      // LATEST BY -- non-empty keyFields (["trade_id"]).
      {
        await using latest = await client.table(engine.latestTable, { timeoutMs: 30_000 }); // no key=
        await client.push(engine.source, [{ trade_id: `${tag}-T1`, desk: "Rates", notional: 100 }]);
        await latest.waitFor((rows) => rows.some((r) => r.trade_id === `${tag}-T1`), 15_000);
        await client.push(engine.source, [{ trade_id: `${tag}-T1`, desk: "Rates", notional: 250 }]);
        await latest.waitFor((rows) => rows.some((r) => r.trade_id === `${tag}-T1` && r.notional === 250), 15_000);
        const mine = latest.rows.filter((r) => r.trade_id === `${tag}-T1`);
        expect(mine.length).toBe(1); // superseded, not duplicated
      }

      // GROUP BY -- non-empty keyFields (["desk"]).
      {
        const desk = `Desk-${tag}`;
        await using agg = await client.table(engine.aggTable, { timeoutMs: 30_000 }); // no key=
        await client.push(engine.source, [{ trade_id: `${tag}-A1`, desk, notional: 40 }]);
        await client.push(engine.source, [{ trade_id: `${tag}-A2`, desk, notional: 60 }]);
        await agg.waitFor((rows) => {
          const row = rows.find((r) => r.desk === desk);
          return row !== undefined && Number(row.total) >= 100;
        }, 15_000);
        expect(agg.rows.filter((r) => r.desk === desk).length).toBe(1);
      }

      // No GROUP BY at all -- keyFields is [] (a global aggregate: one row, one group). If the
      // resolver ever collapsed [] to null (whole-row identity) this table would grow a second
      // row on the next push instead of staying at exactly one.
      {
        await using globalAgg = await client.table(engine.globalAggTable, { timeoutMs: 30_000 }); // no key=
        await client.push(engine.source, [{ trade_id: `${tag}-G1`, desk: "Global", notional: 10 }]);
        await globalAgg.waitFor((rows) => rows.length >= 1, 15_000);
        await client.push(engine.source, [{ trade_id: `${tag}-G2`, desk: "Global", notional: 20 }]);
        await globalAgg.waitFor((rows) => rows.length === 1 && Number(rows[0]?.trade_count ?? 0) >= 2, 15_000);
        expect(globalAgg.rows.length).toBe(1);
      }
    } finally {
      await client.close();
    }
  }, 30_000);

  test("validate + ad-hoc SQL create/replace/drop (transport-agnostic REST)", async () => {
    const client = await connect({
      url: engine.baseUrl,
      grpc: engine.grpcTarget,
      user: engine.user,
      password: engine.password,
      transport: "signalr:ws",
    });
    try {
      const bad = await client.validate("SELECT this is not valid sql (((");
      expect(bad.ok).toBe(false);
      expect(bad.diagnostics.length).toBeGreaterThan(0);

      await using adhocTable = await client.sql(`SELECT desk, SUM(notional) AS total FROM ${engine.latestTable} GROUP BY desk`, {
        name: "contract test scratch",
        key: ["desk"],
        timeoutMs: 30_000,
      });
      const listed = await client.adhoc();
      expect(listed.some((t) => t.name === "adhoc_contract_test_scratch")).toBe(true);

      const dropped = await client.dropAdhoc("adhoc_contract_test_scratch");
      expect(dropped).toBe(true);
      void adhocTable; // already dropped server-side; local handle just needs to close cleanly
    } finally {
      await client.close();
    }
  }, 30_000);
});
