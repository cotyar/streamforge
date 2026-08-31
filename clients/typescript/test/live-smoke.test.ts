/**
 * Read-only smoke test against the demo already running at http://localhost:6199
 * (admin/admin123!). REST + SignalR only, no gRPC -- that instance was started with `--urls`,
 * which trips Program.cs's guard so no gRPC port is ever bound (design doc §3.2's `--urls`
 * trap). This is therefore the natural SignalR check: snapshot `trigger_monitor`, subscribe,
 * watch `seq` advance. Never restarts, mutates or kills the demo -- read-only, on purpose.
 *
 * Skips itself (rather than failing) when :6199 isn't reachable, so the rest of the suite stays
 * green on a machine without the demo running.
 */

import { describe, expect, test } from "bun:test";
import { connect } from "../src/index.js";

const DEMO_URL = "http://localhost:6199";

async function demoReachable(): Promise<boolean> {
  try {
    const res = await fetch(`${DEMO_URL}/api/healthz`, { signal: AbortSignal.timeout(2000) });
    return res.ok;
  } catch {
    return false;
  }
}

const reachable = await demoReachable();
const describeOrSkip = reachable ? describe : describe.skip;
if (!reachable) console.warn(`streamsforge live smoke: SKIPPED -- ${DEMO_URL}/api/healthz not reachable`);

describeOrSkip("live smoke: demo at :6199", () => {
  test("connects (auto -> signalr, since :6199 has no gRPC port bound)", async () => {
    const client = await connect({
      url: DEMO_URL,
      user: "admin",
      password: "admin123!",
      transport: "auto",
    });
    try {
      // The demo is `--urls`-started, so gRPC's own probe must fail and "auto" must land on a
      // SignalR mode -- this is the transport-selection guarantee the design doc's --urls
      // section describes, checked against the real thing rather than just the isolated fixture.
      expect(client.transportName.startsWith("signalr:")).toBe(true);
    } finally {
      await client.close();
    }
  }, 20_000);

  test("REST snapshot of trigger_monitor", async () => {
    const client = await connect({ url: DEMO_URL, user: "admin", password: "admin123!", transport: "signalr:ws" });
    try {
      const rows = await client.snapshot("trigger_monitor");
      expect(Array.isArray(rows)).toBe(true);
      // trigger_monitor is empty right after an engine restart (design doc §5's headroom note) --
      // assert shape, not non-emptiness, so this stays robust across whenever :6199 last restarted.
      for (const row of rows.slice(0, 3)) {
        expect(typeof row).toBe("object");
      }
    } finally {
      await client.close();
    }
  }, 20_000);

  test("subscribe to trigger_monitor and watch seq advance (or stay caught up)", async () => {
    const client = await connect({ url: DEMO_URL, user: "admin", password: "admin123!", transport: "signalr:ws" });
    try {
      await using table = await client.table("trigger_monitor", { timeoutMs: 20_000 });
      const seqAtSubscribe = table.seq;
      expect(table.ready).toBe(true);
      expect(typeof seqAtSubscribe).toBe("number");

      // The demo may be quiescent between synthetic pushes, so "ticks" is a best-effort
      // observation, not a hard requirement -- what's actually asserted is that the live
      // subscription stays connected and reports a monotonically sane seq the whole time.
      let sawChange = false;
      const unsubscribe = table.onChange(() => {
        sawChange = true;
      });
      await new Promise((r) => setTimeout(r, 3000));
      unsubscribe();

      expect(table.seq).toBeGreaterThanOrEqual(seqAtSubscribe);
      console.log(`streamsforge live smoke: trigger_monitor seq ${seqAtSubscribe} -> ${table.seq}, sawChange=${sawChange}`);
    } finally {
      await client.close();
    }
  }, 20_000);
});
