/**
 * Plan 020 wave G -- live contract tests for `AwarenessSession` (../src/awareness.ts) against the
 * SAME isolated engine `contract.test.ts` boots (engine-fixture.ts), reusing its
 * `CRDT_AWARENESS_SOURCE` fixture (a crdt-kind source with `awareness.maxEntries` set to 2
 * deliberately, so the cap refusal is reachable with two real connections rather than dozens).
 *
 * Real SignalR end to end -- no fake HubConnection -- because the interesting behavior here (the
 * server refusing a join, two live connections seeing each other's presence, a cap refusal naming
 * itself) is exactly the thing a fake would risk asserting the wrong side of. The pure
 * authorization/TTL/cap MECHANICS are already pinned server-side with a fake clock in
 * StreamHubAwarenessTests.cs; this suite is the wire-level check that this client's own encode/
 * decode of that contract (camelCase JSON, the `awarenessUpdate` push shape, HubException ->
 * StreamsForgeError) actually matches what ships.
 */

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { connect, StreamsForgeError, type Client } from "../src/index.js";
import { bootEngine, preflightSkipReason, type Engine } from "./engine-fixture.js";

const skipReason = preflightSkipReason();
const describeOrSkip = skipReason ? describe.skip : describe;
if (skipReason) console.warn(`streamsforge awareness tests: SKIPPED -- ${skipReason}`);

describeOrSkip("awareness: isolated engine", () => {
  let engine: Engine;
  let client: Client;

  beforeAll(async () => {
    engine = await bootEngine();
    client = await connect({ url: engine.baseUrl, user: engine.user, password: engine.password, transport: "signalr:ws" });
  }, 180_000);

  afterAll(async () => {
    await client?.close();
    await engine?.stop();
  }, 20_000);

  test("joining returns the caller's own entry, with the AUTHENTICATED identity (not a client-chosen label)", async () => {
    await using session = await client.awareness(engine.crdtAwarenessSource, { clientId: "peer-solo", label: "cursor-red" });

    expect(session.peers).toHaveLength(1);
    const entry = session.peers[0]!;
    expect(entry.clientId).toBe("peer-solo");
    expect(entry.identity).toBe(engine.user); // "admin" -- never "cursor-red"
    expect(entry.label).toBe("cursor-red");
    expect(typeof entry.joinedAt).toBe("string");
    expect(typeof entry.expiresAt).toBe("string");
  }, 20_000);

  test("a non-crdt-kind source refuses rather than joining an empty group", async () => {
    await expect(client.awareness(engine.source, { clientId: "peer-x" })).rejects.toThrow(StreamsForgeError);
    await expect(client.awareness(engine.source, { clientId: "peer-x" })).rejects.toThrow(/not crdt-kind/);
  }, 20_000);

  test("an unknown source refuses by name", async () => {
    await expect(client.awareness("no-such-source-at-all", { clientId: "peer-x" })).rejects.toThrow(/not found/);
  }, 20_000);

  test("two live sessions see each other, and closing one is broadcast to the other", async () => {
    const sessionA = await client.awareness(engine.crdtAwarenessSource, { clientId: "peer-a" });
    try {
      expect(sessionA.peers.map((p) => p.clientId)).toEqual(["peer-a"]);

      const updates: string[][] = [];
      const unsubscribe = sessionA.onUpdate((peers) => {
        updates.push(peers.map((p) => p.clientId).sort());
      });

      const sessionB = await client.awareness(engine.crdtAwarenessSource, { clientId: "peer-b" });
      try {
        // B's own join snapshot already includes both.
        expect(sessionB.peers.map((p) => p.clientId).sort()).toEqual(["peer-a", "peer-b"]);

        // A learns about B via the awarenessUpdate push, not by polling.
        await waitFor(() => updates.some((u) => u.length === 2 && u.includes("peer-b")), 5_000);
        expect(sessionA.peers.map((p) => p.clientId).sort()).toEqual(["peer-a", "peer-b"]);
      } finally {
        await sessionB.close();
      }

      // B leaving is broadcast to A too.
      await waitFor(() => sessionA.peers.map((p) => p.clientId).sort().join(",") === "peer-a", 5_000);

      unsubscribe();
    } finally {
      await sessionA.close();
    }
  }, 20_000);

  test("the cap is enforced live and names itself in the refusal", async () => {
    const first = await client.awareness(engine.crdtAwarenessSource, { clientId: "cap-1" });
    const second = await client.awareness(engine.crdtAwarenessSource, { clientId: "cap-2" });
    try {
      // engine-fixture.ts sets CRDT_AWARENESS_MAX_ENTRIES = 2 -- both slots are now taken by
      // DIFFERENT connections (each AwarenessSession opens its own), so a third is refused.
      await expect(client.awareness(engine.crdtAwarenessSource, { clientId: "cap-3" })).rejects.toThrow(/cap of 2/);
    } finally {
      await first.close();
      await second.close();
    }
  }, 20_000);
});

async function waitFor(predicate: () => boolean, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  if (!predicate()) throw new Error(`condition not met within ${timeoutMs}ms`);
}
