# @streamforge/client

TypeScript client for StreamForge live tables: gRPC (Node only) and SignalR (browser + Node)
behind one `Transport` interface, a framework-free `LiveTable` (the console's
`web/src/hooks/useTableRows.ts` is a thin React wrapper around the same logic, extracted here),
ad-hoc SQL, ingest, and auth. See `docs/python-client-design.md` (ac-co.ai-4 repo) for the shared
design this client and the Python one both implement; this README covers what's TypeScript- and
Node/browser-specific.

## Install

```bash
bun install
bun run build      # emits dist/, copies src/proto/streamforge.proto alongside it
```

## Quick start

```ts
import { connect } from "@streamforge/client";

const sf = await connect({ url: "http://localhost:5199" }); // env/config also supported
const t = await sf.table("trigger_monitor");                // subscribes, snapshots, replays
t.rows;                                                       // Row[], frozen, current state
await t.waitFor((rows) => rows.length > 0, 30_000);
const stop = t.onChange((rows) => console.log(rows.length));
for await (const rows of t) { /* AsyncIterable of change notifications */ }
await using u = await sf.table("desk_exposure");             // closes on scope exit
```

Env vars (first hit wins: explicit `connect()` option, then env): `STREAMFORGE_BASE_URL`,
`STREAMFORGE_GRPC`, `STREAMFORGE_ADMIN_USER`, `STREAMFORGE_ADMIN_PASS`, `SF_INGEST_KEY`.

## gRPC is Node-only

A browser cannot speak h2c (cleartext HTTP/2) gRPC at all -- that's a browser platform
limitation, not a gap in this client. In a browser, `@streamforge/client` is **SignalR-only**:
pass `transport: "signalr"` (or one of its `:ws`/`:sse`/`:lp` variants), or leave it on `"auto"`,
which detects it's running outside Node and skips the gRPC attempt entirely rather than trying
and failing.

The gRPC transport module (`grpc-transport.ts`, and therefore `@grpc/grpc-js` +
`@grpc/proto-loader`) is loaded via a dynamic `import()` gated on a Node-runtime check
(`typeof process !== "undefined" && process.versions?.node`), reached only from `connect()`'s own
gRPC/`"auto"` path -- importing `@streamforge/client` itself never pulls gRPC into a bundle's
static import graph. A bundler will still code-split the dynamic import into its own chunk; that
chunk is simply never fetched in a browser build, since nothing ever calls into it there.

## Transports

`connect({ transport })` accepts `"grpc" | "signalr" | "signalr:ws" | "signalr:sse" | "signalr:lp"
| "auto"` (default). `"signalr"` is an alias for `"signalr:ws"`. `"auto"` tries gRPC (Node only),
then SignalR ws -> sse -> lp, and **always logs which one it got** (`console.info`) -- a client
that silently degrades and lets a caller believe it's on the fast path is worse than one that
fails loudly.

All three SignalR wire modes are real, independent delta streams -- `app.MapHub<StreamHub>
("/hubs/stream")` restricts no transports, so the engine already serves WebSockets, Server-Sent
Events and Long Polling on that one URL, and this client picks one via
`@microsoft/signalr`'s own `HttpConnectionBuilder.withUrl({ transport })` option (never
hand-rolled -- see `signalr-transport.ts`'s own doc comment for why, and how this mirrors
`web/src/realtime/hub.ts`'s `resolveTransport()`).

When gRPC is refused, the error names the likely cause: the engine was started with `--urls`,
which trips `Program.cs`'s guard so **no gRPC port is bound at all** -- start it with
`--Http:Port <n> --Grpc:Port <n+100>` instead (see the design doc's §3.2).

## Public surface

```ts
connect({ url, grpc, user, password, token, ingestKey, transport, verify }) -> Promise<Client>
client.table(name, { key, timeoutMs }) -> Promise<LiveTable>
client.snapshot(name, limit) -> Promise<Row[]>           // one-shot REST/gRPC read, no subscription
client.tables() -> Promise<TableDefinitionDto[]>
client.search(name, query, limit) -> Promise<Row[]>
client.history(name, row, limit) -> Promise<unknown[]>
client.sql(sqlText, { name, key, timeoutMs }) -> Promise<LiveTable>   // validate -> import -> LiveTable
client.validate(sqlText) -> Promise<TableValidateResponse>
client.adhoc() -> Promise<TableDefinitionDto[]>           // tables under the adhoc_ namespace
client.dropAdhoc(name) -> Promise<boolean>                // refuses any name outside adhoc_
client.push(source, rows, { idempotencyKey, partial }) -> Promise<unknown>
client.close() / await using / client[Symbol.asyncDispose]()

table.rows -> readonly Row[]           // frozen; a fresh array on every change, never mutated in place
table.onChange(cb) -> () => void       // unsubscribe; coalesced to ~1 callback per 120ms
table.waitFor(pred, timeoutMs) -> Promise<readonly Row[]>
table.value(col, keys) -> unknown
table.ready / table.seq / table.reconnects
for await (const rows of table) { ... }  // AsyncIterable<readonly Row[]>
table.close() / await using / table[Symbol.asyncDispose]()
```

Auth: `POST /api/auth/login`, token cached ~11h, re-minted **once** on a 401 then rethrown as
`AuthError`. Typed errors: `StreamForgeError` (base), `AuthError`, `SqlError` (`.diagnostics` with
line/column, `.message` renders a caret against the offending SQL line), `IngestRejected`
(`.rowErrors`), `NotReady` (a `LiveTable` never filled, or `waitFor`'s predicate never matched --
the common cause is a brand-new table with no backfill).

## The Z-set reducer

`zset.ts` is a line-for-line port of `web/src/hooks/useTableRows.ts`'s reducer (this repo's own
console) -- same canonical-row identity, same group-key supersession, same content-based
"already reflected" replay heuristic for the subscribe/snapshot race. Read its module doc comment
before touching it; the hazards it defends against are non-obvious by design (arrival order isn't
guaranteed, the snapshot's `seq` and the delta stream's `seq` are different counters on different
scales). `bun test test/conformance.test.ts` runs it against
`../conformance/zset-cases.json`, the cross-language conformance suite every StreamForge client
(this one, Python, the console, the Excel add-in) must agree with bit-for-bit.

## Duplication with `web/`

As of this package's addition, `web/src/hooks/useTableRows.ts` and `web/src/realtime/hub.ts` were
migrated to consume this package (see their own doc comments) rather than carrying a second copy
of the reducer/hub-multiplexing logic. If you find drift between them again in the future, this
package is the one that owns the logic -- `web/`'s hook should stay a thin adapter.

## Testing

```bash
bun test                                    # everything below, in one run
bun test test/conformance.test.ts           # 14/14 cross-language Z-set cases, no engine needed
bun test test/contract.test.ts              # boots an isolated engine on 8199/8299, both transports
bun test test/live-smoke.test.ts            # read-only against a demo at :6199, if one is running
```

The contract suite never binds `5199`/`5299` (the live dev server) or `6199` (a demo instance) --
it asserts those two ports (`8199`/`8299`) are free first and skips with a clear message rather
than colliding, and it kills what it starts. Set `SF_TEST_PUBLISH_DIR` to a pre-published
`StreamForge.Host` output directory to skip the ~2-minute `dotnet publish` on every run.

## What the Python design doc got wrong for this port

- It assumed a Node client would need to hand-roll SignalR's wire protocol the way the Python
  client's `_hub.py` does (Python has no first-party SignalR client). It doesn't: `@microsoft/signalr`
  already implements all three wire modes, so `signalr-transport.ts` is a thin adapter, not a
  protocol implementation.
- §3.4's "the token goes in the query string only for the `ws` mode, not the two HTTP modes" is a
  Python-vs-browser distinction (`httpx` can send headers on negotiate/SSE/long-poll; a browser's
  `EventSource` cannot). `@microsoft/signalr` handles this internally regardless of environment --
  not a decision this client makes itself.
