# @streamsforge/client

TypeScript client for StreamsForge live tables: gRPC (Node only) and SignalR (browser + Node)
behind one `Transport` interface, a framework-free `LiveTable` (the console's
`web/src/hooks/useTableRows.ts` is a thin React wrapper around the same logic, extracted here),
ad-hoc SQL, ingest, and auth. See `docs/python-client-design.md` (ac-co.ai-4 repo) for the shared
design this client and the Python one both implement; this README covers what's TypeScript- and
Node/browser-specific.

## Install

```bash
bun install
bun run build      # emits dist/, copies src/proto/streamsforge.proto alongside it
```

## Quick start

```ts
import { connect } from "@streamsforge/client";

const sf = await connect({ url: "http://localhost:5199" }); // env/config also supported
const t = await sf.table("trigger_monitor");                // subscribes, snapshots, replays
t.rows;                                                       // Row[], frozen, current state
await t.waitFor((rows) => rows.length > 0, 30_000);
const stop = t.onChange((rows) => console.log(rows.length));
for await (const rows of t) { /* AsyncIterable of change notifications */ }
await using u = await sf.table("desk_exposure");             // closes on scope exit
```

Env vars (first hit wins: explicit `connect()` option, then env): `STREAMSFORGE_BASE_URL`,
`STREAMSFORGE_GRPC`, `STREAMSFORGE_ADMIN_USER`, `STREAMSFORGE_ADMIN_PASS`, `SF_INGEST_KEY`.

## gRPC is Node-only

A browser cannot speak h2c (cleartext HTTP/2) gRPC at all -- that's a browser platform
limitation, not a gap in this client. In a browser, `@streamsforge/client` is **SignalR-only**:
pass `transport: "signalr"` (or one of its `:ws`/`:sse`/`:lp` variants), or leave it on `"auto"`,
which detects it's running outside Node and skips the gRPC attempt entirely rather than trying
and failing.

The gRPC transport module (`grpc-transport.ts`, and therefore `@grpc/grpc-js` +
`@grpc/proto-loader`) is loaded via a dynamic `import()` gated on a Node-runtime check
(`typeof process !== "undefined" && process.versions?.node`), reached only from `connect()`'s own
gRPC/`"auto"` path -- importing `@streamsforge/client` itself never pulls gRPC into a bundle's
static import graph. A bundler will still code-split the dynamic import into its own chunk; that
chunk is simply never fetched in a browser build, since nothing ever calls into it there.

## Transports

`connect({ transport })` accepts `"grpc" | "signalr" | "signalr:ws" | "signalr:sse" | "signalr:lp"
| "ws" | "sse" | "auto"` (default). `"ws"`/`"sse"` are the **plain** (non-SignalR) transports
for a server speaking the bare `tableDelta` contract over a raw WebSocket / Server-Sent Events
stream -- `@streamsforge/server` (`server/`), the embeddable Bun/Hono/Next.js dataset layer; the
wire shape is in `plain-transport.ts`. They are never picked by `"auto"`. `"signalr"` is an alias for `"signalr:ws"`. `"auto"` tries gRPC (Node only),
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

## TLS

Point `url` at `https://…` (a host started with `--Tls:Enabled true`, see `tools/tls/dev-cert.sh`
and `SECURITY.md`) and REST works like any HTTPS client. For gRPC, `grpc=` carries the scheme too
-- `"host:port"` and `"http://host:port"` stay plaintext (unchanged), `"https://host:port"` dials
TLS; omitted, `defaultGrpcTarget` guesses `grpc=` from `url` and **keeps its scheme**, so
`url: "https://h:7199"` alone yields a TLS gRPC guess at `"https://h:7299"`, not a plaintext one
against what is a TLS-only port once `Tls:Enabled` is on.

- **`ca`** (also `STREAMSFORGE_CA`) -- a custom CA to trust: PEM text (containing `-----BEGIN`) or
  a file path (Node only; a browser has no filesystem, so it must pass PEM text). Point it at the
  `cert.pem` `dev-cert.sh` wrote -- that file is its own trust anchor (self-signed, `CA:TRUE`) --
  to talk to a dev instance without disabling verification. Applies to REST and gRPC.
- **`verify: false`** -- accept any certificate (self-signed/invalid), on REST **and** gRPC. Dev
  only; spelled out rather than defaulted on purpose (see `ConnectOptions.verify`'s doc comment).
- **SignalR has no `ca`/`verify` hook in this client** -- `@microsoft/signalr`'s Node HTTP client
  builds its own request/socket options and never sees this client's `RestClient`, so there is no
  clean way to thread a per-connection CA or "skip verification" into it. Set
  `NODE_EXTRA_CA_CERTS=/path/to/cert.pem` (process-wide, before your process starts) to trust a
  custom CA for SignalR under Node; there is no equivalent fallback for `verify: false` short of
  the blunt, process-wide `NODE_TLS_REJECT_UNAUTHORIZED=0`.

## Public surface

```ts
connect({ url, grpc, user, password, token, ingestKey, transport, verify, ca }) -> Promise<Client>
client.table(name, { key, timeoutMs, flushMs }) -> Promise<LiveTable>
client.snapshot(name, limit) -> Promise<Row[]>           // one-shot REST/gRPC read, no subscription
client.tables() -> Promise<TableDefinitionDto[]>
client.search(name, query, limit) -> Promise<Row[]>
client.history(name, row, limit) -> Promise<unknown[]>
client.sql(sqlText, { name, key, timeoutMs, flushMs }) -> Promise<LiveTable>   // validate -> import -> LiveTable
client.validate(sqlText) -> Promise<TableValidateResponse>
client.adhoc() -> Promise<TableDefinitionDto[]>           // tables under the adhoc_ namespace
client.dropAdhoc(name) -> Promise<boolean>                // refuses any name outside adhoc_
client.push(source, rows, { idempotencyKey, partial }) -> Promise<unknown>
client.close() / await using / client[Symbol.asyncDispose]()

table.rows -> readonly Row[]           // frozen; a fresh array on every change, never mutated in place
table.onChange(cb) -> () => void       // unsubscribe; leading-edge + trailing-coalesce, see below
table.waitFor(pred, timeoutMs) -> Promise<readonly Row[]>
table.value(col, keys) -> unknown
table.ready / table.seq / table.reconnects
for await (const rows of table) { ... }  // AsyncIterable<readonly Row[]>, latest-wins buffer -- see below
table.close() / await using / table[Symbol.asyncDispose]()
```

Auth: `POST /api/auth/login`, token cached ~11h, re-minted **once** on a 401 then rethrown as
`AuthError`. Typed errors: `StreamsForgeError` (base), `AuthError`, `SqlError` (`.diagnostics` with
line/column, `.message` renders a caret against the offending SQL line), `IngestRejected`
(`.rowErrors`), `NotReady` (a `LiveTable` never filled, or `waitFor`'s predicate never matched --
the common cause is a brand-new table with no backfill).

## Change-notification latency and backpressure

**The coalescing window.** Every applied delta batch could, in principle, fire its own
notification -- but a firehose of tens of thousands of deltas/sec would then fire one callback per
delta and melt the consumer. So `LiveTable` coalesces with a `flushMs` window (`TableOptions.flushMs`
/ `SqlOptions.flushMs`, **default 16ms** -- one frame at 60Hz, the natural ceiling for a UI consumer
that cannot display more than one frame per 16ms anyway): if at least `flushMs` has elapsed since the
last emit, a batch is delivered **immediately** (leading edge -- no timer, no wait); otherwise it's
merged into a single pending emit fired at `lastEmit + flushMs` (trailing coalesce), and any further
batches arriving before that instant merge into the same pending emit. `flushMs: 0` disables
coalescing entirely and emits synchronously per applied batch.

This **replaces** an earlier unconditional 120ms trailing-only window (every version before 0.2.0):
that older scheme scheduled a timer on the FIRST touched batch and only ever emitted when it fired,
which meant a lone update on an otherwise-quiet table -- precisely the case where coalescing buys
nothing -- was always delivered up to 120ms late. The leading-edge/trailing-coalesce scheme fixes
exactly that case while still protecting against a firehose. **This is a behaviour change**: code
that happened to rely on updates always landing in >=1 batch-sized clumps, or on a fixed ~120ms
delivery latency, will now see updates land sooner and more often as individual emits.

**`onChange` vs the AsyncIterable -- two different consumption models, on purpose.** `onChange`
supports many independent listeners; each handler runs synchronously on the reader loop that also
drains the transport, so **a handler must not block** (no synchronous heavy work, no unresolved
promise it awaits inline) -- the loop cannot pull the next delta batch off the wire until every
listener for the current emit has returned. A handler that throws is caught and logged
(`console.error`), not left to crash the reader loop or take down other listeners. The
`AsyncIterable` is the other shape: **one owner**, pulling via `for await`, and as of 0.2.0 its
internal buffer is **latest-wins with capacity 1** rather than an unbounded array -- a consumer that
stops calling `next()` for a while (a slow loop body) now sees the LATEST `rows` snapshot on its next
`next()` call, not a replay of every intermediate one it missed.

**Why a stream of state snapshots must not be treated as a back-pressured queue.** Each emission
carries the table's full current `rows`, not a delta -- so a snapshot a consumer hasn't gotten to yet
is not lost work, it's just stale. Two things follow: blocking the reader loop until a slow consumer
catches up is not an option, because the loop's other job is draining the transport, and falling
behind there risks the transport's own buffers or connection; and buffering every snapshot a slow
consumer hasn't drained yet is pointless work for a result nobody will read once a newer one exists.
Capacity-1 latest-wins is the only sane middle ground: the consumer always gets the truth as of "now"
whenever it next asks, and memory use for the buffer is O(1) regardless of how fast deltas arrive or
how slow the consumer is.

## The Z-set reducer

`zset.ts` is a line-for-line port of `web/src/hooks/useTableRows.ts`'s reducer (this repo's own
console) -- same canonical-row identity, same group-key supersession, same content-based
"already reflected" replay heuristic for the subscribe/snapshot race. Read its module doc comment
before touching it; the hazards it defends against are non-obvious by design (arrival order isn't
guaranteed, the snapshot's `seq` and the delta stream's `seq` are different counters on different
scales). `bun test test/conformance.test.ts` runs it against
`../conformance/zset-cases.json`, the cross-language conformance suite every StreamsForge client
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
`StreamsForge.Host` output directory to skip the ~2-minute `dotnet publish` on every run.

## What the Python design doc got wrong for this port

- It assumed a Node client would need to hand-roll SignalR's wire protocol the way the Python
  client's `_hub.py` does (Python has no first-party SignalR client). It doesn't: `@microsoft/signalr`
  already implements all three wire modes, so `signalr-transport.ts` is a thin adapter, not a
  protocol implementation.
- §3.4's "the token goes in the query string only for the `ws` mode, not the two HTTP modes" is a
  Python-vs-browser distinction (`httpx` can send headers on negotiate/SSE/long-poll; a browser's
  `EventSource` cannot). `@microsoft/signalr` handles this internally regardless of environment --
  not a decision this client makes itself.
