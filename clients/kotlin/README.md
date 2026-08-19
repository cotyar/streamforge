# streamforge-client (Kotlin)

A coroutines-first Kotlin client for StreamForge, mirroring the Python client's API surface
(`clients/python`) and design (`apps/websites/otc-terms/docs/python-client-design.md` in the
`ac-co.ai-4` repo) but idiomatic to Kotlin: `Flow`/`StateFlow` instead of a reader thread,
`suspend` instead of blocking calls, structured concurrency instead of manual thread/queue
bookkeeping, a sealed exception hierarchy instead of one flat error type.

```kotlin
val sf = StreamForge.connect(url = "http://localhost:6199", user = "admin", password = "admin123!")
                                                                         // transport = AUTO by default
val t = sf.table("trigger_monitor")            // subscribes, snapshots, replays; suspends until ready
t.rows                                          // List<Map<String, Any?>>, immutable snapshot
t.rowsFlow                                      // StateFlow<List<Row>>, for reactive consumers
t.waitFor(30.seconds) { rows -> rows.isNotEmpty() }
sf.snapshot("mc_path_pnl")                      // one-shot REST read, no subscription
sf.close()                                      // cancels every LiveTable this client created
```

## Transports

Two, behind one internal interface (`TableTransport`) -- `LiveTable` and the reducer never know
which is underneath:

- **gRPC (`Transport.GRPC`, default candidate under `AUTO`)** -- `io.grpc:grpc-kotlin-stub` +
  `protobuf-gradle-plugin` compiling a private copy of `streamforge.proto`
  (`src/main/proto/streamforge.proto`, `java_package`/`java_multiple_files` added for codegen
  ergonomics only -- the wire contract is untouched). `StreamService.SubscribeTable` for deltas,
  `TableService.Rows/List` for the snapshot/catalog, bidi `IngestService.Ingest` for `push()`. One
  insecure h2c channel (`usePlaintext()`), matching how the engine runs from source. Auth is an
  `authorization: Bearer <jwt>` metadata entry per call.
- **SignalR (`Transport.SIGNALR`)** -- `com.microsoft.signalr:signalr`, the official Java client,
  against `/hubs/stream`. `SubscribeTable` is a fire-and-forget `send()`; `tableDelta(name, deltas,
  seq)` arrives as a hub callback turned into a `Flow`. **The Java SignalR client only supports
  WebSockets and Long Polling -- it has no SSE transport**, unlike the browser/Python clients'
  three-way split (ws/sse/lp). That is a limitation of the library, not something hand-rolled
  around here, per the task brief. It negotiates and prefers WebSockets, falling back to Long
  Polling itself.
- **`Transport.AUTO`** tries gRPC first (a real `TableService.List` call, proving the channel AND
  the JWT work), falls back to SignalR on any failure, and always logs which one it got
  (`java.util.logging`, logger name `"streamforge"`) -- a client that silently degrades is worse
  than one that fails loudly. When gRPC is refused, the likely cause is the host having been
  started with `--urls`, which trips `Program.cs`'s guard so no gRPC port is bound at all -- start
  it with `--Http:Port`/`--Grpc:Port` instead.

### A load-bearing detail: `subscribeTable` is `suspend`

`TableTransport.subscribeTable(name): Flow<DeltaBatch>` is declared `suspend`, not a bare
Flow-returning function. This isn't decoration -- a first implementation returned a lazily-cold
`Flow` (SignalR's connection setup deferred to whenever the `Flow` got collected), and the
"subscribe races the snapshot" hazard flip: the REST snapshot read routinely finished, and
`LiveTable` reported the table "ready" for pushes, *before* the SignalR negotiate/upgrade/hub
handshake had actually registered the subscription server-side. A push made right after `ready`
fired could be broadcast to a subscriber list that didn't include us yet, and since there is no
backfill on subscribe and nothing else re-asserts a row nobody pushes to twice, the live channel
would then wait forever. Making `subscribeTable` a suspend function that completes the slow part of
connection setup before returning closes that window; `LiveTable`'s existing buffer/replay logic
still handles the case where a delta *does* arrive during the snapshot read.

## Change-notification latency and backpressure

`LiveTable` publishes through `rowsFlow`, a `StateFlow<List<Row>>`, under a LEADING edge +
TRAILING coalesce window (`flushWindow`, a `Duration` parameter on `StreamForgeClient.table()` /
`.sql()`, default **16ms** -- one frame at 60Hz, the natural ceiling for a UI consumer that cannot
display more than one frame per 16ms anyway). This is a **behavior change** from an earlier
version of this client, which coalesced with an unconditional TRAILING-only 120ms window: every
delta batch, however isolated, waited out the full 120ms before publishing. The engine's
push-stream transport takes `tableDelta` from p50 115ms to p50 1ms (`--Streams:Transport push`;
Dapr ~7ms) -- a flat 120ms hold-back after every batch handed that entire win straight back to a
lone, quiet-table update, exactly the case where coalescing buys nothing.

Under the current window: if at least `flushWindow` has elapsed since the last publish, the next
applied batch publishes immediately -- no delay, no wait. Only a batch that lands *inside* the
window opened by the previous publish gets merged into a single pending publish, fired at
`lastPublish + flushWindow`; further batches inside that same window merge into the same pending
publish, so at most one publish is ever pending. `flushWindow = Duration.ZERO` turns coalescing off
entirely and publishes every applied batch on its own. The window exists at all because a firehose
of tens of thousands of deltas/sec would otherwise fire one `StateFlow` emission per delta and melt
a collector -- not because this repo's own hub-driven UI does anything similar:
`web/src/hooks/useTableRows.ts` publishes on every batch with no coalescing window of its own (its
900ms timer is an unrelated flash-highlight effect, not a change-notification delay).

`rowsFlow` is a `StateFlow`, which conflates by construction: assigning `.value` never suspends,
and a collector that falls behind only ever sees the *latest* value on its next resumption, not a
backlog of every intermediate one. That is exactly the right contract here, and requires no code
change to get -- `rowsFlow` is state snapshots (each publish carries the full current `rows`, not a
delta), not an audit log of every batch, so a slow collector skipping intermediate snapshots is by
design, not data loss. This is why a stream of state snapshots must never be modeled as a
back-pressured queue: blocking the reader coroutine on a slow collector would stall it from
draining the transport (and therefore from ever catching up), and buffering snapshots a collector
no longer wants to see is pure waste -- only the latest one is ever going to be read. The reader
coroutine's own trailing-coalesce wait (above) keeps draining the transport the whole time a
publish is pending, for the same reason: it never stalls to wait out a window in isolation.

## Reducer (`ZSet.kt`)

A literal port of the Python client's `_zset.py` (whose module docstring is the fullest account of
the hazards -- summed weights, group/supersession, the snapshot-race content heuristic). Tested
against the shared cross-language fixture: `src/test/kotlin/streamforge/ZSetConformanceTest.kt`
reads `../conformance/zset-cases.json` and runs the runner contract from that suite's README
verbatim. **All 14 cases pass.**

## Public surface

`StreamForge.connect(url, grpcTarget?, user?, password?, token?, ingestKey?, transport = AUTO):
StreamForgeClient`, then on the client: `table(name, keyFields?, timeout, flushWindow = 16ms)`,
`snapshot(name, limit)`, `tables()`, `search(name, query, limit)`, `validate(sql)`, `sql(sql, name,
keyFields?, timeout, flushWindow = 16ms)` (validate -> `POST /api/config/import?mode=merge` ->
`table()`), `adhocTables()` / `dropAdhoc(name)` (refuses any name outside the `adhoc_` prefix),
`push(source, rows, idempotencyKey?, partial)` (gRPC bidi when the live transport is gRPC, REST
otherwise). `flushWindow` governs the returned `LiveTable`'s change-notification coalescing -- see
"Change-notification latency and backpressure" below.

Errors are a sealed hierarchy (`Errors.kt`): `StreamForgeException` (sealed base) ->
`AuthException`, `NotReadyException`, `IngestRejectedException` (carries `rowErrors`),
`SqlException` (carries `diagnostics`, renders a caret under the offending column like the `/sql`
editor does).

Not ported from the Python client: the demo-specific `_keyfields.py` catalog (a hand-maintained
`table name -> key columns` map that is explicitly documented there as "copy #4 of the same list" --
wishlist #18 is what deletes all four; adding a fifth copy here felt like the wrong direction).
`keyFields` defaults to `null` (whole-row identity) unless the caller passes it explicitly -- never
guess a key.

## Build & test

```bash
gradle build                                  # compiles proto, main, tests; assembles the jar
gradle test --tests "streamforge.ZSetConformanceTest"   # offline, no engine needed
gradle test --tests "streamforge.LiveTableFlushTest"     # offline, no engine -- flushWindow behavior against a fake transport
gradle test --tests "streamforge.ContractTest"           # boots an isolated engine on 9199/9299
gradle test --tests "streamforge.LiveSmokeTest"           # read-only against localhost:6199
```

No standalone `kotlinc` is used -- `protobuf-gradle-plugin` downloads a matching `protoc` binary
and the `grpc-java`/`grpc-kotlin` protoc plugins from Maven Central, and the Kotlin Gradle plugin
compiles everything else. JVM target is 21 (`compilerOptions.jvmTarget` / `java.targetCompatibility`
pinned explicitly) so the build doesn't need a JDK 21 *toolchain* installed -- it compiles fine
under the ambient JDK 23.

### Contract-test fixture (`EngineFixture.kt`)

Ported from `clients/python/tests/conftest.py`. Boots `orleans/src/StreamForge.Host` in isolation:

- Ports **9199/9299** (overridable via `SF_TEST_HTTP_PORT`/`SF_TEST_GRPC_PORT` -- several client
  tasks share this repo and were briefed onto the same defaults before being split up). Never
  5199/5299 (the live dev server) or 6199 (the shared demo) -- the fixture asserts both ports are
  free first and skips with a clear reason (`Assumptions.assumeTrue`) rather than colliding.
- Reuses a prebuilt `dotnet publish` output via `SF_TEST_PUBLISH_DIR` (or a known scratchpad path
  from this session) when present, since publishing takes ~2 minutes otherwise.
- Runs the published DLL directly, **with its working directory set to the publish folder** --
  `WebApplication.CreateBuilder` resolves the content root from the current directory, not the
  assembly's, so running it from anywhere else leaves `Jwt:Key` null and every request 500s,
  `/api/healthz` included.
- Drains the child process's merged stdout continuously on a daemon thread into a bounded ring
  buffer (`EngineFixture.Drain`) rather than reading it only on failure. The OS pipe buffer is
  finite (64KB on macOS); a long contract-test run produces more log output than that, and an
  unread pipe's next write blocks forever -- an engine that "hangs mid-suite for no reason" is
  almost always this, not an unstable engine.

`ContractTest` shares one engine instance (and its two fixture tables) across every
`@ParameterizedTest` case; `@TestMethodOrder(OrderAnnotation)` pins `handshakeAndSnapshot` first so
its "a freshly-imported table is empty" assertion isn't racing other tests' pushes into the same
shared table -- JUnit5 does not otherwise guarantee method order the way pytest's file order does.

### Live smoke test

`LiveSmokeTest` is read-only against the demo already running at `http://localhost:6199`
(`admin`/`admin123!`) -- SignalR + REST only, since that instance was started with `--urls` and has
no gRPC port bound. It snapshots `trigger_monitor` and watches a live subscription's `seq` advance
over a few seconds; never mutates, restarts, or kills the demo.

## What the design doc's choices didn't carry over verbatim

- **Sync-first / reader thread -> coroutines-first.** The design doc chose threads for Python
  specifically because a Jupyter kernel already runs an event loop that `async def` would fight;
  none of that applies to a JVM library, so this client is `suspend`/`Flow` throughout instead.
  `LiveTable`'s subscribe/snapshot/replay/coalesce logic is a direct structural port of
  `live.py`'s, just expressed with a `produceIn`'d channel instead of a `queue.Queue` fed by a
  daemon thread, and a `CompletableDeferred` instead of a `threading.Event`.
- **`CancellableIterator`'s cross-thread `.cancel()` workaround doesn't exist here.** Python needed
  it because a generator's own `.close()` isn't safe to call from a thread other than the one
  iterating it. Cancelling a `Flow` collection is a first-class, thread-safe operation in
  structured concurrency, so `LiveTable.close()` is just `job.cancel()`.
- **SignalR's SSE mode is absent, not by policy but by library.** The design doc's `_hub.py` speaks
  all three SignalR wire modes by hand (ws/sse/lp) because it needed to for Python. The task brief
  for this client explicitly said to use the official Java client rather than hand-roll the
  protocol, and that library exposes WebSockets and Long Polling only.
