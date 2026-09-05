# 025 — Dapr parity: close every owed item in `dapr/PARITY.md`, and make "verified live on Dapr" true

Status: **DONE** (2026-09-05, two waves, 5 + 2 agents plus orchestrator fixes). Everything in
`dapr/PARITY.md` § 2 is closed in code and the Live suite verifies it on a real sidecar; what is still
NOT verified live is listed by name in that file's § 3.

## Why

`dapr/PARITY.md` (2026-08-20) split the Dapr flavor's state into permanent descopes, debt, and a third
section — "unverified" — that was the largest: the Dapr host could not even boot for 164 commits
(`db01ab1` → `32ea87e`), this machine had never run `dapr init`, and the flavor had no isolated-instance
mode, so every "verified live on Dapr" sentence since plan 009 was written by someone who had not run
it. Plans 023 (source stability) and 024 (TLS) then landed Orleans-first and added four more owed
items (D6) plus a whole TLS surface the Dapr host did not have. The user asked for the Dapr flavor to
be brought level with Orleans — code AND verification — after the TLS work.

## What "parity" means here (scope)

Everything in PARITY.md § 2 (debt) and the TLS surface from plan 024; NOT § 1 (permanent descopes:
partitioned execution, frontier reads, shared arrangements, per-key sharding, cluster-aggregated
ingress stats, the Orleans stream-transport knob — those stay refused by decision D-F/011 D1). Also
NOT a Dapr `crdt` document runtime (plan 020 D9 keeps that Orleans-first; PARITY D5 asks only for an
honest Failed status, which this plan delivers).

## Decisions

| # | Decision | Instead of |
|---|---|---|
| D1 | The seven gRPC services (`Source/Pipeline/Table/Stream/Ingest/DynamicStream/ServerReflection`) MOVE from the Orleans host into `shared/StreamsForge.Api/Grpc/`, retargeted from `IClusterClient`/`IRegistryGrain` to the environment-scoped `ICatalogFacadeFactory` both hosts already register, plus ONE new runtime primitive, `IEntityStreamFacade` (subscribe one entity's live stream for the life of one RPC call). Orleans implements it over its stream provider; Dapr over an in-process per-key fan-out fed by the fixed pub/sub topics. | Duplicating ~1,500 lines of host-only gRPC code into the Dapr host, which is how PARITY D1 read the job. Behaviour-preserving on Orleans: every existing test stays green unmodified (hard rule 1). |
| D2 | `sf-pipeline-out` becomes a generic `IPipelineResultsSink` fan-out like the other two raw-ingress topics (bridge + NATS publisher become entries), because two new consumers need it at once: table-over-pipeline routing and the gRPC entity fan-out. | Two more direct concrete-type calls at the endpoint. |
| D3 | Late-consumer replay on Dapr mirrors Orleans' protocol exactly (`IConnectorActor.BeginAttachAsync`/`EndAttachAsync`, the shared `SourceReplayBuffer` ring, hold-and-flush, 10 s safety timer). The subscriber kinds (grpc/nats/fix/transports), which published from a background thread, now marshal their rows onto the actor turn through one more actor method so the hold covers every kind — one proxy call per batch, which those callbacks already paid for bookkeeping. | A polled-kinds-only protocol with a documented hole. |
| D4 | Boot becomes ONE coordinated pass — Running pipelines → Running tables (topo by table inputs) → enabled sources, per environment — behind a `BootGate` the four self-healing supervisors await (bounded) before their first sweep. The supervisors stay exactly what they were: 15 s self-healing, not the resume mechanism. | Adding ordering inside each independent sweep, which cannot express "pipelines of environment X before sources of environment X" across four services. |
| D5 | The SignalR source relay is PACED, not sampled: the Orleans rule (50 ms slot, `Task.Delay` the remainder, 40-deep streak cap that degrades a sustained firehose to sampling) ported into a clock-injectable pacer; the test that pinned "exactly one relayed event per batch" is rewritten to "three for three, in order" — a behaviour decision, recorded here. | Keeping the drop rule because a test pinned it. |
| D6 | Table-over-pipeline on Dapr: `CatalogStore` publishes `PipelineDefinition.OutputFields`, offers pipelines as relations, splits `PipelineInputs`, and enforces the same three name refusals as Orleans (a pipeline may no longer take a source's or table's name — the doc comment that called `trades FROM trades` legal was rewritten with the reason: a table's SQL resolves a relation name to exactly one entity). `TableEventRouter` routes `sf-pipeline-out` by BARE pipeline id (a GUID, already unique — `PipelineActor` publishes it unqualified) to a new `ITableActor.ProcessPipelineResultsAsync`. | Leaving validate optimistic and create refusing. |
| D7 | `crdt` on Dapr gets its Failed status from the connector-status facade (synthesized, no actor activated), carrying the plan 020 D9 message. | Building a `CrdtDocActor` just to hold a status. |
| D8 | An isolated Dapr test instance exists: app-id `streamsforge-dapr-test`, `dapr/components-test/` (statestore + pubsub scoped to it, both on Redis **DB 1**), app 5799 / gRPC 5899 / sidecar 3799/4799, a dedicated placement container `dapr_placement_test` on 6150 when placement cross-routes actor types across app-ids (measured by the wave, see outcomes). `docker exec dapr_redis redis-cli -n 1 FLUSHDB` is that instance's `reset.sh`. | Every Dapr live check running against the shared `:5399` instance, which is why they kept being skipped. |
| D9 | TLS on the Dapr host mirrors plan 024's Orleans shape (`Tls:Enabled`, `Kestrel:Certificates:Default`, both listeners, fail-fast, HSTS via the shared middleware, `OutboundTls.Configure`); the one Dapr-only fact is that the sidecar itself calls the app port, so a TLS app port needs `dapr run --app-protocol https` — recorded in `run.sh` and the docs. | A cleartext sidecar-facing port next to a TLS public port (two listeners for the same REST surface). |
| D10 | `instance.json` stays a file in `DataDir` on Dapr (PARITY D3 resolved by documentation): the directory is load-bearing for exactly one file, and `reset.sh` does not touch it — which is the behaviour an operator wants (a reseed keeps the instance identity peers already know). | Moving identity into the statestore, which would make `reset.sh` reissue the identity. |

## Waves

- **Pre-wave** (orchestrator): `IPipelineResultsSink` fan-out; `IConnectorActor.BeginAttachAsync`/
  `EndAttachAsync` + `SourceAttachSnapshot` pinned and stubbed (a7c5092); `dapr init` on this machine
  (runtime 1.18.3); baseline boot + smoke of `:5399` (login, catalog 6/7/5, `positions` filling).
- **Wave 1** (five agents, disjoint files, worktrees): A (Opus) D3 producer side + pipeline consumer
  side + D4 boot; B (Sonnet) D6 + table consumer side + source lifecycle events; C (Sonnet) D5 + D7;
  G (Opus) D1 + D9 with live proof on `:5399/:5499`; D (Sonnet) D8 harness + seven live tests.
- **Wave 2**: live tests for D3/D4/D6/D9 and federation on the D8 harness; docs (`PARITY.md` rewrite,
  `dapr/ARCHITECTURE.md`, `AGENTS.md`, `orleans/docs/index.html` TLS + comparison); final gates.

## Outcomes

### Pre-wave (orchestrator, 2026-09-05)

- `dapr init` (runtime 1.18.3; the machine had a slim install from 2026-08-11 with no containers —
  `dapr uninstall` first). `cd dapr && ./tools/run.sh` boots; login, catalog 6/7/5, `positions` at 8
  rows / 472 deltas after ~30 s. First credible Dapr live baseline since plan 009.
- a7c5092: `IPipelineResultsSink` + `IConnectorActor.BeginAttachAsync`/`EndAttachAsync` pinned.

### Wave 1 (five agents, merged 51eed72 → 4b6a744 → G → B → D, post-merge fixes 07833a4, e1a2d34)

Per-agent gates, all `dotnet test dapr/StreamsForge.Dapr.sln` whole-suite green in their worktrees;
merged master: **1636 passed / 0 failed / 52 Docker-skipped** (`Dapr.Tests` 509), then 502 unit +
7 Live after the actor-wire fix. Orleans (agent G, behaviour-preserving move of the gRPC services):
`Host.Tests` 333/333 on the gRPC/dynamic/reflection/proto/meta/discovery/TLS filter, `Chain.Tests`
11/11 (GrpcChainTests + TlsChainTests spawning real hosts), TypeScript client 58/60 (the 2 fails are
the `LiveSmoke` tests that need the demo container on :6199; `tls-live` connected over gRPC/TLS
against a published single-file host), Orleans `dotnet publish` boots and still serves
`/api/meta/protos/static` + `grpcurl list`.

Live on the merged build (`:5399`, orchestrator):
- gRPC: `grpcurl -plaintext 127.0.0.1:5499 list` → six services; `/api/meta/instance` →
  `endpoints.grpc`, `capabilities: ["grpc"]`. Agent G additionally streamed `SubscribeTable positions`,
  `SubscribeSource trades`, `SubscribePipeline`, `SubscribeEntity table:…` and round-tripped a gRPC
  `Ingest` (`{"accepted":1}`, the row arriving on a concurrent `SubscribeSource`).
- TLS (agent G): `--app-protocol https` is the one sidecar flag; https healthz 200, plain http exit 52,
  https endpoints in meta, `grpcurl -cacert` streaming, seeded table `seq 12 → 17` over 15 s of topic
  traffic through the https app port. Without the flag daprd loops on "waiting for application to
  listen on port 5399" forever.
- Late-consumer replay: enabled `file` source (300 rows, dedup `id`) first, status
  `eventsEmittedTotal == 300`, THEN table created + started → **300 rows, ids 0..299 exact**; append
  200 → **500 exact**.
- Table-over-pipeline: `parity_pipe` (`SELECT symbol, price, qty FROM trades`) → `outputFields`
  present; table `FROM parity_pipe LATEST BY (symbol)` → `pipelineInputs: ["parity_pipe"]`, 7 rows.
  The seeded `VWAP by symbol (5s)` pipeline had `outputFields` filled by the init-time backfill.
- Name refusals: table-vs-pipeline 409 with the Orleans message; source-vs-pipeline and
  pipeline-vs-source came back **500 with an empty body** — found and fixed (e1a2d34): the
  `InvalidOperationException` crossed the actor wire as `ActorMethodInvocationException`, so the
  shared endpoints' `catch` never fired; `UpsertSourceAsync`/`CreatePipelineAsync`/
  `UpdatePipelineAsync` are now `ActorResult`-wrapped like the table methods. Every pre-existing
  refusal on those paths (a `.` in a source name, for one) had been a bare 500 on Dapr too. After the
  fix: 400 / 409 / 400 with bodies.
- Boot: `boot resume for environment '' — 4 pipeline(s), 3 table(s), 6 source(s).` on every start.
- Live harness (agent D): 7/7 in 1 m 26 s on the merged build. Placement is NOT app-id-scoped —
  two app-ids hosting the same actor types on one placement service produced in-flight lock
  timeouts, i/o timeouts on actor calls routed to the other host, and a fresh app reading `[]` from
  its own just-seeded catalog — hence the dedicated `dapr_placement_test` container on 6150.

Found on the way, not fixed here:
- The replay ring is per-activation (as on Orleans), and Dapr deactivates idle actors on its own
  schedule; a connector with a live timer or subscriber is not idle, but the window is wider than
  Orleans' collection age.
- Subscriber-kind publish failures used to propagate into the subscriber core's reconnect backoff;
  routed through the actor's single publish door they are now a logged warning and a dropped batch —
  the polled path's at-least-once semantics, applied uniformly (agent A's report).
- Three of five wave-1 worktrees branched from `2df93dc` instead of `a7c5092` (see memory note);
  agents A and B noticed and fast-forwarded themselves, G rebuilt the missing seam by concrete-type
  calls (reconciled at merge), D was fast-forwarded by the orchestrator before it started.

### Wave 2 (agents L and W, merged; orchestrator fixes)

- **Live tests (agent L, `dapr/tests/StreamsForge.Dapr.Live.Tests`)**: 24 facts now — wave 1's seven
  plus `LateConsumerTests` (300 → exactly 300, +200 → exactly 500; a pipeline created late gets ≥ 300
  because `PipelineActor` re-delivers its replay on every (re)start and its `_totalRowsOut` counter is
  not reset — at-least-once by construction, exactness recovered downstream by a `LATEST BY` table),
  `TableOverPipelineTests` (100 rows through pipeline → table, the three refusals with codes AND bodies,
  pipeline deleted under a running table), `BootOrderRestartTests` (url source + dedup key survives a
  restart; the boot-pass log line is present with a non-zero source count and no supervisor timed out
  waiting), `TlsHostTests` (five facts, `--app-protocol https`, `grpcurl -cacert`), `GrpcServingTests`,
  `FederationTests` — **both directions, 150/150 exact**: Orleans file source → Dapr `grpc` source →
  Dapr table, and Dapr file source → Orleans `grpc` source over `:5899` → Orleans table, a direction
  that did not exist before this plan. Agent L's own gate: 23 passed / 1 skipped, twice in a row
  (5 m 44 s / 5 m 26 s). One structural fix: xunit builds a test class per fact, so wave 1's
  `IAsyncLifetime`-on-the-class shape boots one Dapr instance per fact; L's classes use an
  `IClassFixture` (`LiveHostFixture`), and `DaprHostProcess.StopAsync` now reaps an orphaned app when the
  `dapr run` CLI exits without it.
- **The one skip was a real Dapr-only data-loss bug, found by L and fixed by the orchestrator.** A live
  source batch above ~10 rows into a Running pipeline produced `totalEventsIn N, totalRowsOut 0` with
  nothing logged (Orleans: 100/100 on the identical scenario). Reproduced on `:5399` with timing: 50
  rows with one `_ts`, visible to the table 2.9 s later, pipeline 50 in / 0 out. Two causes:
  1. The paced SignalR relay (D5) awaited its 50 ms slot INSIDE the `sf-sources` endpoint, whose sinks
     run sequentially with the bridge first — so every row waited 50 ms × N before the pipeline/table
     routers saw the envelope. On Orleans the bridge is an independent stream subscriber; the Dapr port
     had put a console nicety on the data path. `DaprStreamBridge` now enqueues into a bounded channel
     and paces on its own task (`IdleAsync()` for tests).
  2. Even off the bridge, Dapr's pub/sub + two sidecar hops exceed the Engine's 1 s lateness allowance
     under CPU pressure: with whole-solution builds running alongside, the SEEDED generator pipelines
     were discarding 15–25 % of their input as late, and a 100-row batch arrived 24 s late once.
     `PipelineActor`'s wall-clock watermark tick now runs `Pipelines:TransportLagAllowanceMs` (default
     4000) behind; `0` restores Orleans-identical timing. Verified: the same 100-row batch at load 173,
     delivered 2.3 s after its stamp → 100 in / 100 out / 0 late.
  And the loss is visible now on both flavors: `PipelineMetrics.LateEvents` (`[Id(8)]`, additive;
  `lateEvents` on `GET /api/pipelines/{id}/metrics`). L's `[Fact(Skip)]` became
  `A_single_large_live_batch_into_a_pipeline_lands_in_full`.
- **Found by the orchestrator's own gate, not a product bug**: the first Live run after the fix lost its
  five TLS facts to the orchestrator's `pkill -f <dll path>` aimed at the `:5399` dev instance — the
  pattern also matched the test fixture's app. Kill by port, not by binary path, when two instances run
  the same DLL.
- **Docs (agent W)**: `dapr/PARITY.md` rewritten — § 2 is closed debt with pointers, § 3 an honest
  by-test-name table of what is verified live and what still is not (database connectors/CDC on Dapr
  are Docker-gated, FIX/fix-duplex, NATS incl. its TLS group, peer-name discovery FROM a Dapr instance,
  SignalR pacing live, D2 table-over-table live); `dapr/ARCHITECTURE.md` § "Dapr parity (plan 025)";
  `AGENTS.md` (ports, TLS both flavors, hard rule 6 no longer says validate is optimistic on Dapr);
  `orleans/docs/index.html` + `comparison.html`; the `orleans/src/StreamsForge.Host/Grpc/**` path
  references; the `sf-run`/`sf-verify`/`sf-client-gen` skills. `landing/public/docs/*` is a published
  snapshot from the rebrand commit and was deliberately not re-synced.
- **Gates on merged master** (this machine at load 80–300 throughout, agents building alongside):
  Dapr solution 1636 → after wave 2: unit 502 (`Dapr.Tests`) + 560 + 373/52 skipped + 122 + 52 + 20,
  all green. Orleans whole solution: `Engine` 994, `AppCore` 560, `Chain` 11/11, `Host.Tests` 1578
  passed / 20 failed in 31 m 56 s — 6 are the known `CodecNotFoundException` set (plans/022), the other
  14 were re-run in isolation: 15/16 passed, the one failure being `RepeatedKeylessRowsReads_
  DoNotWakeIdleShards`, i.e. one of the known six. A second filtered Orleans run after the `LateEvents`
  change lost `TableOverPipelineClusterTests.A_table_reads_a_pipeline_by_name…` and
  `TlsChainTests.Every_row_crosses_a_TLS_gRPC_hop…` once each; both passed in isolation (6/6, 2/2).
  TypeScript client: 56/60 whole (2 `LiveSmoke` facts need the demo container on `:6199`; the two
  `signalr:ws|sse no delta lost` facts are 10 s deadlines that passed 4/4 alone at load 140).
- **Final Live run on the fixed build** (`dotnet test dapr/StreamsForge.Dapr.sln --filter Live`,
  orchestrator, load ~150): **24 passed / 0 failed / 0 skipped in 4 m 51 s.**

## Found and not fixed

- `PipelineActor` re-delivers its attach replay on every (re)start and does not reset `_totalRowsOut`,
  so a pipeline's out-counter is at-least-once across a supervisor-driven restart; a downstream
  `LATEST BY` table stays exact. Cosmetic on the counter, real on a non-keyed consumer of
  `GET /api/pipelines/{id}/results` — the Orleans grain has the same shape.
- Federation Dapr → Orleans lost one row once (149/150) with a 1 s cushion after the subscriber's
  `lastStatus: "ok"`, none with 3 s: the producer's first publish can beat the subscriber's registration
  into `EntityStreamFanout`. The test waits 3 s; the product-side fix would be a subscribe-then-ack
  handshake in the gRPC `SubscribeEntity` path, same on both flavors.
- The replay ring is per-activation and Dapr deactivates idle actors on its own schedule (wave 1 note).
- The seeded generator pipelines on Dapr still discard input as late whenever the sidecar hop exceeds
  5 s (the new allowance) — measured once at load ~150 with builds running; `lateEvents` is the number
  to watch. A larger default is a deployment decision, not a code one.
- Still unverified live on Dapr (by test name, `dapr/PARITY.md` § 3): database connectors and CDC
  (Docker-gated on both flavors), FIX / `fix-duplex`, NATS including its `tls` group, peer-name discovery
  initiated FROM a Dapr instance, SignalR pacing against a real browser, D2 table-over-table warm attach.
- `landing/public/docs/*` is a snapshot from the rebrand commit and still describes gRPC on Dapr as
  "phase 2"; it is not synced from `orleans/docs/` by anything.

