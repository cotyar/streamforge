# Dapr parity: what is deliberately absent, what is owed, what was never checked

**Working rule as of 2026-08-20.** The Orleans flavor is the primary runtime. New plans land there
first and are verified live there; the Dapr flavor is brought level afterwards, and this file is the
list that "afterwards" works from. Nothing here says the Dapr flavor is broken — it builds, its suite
is green, and its host boots. It says which claims about it are *earned* and which are not.

This file has three sections and the split between them is the point:

1. **Permanent descopes** — never coming to Dapr, by decision. Do not file these as debt.
2. **Debt** — implemented on Orleans, absent or stubbed on Dapr, intended to be closed.
3. **Unverified** — code exists on both flavors, but nobody has run the Dapr side live. This is the
   largest section and the least visible one, because unverified code looks exactly like verified
   code from the outside.

---

## 1. Permanent descopes — not debt

`dapr/ARCHITECTURE.md`'s "What's NOT here yet (by design)" section is the authority; this is the index.

| Descoped | Where it is refused | Decision |
|---|---|---|
| Partitioned table execution (`Parallelism` 2–16, the ingest/stage/output grid) | `Catalog/CatalogStore.cs` `ValidateParallelism` → 409; defensively again in `Actors/TableActor.cs` | 005 D-F |
| Frontier-consistent reads (`frontierEpoch`) | `Facades/StubFacades.cs` — always `null` | 005 D-F |
| Shared arrangements, `GET /api/meta/arrangements` | `EmptyArrangementMetaFacade` — always `[]`, explicitly *not* a stub awaiting a wave | 005 D-F |
| Per-key table sharding (`ShardBy`) | refused at **start**, not at upsert, so the definition still round-trips and can be promoted back to an Orleans instance without loss | 011 D1 |
| Table text search (`SearchEnabled`) on a sharded table | inherited from sharding | 011 D1 |
| Cluster-aggregated ingress stats | `Ingest/DaprIngressFacade.cs` returns the local view with `Aggregated=false` | 009 D-F |
| `IngestStatus.DownstreamDropped` | always `0` — there is no Dapr equivalent of `PushStreamBus.TotalDropped` to observe | 009 |
| `--Streams:Transport push\|pull` | an Orleans stream-provider knob; Dapr's transport is Redis pub/sub and has nothing to select | — |

Plan 020's `crdt` source kind is declared Orleans-first (020 D9). It is a **descope with an escape
hatch**, not a permanent one — see debt item D5 below for what the Dapr side does today and what it is
missing.

---

## 2. Debt — owed to the Dapr flavor

### D1 · gRPC serving on `:5499` — the port is reserved and nothing listens

`dapr/src/StreamForge.Dapr.Host/Program.cs` passes `GrpcStaticServices: []` and Kestrel maps only the
HTTP app port. The Orleans host maps seven gRPC services on `:5299`
(`orleans/src/StreamForge.Host/Program.cs`). Called "phase 2" since plan 005, never revisited.

Three consequences that read as separate bugs if you meet them without this context:

- **`GET /api/meta/instance` reports a Dapr instance as gRPC-incapable.** `servesGrpc` is derived
  purely from `GrpcStaticServices.Count > 0`
  (`shared/StreamForge.Api/Endpoints/MetaEndpoints.cs`), so the `endpoints.grpc` key is omitted and
  `"grpc"` never appears in `capabilities`. A peer discovering a Dapr instance cannot federate *from*
  it. This is correct behaviour — it refuses to advertise a port nothing is listening on — and it is
  also the visible face of this debt item.
- **Dapr cannot be a federation server.** It subscribes to any instance (proven live in plan 005 W6,
  Dapr `:5399` subscribing the Orleans flavor's `positions` table); it never accepts `SubscribeEntity`.
  ⚠️ The repo labels the *cause* "phase 2" (debt) and the *consequence* "stays Orleans-only"
  (permanent). Both trace to decision D-F. Whoever picks this up decides which label is true and makes
  the two agree; today they do not.
- **The four generated clients lose their gRPC transport against Dapr.** `/proto` downloads work
  (the descriptor machinery is shared and runtime-agnostic), so codegen succeeds — the generated gRPC
  client simply has nothing to dial. The TypeScript client's `"auto"` transport falls back to SignalR;
  gRPC ingest has no Dapr equivalent at all, REST ingest does.

### D2 · Table-over-table warm attach — Orleans admits the snapshot, Dapr only warns

Orleans' `TableGrain.AttachToTableInputAsync` does a real atomic `(rows, LastEpoch)` read of the
upstream table and admits the rows. `Actors/TableActor.cs` on Dapr still ships option (b): a loud
diagnostic, no back-fill. Its own doc comment names both blockers — no `AttachSnapshotAsync` on
`ITableActor`, and `DaprLifecycleOrchestrator.StartTableAsync` registers the event router *after*
`StartAsync` returns, so there is no subscribe-before-attach point to hook — and hands off explicitly
to whoever next owns `ITableActor.cs` / `TableEventRouter.cs` / `DaprLifecycleOrchestrator.cs`. The
wire half is already done: `TableDeltaDto.Epoch` is stamped on every batch this actor publishes.

This is the clearest genuinely-scoped debt item in the repo. Closing it is three files, not a design.

### D3 · `instance.json` lives in a `DataDir` this flavor otherwise does not use

Plan 016 wave 5 persists instance identity to a file. Dapr keeps everything else in Redis, so deleting
that directory silently reissues the instance's identity — on Orleans deleting `data/` *is* the
documented reseed, on Dapr it is an unrelated directory with one meaningful file in it. Either move
the identity into the statestore or document the directory as load-bearing on this flavor.

### D4 · One stale comment, corrected in place

`Facades/StubFacades.cs` claimed `CatalogStore.CreateTableAsync` "refuses a non-empty `ShardBy` at
upsert (the same way it refuses Parallelism > 1)". It does not — `ShardBy` is refused at **start**,
deliberately, so the field round-trips. The behaviour was right and the comment was wrong; the comment
is fixed as of this document.

While fixing it: the long "WHERE KEY SHARDING IS REFUSED ON THIS FLAVOR" explanation in
`Catalog/CatalogStore.cs` is an **orphan** — it sits as a second stacked `<summary>` on
`ValidateParallelism`, a method about something else, because there is no `ValidateShardBy` for it to
document (the refusal lives in `TableActor.StartAsync`). Left as found: it is the best explanation of
the asymmetry in the repo and moving it is a bigger edit than this document's scope. Anyone reading
`ValidateParallelism` should know the first paragraph above it is not about `ValidateParallelism`.

### D5 · The `crdt` source kind is refused, but without a status to refuse into

Plan 020 D9 is Orleans-first, so this is expected — the entry exists because the refusal is currently
*less* honest than the `ShardBy` precedent it copies. `Lifecycle/DaprLifecycleOrchestrator.cs` logs an
error when an enabled `crdt` source is seen and does nothing else; the definition is stored so an
Orleans export imports intact. What is missing is a **Failed status carrying that message**: a sharded
table gets one because `TableActor` exists to hold it, and a `crdt` source has no actor on this flavor at
all. Until a document runtime lands here, the intended escape hatch is plan 006's cross-flavour link — a
Dapr instance subscribing to a document projected by an Orleans one over a `grpc` source.

The intake endpoint answers **501**, not 404, via `ICrdtFacade.Enabled` — deliberately, so an operator
can tell "this build cannot do that" from "you typed the wrong source name".

### Explicitly NOT debt — checked and present on Dapr

Plan 014 database connectors, 015 entitlements/approvals/audit (`AccessPolicyActor`, `ApprovalActor`,
`AuditLogActor`, and the shared sweeper), 016 peer discovery + `@name` endpoints, 017 CDC, 018 FIX,
019 `fix-duplex`, 021 environments (`EnvironmentRegistryActor`, `CatalogFacadeFactory`). All wired in
`dapr/src/StreamForge.Dapr.Host/Program.cs`. Present is not the same as verified — see section 3.

---

## 3. Unverified — the honest state of every "verified live on Dapr" claim

### The window: `db01ab1` → `32ea87e`

Plan 009's `db01ab1` added two C# **optional parameters** to
`IConnectorActor.RecordSubscriberBatchAsync`. Dapr's actor-interface validation forbids optional
parameters outright and threw inside `MapActorsHandlers` during startup, so the host died before
serving its first request. It was found and fixed in plan 021 wave 1 (`32ea87e`) by making both
parameters required.

**164 commits sit between those two.** For that entire span the Dapr host could not boot. Any claim
in that window that the Dapr flavor was verified live is therefore false, whatever it says — not
dishonest, just written by someone who did not run it.

| Plan | Dapr live claim | Reading |
|---|---|---|
| 009 (post-`db01ab1` waves) | gate says "a live check on isolated ports", flavor unspecified | suspect |
| 010, 011, 012 | none found | fine — nothing claimed |
| 013 CLI/MCP | claims parity "with nothing but a different base URL" | plausible but untested against Dapr |
| **014 DB connectors** | **wave L's gate is literally "live sweep on both flavours"** | **impossible in this window; no outcome section records it happening** |
| 015 entitlements | many "verified live" claims, all Orleans-shaped | the Dapr access/approval/audit actor path is entirely unexercised |
| 016 identity/discovery | federation verified live "between two instances" — both Orleans | Dapr's peer directory + `@name` wiring unexercised |
| 017 CDC | live-DB suite is transport-level and flavor-independent | the Dapr polled arm was never driven |
| **018 FIX** | **self-reports the gap in the plan itself**: no `dapr init` on this machine, the Dapr claim rests on identical wiring plus a green suite | **the pattern every other plan should have followed** |
| 019 fix-duplex | live check used a real acceptor against an Orleans instance | Dapr's duplex twin is unit-tested only |
| 021 environments | Verification demands the same script run against both flavors; recorded evidence is Orleans-file-shaped (`catalog.registry_staging.catalog.json` — JSON grain storage, not Redis keys) | Orleans half done, Dapr half open |

**Net: no Dapr live verification recorded anywhere in this repo after `db01ab1` can be trusted, and
none has been recorded since the fix.** The last credible live evidence predates plan 009 — the W5–W9
sections of `dapr/ARCHITECTURE.md` (the Redis key listing, the `409 Parallelism` response body, the
cross-runtime federation counters).

### Why this was not simply fixed by running it

Two independent obstacles, and the second one is structural:

1. **This machine has never run `dapr init`.** No `dapr_redis` / `dapr_placement` / `dapr_scheduler`
   containers exist. Running it pulls images and creates long-lived containers on the user's machine,
   which is theirs to authorize.
2. **The Dapr flavor has no isolated-instance mode.** `dapr/components/statestore.yaml` pins
   `scopes: [streamforge-dapr]`, so an isolated `--app-id` test instance — the pattern AGENTS.md
   prescribes for Orleans' 6xxx–9xxx port instances — has no statestore in scope for actors at all and
   **panics the 1.18 sidecar**. Every Dapr live check must therefore run against the shared fixed-port
   `streamforge-dapr` instance on `:5399`, reset via `tools/reset.sh` first. That is a much heavier
   gate than Orleans', which is a large part of why it kept being skipped.

### How to close it

```bash
dapr init                      # one-time; pulls images, creates the three containers
cd dapr && ./tools/reset.sh && ./tools/run.sh   # :5399, sidecar 3599/4599
```

Then, in rough order of how much each buys:

1. **Boot + smoke**: `/healthz`, login, catalog CRUD. Re-establishes a credible baseline for the first
   time since plan 009.
2. **Plan 021 environments** — the one open cross-flavor requirement from the most recent plan. Create
   `staging`, create a same-named table in both environments, confirm two separate Redis keys rather
   than two filtered reads, force-delete, confirm no re-seed on restart.
3. **Plan 015 access path** — one grant, one revocation, one approval round trip, one audit read.
   Entirely unexercised on this flavor.
4. **Plan 014/017 connectors** — the "live sweep on both flavours" wave L promised.
5. **Plan 016 federation** — a Dapr instance discovering an Orleans peer by name.

Record what actually ran in each plan's own outcomes section, per flavor and by name. "Verified live"
without a flavor is the sentence that produced this document.
