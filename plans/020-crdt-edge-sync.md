# Plan 020 — CRDT edge sync: offline-tolerant ingress on Yjs

**Status: PLANNED.**

**Depends on**: 010 (`IInboundTransport`, the registry seam this plan deliberately does *not* fit into —
see D3), 014 (the out-of-core connector project precedent, `StreamForge.Connectors.Database`), 015 for
wave D only (field-level authorization has nothing to hang off until entitlements exist).

**External dependency, new to this repository**: [`cotyar/ycs`](https://github.com/cotyar/ycs) — a fork of
the archived official .NET Yjs port, brought to Yjs 13.6.32 parity (v1 update encoding, `y-protocols/sync`,
XML types, `mergeUpdates`/`diffUpdate`/`encodeStateVectorFromUpdate`, snapshots, relative positions,
`PermanentUserData`) and cross-checked against real Yjs through a round-trip harness. Two PRs are open
upstream. Nothing of it is in this repository today.

## Why

Two customer shapes that the platform currently has no answer for, and that are the same problem
underneath:

- **An ERP integration across a link that drops.** The edge keeps producing changes while disconnected.
  Today every ingress kind either loses them (`nats` at-most-once, `fix`, `ingest`) or requires the *source
  database* to hold them (`postgres-cdc`'s slot, which is the hazard `TRANSPORTS.md` spends a section
  warning about). Neither is "the edge buffers its own work and reconciles on reconnect".
- **Digital twins whose connectivity is not guaranteed.** A twin is a *document* — an asset's current
  attributes, edited from more than one place — not an event stream. Replaying an ordered log is the wrong
  primitive; converging two independently-edited copies is the right one.

A CRDT solves exactly this and nothing else: concurrent edits made in ignorance of each other converge to
the same state without a coordinator, and re-delivering the same update is a no-op. What it does *not* do
is enforce domain invariants, and this plan is written to keep that boundary visible rather than sell
around it.

## What this is not

- **Not multi-master for the platform.** One document kind, one ingress seam. Tables, pipelines, SQL and
  the dataflow are untouched and stay CRDT-unaware.
- **Not a collaborative editor.** No `Y.Text` formatting, no cursors-in-a-document, no time-travel UI.
- **Not a general offline mode.** A `url` source does not become offline-tolerant because this lands.

## Decisions, and what they cost

**D1 · The CRDT does not enter the dataflow.** Differential dataflow needs a deterministic
`(data, time, diff)` stream with monotone logical time; a CRDT converges precisely by *not* having one. So
a `YDoc` never crosses into `StreamForge.Engine`, never appears in a plan, and the SQL dialect gains
nothing CRDT-shaped. What crosses is what every other source produces: `EventRecord` rows on
`(StreamConstants.SourcesNamespace, sourceName)`.

*Cost:* two representations of the same twin — the document (authoritative, mergeable) and its projection
(queryable, immutable-per-emit). They can disagree for exactly as long as one emit takes.

**D2 · The CRDT sits at ingress, never downstream.** A CRDT fed *from* a table closes a
table → CRDT → table cycle, which is the class of thing AGENTS.md's hard rule 3 exists to prevent. Ingress
is also where the property is actually wanted: the unreliable link is upstream of the platform, not inside
it.

**D3 · A new grain, not a new transport — and the reason is not preference.**
`IInboundTransport`'s seam is *bytes → rows through a named format parser* (`FormatOf` returns
`"json"`/`"csv"`/`"fix"`; `ConnectorPollCycle.ExecuteMessage` does the rest). A Yjs update is not rows in
any format: it is a delta against **stateful, durable, per-document** state, and it produces rows only
after being merged into that state. Bending it into `FormatOf` would mean a transport that secretly owns
persistence — the exact "second extraction path with its own subtly different NULL handling" that
interface's doc comment says the seam exists to prevent.

So `crdt` is a source kind dispatched to its own `CrdtDocGrain`, the way `generator` is dispatched to
`IGeneratorGrain`. *Cost:* a `crdt` source is not free the way a new broker is — it is a grain, a Dapr
actor, and two host wirings.

**D4 · `CrdtDocGrain` copies `ConnectorGrain`, not `TableShardGrain`.** From `ConnectorGrain`:
`[PersistentState]` written after every completed cycle, `OnActivateAsync` self-resume when persisted
`Running` is true, a `_generation` counter so a yielded turn's stale continuation abandons its result, and
emission through the one existing door — `OnNextAsync` per row onto `(SourcesNamespace, sourceName)`.
`TableGrain`/`TableIngestGrain` subscribe **by name**, never branching on `SourceDefinition.Kind`, so a
table cannot tell a document from a generator.

`TableShardGrain` is the wrong ancestor twice over: it is built around `ConsolidationLedger`, a *weighted
Z-set* merge that has nothing semantically in common with a Yjs merge, and its defining property is never
calling `DelayDeactivation` — the opposite of what a live document wants.

`SourceKinds.Ingest` is likewise not reusable: it is served by the `SourceIngressRegistry` host singleton,
which a grain does not go through.

**D5 · Config is carried, never looked up.** `RegistryGrain` is non-reentrant with a six-method
`[MayInterleave]` allowlist, all of them `Get*`. Schema, tombstone convention and field permissions are
stamped into `CrdtDocGrain`'s state on `StartAsync` and are **never** fetched from inside the merge path.
`TableShardRouterGrain` already states this rule; this plan restates it because a merge path that "just
needs the current schema" is the natural way to reintroduce the deadlock.

**D6 · If a loop is ever closed, reuse `LoopbackHub` + `SinkStepGuard`.** Both live in
`shared/StreamForge.AppCore/` and are already exercised over hundreds of turns by `LoopbackCycleTests`. No
new cycle-breaking mechanism.

**D7 · Idempotence is free, and is the point.** Re-applying an already-merged update changes no state, so
the projection is unchanged and no delta is emitted. A flaky link that redelivers the same batch four times
costs four merges and zero downstream events. This is the property that makes store-and-forward safe, and
it is worth a test of its own rather than an assumption.

**D8 · `Gc = true`.** Disabling GC keeps every deleted item forever, which buys CRDT-native history — and
buys it in the one place it is most expensive and least wanted. History for a twin already exists outside
the document: the delta journal (plan 009 A2) and table row history, both with retention and compaction.
`Gc = false` would also make personal data in a document effectively undeletable, which is a GDPR problem
dressed as a feature.

*What this costs, stated so the trade is reversible:* no `Y.Snapshot`-based "the document as of T", no
CRDT-level audit of who overwrote whom outside what wave D's `PermanentUserData` attribution records. If a
concrete case for either appears, it is a per-document opt-in with its own memory budget, not a default.

**D9 · Orleans-first.** Dapr stores the `crdt` kind and refuses to start it, exactly as it does for
`TableDefinition.ShardBy`. The escape hatch, if a Dapr deployment needs a document before a Dapr
implementation exists, is the cross-flavour gRPC link plan 006 already proved end to end.

**D10 · Vendoring: a NuGet package built from the fork.** Not a submodule, not vendored sources. A pinned
version from GitHub Packages keeps a clean boundary, makes an upgrade a version bump, and keeps the fork
from diverging from the upstream it has PRs open against.

## The projection is the dangerous part

Turning a `YDoc` into rows is where this plan can silently produce wrong data. Every item below is a
required deliverable of wave B, not a note.

- **Deletion cannot be expressed.** A source emits `EventRecord`s with no weight — weight-1 asserts are the
  convention on the source stream, and Z-set weights only exist downstream on a table's *output*. So
  removing a key from a `YMap` has no representation that reaches a table. The projector therefore needs an
  explicit **tombstone convention** (a reserved field on the projected row) that SQL and every downstream
  consumer must read as "deleted", not as "empty". Without it, deletions simply do not arrive, and the twin
  is wrong in the direction nobody checks.
- **`_ts` and `_source` are reserved** (`EventRecord.TimestampField`/`SourceField`). An ERP document is
  entirely likely to carry fields with those names. The projector renames defensively and says so in the
  emitted schema; silently letting them through corrupts `EventRecord.Timestamp`/`.Source`.
- **`Y.Text` loses formatting.** No `FieldKind` carries rich text (`String, Double, Long, Bool, Timestamp,
  Json`). `Y.Text` projects as its plain string. Declared out of scope in v1 rather than half-supported.
- **Nested Y-types do not fit `FieldKind.Json`'s leaves** (primitives inside `Dictionary`/`List`). Two
  options: flatten recursively, losing the identity of nested elements, or emit one row per nested element,
  which breaks "one document = one row sequence". **v1 flattens**, and the plan records that choice so the
  alternative is a decision to revisit rather than a bug to discover.
- **Document keys are field NAMES.** The permanent field numbers (`FieldNumberMap`) belong to the
  proto-generation layer and have nothing to do with how rows are represented internally.
- **Type drift** (a field that was a number and is now a string) goes through `FieldValueCoercion`, like any
  weakly-typed connector.

## Waves

Every wave gates on both solutions building and both suites green (Orleans, Dapr — currently 2424 / 695,
excluding the `DockerGate` integration tests), `cd web && bun run build` when `web/` is touched, and a live
check on isolated ports (`--Http:Port 74xx --Grpc:Port 75xx --DataDir <temp>`) with the instance killed and
its temp dir removed afterwards. Never 5199/5299/5399.

### Round 1

| Wave | Owns | Delivers | Model |
|---|---|---|---|
| **A · Vendoring** | `Directory.Packages.props`, both `.csproj` sets, one smoke test | The Ycs package from the fork, version pinned, restoring on both flavours; a test that round-trips a v1 update through `Ycs` and asserts the bytes match a fixture produced by real Yjs — so a bad bump fails here rather than in wave B | Sonnet 5 high |

### Round 2

| Wave | Owns | Delivers | Model | Depends |
|---|---|---|---|---|
| **B · Sync core** | `shared/StreamForge.Connectors.Crdt/**`, `orleans/src/.../Grains/CrdtDocGrain.cs`, `SourceKinds.Crdt` + `CrdtSourceConfig` (additive `[Id(n)]`) | The document grain on `ConnectorGrain`'s shape, the projector with every hazard above handled and tested (tombstone convention first), store-and-forward update ingestion modelled on `StreamForge.Connectors.Fix`'s bridge, and the idempotence test D7 names | Sonnet 5 high | A |

### Round 3

| Wave | Owns | Delivers | Model | Depends |
|---|---|---|---|---|
| **C · Durability** | `CrdtDocGrain` state + compaction | Snapshot plus an update log in grain state, compacted with `MergeUpdates`; a silo recycle mid-stream loses nothing, verified against a killed and restarted instance rather than in-process | Sonnet 5 high | B |
| **E · Reconciliation** | SQL + docs, one exception table | The pattern for what a CRDT cannot do: an exceptions/DLQ table fed by streaming SQL over the projection, and compensation written as a pipeline. This is the honest answer to "domain conflicts", and it is deliberately not a CRDT feature | Sonnet 5 high | B |

### Round 4

| Wave | Owns | Delivers | Model | Depends |
|---|---|---|---|---|
| **D · Authz + audit** | `CrdtDocGrain` authorization path, audit emission | Coarse per-document ACL first, then field-level: `DecodeUpdate`/`ParseUpdateMeta` inspect what an update actually touches *before* merging it, and `PermanentUserData` attributes the change. **Blocked on 015** — there is no entitlement model to evaluate against until it lands | Sonnet 5 high | B, **015** |
| **F · Escrow counters** | `Ycs`-side helper + a rebalance RPC | Bounded counters (below) | Sonnet 5 high | B |

### Round 5

| Wave | Owns | Delivers | Model | Depends |
|---|---|---|---|---|
| **G · Awareness** | SignalR hub + one client | Ephemeral presence/liveness, **off by default**, with a TTL and a documented cap | Sonnet 5 high | B |

**Why awareness is last, not first.** In this platform a new transport *kind* costs one registry line —
`SourceValidation`, config export/import, secret masking and the SPA form all pick it up automatically. A
new *message type* is the opposite: a DTO, `StreamBridgeService`, `DaprStreamBridge`, `streamforge.proto`,
four hand-written language clients, possibly `zset-cases.json` with four reducers, and
`web/src/hooks/useTableRows.ts`. Worse, the platform has **no ephemeral, non-journalled channel at all** —
that is a new concept, not a new instance of an existing one. Wave G is therefore scoped to SignalR and one
client, and everything else is in the cut list.

Awareness is also the thing most likely to flood a link that this whole plan exists because it is bad. It
ships disabled, with an explicit interval and TTL, and turning it on is an operator decision made against a
measurement.

## Escrow: bounded counters on a plain `YMap`

A CRDT cannot enforce "stock never goes below zero" — that is a global invariant and CRDTs give up global
agreement by construction. A **bounded counter** (Balegas et al., 2015) gets a *one-sided numeric* bound
back without synchronous coordination, by pre-allocating the allowance across replicas: replica `i` may
only spend what it holds, so the sum can never breach `K` even if every replica spends at once in ignorance
of the others.

State is two monotone maps — `T[i][j]`, the allowance transferred from `i` to `j`, and `D[i]`, what `i` has
spent — with local allowance
`initial_i + Σⱼ T[j][i] − Σⱼ T[i][j] − D[i]`.

**The observation that removes the need for a new CRDT type:** every key has exactly one writer. Only
replica `i` ever writes `D[i]` or `T[i][*]`. Under single-writer-per-key discipline a `YMap`'s
last-writer-wins is never actually exercised, so the counter is representable as an **ordinary `YMap`**
with keys `d:<replica>` and `t:<from>:<to>`. No new type in Ycs, a representation real Yjs can read, and no
divergence from the upstream the fork has PRs against.

Limits, written down rather than discovered:

- One-sided numeric bounds only. "Total across these three fields stays consistent" is not this mechanism —
  it is wave E.
- Rebalancing is pairwise coordination, i.e. an **online** operation. A node that has spent its share stops
  until it reconnects. That is the correct failure mode (refuse rather than oversell) and it must be
  visible to the operator, not silent.
- Key count is O(replicas²), so this is a mechanism for **named sites** — a warehouse, a shop floor, a
  vessel — not for thousands of browser tabs.
- The allocation policy is domain knowledge and is configured, never inferred.

## Verification

Beyond the per-wave gates: a live check on an isolated instance where a document is edited **while the link
is severed**, and after reconnect the corresponding table row converges to the expected value; redelivering
the same update batch produces zero further deltas (D7); cross-checking the wire format against real Yjs
through the fork's `interop/` harness rather than against ourselves; and memory measured with
`tools/soak/run-soak.sh` **before** any claim about how many documents fit — plan 011 already established
that intuition about this platform's memory is wrong.

## Cut, ranked

1. **Dapr sharding and a Dapr CRDT tier.** Orleans-first (D9); the gRPC link covers a mixed deployment.
2. **`Y.Text` with formatting, and any editor-shaped scenario.** A different product.
3. **Time-travel via `Gc = false`.** D8 — history lives in the journal and row history, with retention.
4. **Invariants beyond one-sided numeric bounds.** Wave E's reconciliation, not a CRDT feature.
5. **Awareness in all four typed clients.** Wave G ships SignalR and one client.
6. **An online REST/gRPC sync endpoint for live peers.** Store-and-forward first; a request/response sync
   protocol is a second seam and should wait until the first one has a user.
7. **A document browser in the console.** The projected table is already viewable; a raw-document inspector
   is a debugging tool nobody has asked for yet.

---

## Wave outcomes

### Wave A · Vendoring — DONE (2026-08-20)

**D10 is overruled: a git submodule, not a NuGet package.** The plan said "a pinned version from GitHub
Packages built from the fork". There is no such package and there was no way to make one without acting
on somebody's GitHub account: `cotyar/ycs` has **zero releases**, nothing is published to nuget.org
(searched), and listing GitHub Packages needs a `read:packages` scope the local token does not carry.
D10's stated goals — a clean boundary and an upgrade that is one line — are met by a submodule pinned to
a commit just as well; what is lost is that a fresh clone must now run `git submodule update --init` or
both solutions fail to build, which is written into `README.md` and `AGENTS.md`. If the package ever gets
published, going back is a one-line csproj change.

**The pin is a branch, and the branch matters.** `external/ycs` is held at `75a815c` on
**`parity-yjs-13.6.32`**, not `main`. This was nearly a silent trap: `main` carries only the v2 update
encoding and has neither `UpdateOperations` (`MergeUpdates` / `DiffUpdate` / `EncodeStateVectorFromUpdate`
/ `DecodeUpdate` / `ParseUpdateMeta`) nor `PermanentUserData`. **Wave C compacts its update log with
`MergeUpdates` and wave D inspects updates with `DecodeUpdate` before merging** — on `main` neither wave
can be written at all. The plan's own dependency paragraph describes the parity branch's contents while
naming the repository, so a reader would reasonably have cloned `main` and discovered this two waves in.

**What the fork already does, so this wave did not redo it:** 274 tests green on that branch, an
`interop/` harness that round-trips against the published `yjs@13.6.32` *and* `yjs@14.0.0-16` packages
through Node, and real-Yjs updates pinned as base64 fixtures inside its own test project. Wave A's brief
asked for "a test that round-trips a v1 update through Ycs and asserts the bytes match a fixture produced
by real Yjs" — that test already exists upstream, and duplicating it here would be re-testing somebody
else's library.

So `shared/StreamForge.Connectors.Crdt.Tests/YcsPinTests.cs` (4 tests) tests **the pin** instead:
- a v1 update produced by real Yjs decodes to the expected values, nested Y-types included;
- re-encoding converges to the same document — byte equality is deliberately **not** asserted, because
  Yjs guarantees no such thing across versions and pinning it would turn a legal encoder change into a
  red suite;
- `MergeUpdates` and `DecodeUpdate` are called, so a submodule bumped onto `main` fails **here** rather
  than in wave C;
- **D7's idempotence** is asserted directly: applying the same update three times leaves the encoded
  state identical to after the first. It is the property the entire store-and-forward design rests on and
  it costs four lines to pin.

**No empty library was created.** Wave B owns `shared/StreamForge.Connectors.Crdt`; until it exists the
test project references `external/ycs/src/Ycs/Ycs.csproj` directly. `Ycs.csproj` is deliberately **not**
listed in either solution — a `ProjectReference` builds only its `net10.0` target, whereas a solution
entry would build all three (`netstandard2.0`, `net8.0`, `net10.0`) on every build forever.

**One dependency fact worth knowing:** Ycs pulls `Newtonsoft.Json`. On `main` that is `12.0.3`, which
carries a known high-severity advisory (`NU1903`); the parity branch bumps it to `13.0.3`, which is
clean. A third reason the branch pin is the right one.

Gates: `dotnet build` green on both solutions; `YcsPinTests` 4/4.

### Wave B · Sync core — DONE (2026-08-20)

Landed in three parts: **B-0** the contract seam (orchestrator), **B-1** the projector, **B-2** the grain,
the intake route and the Orleans dispatch wiring.

**Two decisions the plan left to the implementer, made in B-0 because they are the contract:**

*The document's shape.* The root `YMap`'s keys are entity keys and its values are that entity's
attributes; one key projects to one row, carrying its key in `keyField`. The alternative — the root map
IS one entity, whole document to a single row — was rejected because it leaves per-entity deletion
inexpressible, and deletion is the half of this feature that goes wrong silently.

*Deletion reuses the platform's vocabulary instead of the new tombstone the plan asked for.* `_op`/
`_weight` are already spoken by CDC (`CdcStamp`) and already understood by the database sink planner. A
third spelling would have meant every consumer learning it.

**The projector** (`shared/StreamForge.Connectors.Crdt`) handles every hazard the plan's "the projection
is the dangerous part" section names, each with its own test: reserved-column rename (`_ts` → `doc_ts`,
never passed through, never silently dropped), recursive flattening on a dotted path, `YText` as its plain
string, undeclared keys dropped rather than guessed, coercion through the platform's own
`FieldValueCoercion`, and no throw on any document content.

**Three things were found by looking rather than reasoning, and all three were real:**

1. *A Y-type projecting its own class name.* The B-1 agent reported the bare-`YArray`-under-an-entity-key
   path as untested and predicted it "fails coercion honestly". It does not — `FieldValueCoercion` coerces
   any object into a String column via `ToString()`, so the row carried the literal string `"Ycs.YArray"`
   with no diagnostic. Both the scalar path and `FlattenValue`'s fallthrough now refuse an `AbstractType`
   by name. **"Not covered by a test" in a wave report is an address, not a footnote.**

2. *A stopped document answering like a successful replay.* The grain's defensive floor returned a bare
   `CrdtMergeResult` — "0 applied, 0 rows" — which is byte-identical to the idempotent replay D7
   guarantees. An edge draining its store-and-forward buffer into a stopped document would have read its
   own data loss as success. It now says so in `Diagnostics`.

3. *The tombstone did not converge a table.* Live: deleting a document key left the table holding BOTH the
   original row and a second all-null row for the same key, each at weight 1. `_weight` on an inbound row
   **is just a column** — the Engine's Z-set weights are computed from table SQL, never carried in from
   ingress. `CdcEnvelope`'s class doc has always said this; **a Debezium/CDC delete has exactly the same
   limit**, so this is a platform property plan 020 walked into, not one it created. The fix is the
   platform's own existing mechanism: the tombstone also stamps `_retract = true`
   (`IngressRowAcceptance.RetractField`), which `TableIngestOp` honours unconditionally. Re-verified live
   against a `LATEST BY (id)` table: the key is genuinely freed, the table goes to 0 rows. Three stamps,
   three readers — `_op` for SQL, `_weight` for a sink, `_retract` for a table.

   Note the one asymmetry this creates, recorded rather than hidden: a REST-pushed `_retract` is gated by
   `RetractConsumerValidation` (rejected unless every running consumer is `LATEST BY`-shaped); the CRDT
   path does not cross that boundary and so is not pre-validated. It relies on `TableIngestOp`'s stated
   safety contract for other table shapes — never corrupt, at worst under-report. `TableIngestOp`'s doc
   comment now names the CRDT path as its second producer.

**Live check** (isolated instance, ports 74xx–77xx, temp data dir, torn down afterwards). The plan's own
Verification asks for a document edited while the link is severed, converging after reconnect:

- Three transactions made entirely offline — create `AAPL`+`MSFT`, correct `AAPL`'s quantity, delete
  `MSFT` — then drained as one batch on "reconnect": **3 updates applied, 1 row emitted**, table holds
  `AAPL / Apple / 250`. The corrected quantity, not the original; `MSFT` never appears downstream at all,
  because create-then-delete inside one offline session has no net effect. An edge's whole offline session
  costs the platform exactly its net result.
- A second offline session deletes `AAPL`: the `LATEST BY` table goes to **0 rows**.
- **D7 live**: both batches redelivered — `updatesApplied` 3 and 1, `rowsEmitted` **0** and **0**, table
  `seq` unchanged.
- **D7 across a restart**: the instance was killed and restarted; `crdtDoc.crdtdoc_twin_book.json` rehydrated,
  status identical (`updatesMerged: 8`, `entityCount: 0`), and replaying the entire history still emitted
  **0 rows** — which is the property that actually matters to a reconnecting edge.
- Route refusals: unknown source **404**, non-crdt source **409**, invalid base64 **400**, no token **401**.

**Known and not fixed:** a crdt source leaves an inert `connector.connector_<name>.json` (`Def: null`,
`Running: false`, `LastStatus: "never"`) because the dispatch defensively stops the connector grain for
every non-Connector kind. One empty file per document, no second driver running.

**Not built here:** wave C's durability (the grain persists the whole document per merge — ceiling and
upgrade path marked with a `ponytail:` comment), waves D/E/F/G. The Dapr flavor stores the kind and
refuses to run it (D9); its gap is item D5 in `dapr/PARITY.md`.

### Wave C · Durability — DONE (2026-08-20)

**What the wave asked for, and what it actually buys.** `CrdtDocGrainState.DocBytes` is now a *compacted
snapshot* plus `PendingUpdates`, the raw bytes of every update that actually applied since it;
`RehydrateDoc` applies the snapshot then replays the log; `MergeUpdates` folds the log back in at 32
entries or 2 MB, whichever trips first. `EncodeStateAsUpdateV1()` no longer runs on every merge.

The obvious framing for this wave — "fewer bytes written" — is **false, and was checked before being
written down**. Orleans grain storage has no append: `WriteStateAsync` serializes the entire
`CrdtDocGrainState` blob every time, so the snapshot is written on every merge exactly as wave B wrote it,
and between compactions the blob is temporarily **larger** (snapshot *and* a growing log). What the wave
genuinely buys is the per-merge `EncodeStateAsUpdateV1` CPU — O(document), tombstones included — which now
runs only at compaction. That accounting is in `DocBytes`'s own doc comment so nobody has to re-derive it.

**Three hazards, each with its own test** (`CrdtDurabilityTests`): a frame that failed to apply never
enters the log — if it did, `RehydrateDoc` would rethrow it on *every* activation and one corrupt byte
from an edge would be a permanent denial of service on that document; a wave-B-shaped state file (full
`DocBytes`, no `pendingUpdates` key at all) rehydrates byte-identically, so the upgrade needs no migration;
and compaction does not change what the document says, pinned across a real deactivation.

**The wave's acceptance criterion failed, and that is the finding.** "A silo recycle mid-stream loses
nothing" was checked live — `kill -9` on the host PID, restart on the same data dir — and the **document**
came back perfectly while the **table** came back at `rowCount: 0`, `rebuilding: true`, and stayed there.

The cause is a collision between two existing, individually correct properties. `TableGrain`'s
RESTART-RESUME LIMITATION (its class doc) resets a resuming table to empty and rebuilds it "purely from
live traffic going forward" — fine for a generator or a broker, which keep producing. And D7 guarantees
that re-delivering an edge's update history emits **nothing**. A document is not a stream of new events:
its value *is* its current state, so the one thing that could refill the table is the one thing
idempotence forbids. Wave B's own restart check never caught this because it verified document status and
a zero-row replay, never table content. The same defect appears with no restart at all: a table created
over an already-populated document also starts empty.

The fix is a re-assert, not a new mechanism: `ICrdtDocGrain.ReplayAsync()` diffs the live projection
against an **empty** before-state, which is exactly `CrdtProjector.Diff`'s existing create-row path — no
second projection path, and tombstoned keys do not enumerate, so a deleted entity is not resurrected. It
merges nothing, so it cannot corrupt the document. `RegistryGrain.EnsureInitializedAsync` issues it for
every enabled document **after** the tables loop, and `POST /api/sources/{name}/crdt/replay` (Editor —
it publishes to every downstream consumer) covers the runtime case.

**A cheap optimization broke the wave, and the live check caught it.** Latching the boot replay to
once-per-activation put the table straight back to `rowCount: 0`. `EnsureInitializedAsync` has two callers
on boot and no idempotency guard, so the tables loop runs **twice**, and the second pass's
`ITableGrain.StartAsync` re-runs the resume path and resets the table again — meaning it was never the
first replay that stuck, it was the second. The replay must follow *every* table-resume pass. The reverted
latch and the reason are commented at the call site; this is why the acceptance gate is a killed instance
and not an in-process test.

**Live transcript** (isolated instance, ports 7440/7540/7640/7740, temp data dir, torn down): offline
batch of 3 updates → 1 row, table holds `AAPL / Apple / 250`; `kill -9` + restart → document
`entityCount: 1` and table back to `rowCount: 1`, `rebuilding: false`; a second table created over the
already-populated document starts empty and converges on `POST .../crdt/replay`
(`{"updatesApplied":0,"rowsEmitted":1}` — zero applied is the truth, not a replay-was-a-no-op signal);
D7 unchanged (`3 applied, 0 rows`); the tombstone still converges the table to 0 rows; a replay against an
emptied document emits 0 and does not resurrect the key. Route refusals: unknown 404, non-crdt 409, no
token 401.

**A guard test earned its keep.** `AuthorizationCoverageTests` failed on the new route with "Nobody has
decided what it should be guarded by" — exactly its job. The row is pinned with the reason Editor and not
Viewer.

**Gates:** both solutions build; Orleans 1531 host tests with 4 failures, all four green under `--filter`
(`LoopbackCycleTests` 3/3, `TablePersistenceModeClusterTests` 7/7, `BackfillOnAttachClusterTests` 2/2,
`ShardedTableD2ClusterTests` 10/10) — the last of those was **not** on AGENTS.md's known-flake list and has
been added to it with the reason: its shard-history assertion sits behind a fixed `Task.Delay(600)` rather
than a poll. Dapr 1482 across six projects, zero failures.

**Not built here:** waves D/E/F/G. Dapr still stores the `crdt` kind and refuses to start it (D9).
