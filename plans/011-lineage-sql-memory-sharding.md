# 011 — Lineage edges, SQL editor UX, memory stability, sharded tables

**Status: DONE** (A, B, C1, C2, D1, D2, D3, E, F). Baseline `a514190` — Orleans **1508** tests, Dapr
**280**. Landed at Orleans **1591**, Dapr **308**.

**The one result to carry forward: `ShardBy` is NOT a memory optimisation.** It was built to be one. Wave
D3 measured a trail-depth sweep at fixed event volume and it never reduced total process RSS beyond the
±37 MB noise floor at any point, while costing up to +68% below ~100 versions per key. The cost tracks
shard ACTIVATION RATE ≈ `events-per-sec ÷ versions-per-key`. What it does deliver, and what it should be
described as, is per-key query locality and swap-out of cold keys: strictly-consistent per-key reads that
wake one grain, a resident set tracking the active key set (4–15% measured), and full trails kept durably
per key. The memory work that actually paid was C1 (the flush amplifier, 287 → 109 MB/min) and C2
(retention, which plateaus a table instead of letting it grow). Full curve and caveats: `orleans/DESIGN.md`,
"Sharded tables".

## Context

Four user-reported problems, three of which turned out to have a single confirmed root cause each.

1. **Lineage shows no edges into most pipelines.** Not a React Flow problem: the seed path never
   compiles pipelines, so all 7 seeded pipelines ship with `SourceNames = []` and get zero incoming
   edges. Both pipelines reading `orders` are in that set.
2. **The SQL builder eats the SQL editor's text, there is no revert, and the SQL is one long line.**
   The builder is never seeded from SQL, so switching tabs overwrites the text with a render of an
   empty builder. No formatter exists anywhere in the repo.
3. **Long runs exhaust memory.** Confirmed and reproducible from the stock seed: `order_states` is
   `LATEST BY (order_id)` where `order_id` is a fresh GUID per order, seeded **Running**, with
   history enabled (`LastN = 8`). It gains ~1 permanent row/second forever, and that whole structure
   is **deep-cloned on the grain turn every 2 s** by two grains, then serialized to indented JSON.
   Linear-in-message-count state × O(state) copy every 2 s is a GC/LOH stall long before it is a
   heap exhaustion — which matches "Rider froze / the machine died" better than a plain OOM does.
4. **Financial instruments need per-key state.** A state-machine instrument with legs wants "give me
   everything for this key" plus full history, without the whole table's history resident. Today one
   `TableGrain` holds the entire consolidated snapshot and one `TableHistoryGrain` holds the entire
   history, and every grain in the table path calls `DelayDeactivation(TimeSpan.FromDays(365))` — so
   nothing is ever swapped out to state storage. (3) and (4) are the same defect seen from two sides.

Outcome: lineage draws every real dependency; the SQL editor is safe to edit and readable; memory is
flat over a long soak and the ceilings are documented rather than discovered; and a table can opt into
being sharded by key so each grain is small, deactivatable, and exactly-consistent for per-key reads.

Four waves, independently committed and verified. A–C are small-to-medium and independent; D is the
large one and depends on C's soak harness for its proof.

---

## Wave A — Lineage edges *(small, Sonnet 5)*

**Root cause** — `orleans/src/StreamsForge.Host/Grains/RegistryGrain.cs:55-59` adds
`SeedCatalog.Pipelines()` raw, while the block immediately below (`:61-82`) compiles every seeded
*table* and assigns `StreamInputs`/`TableInputs`. Dapr has the identical asymmetry at
`dapr/src/StreamsForge.Dapr.Host/Catalog/CatalogStore.cs:61` vs `:66-90`. The only writers of
`SourceNames` are Create/Update (`RegistryGrain.cs:250,281` via `ApplyPipelineCompileResult` at
`:670`; `CatalogStore.cs:149,178,514`). Starting a pipeline does not backfill it either —
`PipelineGrain.StartAsync` compiles but keeps `SourceNames` local (`PipelineGrain.cs:57-63`).

**Fix — one backfill, not two changes.** In `RegistryGrain.EnsureInitializedAsync` and
`CatalogStore.EnsureInitialized`, after the catalog is loaded (seeded *or* restored from disk), compile
every pipeline whose `SourceNames` is empty and call the existing `ApplyPipelineCompileResult`. That
covers the seed path *and* repairs the durably-stored empty lists in existing data dirs — a
seed-path-only fix would leave every current install broken until a reseed. Reuse
`BuildStreamSchemas()`, already present in both files.

**Also fix, and say so** — `web/src/components/lineage/LineageNode.tsx:41` has `min-w-44` with no max
width and a `truncate` with nothing to truncate against, so a long name (`"Unfilled orders (LEFT
JOIN)"`) grows the node past the 240px column pitch in `LineagePage.tsx:19`; nodes paint opaque
`bg-card` over the edge layer. Cap the node width and widen the pitch. This is a second, real cause of
"the edge is simply not visible" — it just is not the one producing the `orders` symptom.

**Verify:** new tests asserting the *seeded* catalog's lineage on both flavors (the acceptance
criterion `plans/008-...md:87` already claimed and never tested) plus a backfill test that starts from
a persisted pipeline with `SourceNames = []`. Live: `GET /api/pipelines` on a fresh instance shows
both `orders` pipelines with `sourceNames: ["orders"]`; the lineage canvas draws both edges.

---

## Wave B — SQL editor: revert, no-clobber tab switch, Format *(small/medium, Sonnet 5)*

All three land in `web/`; the editor is a hand-rolled textarea-over-`<pre>` overlay
(`web/src/components/SqlEditor.tsx`), not CodeMirror/Monaco, and its `onChange(value: string)`
contract makes injecting formatted text trivial.

- **Revert.** The persisted original is already in memory — `pipeline` (`PipelineDetailPage.tsx:54`)
  and `table` (`TableDetailPage.tsx:969`). Add a Revert control, enabled only when dirty, restoring
  `pipeline.sql`/`table.sql` (and resetting builder state on the pipeline page). No new state.
- **Stop the clobber.** `PipelineDetailPage.tsx:143-146`'s `switchToSql()` unconditionally overwrites
  `sql` with `builderStateToSql(builderState)` — and `builderState` is never seeded from the loaded
  SQL, so it renders `SELECT *\nFROM <source>`. Track whether the builder was actually edited and
  write back only then. Per the accepted scope, no SQL→builder parser: leave the builder honestly
  empty-until-touched rather than half-parsing the dialect.
- **Format.** A dialect-aware formatter in TS — one clause per line, uppercase keywords, indented
  JOIN/ON and nested parens. No new dependency: `sql-formatter` does not know `WITHIN`, `EMIT`,
  `LATEST BY`, `UNNEST`, or `->`/`->>`, and mangling a valid query is worse than not offering the
  button. Reuse `maskLiteralsAndComments` from `web/src/components/sqlScope.ts` so string/comment
  contents are never reflowed. Formatting is text-only and idempotent; it must never change what the
  server parses — the test for that is round-tripping every seed query through it and asserting the
  `/validate` result is unchanged.
- `web/src/builder/sqlgen.ts` is marked a do-not-touch behavioral seam in `plans/001-...md:35` —
  only its consuming UI changes here.

**Verify:** `cd web && bun run build`; live on a Vite dev server against an isolated backend — edit
SQL, switch to Builder and back, confirm the text survives; hit Revert and confirm the persisted SQL
returns; Format a one-line seed query and confirm it still validates identically.

---

## Wave C — Memory stability *(medium, Opus 5 — the amplifier fix touches both flavors' hot paths)*

### The amplifier (fix first — it is the difference between "grows slowly" and "the machine froze")

`TableGrain.CaptureSnapshotIntoState()` (`orleans/.../TableGrain.cs:1035-1050`) rebuilds a brand-new
dictionary with a fresh `Dictionary<string,object?>` **per row** from the whole ledger, on the grain
turn, every `FlushMs` (default 2000), then `JsonFileGrainStorage` serializes it with
`WriteIndented = true` (`Storage/JsonFileGrainStorage.cs:16,57-60`). `TableHistoryGrain` does the same
for every entry *and* every `Versions` list (`TableHistoryGrain.cs:307-317`). Dapr mirrors both
(`TableActor.cs:160`, `TableHistoryActor.cs:496,518`). At N rows that is 2N live row-dicts plus N
allocated and discarded every 2 s plus a full indented serialization.

Fixes, in order of value: turn off `WriteIndented` for state files (it is a storage format, not a
document); make the capture allocation-proportional to *changed* rows rather than to the whole table —
`Journaled` mode already computes exactly that set (`_pendingJournalEntries`), so the work is
extending it, not inventing it; and stop double-materializing where the live copy is already the
authoritative read source.

### The unbounded structures (bound them by policy, and document the rest)

Confirmed unbounded, all "by design" and none previously written down: `TableExecutorImpl._ledger`
(`:67`), `TableLatestByOp.Current` (`:54`), `TableReduceOp.Groups` (`:40`, "groups live forever"),
`TableJoinOp`/`TableOuterJoinOp`/`TableSemiAntiOp` ZSet indexes (no WITHIN eviction, `OnFrontier` is a
documented no-op), `TableDistinctOp._weights` (`:51`), `TableHistoryGrain._liveEntries` (`:108` — per-key
version counts are capped at `AllModeCap = 1000`/`HistoryLimit`, the **key count is not**),
`TableSearchIndex`'s five row-keyed maps (`:40,43,46,49,56` — a ~4–5× multiplier on whatever the table
holds), and `ArrangementGrain._index` (`:68`).

Add a per-table **row retention policy** on `TableDefinition` (next free `[Id(n)]`, additive): a max-row
bound and/or a TTL evaluated per row identity, applied where consolidation already runs so an unbounded
key space is bounded by an explicit, visible policy instead of by luck. Default off — enabling it
silently would change existing tables' results.

Fix the seed: `order_states` (`SeedCatalog.cs:125-133`) is the demo that eats the machine. Either give
it retention or stop seeding it Running — but it must not be a table whose stock configuration grows
without bound.

`EpochBuffer` (`shared/StreamsForge.Engine/Dataflow/EpochBuffer.cs:31`) exposes `BatchCount`/`DeltaCount`
"so a caller can apply backpressure" and **no caller reads either**; a stalled upstream pins the
frontier and the buffer grows unbounded, as does `TableStageGrain._originByBatch` (`:31`, drained only
inside the `observation.Advanced` branch). Wire the signal to a bound with a loud status, or state in
the type's doc that it is unbounded — do not leave the comment claiming a guarantee nothing provides.

`web/src/hooks/useTableRows.ts:76,96-121` grows `mapRef` unboundedly from live deltas and re-renders
every row on every batch — leaving a table page open during a long run is itself a plausible
contributor to the crash. Cap it at the display limit.

### The soak harness (the proof, and the thing wave D is measured with)

`tools/soak/run-soak.sh`, modelled on the existing `tools/bench/run-bench.sh` (same isolated-instance
discipline: fresh temp `--DataDir`, ports 6xxx–9xxx, teardown verified, never touches 5199/5299/5399).
Drives sustained ingest, samples RSS and GC counters over a long run, and reports the slope. A flat
curve is the acceptance criterion; the current stock seed is expected to fail it before the fixes, which
is what makes the fix demonstrable rather than asserted.

Document all of it in `orleans/DESIGN.md` — the "Known ceilings" list (`:173-177`) mentions no memory
bound at all today.

**Verify:** both suites green; the soak run flat over its window on both flavors; `RowCount` on
`order_states` plateaus instead of climbing; state files stop growing every 2 s.

---

## Wave D — Sharded tables *(large, Opus 5, sequenced after C)*

**Why now, when the repo rejected sharding twice.** Plan 003 superseded a one-level sharding idea, and
`TableGrain`'s class doc (plan 009 A2) rejects grain-per-partition because it "costs the atomicity of one
consolidated snapshot and needs an epoch fence at recovery". Both rejections answered a *write-cost*
question, and the journal answered it better. This is a different question: **resident memory and per-key
locality**, which the journal does not touch — it changes what is written, not what is held. And the
epoch fence those docs named as the cost is exactly what this wave builds, deliberately.

### Shape

`TableDefinition.ShardBy: List<string>` (output column names; empty = today's behavior, byte-identical —
the same opt-in discipline `Parallelism` established, D9). Orleans-first: Dapr rejects `ShardBy` with a
clear message, exactly as it already rejects `Parallelism > 1` (`TableActor.cs:276-282`).

The shard tier is **a consumer of the table-delta stream**, not a change to execution — the same hook
`TableHistoryGrain` already uses (`TableHistoryGrain.cs:257-260`), which is D7's stated principle ("the
delta stream is the event log"). So the SQL path, the planner, the partitioned dataflow, and every
downstream table-over-table subscriber are untouched.

- `TableShardGrain`, key `{table}|{encodedShardKey}` — one `ConsolidationLedger` (reused as-is,
  `shared/StreamsForge.Engine/Runtime/ConsolidationLedger.cs`) for that key's rows plus that key's version
  history via the existing pure `TableRowHistoryRetention`. Persists under the table's own
  `TablePersistenceMode`/`FlushMs`. **Critically: no `DelayDeactivation`** — an idle shard deactivates and
  its state lives on disk until the next lookup. That is the entire memory win.
- A router (mirroring `TableHistoryGrain`'s subscribe/extract/apply structure) groups each delta batch by
  shard key and forwards, stamping the epoch.
- A shard directory grain holding the live key set, for fan-out and deletion. Honest limit: it is
  O(distinct keys) of strings, and must be documented as such.
- On a sharded table, the per-key history replaces the single `TableHistoryGrain`; `SearchEnabled` +
  `ShardBy` is **rejected** in v1 rather than silently keeping a table-wide inverted index that would
  defeat the whole point.

### Key derivation

Explicit `ShardBy` columns, validated against `OutputFields` at upsert (same 409-style guard shape as
`ValidateParallelism`, `RegistryGrain.cs:619-628`). `TableGroupKeyExtractor.ExtractIdentityColumns`
(`shared/StreamsForge.AppCore/History/TableRowHistory.cs:37`) is best-effort *textual* matching — fine for
history's fallback, not fine as the thing that decides which grain owns a row. Use it only to
**suggest a default** in the console, never to silently pick one.

### Consistency (the accepted answer)

- **Per-key reads are strictly consistent by construction** — one grain, one ordered delta stream,
  Orleans serializes its turns. This is the query the use case actually cares about ("выдай мне список
  по этому ключу"), and it needs no fence.
- **Whole-table scans get an opt-in epoch fence**: the read picks an epoch E, and each shard awaits
  having applied E before answering, yielding a genuine consistent cut. Costs latency on scans only and
  **no extra memory** — no per-epoch versions are retained. Unfenced remains the default, reporting the
  min epoch observed, matching how `SnapshotFrontierEpoch` is already surfaced on `/rows`
  (`TableGrain.cs:610-617`).

### Surfaces

Key-addressed read on the tables group (`shared/StreamsForge.Api/Endpoints/TablesEndpoints.cs`), modelled
on the existing `POST /{id}/history/lookup` + `HistoryLookupRequest(Dictionary<string, object?> Row)`
(`Dtos.cs:91`) — that endpoint is already the repo's precedent for "address a row by its identity
columns". Per-key rows, per-key history, and shard metrics (shard count, active/resident shards). Console:
a `ShardBy` control on the table form and a per-key lookup on the table detail page.

**Verify:** both suites green with no pre-existing test file modified; a sharded and an unsharded table
built from the same SQL and the same input produce identical rows; per-key reads are exact under
concurrent ingest; a fenced scan taken during ingest is a real cut (no row from before E missing, none
from after E present); and — the actual point — the wave C soak on a high-cardinality sharded table
shows resident memory bounded by the *active* key set rather than the total, with shards observed
deactivating and correctly reactivating from storage.

---

## Cross-cutting rules

- Subagents per the repo's wave discipline: strictly disjoint file ownership per concurrent agent,
  anything shared (`Models.cs`, `types.ts`, `TablesEndpoints.cs`, `RegistryGrain.cs`) pre-assigned to
  exactly one owner or edited between waves. Waves A and B can run in parallel; C is serialized against
  the Engine/table hot path; D follows C.
- Contracts evolve **additively only** (next free `[Id(n)]`, optional fields); `web/src/api/types.ts`
  likewise; backend enums stay PascalCase on the wire. The Engine gains no Orleans/Dapr/ASP.NET types.
- **No pre-existing test file modified.** A behavior-preserving change keeps old tests green unmodified;
  wave C's retention policy is off by default precisely so that stays true.
- Every wave gates on: `~/.dotnet/dotnet build` + `test` both solutions green (1508 / 280 plus new),
  `cd web && bun run build` when `web/` is touched, and a live check on isolated 6xxx–9xxx ports with the
  instance killed and the temp data dir removed afterwards. `dotnet` is at `~/.dotnet/dotnet`, never on
  PATH; JS tooling is bun, never npm; never bind or kill 5199/5299/5399.
- Commit (and push) per wave, one logical change per commit; update `plans/README.md` and the test counts
  in `AGENTS.md` as each lands.
