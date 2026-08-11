# Plan 009 — ingress hardening, NATS transport, and the table delta journal

## Context

Plan 008 shipped client-push ingress and closed with three named, deliberate gaps. Separately, table
persistence still rewrites the whole snapshot on every flush. And the platform speaks gRPC, REST,
SignalR and Dapr pub/sub — but no message-broker protocol, so it cannot sit in an existing event
backbone without a shim.

Six waves in three rounds. Test baseline today: **1208 Orleans (653 Engine + 555 Host) + 236 Dapr**.

---

## Round A — ingress hardening ‖ delta journal

### Wave A1 — idempotency, per-source keys, cluster-wide counters

Three of 008's four known gaps, in one subsystem, so one owner.

**A1.1 Idempotency.** A 429 tells the client to retry; nothing stops that retry from duplicating
rows. Two layers, because they answer different questions:

- **Batch-level, the actual fix for retry-after-429**: `IngestEventsRequest.IdempotencyKey` (and
  `IngestRequest.idempotency_key` on gRPC). The facade remembers the last N *(source, key)* pairs
  with the `IngestResult` each produced; a repeat **returns the original result verbatim** and admits
  nothing. That is the only shape that makes "retry the identical body" safe, and it means a retry's
  202 reports the counts of the original push rather than a second admission.
- **Row-level, for at-least-once upstreams**: `IngestConfig.DedupKeyField` — a declared field whose
  value identifies a row. Reuses `DedupTracker` (`AppCore/Connectors/Polling/DedupTracker.cs`,
  bounded at 10 000 keys, already persistable) rather than a second implementation. Deduped rows
  are counted and reported as `duplicate`, distinct from `dropped` and from `invalid` — three
  different reasons a row didn't land must not share one counter.

Ordering is load-bearing: dedup runs **after** coercion and **before** admission, so a duplicate
never consumes buffer capacity and a 400 still leaves nothing behind.

**A1.2 Per-source ingest keys.** `Editor` is the wrong boundary — a machine pushing telemetry should
not hold a token that can also rewrite SQL. `IngestConfig.Keys`: a list of
`{ Id, Hash, Salt, CreatedAtMs, LastUsedMs }`, hashed with the existing
`AppCore/Auth/PasswordHasher` (never stored in the clear), presented by the client as an
`X-SF-Ingest-Key` header or gRPC metadata entry. A valid key authorizes push **to that one source**
and nothing else. The existing Editor-JWT path keeps working unchanged.

Read-back masks every key with the existing `SourceKinds.SecretMask` convention, and the plaintext is
returned **exactly once**, at generation, from `POST /api/sources/{name}/ingest/keys` (Editor).
`DELETE /api/sources/{name}/ingest/keys/{id}` revokes.

*Not doing a dedicated `Ingestor` role*: a role is a coarser tool that ripples through the `Role`
union in contracts, `types.ts`, the users page and every policy, and still cannot express "may push
to this source only". Per-source keys are strictly the better primitive here; say so in the docs.

**A1.3 Counters that admit what they are.** The buffer is process memory, so under more than one
replica every counter is a per-replica view presented as a global one. Fix in two parts:

- `IngestStatus` gains `InstanceId` and `Aggregated` (bool). An unlabelled number that silently means
  "this replica only" is the same failure mode as an unexplained zero.
- Orleans, where a cluster already exists: a per-source `IIngressStatsGrain` (key = source name, so
  it is a cluster singleton) that each host's `IngestDrainPumpService` reports its local deltas into
  on its existing tick. `GET …/ingest` then answers from the grain with `aggregated: true`. Dapr
  returns the local view with `aggregated: false` and a doc comment saying why — the same
  Orleans-only-capability convention as `IArrangementMetaFacade` (decision D-F).

**Verify:** both suites green; live — a repeated push with the same idempotency key admits once and
returns the first result; a key-authenticated push succeeds with no JWT and a revoked key gets 401;
a row-level duplicate is counted as `duplicate`, not `dropped`.

### Wave A2 — table delta journal with compaction

Today `TableGrain`/`TableActor` rewrite the entire `Snapshot` dictionary on every flush, so the write
cost is O(|table|) no matter how few rows changed — the thing that makes flush cadence a latency
knob in the first place.

**Design: a second persisted state, not a new storage provider.** A new
`TablePersistenceMode.Journaled` (additive, fourth member, default unchanged) keeps a
`[PersistentState("table-journal")]` list of changed `(rowKey, row, weight)` entries since the last
compaction. A flush writes only the journal — O(changed since last compaction). When the journal
passes `JournalMaxEntries` (or a fraction of the snapshot size), a compaction writes the full
snapshot state and clears the journal. Activation loads snapshot then replays journal.

This deliberately does **not** add an append-only storage provider: `JsonFileGrainStorage` and Dapr's
state store both rewrite a whole state object, so an "append" would be a lie at the storage layer.
Rewriting a *small* state is the honest way to get O(changed) inside the existing abstraction, and it
is the only shape that works identically on both flavors.

**Also rejected, and why** (this was the user's own question): handing the write to a separate grain
buys nothing — the argument is serialized on the *calling* grain's turn, so the expensive half stays
where it was and a hop is added. Sharding a table across grain-per-part would give O(changed) too,
but it costs the atomicity of the snapshot and needs an epoch fence at recovery or the parts
resurrect inconsistent with each other. The journal gets the same asymptotics with neither cost.

**Verify:** both suites green; a journaled table survives restart with byte-identical rows to a
batched one; compaction triggers and truncates; flush write volume under a small delta stream is
measurably smaller than snapshot mode (assert on entry counts, not on wall-clock).

---

## Round B — NATS

Nothing in the repo mentions NATS or Kafka today. The `grpc` source kind is the precedent to follow
for ingress: a persistent background subscriber whose callbacks route back through a grain reference
(`ConnectorGrain.StartGrpcSubscriber` + `AppCore/Connectors/Grpc/GrpcSubscriberCore.cs`), with status
flowing through the existing `IConnectorStatusFacade`.

Client library: **`NATS.Net`** (nats-io/nats.net), 3.1.0, verified reachable from this environment.

### Wave B1 — NATS ingress: source kind `nats`

`SourceKinds.Nats` + `ConnectorConfig.Nats` → `NatsSubConfig { Url, Subject, QueueGroup?, Format,
Credentials (token / user+password / creds-file contents, secrets-lite masked), JetStream? }`.
`AppCore/Connectors/Nats/NatsSubscriberCore.cs` modeled on `GrpcSubscriberCore`, reusing the existing
`MappingSpec` / `RecordExtractor` payload path so a NATS message maps to a row exactly as a polled
HTTP body does.

**Core vs JetStream**: core subscribe is at-most-once and has no cursor; JetStream durable consumers
give redelivery and acks. Ship both — `JetStream` null means core — and default to core, because a
durable consumer that nobody drains is a server-side resource this platform would be creating and
never cleaning up. `QueueGroup` is how two replicas share a subject without double-ingesting; it is
the answer to A1.3's replica problem on this path and should be documented as such.

### Wave B2 — NATS egress: the first sink

The platform has **no** outbound concept at all — every transport is inbound or read-on-demand. So
this wave introduces one, minimally: `SinkSpec { Kind, Nats }` lists on `PipelineDefinition` and
`TableDefinition` (additive `[Id(n)]`), and a `NatsPublisherService` background service that
subscribes to the *same* streams `StreamBridgeService` already consumes (pipeline results, table
deltas, source events) and republishes to a subject. Inserting at that seam means zero changes to
grains, actors or the engine.

Deliberately out of scope: sinks as first-class catalog entities with their own CRUD page, delivery
guarantees beyond fire-and-forget, and per-sink backpressure — the same honest limit as 008's
ingress, restated in the docs rather than pretended away.

### Wave B3 — console

Source editor gains the `nats` kind (subject, queue group, credentials with the mask convention,
JetStream toggle); an ingest-key panel (generate → show once → revoke) on ingest sources; the
`duplicate` counter and the `instanceId`/`aggregated` labelling on the ingress card; a sinks section
on pipeline and table detail pages; the `Journaled` mode in the persistence toggle.

---

---

## Round C — typed values out of stringly-typed messages

Real feeds arrive with every field a string: CSV, form-encoded HTTP, JSON written by a producer that
quoted its numbers, a NATS payload from a system with no schema. Today such a field can only be
declared `String`, and then it cannot be summed, compared numerically, or windowed on.

Two complementary answers, because they solve it at different moments.

### Wave C1 — conversion functions in the SQL dialect *(Engine-exclusive)*

The declarative answer, usable on any existing stream or table without re-declaring a schema.

- **Functions**: `TO_LONG`, `TO_DOUBLE`, `TO_BOOL`, `TO_TIMESTAMP`, and the inverse `TO_STRING`.
  Added to `Validator.KnownFunctions` + its arity/result-kind switches and to
  `ExpressionEvaluator`'s dispatch — the same three seams `ABS`/`ROUND`/`COALESCE` already use.
- **`CAST(expr AS type)` as sugar** desugaring to those same nodes, because that is what people type.
  Cheap in a recursive-descent parser; `AS` is already a keyword.
- **Total, never throwing**: an unconvertible value yields `NULL`, not an error. A streaming operator
  cannot throw per row without killing the pipeline for everyone, and `COALESCE` already covers
  "supply a default". This goes in DESIGN §D11's honesty list — silent NULL on bad input is a real
  tradeoff, not a detail.
- **JSON leaves are in scope**: `payload -> 'qty'` yields a JSON scalar, and casting that is exactly
  the case this wave exists for — it is also the repo's own documented `->` vs `->>` gotcha, which
  these functions make survivable.
- **`TO_TIMESTAMP`** accepts epoch-ms (numeric or numeric string) and ISO-8601, reusing the rules
  `AppCore/Ingest/RowTimestamp.Resolve` already applies on the inbound path. **`TO_STRING`** is
  culture-invariant and ISO-8601 for timestamps: locale-dependent formatting inside a data pipeline
  is a bug factory.
- **One implementation, not two.** `AppCore/Ingest/FieldValueCoercion.TryCoerce` already implements
  exactly these conversions for every inbound path. The dependency runs **AppCore → Engine** (it is
  right there in the csproj), so the canonical version moves *down* into the Engine — keyed on
  `FieldKind`, since the Engine may not reference Contracts either — and `FieldValueCoercion` becomes
  a `FieldType` → `FieldKind` adapter over it. Same for `RowTimestamp.Resolve` and `TO_TIMESTAMP`.
  Behavior must not change in the move: `Timestamp` and `Long` share one epoch-millis representation,
  `Json` is structural and passes through unconverted, and the contract is a non-throwing
  `bool TryX(..., out …)` over an already-JSON-normalized leaf.

### Wave C2 — declare the type, let the inbound path parse *(with round B, shares connector files)*

The imperative answer, for when the producer will never change: declare the field as `Long`/`Double`/
`Timestamp` and have ingestion coerce on arrival, uniformly across **every** inbound path — push
ingress, the four connector kinds, and NATS. Push ingress already does this; the connector/mapping
path does not, which is the actual gap.

A per-source `OnCoercionFailure` policy — `Null` (default), `DropRow`, or `RejectBatch` — with the
failures counted and surfaced, never silently dropped. Round A's rule stands: coerce before admission,
so a rejection leaves nothing behind.

---

## Cross-cutting rules

Same as plan 008: Sonnet 5 subagents at high effort with strictly disjoint file ownership; contracts
pre-assigned to the orchestrator and evolved additively only (next free `[Id(n)]`); the Engine gains
no Orleans/Dapr/ASP.NET types; new test files only; every wave gates on both solutions building, both
suites green, `bun run build` when `web/` is touched, and a live check on isolated 6xxx–9xxx ports
with the instance killed afterwards. Commit and push per wave.
