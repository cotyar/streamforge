# 023 — Source stability (no lost rows) and plugin hooks — the integrator report

Status: **DONE on Orleans** (2026-09-04, two waves, six + one agents). Dapr owes three items, see
[`dapr/PARITY.md`](../dapr/PARITY.md) § D6.

## Why

The first out-of-tree connector team reported seven items after operating their plugin against a
real daemon: two real relay bugs (a newly enabled source's first burst never reached the console;
the per-source tape *dropped* events inside its 50 ms slot), two UI-plugin gaps (no cache-busting,
no way to suggest the entity's Name), and three asks (push discovered Fields into the editor, make
the loader's dependency/version failures legible, document `CopyLocalLockFileAssemblies`). The
user's own framing was blunter: rows from non-generator sources appeared to vanish, and that is
unacceptable. So the brief became: prove, with tests, that `file`/`folder`/`url` sources and a
gRPC-chained pair of hosts lose nothing; fix whatever the tests find; and make UI plugins
extensible (Name, Description, Fields, tags) and writable in TypeScript.

None of the integrator's four "already fixed in our fork" patches existed in this checkout.

## What exploration found (the real losses, in order of severity)

1. **Folder kind dropped good rows forever.** `ExecuteFolder` ledgered every file that parsed but
   returned an aggregate `Error` if any file failed; the driver emits nothing on an error and
   persists the ledger regardless. Three good files next to one bad one: 0 rows, never retried.
   Reproduced by `SourceExactCountClusterTests.Folder_a_malformed_file…` before the fix
   (expected 60, got 0).
2. **A source could stop silently.** The emission loop, the state write and the timer re-arm sat
   outside the cycle's try/catch; the grain timer is one-shot, so one `OnNextAsync` throw stopped
   the source until its activation was recycled, and rows after the throw were ledgered-and-lost.
3. **Creation-time window.** A source has no memory of what it emitted. The natural console flow
   — create the source (enabled), then write the table SQL — meant the first poll fired long
   before the table subscribed; with a dedup key those rows never came back.
4. **Boot window.** `EnsureInitializedAsync` started sources before pipelines/tables, and ran
   TWICE at boot (Program.cs and the bridge both call it) with `TableGrain.StartAsync` resetting a
   live table each time, so the second pass wiped whatever the first let in. The supervisor's
   15 s sweep could also wake a persisted-Running connector (immediate overdue poll) early.
5. **The console tape** (the integrator's #3/#4): no source lifecycle events; the bridge found a
   new source only via a 30 s add-only poll and dropped anything <50 ms after the last relay.

## Decisions

- **D1. Per-file isolation, error means "nothing usable".** `PollCycleResult` gained an additive
  `Note`; a bad file becomes a Note (surfaced through `LastError` like coercion notes), stays
  un-ledgered and is retried every cycle; the good files land. "folder not found" stays an Error.
- **D2. The cycle always re-arms.** Emission failures fold into the ordinary error path; cursor,
  dedup keys and ledger persist only on emission success (a failed emit re-reads the page —
  at-least-once, a dedup key suppresses what got out); `finally` re-arms the timer, guarded by
  the generation counter so a raced `StartAsync` keeps the timer it armed.
- **D3. Late-consumer replay: a bounded in-memory ring plus an attach gate**, not a sequence
  column. `IConnectorGrain.BeginAttachAsync/EndAttachAsync`: a consumer that starts late holds
  the source's publishing, takes the ring's snapshot, subscribes, feeds the snapshot through its
  normal handler, releases; rows produced meanwhile are held and flushed afterwards, so the late
  consumer gets them once and existing consumers see no duplicates. Ring = last 10 000 rows per
  source (`ponytail:` ceiling named in code), in-memory only; a 10 s safety timer releases a hold
  whose owner died. **Measured hole**: the gate stops the source *publishing*; it cannot reach
  into Orleans' delivery pipeline, so a consumer subscribing within ~one pull period (100 ms) of
  a publish can see an in-flight row live AND in the replay (501/500 idle, 554/500 under
  whole-suite load, exactly 500 once quiescent). Exactly-once outside that window, at-least-once
  inside it; keyed (`LATEST BY`) tables are exact regardless. Documented on the interface.
- **D4. Consumers before producers, one resume pass per activation.** The resume block resumes
  pipelines and tables first, then sources; a `_resumed` latch (set as the LAST statement, so a
  mid-way throw lets the other boot caller retry) makes the second boot pass a no-op; the CRDT
  replay that used to re-run every pass now runs once. The supervisor awaits the latched
  `EnsureInitializedAsync` before pinging.
- **D5. Pace, then degrade.** The tape relay waits out the remaining 50 ms slot instead of
  dropping (per-subscription callbacks are sequential on both transports); after 40 consecutive
  paced sends (~2 s) a sustained >20 msg/s firehose falls back to one send per slot instead of
  trailing indefinitely. Never keyed on `_ts` (ingest rows carry historical timestamps).
  Measured: a 6-event burst relayed 1–2 of 6 under the old drop, 6 of 6 paced; 300 eps for 60 s
  on an isolated host produced no `QueueCacheMiss`, no bridge warning.
- **D6. One UI hook, not N.** `TransportEditorProps` gains `draft?` (read-only
  `{name, description, fields, tags}`) and `onSuggest?(patch)`; `applySuggestion` is a pure
  function: name/description only while the user's field is blank, fields replace (like
  Discover schema), tags union, `{}` on no-op so an effect-driven plugin cannot loop.
  `apiVersion` 3. Sinks get neither (no name/description on a sink).
- **D7. TypeScript plugins are transpiled in the browser**, by sucrase loaded lazily only when a
  `.ts`/`.tsx` is listed (200 kB chunk, never in the entry bundle), classic JSX off
  `window.streamsforge.react`, imported through a `blob:` URL. Single file, no `import`
  statements at all — the automatic JSX runtime is a blocker (it emits `import "react/jsx-runtime"`)
  and blob modules resolve nothing. Typings: `web/plugins-example/streamsforge-plugin.d.ts`.
- **D8. Cache-busting by URL.** The listing is `Cache-Control: no-store` and every URL carries
  `?v=` (mtime ticks on disk, MVID for an embedded module); the file response is unchanged.
- **D9. Two-pass plugin loading + a version-conflict line.** Every DLL is made resident before
  any is scanned (order-independent), and a plugin built against a NEWER assembly than the host
  holds gets one report line naming both versions and the `TypeInitializationException` it
  will cause. Host-newer is normal unification and stays silent.

## Waves

- **Wave 1** (six worktree-isolated agents, disjoint files): A folder isolation + re-arm + replay
  (Opus); B lifecycle events + pacing + boot order + supervisor (Opus); C exact-count cluster
  tests; S loader + endpoint; W web hooks + TS; D docs (Sonnet). Cherry-picked onto master in
  order D, W, S, B, C, A.
- **Wave 2** (Opus): `orleans/tests/StreamsForge.Chain.Tests` — `HostProcess`, `TwoHostFixture`,
  a two-host folder→gRPC-by-peer-name→table chain (1000/1000 rows, `seq` set complete,
  `eventsEmittedTotal == 1000`, ~30–55 s), and a host restart test.

## Acceptance criteria — outcomes

- File 500 → 700, folder 15 files slipping in mid-poll → 300, url 300 → 450: exact, twice.
- Folder with one malformed file: 60 then 80, `LastStatus == "ok"`, `LastError` names the file.
- Table created AFTER a file source already emitted 500 rows: exactly 500, then 700; the same
  with `Parallelism = 2`; a late pipeline counts every delivery via `TotalRowsOut`.
- Bridge: a new source's first event reaches the hub within 10 s; 6/6 burst; delete unsubscribes.
- Chain: every one of 1000 rows written on host A lands in a table on host B, four green runs.
- Restart: see "Found and not fixed" — the plan's literal shape is unsatisfiable on any code.
- Plugin loader: 7 pure diagnostics tests, `.tsx` listed/served/embedded, `?v=` changes with bytes.
- Web: 53 bun tests; the transpiled TSX example registers `['nats', fn, 'inbound']`.
- Full suites: Dapr fully green (1602); Orleans green except the 6 known pre-existing
  `CodecNotFoundException` tests and the load-induced re-runs recorded below.

## Found and not fixed

- **Restart empties a table over a `url` source with a dedup key — on every version of the
  code.** `TableGrain` resets a resuming table to empty (the plan-020 restart-resume limitation)
  and `ConnectorGrainState.DedupKeys` is persisted every cycle, so the first post-restart poll
  emits nothing and the table never refills. `HostRestartTests` therefore grows the served
  dataset across the restart and asserts that exactly the NEW rows land (and the old ones do
  not, which is the proof the dedup keys suppressed them). The pre-fix registry passed that test
  3/3 on this machine (both boot passes fit inside one 1 s poll interval), so it is a regression
  guard on the ordering, not a reproduction. The real fix is a persisted-snapshot resume for
  tables (or a replay for polled sources on resume) — its own plan.
- **File-kind mtime race.** The ledger is (name, mtime in ms); a poll landing mid-append records
  the mtime it saw before reading, and if the append finishes in the same millisecond the tail
  waits for the next real edit (observed 73/200 under whole-suite load). Pre-existing.
- **Replay-ring ceiling and the in-flight overlap** (D3). Per-kind policy or a persisted ring, and
  a seam into the delivery pipeline, are the upgrade paths.
- **Grain-reactivation tests** remain the six known `CodecNotFoundException` failures, which is
  why the boot order is verified at process level, not in a `TestCluster`.
- Dapr: D6 in `dapr/PARITY.md` (lifecycle + pacing, boot order, attach protocol). gRPC reconnect
  backoff is a private 30 s constant; a knob is a contract change.
