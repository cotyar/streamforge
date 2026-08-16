using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Serialization.Invocation;
using Orleans.Streams;
using StreamForge.Abstractions;
using StreamForge.Engine;
using StreamForge.Engine.Dataflow;
using StreamForge.Engine.Runtime;
using StreamForge.Host.Search;

namespace StreamForge.Host.Grains;

public sealed class TableGrainState
{
    /// <summary>Consolidated output snapshot: canonical rowKey -> (row, weight). Write-behind persisted
    /// (dirty flag + periodic flush) — see TableGrain's class comment for the restart-resume tradeoff.
    ///
    /// Plan 011 D2: left permanently EMPTY for a SHARDED table (non-empty <c>TableDefinition.ShardBy</c>),
    /// where the shard grains are the durable per-key copy and this would be a second copy of the same
    /// rows — see TableGrain's "sharded tables" paragraph. <see cref="HadRows"/> carries the one thing
    /// this mirror was actually load-bearing for.</summary>
    public Dictionary<string, TableRowDto> Snapshot { get; set; } = [];
    public long Seq { get; set; }

    /// <summary>Plan 011 D2 — RESUME DETECTION WITHOUT THE MIRROR, and the reason shedding it is even
    /// possible. A non-empty <see cref="Snapshot"/> is how StartClassicAsync/StartCoordinatorAsync tell a
    /// resume from a first-ever start, and that is the ONLY thing they use it for: the resumed rows are
    /// then thrown away and the table rebuilds from live traffic (the restart-resume limitation in
    /// TableGrain's class doc). So the mirror was durably storing every row of the table in order to
    /// answer one boolean. On a sharded table the boolean is stored directly instead — O(1) where the
    /// mirror was O(distinct keys) — and behavior is unchanged.
    ///
    /// Maintained ONLY for sharded tables; an unsharded table never writes it and its resume detection is
    /// byte-identical to before. Absent in an existing data dir, which reads as false, which is right for
    /// any table that has never been sharded.</summary>
    public bool HadRows { get; set; }
}

/// <summary>Plan 009 A2: one journaled change since the last compaction — the second, SMALL persisted state
/// <see cref="TablePersistenceMode.Journaled"/> writes on every flush instead of rewriting the whole
/// <see cref="TableGrainState.Snapshot"/> (see TableGrain's class doc, Plan 009 A2 paragraph, for why a
/// second small state — not an append-only storage provider — is the honest way to get an O(changed) write
/// out of JsonFileGrainStorage's rewrite-the-whole-object model). <see cref="Weight"/> &gt; 0 means "this
/// canonical row key is (now) present with this row/weight"; &lt;= 0 is an explicit REMOVAL TOMBSTONE — Row
/// is left empty because replay only needs to know to remove the key, never what it used to contain. An
/// ABSENT key means "unchanged since the last compaction", never a removal — collapsing that distinction
/// (e.g. by just not recording a deleted row at all) is exactly the bug that resurrects deleted rows on
/// replay; see TableGrain.ReplayJournalIntoSnapshot.</summary>
public sealed class TableJournalEntry
{
    public Dictionary<string, object?> Row { get; set; } = [];
    public long Weight { get; set; }
}

/// <summary>Plan 009 A2: <see cref="TablePersistenceMode.Journaled"/>'s second persisted state — keyed by
/// canonical row key so repeated changes to the SAME key before the next compaction coalesce into one entry
/// (bounding the journal's size by DISTINCT keys touched since the last compaction, not by delta volume).
/// Cleared on every compaction (TableGrain.CompactAsync) and on every StartAsync (TableGrain.StartClassicAsync/
/// StartCoordinatorAsync — see their mode-switch-consistency comment); replayed onto
/// <see cref="TableGrainState.Snapshot"/> on activation, before that reset.</summary>
public sealed class TableJournalState
{
    public Dictionary<string, TableJournalEntry> Entries { get; set; } = [];
}

/// <summary>
/// Key = table name. One activation per running table. Subscribes to its SQL's stream inputs (the
/// existing "sources" namespace) and table inputs ("table-delta", upstreamName), feeds every delta
/// through a StreamForge.Engine <see cref="TableExecutor"/> (Z-set / DBSP-style incremental view
/// maintenance), publishes emitted deltas to ("table-delta", ownName) for downstream tables and
/// StreamBridgeService, and persists a consolidated snapshot with write-behind (dirty flag, flushed every
/// 2s or on deactivate — mirrors PipelineGrain's metrics-timer pattern; one JSON write per delta would
/// thrash).
///
/// RESTART-RESUME LIMITATION: the persisted snapshot only ever captures this table's OUTPUT rows, not its
/// operators' internal state (join indexes, GROUP BY multisets/accumulators). That internal state cannot
/// be reconstructed from the output alone, so a full checkpoint/replay is out of scope here (future work).
/// The honest tradeoff taken: on resume, the last-flushed snapshot is served immediately for read
/// availability (GetRowsAsync keeps returning it), but the table is marked "rebuilding" and its executor +
/// snapshot are reset to empty — it rebuilds purely from live traffic going forward, exactly like a table
/// that just started for the first time. GetMetricsAsync exposes the Rebuilding flag.
///
/// PLAN 003 M2 — PARALLELISM &gt;= 2 (coordinator mode): everything above this paragraph describes the
/// Parallelism==1 fast path, kept byte-for-byte unchanged (see the Parallelism&lt;=1 branch in StartAsync/
/// StopAsync below — zero-risk default per the M2 task). For Parallelism &gt;= 2, this grain becomes a
/// coordinator + read grain instead of running the SQL itself: StartAsync deploys the partitioned graph
/// (one ITableOutputGrain, one ITableStageGrain per (non-Ingest stage, partition), one ITableIngestGrain
/// per real external input — see StreamForge.Engine.Dataflow.TableDataflowPlan and TableIngestGrain/
/// TableStageGrain/TableOutputGrain's class docs), then subscribes to its OWN
/// (StreamConstants.TableDeltaNamespace, tableName) delta stream — the same stream TableOutputGrain
/// publishes to — and feeds those deltas into EXACTLY the same read-side machinery
/// (state.State.Snapshot + TableSearchIndex) the Parallelism==1 path already uses, just fed by the
/// partitioned graph's output instead of a locally-run TableExecutor. Rows/search/metrics/history/SignalR
/// all therefore go through the identical code paths regardless of Parallelism — see
/// GetRowsAsync/GetMetricsAsync/SearchAsync below, none of which branch on Parallelism at all. Consolidation
/// of the incoming delta stream (Z-set summation: weight &lt;= 0 removes, else updates) is reimplemented
/// here directly on the public TableRowDto shape rather than reusing StreamForge.Engine.Runtime's internal
/// consolidation (Host has no InternalsVisibleTo into Engine — see AssemblyInfo.cs — matching the existing
/// precedent in TableRowHistory.cs, which re-derives its own key logic rather than reaching into Engine
/// internals); a scratch TableExecutor (created, never fed any events) supplies CanonicalRowKey, the one
/// piece of key-derivation logic that IS already public.
///
/// PLAN 003 M4 — FRONTIER-CONSISTENT READS: coordinator mode no longer self-subscribes to its own output
/// delta stream to feed the read-side snapshot (that was the pre-M4 design). Each terminal-stage
/// TableStageGrain's ITableOutputGrain.PublishAsync call now ALSO calls this grain's
/// <see cref="OnOutputBatchAsync"/> directly, carrying (fromPartition, epoch) alongside the deltas; this
/// grain buffers those per (partition, epoch) with the same FrontierTracker+EpochBuffer M0 primitives every
/// other dataflow hop uses (_outputFrontier/_outputBuffer below), registered over every terminal-stage
/// partition, and only consolidates a batch into _coordinatorLedger/the search index once EVERY terminal
/// partition has reported reaching that epoch. <see cref="GetSnapshotFrontierEpochAsync"/> (mirrored on
/// TableMetrics.SnapshotFrontierEpoch) then reports that same epoch.
///
/// THE CONSISTENCY STATEMENT this makes true (not just documented, but true BY CONSTRUCTION — see
/// OnOutputBatchAsync): at any point a caller observes SnapshotFrontierEpoch == F via GetRowsAsync/
/// GetMetricsAsync/GetSnapshotFrontierEpochAsync, the rows/search results served AT THAT SAME MOMENT
/// reflect ALL deltas whose epoch is &lt;= F and NONE beyond it. This holds because (a)
/// _coordinatorLedger is only ever mutated inside OnOutputBatchAsync, synchronously, with no `await`
/// between reading the newly-ready batches and updating _snapshotFrontierEpoch (Orleans single-threads one
/// grain's turns, so no GetRowsAsync call can be dispatched mid-way through one OnOutputBatchAsync
/// invocation), and (b) a batch is never consolidated before _outputFrontier's combined frontier (min over
/// every terminal partition's own high-water mark) has reached its epoch — so nothing above F is ever
/// partially reflected, and _snapshotFrontierEpoch is only ever raised to a value that has actually just
/// been applied, never ahead of it (EpochBuffer.OnFrontier returns exactly the batches whose epoch &lt;=
/// the new frontier; OnOutputBatchAsync applies precisely those before advancing _snapshotFrontierEpoch to
/// that same frontier — no gap between "applied" and "reported").
///
/// This is a genuine behavior change from the pre-M4 design (see OnOutputBatchAsync's own doc comment, and
/// ITableOutputGrain.PublishAsync's): the OLD design applied each terminal partition's batch to the
/// snapshot IMMEDIATELY on arrival, with no cross-partition epoch gating at all, so a GetRowsAsync call
/// between two partitions' arrivals for what should be "the same round" could observe a genuinely partial
/// epoch — and there was no frontier signal to report in the first place. The (StreamConstants.
/// TableDeltaNamespace, tableName) delta stream itself — the one SignalR/TableHistoryGrain/downstream
/// tables consume — is UNCHANGED: TableOutputGrain still republishes every incoming batch onto it
/// immediately, in receipt order, exactly as before M4 (see that grain's doc comment).
///
/// LATE/OUT-OF-ORDER INPUT POLICY (documented here as the table-wide doc-of-record — see TableIngestGrain's
/// own doc comment for the partitioned-path mechanism): every event is epoch-stamped at ARRIVAL, by
/// whichever ingest flush window (250ms tick or 1000-event threshold, whichever first) it lands in — there
/// is no retro-dating to an earlier epoch and no watermark-based drop at the dataflow layer. A "late" event
/// (by business-time, e.g. an old order_events row) simply lands in whatever epoch it happens to arrive in;
/// it is processed exactly like any other delta in that epoch — deterministic, honest, no silent loss.
/// Table-mode operators have no epoch/watermark-driven eviction (unlike pipelines' windows/joins — see
/// StreamForge.Engine.Runtime.Ops.ITableOp's doc comment), so nothing here ever expires or gets evicted for
/// being late. The ONE exception, and it is a QUERY semantic, not a dataflow-layer drop: `LATEST BY` (see
/// StreamForge.Engine.Runtime.Ops.TableLatestByOp's doc comment) compares the arriving row's OWN Ts field
/// against the currently-retained row's Ts for that key, and ignores a strictly-older-Ts arrival — that's
/// business-time ordering the query explicitly asked for, orthogonal to (and unrelated to) dataflow-layer
/// epoch/arrival-order lateness, which this table (and every other) never drops.
///
/// PLAN 008 W2.5 — PER-TABLE PERSISTENCE MODE: the write-behind flush described above (dirty flag + timer,
/// mirrors PipelineGrain's metrics-timer pattern) is <see cref="TablePersistenceMode.Batched"/>, still the
/// default and still byte-identical to pre-008 — the flush interval is now configurable
/// (<see cref="TableDefinition.FlushMs"/>, 0 = the pre-008 hardcoded 2000ms) but the write is awaited inside
/// the grain turn exactly as before, so the turn stalls for as long as the whole-snapshot serialize + file
/// write takes. <see cref="TablePersistenceMode.FireAndForget"/> keeps the same timer but does not await the
/// write: <see cref="OnFlushTickAsync"/> captures the snapshot into <c>state.State</c> synchronously (that
/// part MUST stay on the turn — TableExecutor/_coordinatorLedger are not thread-safe and nothing else may
/// touch them mid-capture), then hands the actual `state.WriteStateAsync()` off via <see cref="_pendingWrite"/>
/// without awaiting it, so the turn returns immediately. Single-flight is enforced by checking
/// `_pendingWrite.IsCompleted` before starting a new capture — a tick that lands while the previous write is
/// still in flight is skipped outright (not queued, not overlapped): <see cref="JsonFileGrainStorage"/> does
/// `File.Create` + async serialize straight against the same mutable `state.State` object, so two concurrent
/// `WriteStateAsync` calls would race two file handles on the same path and/or serialize a
/// concurrently-mutated object — a corruption hazard, not a speedup. <see cref="TablePersistenceMode.MemoryOnly"/>
/// registers no flush timer at all (see StartClassicAsync/StartCoordinatorAsync) — `_dirty` keeps getting set
/// by ApplyAndPublishAsync/OnOutputBatchAsync same as always, it just never gets consumed, and both
/// StopAsync's and OnDeactivateAsync's final-flush gates additionally check the mode so a MemoryOnly table's
/// last snapshot is deliberately discarded, not persisted, on the way out — a restart brings it back empty,
/// which is documented as the mode's contract, not a bug (see TablePersistenceMode's own doc comment).
/// FireAndForget's *final* flush (stop/deactivate) is the one place it behaves like Batched again: the grain
/// is going away either way, so a background write would just be abandoned mid-flight — StopAsync/
/// OnDeactivateAsync both await any still-in-flight `_pendingWrite` first (so the final awaited FlushAsync
/// below never overlaps a background one), then call the same awaited <see cref="FlushAsync"/> Batched uses.
///
/// PLAN 009 A2 — JOURNALED MODE: <see cref="TablePersistenceMode.Journaled"/> keeps a SECOND persisted state
/// (<c>[PersistentState("table-journal", ...)] journalState</c>, <see cref="TableJournalState.Entries"/> —
/// see that class's own doc comment) recording only the canonical row keys that changed since the last
/// compaction, instead of rewriting the whole <c>state.State.Snapshot</c> on every flush. WHY A SECOND STATE,
/// NOT AN APPEND-ONLY STORAGE PROVIDER: both <see cref="JsonFileGrainStorage"/> (this project) and Dapr's
/// state store rewrite a WHOLE state object on every write — there is no "append a few bytes" primitive at
/// that layer, so an "append-only journal" built as a storage-provider feature would be a lie: the "append"
/// would still cost a full-object rewrite underneath. Rewriting a SMALL state (the journal) instead of the
/// whole snapshot is the honest way to get an O(changed) write out of that existing abstraction, and it is
/// the only shape that works identically on both flavors (see dapr/src/StreamForge.Dapr.Host/Actors/
/// TableActor.cs's mirror-image paragraph). Two rejected alternatives, and why: handing the write to a
/// separate grain buys nothing — the argument would still be serialized on THIS grain's own turn before the
/// hop, so the expensive half never actually moves; sharding the table across grain-per-partition would give
/// O(changed) too, but costs the atomicity of one consolidated snapshot and needs an epoch fence at recovery
/// or the parts resurrect mutually inconsistent — the journal gets the same asymptotics with neither cost.
///
/// <see cref="JournalFlushAsync"/> (dispatched from <see cref="OnFlushTickAsync"/>/<see cref="FlushDirtyAsync"/>
/// exactly where Batched's <see cref="FlushAsync"/> is) merges <see cref="_pendingJournalEntries"/> (this
/// tick's touched canonical keys — <see cref="RecordJournalEntries"/>, called from the same
/// ApplyAndPublishAsync/OnOutputBatchAsync sites <see cref="ReflectDeltasInSearchIndex"/> is) into the
/// PERSISTED <c>journalState.State.Entries</c>, coalescing repeated changes to the same key, then writes ONLY
/// that journal. Once the journal reaches <see cref="TableDefinition.JournalMaxEntries"/> (0 =
/// <see cref="DefaultJournalMaxEntries"/>), it compacts: <see cref="CompactAsync"/> writes the full snapshot
/// once (the same capture <see cref="FlushAsync"/> uses) and truncates the journal back to empty, resetting
/// the O(changed-since-compaction) counter to zero.
///
/// SAME DURABILITY CONTRACT AS BATCHED, NOT A NEW ONE: Journaled's write (small or, at compaction, full) is
/// always AWAITED inside the grain turn, exactly like Batched — it trades write VOLUME for O(changed), not
/// durability for latency the way FireAndForget does. Because neither JournalFlushAsync nor the CompactAsync
/// it sometimes calls ever runs in the background, and Orleans single-threads one grain's turns, a normal
/// journal flush and a compaction it triggers can never overlap — the two-concurrent-writers hazard
/// FireAndForget's <see cref="_pendingWrite"/> single-flight guard exists to prevent (see the paragraph
/// above) simply cannot arise here, by construction, with no extra guard needed.
///
/// RESUME: StartClassicAsync/StartCoordinatorAsync call <see cref="ResumeJournaledState"/>, UNCONDITIONALLY
/// (not gated on the mode being started — see that method's own doc comment for why: a mode switch away
/// from Journaled must not lose entries a prior Stop already wrote to the journal), to merge
/// <c>journalState.State.Entries</c> onto <c>state.State.Snapshot</c> BEFORE the existing "non-empty
/// snapshot means resume" check just below — so that check (and the reset it performs) sees the
/// TRUE last-known row set, exactly what Batched's own last flush would have captured had it flushed at that
/// same moment. That is what makes a Journaled table's resume-detection, and its resumed row set, byte-
/// identical to Batched's — including the shared reset itself: the RESTART-RESUME LIMITATION above is
/// UNCHANGED and applies here too, so the merged-in row set is immediately reset to empty and marked
/// Rebuilding exactly like Batched's, not served. The journal is then unconditionally cleared — in memory
/// AND on disk — on every StartAsync, regardless of which mode this activation is starting in: a table that
/// WAS Journaled and switches to Batched (or MemoryOnly, or FireAndForget) must not leave a stale journal
/// behind for some LATER switch back to Journaled to incorrectly replay. That unconditional clear is what
/// makes BOTH directions of a mode switch leave the table consistent.
///
/// PLAN 011 WAVE C — THE FLUSH CAPTURE IS O(CHANGED) IN EVERY MODE, AND WHAT THAT DOES *NOT* FIX. The
/// per-tick capture described above (<see cref="CaptureSnapshotIntoState"/>) no longer rebuilds the whole
/// mirror; it applies only the row keys whose ledger entry changed since the previous capture
/// (<see cref="_touchedRowKeys"/> — see that field's and the capture's own doc comments for the invariant).
/// No persistence mode's CONTRACT moves: Batched still awaits a full state write on its interval,
/// FireAndForget still backgrounds it under the same single-flight guard, Journaled still writes only its
/// small journal and compacts on the same threshold, MemoryOnly still never touches storage (and, per
/// <see cref="RecordTouchedRowKeys"/>, does not even accumulate the touched-key set). What changed is the
/// ALLOCATION each tick pays, from O(|table|) to O(rows changed this interval).
///
/// Honestly stated, because it is the difference between a fix and a claim: this bounds the RATE of garbage,
/// not the SIZE of the table. Batched/FireAndForget still SERIALIZE the whole snapshot on every write — that
/// IS Batched's contract (JsonFileGrainStorage, like Dapr's state store, rewrites a whole state object; see
/// the Journaled paragraph above), and a table whose key space grows without bound still grows without
/// bound in memory: the executor's own ledger, the search index, and this mirror are all O(distinct keys).
/// Bounding the key space itself needs a retention policy (plan 011 wave C2) or per-key sharding (wave D).
/// Every unbounded structure in the table path is now enumerated in orleans/DESIGN.md's "Known ceilings".
///
/// PLAN 011 WAVE D2 — A SHARDED TABLE STOPS KEEPING THE DUPLICATE COPY. When
/// <see cref="TableDefinition.ShardBy"/> is non-empty, this grain no longer maintains or persists
/// <c>state.State.Snapshot</c> at all (<see cref="_shardMirrorSuppressed"/>): the capture writes only the
/// O(1) <see cref="TableGrainState.HadRows"/> marker, the journal is not written, and
/// <see cref="_touchedRowKeys"/> is not accumulated.
///
/// WHY THAT IS SOUND, not just smaller. On a sharded table the shard grains ARE the durable per-key copy
/// of the same rows — same content, arrived by the same delta stream — so the mirror was a second full
/// copy, rewritten every FlushMs and pinned in memory forever. It was never the source of a restored row:
/// the resume path reads it, notes "this is a resume", and then RESETS it to empty (see the RESTART-RESUME
/// LIMITATION above). That one boolean is what HadRows now carries. And it was not the read path either
/// for two of the four persistence modes already — MemoryOnly and Journaled read live from the executor
/// (see ClassicModeRows), so a sharded table simply joins them, which makes its <c>/rows</c> FRESHER than
/// the up-to-one-flush-interval-stale mirror it replaced, not staler.
///
/// WHAT IS DELIBERATELY *NOT* SHED, because being precise here is the difference between a measurement and
/// a slogan. Three copies of a table's rows can exist; D2 removes exactly ONE of them.
///   1. <b>The persisted mirror</b> (<c>state.State.Snapshot</c>) — REMOVED on a sharded table. This is
///      the whole of D2's memory claim.
///   2. <b>The Engine executor's own ledger</b> (<c>TableExecutorImpl</c>, and for the motivating shape
///      <c>TableLatestByOp.Current</c>) — KEPT, and unreachable from here. It is O(distinct keys) and it
///      is what the table's SQL is computed from; shedding it would mean sharding EXECUTION, which plan
///      003 superseded and plan 009 A2 rejected on record. A sharded table's memory is therefore NOT
///      O(active keys); it is O(keys) for the executor plus O(active keys) for the shard tier.
///   3. <b>The coordinator ledger</b> (<see cref="_coordinatorLedger"/>, Parallelism &gt;= 2 only) — KEPT,
///      because at Parallelism &gt;= 2 there is no local executor and this IS the table's only live copy
///      of its own output. Serving <c>/rows</c> from the shards instead would fan out across the directory
///      and wake every idle key on every poll, which is precisely the trap the whole tier is designed
///      against.
///
/// PERSISTENCE MODES, none redefined. Batched/FireAndForget keep their contracts exactly: what they write
/// is now an O(1) marker instead of an O(rows) snapshot, and neither ever restored rows in the first place.
/// Journaled's promise — Batched's durability with an O(changed) write — is STRUCTURALLY met rather than
/// bypassed: with no table-level snapshot to rewrite there is nothing to journal, and each shard's own
/// write is already O(one key). MemoryOnly + ShardBy is REFUSED at upsert (RegistryGrain.ValidateShardBy)
/// rather than reinterpreted: a shard's write on deactivation IS its swap-out, so a mode that never writes
/// would turn an idle minute into data loss — which is not what "a restart brings the table back empty"
/// promises.
///
/// [MayInterleave] ON OnOutputBatchAsync (plan 003 M4, mirrors RegistryGrain's identical fix for an
/// identical shape of problem — see that class's doc comment): TableOutputGrain.PublishAsync calls back
/// into THIS grain (the same one orchestrating TableOutputGrain/TableStageGrain's own start/stop) — without
/// interleaving, a StopAsync turn that's mid-teardown (awaiting some TableStageGrain's own StopAsync, which
/// can't complete until an in-flight PushBatchAsync finishes routing through TableOutputGrain.PublishAsync
/// back to THIS grain's OnOutputBatchAsync) deadlocks: OnOutputBatchAsync sits queued behind the very
/// StopAsync turn that's blocked waiting on it. Safe to interleave because OnOutputBatchAsync's body has no
/// `await` (runs to completion atomically once started, regardless of what else is mid-flight) and is
/// guarded by null/status checks that harmlessly no-op against a torn-down or not-yet-fully-started
/// coordinator (see StopAsync clearing _outputFrontier/_outputBuffer early, and StartCoordinatorAsync's own
/// ordering comment) — exactly the same "no-op before ready" pattern TableStageGrain/TableOutputGrain
/// already use for their own `_status != Running` guards.
/// </summary>
[MayInterleave(nameof(MayInterleave))]
public sealed class TableGrain(
    [PersistentState("table", StreamConstants.StorageName)] IPersistentState<TableGrainState> state,
    [PersistentState("table-journal", StreamConstants.StorageName)] IPersistentState<TableJournalState> journalState,
    ILogger<TableGrain> logger)
    : Grain, ITableGrain
{
    private TableDefinition? _def;
    private PipelineStatus _status = PipelineStatus.Stopped;
    private TableExecutor? _executor;
    private TableSearchIndex? _searchIndex;
    private IGrainTimer? _flushTimer;
    private readonly List<StreamSubscriptionHandle<EventRecord>> _streamSubs = [];
    private readonly List<StreamSubscriptionHandle<List<TableDeltaDto>>> _tableSubs = [];

    private bool _dirty;
    private bool _rebuilding;
    private long _deltasIn;
    private long _deltasOut;
    private long _lastUpdateMs;

    // Plan 008 W2.5 — per-table persistence mode (see class doc's own paragraph). Both re-read from the
    // TableDefinition on every StartAsync — same lifetime/refresh rule as every other per-def field above
    // (_status, _executor, etc.) — so a Persistence/FlushMs change (RegistryGrain restart-triggers it, same
    // as SQL/search-config/Parallelism) takes effect on the very next StartClassicAsync/StartCoordinatorAsync.
    private TablePersistenceMode _persistenceMode = TablePersistenceMode.Batched;
    private TimeSpan _flushInterval = TimeSpan.FromSeconds(2);
    /// <summary>FireAndForget's single-flight guard: the in-flight (or already-completed) background
    /// `state.WriteStateAsync()` task. Never faulted — <see cref="WriteStateBestEffortAsync"/> catches and
    /// logs internally — so awaiting it (StopAsync/OnDeactivateAsync) or polling `.IsCompleted`
    /// (OnFlushTickAsync) never throws.</summary>
    private Task _pendingWrite = Task.CompletedTask;

    /// <summary>Plan 009 A2 — default compaction threshold when <see cref="TableDefinition.JournalMaxEntries"/>
    /// is 0 (unset). Large enough that a table's ordinary per-tick churn doesn't compact on nearly every
    /// flush (which would degenerate into Batched with extra steps — see TablePersistenceMode.Journaled's
    /// own doc comment); small enough that activation's journal replay (<see cref="ReplayJournalIntoSnapshot"/>)
    /// stays cheap.</summary>
    public const int DefaultJournalMaxEntries = 200;

    /// <summary>Plan 009 A2 — canonical row keys touched since the LAST flush tick (not since the last
    /// compaction — that's <c>journalState.State.Entries</c>), populated by <see cref="RecordJournalEntries"/>
    /// from the same call sites that feed <see cref="ReflectDeltasInSearchIndex"/>. Only ever non-empty
    /// transiently, between a delta batch landing and the next <see cref="JournalFlushAsync"/> merging it
    /// into the persisted journal — <see cref="TablePersistenceMode.Journaled"/> only.</summary>
    private readonly Dictionary<string, TableJournalEntry> _pendingJournalEntries = [];

    /// <summary>Plan 011 wave C — canonical row keys touched since the last <see cref="CaptureSnapshotIntoState"/>,
    /// i.e. exactly the set of entries in <c>state.State.Snapshot</c> that are currently out of date with
    /// respect to the live ledger. This is what makes the periodic capture O(changed) instead of
    /// O(|table|) for EVERY persistence mode, not just <see cref="TablePersistenceMode.Journaled"/> — see
    /// <see cref="CaptureSnapshotIntoState"/>'s own doc comment for the invariant and why it holds.
    ///
    /// NOT maintained for <see cref="TablePersistenceMode.MemoryOnly"/>: that mode never captures at all
    /// (no flush timer, no final flush), so the set would never be drained and would itself become one more
    /// unbounded per-key structure — the exact class of bug this wave exists to remove.</summary>
    private readonly HashSet<string> _touchedRowKeys = new(StringComparer.Ordinal);

    /// <summary>Plan 011 wave C — forces the next <see cref="CaptureSnapshotIntoState"/> to rebuild
    /// <c>state.State.Snapshot</c> wholesale instead of applying <see cref="_touchedRowKeys"/>. Set on every
    /// StartClassicAsync/StartCoordinatorAsync (where the ledger and the mirror are both empty, so the
    /// "full" rebuild is free) so the incremental path never has to trust state carried across a
    /// start/stop cycle; cleared by the capture itself.</summary>
    private bool _fullCaptureNeeded = true;

    /// <summary>Plan 011 D2 — true when this table has a non-empty <see cref="TableDefinition.ShardBy"/>,
    /// i.e. when the shard tier holds the durable per-key copy and <c>state.State.Snapshot</c> would be a
    /// second one. See the class doc's D2 paragraph for what this suppresses and, more importantly, for
    /// the two copies it does NOT. Re-read from the definition on every StartAsync, like every other
    /// per-def field here.</summary>
    private bool _shardMirrorSuppressed;

    // Plan 003 M2 — Parallelism >= 2 coordinator-mode state (see class doc). Unused, always default, on
    // the Parallelism==1 path.
    private bool _coordinatorMode;
    private int _coordinatorParallelism;
    private List<(int StageId, int PartitionCount)> _deployedStages = [];
    private List<string> _deployedInputs = [];
    /// <summary>Plan 003 M3: every (arrangementGrainKey, consumerId) this table attached, one per
    /// (arrangeable edge, partition) — used by StopAsync to detach cleanly and by GetMetricsAsync to fold
    /// each arrangement's own Rebuilding into this table's and to report ArrangedInputs.</summary>
    private List<(string ArrangementKey, string ConsumerId)> _deployedArrangements = [];
    /// <summary>Coordinator mode's own live consolidated Z-set (canonical row key -> (row, weight)) — the
    /// coordinator-mode analogue of TableExecutor's internal ledger, fed by <see cref="OnOutputBatchAsync"/>
    /// (via <see cref="ApplyCoordinatorConsolidation"/>) and read by ReflectDeltasInSearchIndex/FlushAsync/
    /// SearchAsync exactly where the classic path reads `_executor.Snapshot()`.
    ///
    /// Plan 009 wave D: this used to be two hand-rolled dictionaries here (`_coordinatorSnapshot` + a
    /// separate `_coordinatorDebt` side-table for outstanding negative running weight) — the exact same
    /// shape and arithmetic TableExecutorImpl and ArrangementGrain each separately hand-wrote too. Now a
    /// shared <see cref="ConsolidationLedger"/> (Engine-side, since Host references Engine) — see its own
    /// class doc for the full order-independence argument (why a negative delta with no prior positive
    /// weight is retained as debt rather than dropped: this grain fans terminal-partition batches in from N
    /// partitions, and an upstream outer join emits retraction-driven pads, so per-key causal order is not
    /// guaranteed across that fan-in, replay, or a restart-resume).</summary>
    private readonly ConsolidationLedger _coordinatorLedger = new();

    /// <summary>Plan 003 M4 — the M0 primitives (see FrontierTracker/EpochBuffer's own doc comments),
    /// registered over every terminal-stage partition (one UpstreamId per partition, keyed on the
    /// compiled dataflow's TerminalEdge.EdgeId — the one edge every terminal partition's own
    /// RouteDownstreamAsync call reports on), driving <see cref="OnOutputBatchAsync"/>. Null on the
    /// Parallelism==1 path and before StartCoordinatorAsync has run.</summary>
    private FrontierTracker? _outputFrontier;
    private EpochBuffer? _outputBuffer;
    private EdgeId _terminalEdgeId;
    /// <summary>Plan 003 M4 — the epoch _coordinatorLedger currently, honestly, fully reflects (see class
    /// doc's consistency statement). Null until OnOutputBatchAsync has observed at least one full round
    /// (every terminal partition reporting) since the last StartCoordinatorAsync.</summary>
    private long? _snapshotFrontierEpoch;

    public async Task StartAsync(TableDefinition def)
    {
        await StopAsync();

        _def = def;
        _shardMirrorSuppressed = def.ShardBy.Count > 0;
        _persistenceMode = def.Persistence;
        _flushInterval = TimeSpan.FromMilliseconds(def.FlushMs > 0 ? def.FlushMs : 2000);

        if (def.Parallelism <= 1)
        {
            await StartClassicAsync(def);
        }
        else
        {
            await StartCoordinatorAsync(def);
        }
    }

    private async Task StartClassicAsync(TableDefinition def)
    {
        var registry = GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sources = await registry.GetSourcesAsync();
        var streamSchemas = sources.ToDictionary(
            s => s.Name,
            s => new SourceSchema(s.Name, s.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));

        var tables = await registry.GetTablesAsync();
        var tableSchemas = tables
            .Where(t => t.OutputFields.Count > 0)
            .ToDictionary(
                t => t.Name,
                t => new SourceSchema(t.Name, t.OutputFields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));

        var compileResult = SqlCompiler.CompileTable(def.Sql, streamSchemas, tableSchemas);
        if (!compileResult.Ok || compileResult.Plan is null)
        {
            var message = string.Join("; ", compileResult.Diagnostics.Select(d => $"{d.Line}:{d.Column} {d.Message}"));
            throw new InvalidOperationException(message);
        }

        _executor = compileResult.Plan.CreateExecutor();
        ApplyRetentionPolicy(compileResult.Plan, def);
        _status = PipelineStatus.Running;

        ResumeJournaledState();

        // See class-level comment: a non-empty persisted snapshot means this is a resume (not a first
        // start) — operator internal state can't be rebuilt from it, so mark rebuilding and reset to empty.
        //
        // Plan 011 D2: HadRows is the same signal for a SHARDED table, which keeps no mirror to inspect —
        // see TableGrainState.HadRows. Both are checked on both kinds of table, deliberately: a table that
        // was sharded and has just been un-sharded (or the reverse) must resume correctly across the
        // transition, and whichever of the two marks its previous life is the one that fires.
        if (ClearResumeMarkersAndDetect())
        {
            _rebuilding = true;
        }

        await ClearStaleJournalAsync();

        // Plan 011 wave C: the incremental capture's invariant is re-established from scratch here — a
        // brand-new (empty) TableExecutor above and an empty state.State.Snapshot either way, so the one
        // forced full capture this schedules is O(0). See CaptureSnapshotIntoState's doc comment.
        _touchedRowKeys.Clear();
        _fullCaptureNeeded = true;

        // Either branch above leaves the current row set empty (fresh start, or reset-for-rebuild), so a
        // freshly built (empty) index is accurate here — it fills back in incrementally as
        // ApplyAndPublishAsync observes deltas going forward, exactly like state.State.Snapshot does via
        // FlushAsync (just without the 2s lag, since Snapshot() is an O(1) live dictionary reference).
        _searchIndex = def.SearchEnabled ? new TableSearchIndex(def.SearchMode) : null;

        await WarnIfTableInputsAlreadyHoldRowsAsync(def.Name, compileResult.TableInputs);

        var streamProvider = this.GetStreamProvider(StreamConstants.ProviderName);
        foreach (var name in compileResult.StreamInputs.Distinct())
        {
            var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, name));
            var handle = await stream.SubscribeAsync((evt, _) => OnStreamEventAsync(name, evt));
            _streamSubs.Add(handle);
        }
        foreach (var name in compileResult.TableInputs.Distinct())
        {
            var stream = streamProvider.GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, name));
            var handle = await stream.SubscribeAsync((deltas, _) => OnTableDeltaBatchAsync(name, deltas));
            _tableSubs.Add(handle);
        }

        // MemoryOnly registers no flush timer at all — see class doc's persistence-mode paragraph.
        if (_persistenceMode != TablePersistenceMode.MemoryOnly)
        {
            _flushTimer = this.RegisterGrainTimer(OnFlushTickAsync, _flushInterval, _flushInterval);
        }

        // Keep this activation alive for as long as the table is running — mirrors PipelineGrain.
        this.DelayDeactivation(TimeSpan.FromDays(365));
    }

    /// <summary>
    /// Wishlist #14, option (b) — "at minimum a diagnostic at create time when an input LATEST BY table is
    /// non-empty". Deliberately the LOUD half rather than the fix: replaying an already-populated upstream
    /// table's current contents into a brand-new consumer (option (a)) needs the snapshot read and the
    /// delta-stream subscription to agree on exactly one point in the upstream's own history, and this
    /// table's classic (Parallelism==1) subscription path has no such point to agree on — SnapshotFrontierEpoch
    /// (see class doc's PLAN 003 M4 paragraph) is a coordinator-mode-only concept, and giving the classic
    /// path an equivalent would mean putting an epoch stamp on every element of the
    /// (StreamConstants.TableDeltaNamespace, tableName) delta stream, a wire-contract change every consumer
    /// of that stream would have to agree on (TableHistoryGrain, StreamBridgeService/SignalR, this table's
    /// own downstream tables) — out of scope here. So: this table starts anyway (a startup failure would be
    /// a worse outcome — same call ApplyRetentionPolicy already makes for an unsupported retention policy),
    /// but if a declared table input already holds rows AT THE MOMENT this table is about to start consuming
    /// its delta stream, that is exactly the situation wishlist #14 describes — this table will NOT see
    /// those rows, ever, only future changes to them — and it is logged by name and row count instead of
    /// surfacing later as a silently-incomplete projection or (see TableExecutor.UnmatchedRetractions) a
    /// GROUP BY that ping-pongs between one row and none.
    ///
    /// Advisory only, and the row count it reports is itself a best-effort snapshot with no synchronization
    /// guarantee against the subscription this method runs just before — it can be stale in either direction
    /// by the time the subscription below goes live. That is fine for a diagnostic (it only needs to catch
    /// the common, not-actively-racing case: an operator creating a new aggregate over a long-since-warm
    /// table mid-session, exactly the demo's reported symptom) and is exactly why this is NOT the fix: a
    /// real backfill cannot tolerate that same race (see docs/otc-demo-wishlist.md #14's "THE TRAP" and "Not
    /// done" paragraphs).
    /// </summary>
    private async Task WarnIfTableInputsAlreadyHoldRowsAsync(string tableName, IReadOnlyList<string> tableInputs)
    {
        foreach (var upstreamName in tableInputs.Distinct())
        {
            // A table cannot legitimately depend on itself (the SQL compiler has no recursive-table
            // feature to produce one) — skip defensively rather than ever awaiting a call back into this
            // same not-yet-finished StartAsync turn, which would deadlock.
            if (upstreamName == tableName) continue;

            int upstreamRowCount;
            try
            {
                upstreamRowCount = await GrainFactory.GetGrain<ITableGrain>(upstreamName).GetRowCountAsync();
            }
            catch (Exception ex)
            {
                // Best-effort: an upstream table that hasn't been created/started yet (or that errors for
                // its own reasons) is not this diagnostic's concern — it holds no rows for THIS table to
                // have missed either way. Never let the check itself block this table from starting.
                logger.LogDebug(ex, "Table '{Table}': could not read row count of table input '{Upstream}' for the warm-upstream diagnostic — skipping.", tableName, upstreamName);
                continue;
            }

            if (upstreamRowCount > 0)
            {
                // NOTE: each placeholder name appears exactly once — Microsoft.Extensions.Logging's
                // structured-logging formatter binds placeholders to args POSITIONALLY, so repeating a
                // name (e.g. two "{Table}" occurrences) silently desyncs every placeholder after the
                // first repeat from the 3-argument list actually supplied, rather than substituting the
                // same value twice.
                logger.LogWarning(
                    "Table '{Table}' is starting with table input '{Upstream}' ({RowCount} row(s) already present). " +
                    "Those rows are NOT backfilled — this table starts empty and only reflects changes to its table " +
                    "input from now on. For a plain projection this self-heals as rows churn; for a GROUP BY/aggregate " +
                    "it does not (see TableExecutor.UnmatchedRetractions) — the aggregate can report a wrong count " +
                    "until restarted. See docs/otc-demo-wishlist.md #14.",
                    tableName, upstreamName, upstreamRowCount);
            }
        }
    }

    /// <summary>Plan 003 M2 — see class doc's coordinator-mode paragraph. Deploys the partitioned graph in
    /// dependency order (TableOutputGrain, then every TableStageGrain, THEN every TableIngestGrain last) so
    /// no early delta gets silently dropped by a not-yet-started downstream grain (every M2 grain no-ops a
    /// call received before its own StartAsync — see TableStageGrain/TableOutputGrain.PushBatchAsync/
    /// PublishAsync's `_status != Running` guard); subscribes to this table's own output stream BEFORE any
    /// of that deployment, for the same reason on the read side.</summary>
    private async Task StartCoordinatorAsync(TableDefinition def)
    {
        var (compileResult, dataflow) = await TableDataflowFactory.BuildAsync(GrainFactory, def);

        _executor = compileResult.Plan!.CreateExecutor(); // scratch instance: CanonicalRowKey only, never fed an event
        _status = PipelineStatus.Running;
        _coordinatorMode = true;
        _coordinatorParallelism = def.Parallelism;

        // _coordinatorLedger (unlike _executor above) is a grain-instance field that outlives a single
        // StartAsync/StopAsync cycle — the grain activation itself isn't torn down on StopAsync, only its
        // subscriptions/sub-grains are. Without clearing it here, a restart-resume would silently resurrect
        // pre-restart rows into the freshly-reset state.State.Snapshot on the next flush, breaking the same
        // "rebuild purely from live traffic" contract the classic path gets for free by allocating a brand
        // new (empty) TableExecutor on every StartClassicAsync call.
        _coordinatorLedger.Clear();

        ResumeJournaledState();

        if (ClearResumeMarkersAndDetect())
        {
            _rebuilding = true;
        }

        await ClearStaleJournalAsync();

        // Plan 011 wave C: same re-establishment the classic path does — _coordinatorLedger was just
        // cleared above and state.State.Snapshot is empty either way. See CaptureSnapshotIntoState.
        _touchedRowKeys.Clear();
        _fullCaptureNeeded = true;

        _searchIndex = def.SearchEnabled ? new TableSearchIndex(def.SearchMode) : null;

        // Plan 003 M4: initialize the coordinator's own frontier tracker BEFORE deploying anything that
        // could call back into OnOutputBatchAsync — one UpstreamId per terminal-stage partition, keyed on
        // the compiled dataflow's own TerminalEdge.EdgeId (see class doc). Safe to set up synchronously
        // here even though TableGrain is non-reentrant and this call could theoretically race a downstream
        // callback: any OnOutputBatchAsync call arriving while this StartAsync turn is still in flight is
        // queued by Orleans behind it, never interleaved — so these fields are always fully initialized
        // before the first real invocation runs.
        _terminalEdgeId = dataflow.TerminalEdge.EdgeId;
        int terminalPartitionCount = dataflow.PartitionCountOf(dataflow.TerminalEdge.FromStageId);
        _outputFrontier = new FrontierTracker(Enumerable.Range(0, terminalPartitionCount).Select(p => new UpstreamId(_terminalEdgeId, p)));
        _outputBuffer = new EpochBuffer();
        _snapshotFrontierEpoch = null;

        await GrainFactory.GetGrain<ITableOutputGrain>(def.Name).StartAsync(def);

        _deployedStages = dataflow.Stages
            .Where(s => s.Kind != TableStageKind.Ingest)
            .Select(s => (s.StageId, dataflow.PartitionCountOf(s.StageId)))
            .ToList();
        foreach (var (stageId, partitionCount) in _deployedStages)
        {
            for (int p = 0; p < partitionCount; p++)
            {
                await GrainFactory.GetGrain<ITableStageGrain>($"{def.Name}:{stageId}:{p}").StartAsync(def, stageId, p);
            }
        }

        // Plan 003 M3: attach shared arrangements for every edge TableDataflowBuilder marked arrangeable —
        // AFTER the TableStageGrains above are started (AttachAsync immediately pushes a seed snapshot to
        // them — see ArrangementGrain's class doc) but BEFORE deploying private TableIngestGrains below, so
        // an input whose every edge is arrangeable never gets a redundant ingest activation.
        _deployedArrangements = [];
        foreach (var edge in dataflow.ArrangeableExternalEdges)
        {
            var inputName = dataflow.ExternalInputNameOf(edge);
            bool isTableInput = compileResult.TableInputs.Contains(inputName);
            var keySpec = dataflow.KeySpecOf(edge);
            var hash = ArrangementKeySpec.HashOf(keySpec);
            int pcount = dataflow.PartitionCountOf(edge.ToStageId);

            for (int p = 0; p < pcount; p++)
            {
                var arrangementKey = $"{inputName}:{hash}:{p}";
                var consumerId = $"{def.Name}:{edge.EdgeId.Value}:{p}";
                var targetGrainKey = $"{def.Name}:{edge.ToStageId}:{p}";
                await GrainFactory.GetGrain<IArrangementGrain>(arrangementKey).AttachAsync(new ArrangementAttachRequest
                {
                    ConsumerId = consumerId,
                    TargetGrainKey = targetGrainKey,
                    TargetEdgeId = edge.EdgeId.Value,
                    InputName = inputName,
                    IsTableInput = isTableInput,
                    KeyFields = edge.ArrangeKeyFields!.ToList(),
                    KeySpec = keySpec,
                    PartitionCount = pcount,
                    Partition = p,
                });
                _deployedArrangements.Add((arrangementKey, consumerId));
            }
        }

        _deployedInputs = compileResult.StreamInputs.Concat(compileResult.TableInputs).Distinct()
            .Where(name => dataflow.EdgesForExternalInput(name)
                .Any(e => e.Mode == TableEdgeMode.Broadcast || dataflow.OutEdgeOf(e.ToStageId).ArrangeKeyFields is null))
            .ToList();
        foreach (var inputName in _deployedInputs)
        {
            await GrainFactory.GetGrain<ITableIngestGrain>($"{def.Name}:{inputName}").StartAsync(def, inputName);
        }

        // MemoryOnly registers no flush timer at all — see class doc's persistence-mode paragraph.
        if (_persistenceMode != TablePersistenceMode.MemoryOnly)
        {
            _flushTimer = this.RegisterGrainTimer(OnFlushTickAsync, _flushInterval, _flushInterval);
        }
        this.DelayDeactivation(TimeSpan.FromDays(365));
    }

    public async Task StopAsync()
    {
        _status = PipelineStatus.Stopped;

        _flushTimer?.Dispose();
        _flushTimer = null;

        foreach (var handle in _streamSubs)
        {
            try { await handle.UnsubscribeAsync(); } catch { /* best-effort */ }
        }
        _streamSubs.Clear();

        foreach (var handle in _tableSubs)
        {
            try { await handle.UnsubscribeAsync(); } catch { /* best-effort */ }
        }
        _tableSubs.Clear();

        // Plan 003 M4: no stream subscription to tear down anymore (see class doc) — just drop the
        // frontier-tracking state so a stopped table reports no stale frontier.
        _outputFrontier = null;
        _outputBuffer = null;
        _snapshotFrontierEpoch = null;

        if (_coordinatorMode && _def is not null)
        {
            // Detach arrangements FIRST — removes this table's consumer id from every attached arrangement's
            // live-push list immediately, before the TableStageGrains it was pushing to are torn down below.
            foreach (var (arrangementKey, consumerId) in _deployedArrangements)
            {
                try { await GrainFactory.GetGrain<IArrangementGrain>(arrangementKey).DetachAsync(consumerId); } catch { /* best-effort */ }
            }
            foreach (var inputName in _deployedInputs)
            {
                try { await GrainFactory.GetGrain<ITableIngestGrain>($"{_def.Name}:{inputName}").StopAsync(); } catch { /* best-effort */ }
            }
            foreach (var (stageId, partitionCount) in _deployedStages)
            {
                for (int p = 0; p < partitionCount; p++)
                {
                    try { await GrainFactory.GetGrain<ITableStageGrain>($"{_def.Name}:{stageId}:{p}").StopAsync(); } catch { /* best-effort */ }
                }
            }
            try { await GrainFactory.GetGrain<ITableOutputGrain>(_def.Name).StopAsync(); } catch { /* best-effort */ }
            _deployedArrangements = [];
            _deployedInputs = [];
            _deployedStages = [];
        }

        // Plan 008 W2.5: await any FireAndForget background write BEFORE the final flush below — otherwise
        // the awaited FlushAsync a few lines down could run its own state.WriteStateAsync() concurrently
        // with one still in flight from the last tick, the exact two-writers-one-file corruption hazard the
        // single-flight design (see class doc) exists to prevent. Never faulted (WriteStateBestEffortAsync
        // catches internally), so this can't throw and doesn't need a try/catch.
        await _pendingWrite;

        // Plan 003 M4 fix: flush BEFORE clearing _coordinatorMode, not after — FlushAsync's `_coordinatorMode
        // ? _coordinatorLedger.Visible : _executor.Snapshot()` branch must still see _coordinatorMode==true here,
        // otherwise a coordinator-mode table's final on-stop flush silently persists the scratch executor's
        // (always-empty — see class doc, "never fed an event") snapshot instead of the real
        // _coordinatorLedger, losing every row from the persisted state.State.Snapshot on every stop. This
        // was a latent pre-M4 bug masked by GetRowsAsync/GetRowCountAsync previously reading the (up to 2s
        // stale) persisted copy — by the time a poll observed the row count, the periodic 2s flush timer had
        // usually already run it correctly WHILE _coordinatorMode was still true. M4's live-read fix (see
        // GetRowsAsync's doc comment) made reads fast enough to reliably outrace that timer, exposing this
        // ordering bug as a real, reproducible restart-resume data loss — see
        // TableFrontierClusterTests/PartitionedTableClusterTests' restart-resume assertions.
        //
        // Plan 008 W2.5: MemoryOnly additionally skips this final flush outright (even if _dirty) — its
        // whole contract is "nothing ever touches storage" (see class doc); FireAndForget's final flush is
        // deliberately the awaited FlushAsync below, not another background write, because the grain is
        // going away and a background write here would just be abandoned mid-flight. Plan 009 A2: Journaled's
        // final flush goes through FlushDirtyAsync too — its own small journal write, not a forced
        // compaction (see that method's doc comment).
        if (_persistenceMode != TablePersistenceMode.MemoryOnly && _dirty)
        {
            await FlushDirtyAsync();
        }
        _coordinatorMode = false;

        _executor = null;
        _searchIndex = null;
        this.DelayDeactivation(TimeSpan.Zero);
    }

    public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        // Plan 008 W2.5: same reasoning as StopAsync's final flush (see its own comment) — await any
        // in-flight FireAndForget background write first (never faulted, so no try/catch needed), then skip
        // entirely for MemoryOnly, otherwise flush (awaited) exactly like Batched always has.
        try { await _pendingWrite; } catch { /* defensive only — WriteStateBestEffortAsync never faults */ }
        if (_persistenceMode != TablePersistenceMode.MemoryOnly && _dirty)
        {
            try { await FlushDirtyAsync(); } catch { /* best-effort */ }
        }
        await base.OnDeactivateAsync(reason, cancellationToken);
    }

    /// <summary>Plan 003 M4: coordinator mode (Parallelism &gt;= 2) serves rows from the LIVE
    /// <see cref="_coordinatorLedger"/> rather than the write-behind-persisted state.State.Snapshot (which
    /// can lag up to the 2s flush-timer interval — see class doc / OnFlushTickAsync). This is required for
    /// the frontier-consistency statement to actually hold: <see cref="_snapshotFrontierEpoch"/> is advanced
    /// synchronously, in the same OnOutputBatchAsync call that updates _coordinatorLedger, so reading rows
    /// from anything else (state.State.Snapshot included) could report a frontier ahead of what the rows
    /// served actually reflect. state.State.Snapshot remains the PERSISTED copy either way — still flushed
    /// on the same 2s cadence, still what restart-resume reads (see StartCoordinatorAsync's Rebuilding
    /// logic) — this change only affects which copy live reads are served from, for coordinator mode only;
    /// the Parallelism==1 path is completely unchanged (still reads state.State.Snapshot, exactly as
    /// before M4).</summary>
    public Task<List<TableRowDto>> GetRowsAsync(int limit, int offset)
    {
        var source = _coordinatorMode
            ? _coordinatorLedger.Visible.Values.Select(v => new TableRowDto { Row = new Dictionary<string, object?>(v.Row), Weight = v.Weight })
            : ClassicModeRows();
        var rows = source
            .Skip(Math.Max(0, offset))
            .Take(Math.Max(0, limit))
            .ToList();
        return Task.FromResult(rows);
    }

    public Task<int> GetRowCountAsync() =>
        Task.FromResult(_coordinatorMode ? _coordinatorLedger.Visible.Count : ClassicModeRowCount());

    /// <summary>Plan 008 W2.5: classic-mode (non-coordinator) row source for GetRowsAsync/GetRowCountAsync/
    /// GetMetricsAsync's RowCount. MemoryOnly NEVER populates state.State.Snapshot — it registers no flush
    /// timer at all and skips every final flush too (see class doc's persistence-mode paragraph), so reading
    /// state.State.Snapshot for a MemoryOnly table would report EMPTY forever, not just "stale by up to one
    /// flush interval" like Batched/FireAndForget — that would defeat the whole point of "the table lives
    /// entirely in the activation" (TablePersistenceMode.MemoryOnly's own doc comment): a MemoryOnly table
    /// must still be fully queryable, it just never touches disk. So for MemoryOnly this reads live from
    /// _executor.Snapshot() instead — the exact same live source SearchAsync already always uses for classic
    /// mode, regardless of persistence mode, even pre-008 (see its own `_executor?.Snapshot()` line below).
    /// Batched/FireAndForget are UNCHANGED: still the persisted state.State.Snapshot, lagging by up to one
    /// flush interval, exactly as pre-008.
    ///
    /// Plan 009 A2: <see cref="TablePersistenceMode.Journaled"/> joins MemoryOnly in reading live — for a
    /// DIFFERENT reason, but the same fix. Journaled's whole point is that <c>state.State.Snapshot</c> is
    /// NOT rewritten on every flush (only the small journal is — see <see cref="JournalFlushAsync"/>); it
    /// only catches up at a compaction (<see cref="CompactAsync"/>), which can legitimately be far rarer
    /// than the flush interval (up to <see cref="TableDefinition.JournalMaxEntries"/> distinct row changes
    /// apart). Reading state.State.Snapshot here would therefore report stale-until-first-compaction (in the
    /// worst case, EMPTY forever for a low-churn table) rather than "stale by up to one flush interval" —
    /// exactly the MemoryOnly failure mode this method's guard already exists to avoid, so Journaled gets the
    /// identical live-read fix. This does NOT change what gets WRITTEN to disk (still just the journal, per
    /// flush) — only which copy classic-mode READS are served from, mirroring the split MemoryOnly already
    /// established.</summary>
    private IEnumerable<TableRowDto> ClassicModeRows() =>
        ReadsLive
            ? _executor!.Snapshot().Values.Select(v => new TableRowDto { Row = new Dictionary<string, object?>(v.Row), Weight = v.Weight })
            : state.State.Snapshot.Values;

    private int ClassicModeRowCount() =>
        ReadsLive ? _executor!.Snapshot().Count : state.State.Snapshot.Count;

    /// <summary>Plan 011 D2 — SHARDED tables join MemoryOnly and Journaled in reading rows live from the
    /// executor, for the strongest of the three reasons: they do not merely lag the mirror, they have no
    /// mirror at all (see the class doc's D2 paragraph — it would be a second copy of what the shards
    /// durably hold). The result is FRESHER than the up-to-one-flush-interval-stale copy it replaces, and
    /// still consults no shard, so a two-second <c>/rows</c> poll still wakes nothing.</summary>
    private bool ReadsLive =>
        _executor is not null
        && (_shardMirrorSuppressed
            || _persistenceMode is TablePersistenceMode.MemoryOnly or TablePersistenceMode.Journaled);

    /// <summary>Plan 003 M2: in coordinator mode (Parallelism &gt;= 2), additively fans out to every
    /// deployed TableStageGrain for per-partition detail (TableMetrics.Partitions) — null/absent on the
    /// Parallelism==1 path, so existing consumers see byte-identical JSON.</summary>
    public async Task<TableMetrics> GetMetricsAsync()
    {
        List<TablePartitionMetrics>? partitions = null;
        bool arrangementsRebuilding = false;
        List<string>? arrangedInputs = null;
        if (_coordinatorMode && _def is not null)
        {
            var tasks = _deployedStages
                .SelectMany(s => Enumerable.Range(0, s.PartitionCount)
                    .Select(p => GrainFactory.GetGrain<ITableStageGrain>($"{_def.Name}:{s.StageId}:{p}").GetMetricsAsync()));
            partitions = (await Task.WhenAll(tasks)).ToList();

            if (_deployedArrangements.Count > 0)
            {
                // Plan 003 M3: fold every attached arrangement's own Rebuilding (checkpoint-not-yet-caught-up
                // — see ArrangementGrain's class doc) into this table's — a table currently served by a
                // still-rebuilding arrangement is itself honestly "rebuilding" (its join-side state is
                // partly stale checkpoint data), even though its OWN _rebuilding flag (output-snapshot
                // resume) may already be false.
                var infoTasks = _deployedArrangements.Select(a => GrainFactory.GetGrain<IArrangementGrain>(a.ArrangementKey).GetInfoAsync());
                var infos = await Task.WhenAll(infoTasks);
                arrangementsRebuilding = infos.Any(i => i.Rebuilding);
                arrangedInputs = _deployedArrangements.Select(a => a.ArrangementKey.Split(':')[0]).Distinct().ToList();
            }
        }

        return new TableMetrics
        {
            TableId = _def?.Id ?? this.GetPrimaryKeyString(),
            Status = _status,
            // Plan 003 M4: live count for coordinator mode — see GetRowsAsync's doc comment for why
            // (state.State.Snapshot lags up to the 2s flush interval; RowCount must agree with
            // SnapshotFrontierEpoch below, which is also updated live). Plan 008 W2.5: ClassicModeRowCount
            // additionally goes live for MemoryOnly (see its own doc comment) — state.State.Snapshot is
            // never populated at all in that mode, not just lagging.
            RowCount = _coordinatorMode ? _coordinatorLedger.Visible.Count : ClassicModeRowCount(),
            DeltasIn = _deltasIn,
            DeltasOut = _deltasOut,
            LastUpdateMs = _lastUpdateMs,
            Rebuilding = _rebuilding || arrangementsRebuilding,
            Partitions = partitions,
            ArrangedInputs = arrangedInputs,
            // Plan 003 M4: see class doc's consistency statement — null for Parallelism==1 (no partitioned
            // frontier exists) and until the first full round has been observed.
            SnapshotFrontierEpoch = _coordinatorMode ? _snapshotFrontierEpoch : null,
        };
    }

    /// <summary>Plan 003 M4 — O(1) mirror of TableMetrics.SnapshotFrontierEpoch, for the /rows endpoint (see
    /// TableRowsResponse.FrontierEpoch) so it doesn't have to pay for GetMetricsAsync's per-partition
    /// fan-out on every poll just to read one long.</summary>
    public Task<long?> GetSnapshotFrontierEpochAsync() => Task.FromResult(_coordinatorMode ? _snapshotFrontierEpoch : null);

    public Task<long> GetSeqAsync() => Task.FromResult(state.State.Seq);

    public Task<List<TableRowDto>> SearchAsync(string query, int limit)
    {
        if (_searchIndex is null || string.IsNullOrWhiteSpace(query))
        {
            return Task.FromResult(new List<TableRowDto>());
        }

        IReadOnlyDictionary<string, (EventRecord Row, long Weight)>? snapshot = _coordinatorMode ? _coordinatorLedger.Visible : _executor?.Snapshot();
        var hits = _searchIndex.Search(query, limit);
        var rows = hits.Select(h =>
        {
            long weight = snapshot is not null && snapshot.TryGetValue(h.RowKey, out var current) ? current.Weight : 1;
            return new TableRowDto { Row = new Dictionary<string, object?>(h.Row), Weight = weight };
        }).ToList();
        return Task.FromResult(rows);
    }

    private async Task OnStreamEventAsync(string source, EventRecord evt)
    {
        if (_executor is null) return;

        _deltasIn++;
        _lastUpdateMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _rebuilding = false; // live traffic observed since resume (or this is a first-ever start — already false)

        var deltas = _executor.OnStreamEvent(source, evt);
        if (deltas.Count > 0)
        {
            await ApplyAndPublishAsync(deltas);
        }
    }

    /// <summary>
    /// Wishlist #15 — <paramref name="batch"/> is everything the upstream table's OWN
    /// ApplyAndPublishAsync/OnOutputBatchAsync call published in ONE <c>stream.OnNextAsync</c>, i.e. one
    /// upstream epoch's worth of deltas (see that method's own doc comment — a batch there is now itself
    /// consolidated, atomic output of ITS single-epoch admission, since Engine-side
    /// TableExecutor.OnTableDeltaBatch/ConsolidateEpochOutput apply the same fix one hop upstream). This
    /// used to loop <paramref name="batch"/> through <see cref="TableExecutor.OnTableDelta"/> ONE ELEMENT AT
    /// A TIME, which gave each element its OWN epoch on THIS table — exactly the bug wishlist #15 reports:
    /// an upstream retract(-1)+assert(+1) pair for one changed row arrived here as two separate deltas and
    /// got applied (and republished on THIS table's own delta stream, and folded into the search
    /// index/journal/snapshot mirror below via ApplyAndPublishAsync) as two separate epochs, with the
    /// retracted-but-not-yet-reasserted state observable in between — the NULL-flapping joined column the
    /// wishlist describes. Admitting the whole batch through <see cref="TableExecutor.OnTableDeltaBatch"/>
    /// in ONE call keeps it one epoch here too: the executor consolidates its own output before returning
    /// (see ConsolidateEpochOutput), so the intermediate state never reaches ApplyAndPublishAsync at all.
    /// </summary>
    private async Task OnTableDeltaBatchAsync(string table, List<TableDeltaDto> batch)
    {
        if (_executor is null) return;

        var deltas = batch.Select(d => new TableDelta(new EventRecord(d.Row), d.Weight)).ToList();
        _deltasIn += deltas.Count;
        var outAll = _executor.OnTableDeltaBatch(table, deltas);
        _lastUpdateMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _rebuilding = false;

        if (outAll.Count > 0)
        {
            await ApplyAndPublishAsync(outAll);
        }
    }

    /// <summary>Plan 003 M4 coordinator-mode read path (replaces the pre-M4 self-subscription design — see
    /// class doc): called by this table's own ITableOutputGrain.PublishAsync once per terminal-stage
    /// partition's own frontier advance. Buffers (fromPartition, epoch, deltas) with _outputBuffer and
    /// observes it on _outputFrontier (the same FrontierTracker+EpochBuffer pattern TableStageGrain.
    /// PushBatchAsync already uses for every OTHER hop — see that method); only once EVERY terminal
    /// partition has reported reaching a given epoch does that epoch's batches become "ready", at which
    /// point — synchronously, no `await` in between — they are consolidated into
    /// <see cref="_coordinatorLedger"/>/the search index AND _snapshotFrontierEpoch is advanced to match.
    /// That synchronous, no-await body is exactly what makes the class doc's consistency statement true —
    /// and still holds despite this method being [MayInterleave] (see class doc): interleaving only changes
    /// which QUEUED turn Orleans picks up next when another turn is suspended at an `await`; it never
    /// interrupts a synchronous method's body mid-statement. Since neither this method nor GetRowsAsync
    /// ever yields, one always runs to completion before the other can start — no GetRowsAsync call can
    /// ever observe _coordinatorLedger mid-update, and _snapshotFrontierEpoch never claims more than what
    /// was just applied. No SQL runs here — the partitioned graph already computed these deltas; this grain
    /// only consolidates + persists + indexes them for reads.</summary>
    public Task OnOutputBatchAsync(int fromPartition, long epochValue, List<TableDeltaDto> deltas)
    {
        if (!_coordinatorMode || _status != PipelineStatus.Running || _executor is null
            || _outputFrontier is null || _outputBuffer is null)
        {
            return Task.CompletedTask;
        }

        var epoch = new Epoch(epochValue);
        IReadOnlyList<TableDelta> tableDeltas = deltas.Count == 0
            ? []
            : deltas.Select(d => new TableDelta(new EventRecord(d.Row), d.Weight)).ToList();
        _outputBuffer.Add(new DeltaBatch(_terminalEdgeId, fromPartition, epoch, tableDeltas));

        var observation = _outputFrontier.Observe(new UpstreamId(_terminalEdgeId, fromPartition), epoch);
        if (!observation.Advanced) return Task.CompletedTask; // another terminal partition still holds the frontier back

        var ready = _outputBuffer.OnFrontier(observation.Frontier);
        var allDeltas = new List<TableDelta>();
        foreach (var batch in ready) allDeltas.AddRange(batch.Deltas);

        if (allDeltas.Count > 0)
        {
            foreach (var delta in allDeltas) ApplyCoordinatorConsolidation(delta);
            _deltasIn += allDeltas.Count;
            _deltasOut += allDeltas.Count; // pure read-side relay: "consumed" and "reflected" are the same count here
            _lastUpdateMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            _rebuilding = false; // real traffic observed since resume — mirrors the classic path's own rule (empty epoch markers do NOT clear this)
            _dirty = true;

            // Plan 011 wave C: one key-derivation pass per batch feeds all three consumers (was up to two
            // separate passes) — see TouchedKeys' own doc comment.
            // Plan 011 D2: on a sharded table nothing downstream of this pass exists — no mirror to keep
            // incrementally, no journal to write, and no search index (the combination is refused) — so the
            // per-delta canonical-key derivation itself is skipped rather than computed and discarded.
            var touched = NeedsTouchedKeys ? TouchedKeys(allDeltas) : [];
            RecordTouchedRowKeys(touched);

            if (_persistenceMode == TablePersistenceMode.Journaled && !_shardMirrorSuppressed)
            {
                RecordJournalEntries(touched);
            }

            if (_searchIndex is not null)
            {
                ReflectDeltasInSearchIndex(touched);
            }
        }

        // Frontier progress is reported regardless of whether this round carried any real deltas — an
        // empty epoch still honestly advances what the snapshot is known to reflect (see EpochBuffer's own
        // doc comment: "an empty epoch still advances ... downstream consumer learn[s] that upstream
        // reached that epoch with nothing to say").
        _snapshotFrontierEpoch = observation.Frontier.Value;
        return Task.CompletedTask;
    }

    /// <summary>[MayInterleave] predicate (see class doc) — only OnOutputBatchAsync is allowed to jump the
    /// queue ahead of/alongside another in-flight turn; everything else (StartAsync/StopAsync/GetRowsAsync/
    /// etc.) stays strictly serialized, exactly like RegistryGrain's identical MayInterleave predicate.</summary>
    public static bool MayInterleave(IInvokable req) => req.GetMethodName() == nameof(ITableGrain.OnOutputBatchAsync);

    private void ApplyCoordinatorConsolidation(TableDelta delta)
    {
        // CanonicalRowKey is the one piece of Engine key-derivation logic exposed publicly (see class doc);
        // the weight-folding arithmetic itself now lives in ConsolidationLedger.Apply — see its class doc
        // for the order-independence argument this method used to carry locally.
        var key = _executor!.CanonicalRowKey(delta.Row);
        _coordinatorLedger.Apply(key, delta.Row, delta.Weight);
    }

    /// <summary>Plan 011 C2 — installs this table's row retention policy on the freshly built executor.
    /// The eviction retractions it produces come back through <see cref="TableExecutor.OnStreamEvent"/>/
    /// <see cref="TableExecutor.OnTableDelta"/>'s own return values, so <see cref="ApplyAndPublishAsync"/>
    /// already publishes them, indexes them and journals them with no retention-specific branch anywhere on
    /// the hot path — see TableExecutor.ConfigureRetention's doc comment for why that shape was chosen.
    ///
    /// A policy this plan shape cannot honor is REFUSED by the Engine, and refused loudly here rather than
    /// swallowed: RegistryGrain.ValidateRetention already rejects the combination at create/update, so
    /// reaching this branch means a definition arrived some other way (a hand-edited catalog file, an
    /// import from an older build). The table still starts — a startup failure would be a worse outcome
    /// than an unbounded table — but the log line says exactly which table is running without the bound it
    /// asks for.</summary>
    private void ApplyRetentionPolicy(TablePlan plan, TableDefinition def)
    {
        var policy = new TableRetentionPolicy(def.RetentionMaxRows, def.RetentionTtlMs);
        if (!policy.IsEnabled) return;

        if (!plan.SupportsRetention)
        {
            logger.LogWarning(
                "Table '{Table}' requests row retention (maxRows={MaxRows}, ttlMs={TtlMs}) but its plan shape does not support it — starting WITHOUT the bound.",
                def.Name, def.RetentionMaxRows, def.RetentionTtlMs);
            return;
        }

        _executor!.ConfigureRetention(policy);
    }

    private async Task ApplyAndPublishAsync(IReadOnlyList<TableDelta> deltas)
    {
        _dirty = true;
        _deltasOut += deltas.Count;

        // Plan 011 wave C: one key-derivation pass per batch, shared by the search index, the journal and
        // the incremental snapshot capture — see TouchedKeys' own doc comment.
        var touched = NeedsTouchedKeys ? TouchedKeys(deltas) : [];
        RecordTouchedRowKeys(touched);

        if (_searchIndex is not null)
        {
            ReflectDeltasInSearchIndex(touched);
        }

        if (_persistenceMode == TablePersistenceMode.Journaled && !_shardMirrorSuppressed)
        {
            RecordJournalEntries(touched);
        }

        // Plan 011 C2: Evicted rides along so the row-history grain can tell a retention eviction apart
        // from an ordinary upstream retraction — see TableDeltaDto.Evicted. Every other consumer ignores it.
        var dtos = deltas.Select(d => new TableDeltaDto { Row = new Dictionary<string, object?>(d.Row), Weight = d.Weight, Evicted = d.Retention }).ToList();
        var stream = this.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, _def!.Name));
        await stream.OnNextAsync(dtos);
    }

    /// <summary>Plan 011 wave C — the distinct canonical row keys one delta batch touched, derived ONCE
    /// and then shared by every per-batch consumer (<see cref="ReflectDeltasInSearchIndex"/>,
    /// <see cref="RecordJournalEntries"/>, <see cref="RecordTouchedRowKeys"/>). Each of those used to
    /// re-derive the same keys itself (CanonicalRowKey hashes the whole row, so that was a real per-delta
    /// cost paid two or three times over); the dedup-by-key `seen` set each of them carried is now this
    /// one. Order is batch order, first occurrence wins — the same order the per-consumer loops produced
    /// before, so nothing downstream sees a different sequence.</summary>
    private List<string> TouchedKeys(IReadOnlyList<TableDelta> deltas)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var keys = new List<string>(deltas.Count);
        foreach (var delta in deltas)
        {
            var key = _executor!.CanonicalRowKey(delta.Row);
            if (seen.Add(key)) keys.Add(key); // a batch can touch the same row's key more than once
        }
        return keys;
    }

    /// <summary>Plan 011 wave C — accumulates this batch's touched keys into <see cref="_touchedRowKeys"/>
    /// so the next <see cref="CaptureSnapshotIntoState"/> can be O(changed). Skipped entirely for
    /// <see cref="TablePersistenceMode.MemoryOnly"/>, which never captures — see the field's own doc
    /// comment for why accumulating there would be a leak rather than an optimization.</summary>
    private void RecordTouchedRowKeys(List<string> touched)
    {
        if (_persistenceMode == TablePersistenceMode.MemoryOnly || _shardMirrorSuppressed) return;
        foreach (var key in touched) _touchedRowKeys.Add(key);
    }

    /// <summary>Plan 011 D2 — is there any consumer left for the per-batch canonical-key pass? On a sharded
    /// table there is not: the mirror is gone, the journal with it, and searchEnabled + shardBy is refused
    /// at upsert. The <c>_searchIndex is not null</c> half is defensive rather than reachable, and costs
    /// one null check.</summary>
    private bool NeedsTouchedKeys => !_shardMirrorSuppressed || _searchIndex is not null;

    /// <summary>Keeps the search index in sync with the consolidated Z-set as deltas land: for each row
    /// touched by this batch, look its canonical key up in the (already-updated, O(1) live) consolidated
    /// snapshot — present with weight &gt; 0 means Add/update, absent means the row's weight returned to 0
    /// (Remove). Only rows actually touched by this batch are re-checked, not the whole table.</summary>
    private void ReflectDeltasInSearchIndex(List<string> touched)
    {
        IReadOnlyDictionary<string, (EventRecord Row, long Weight)> snapshot = _coordinatorMode ? _coordinatorLedger.Visible : _executor!.Snapshot();
        foreach (var key in touched)
        {
            if (snapshot.TryGetValue(key, out var current))
            {
                _searchIndex!.Add(key, current.Row);
            }
            else
            {
                _searchIndex!.Remove(key);
            }
        }
    }

    /// <summary>Plan 009 A2 — populates <see cref="_pendingJournalEntries"/> for <see cref="TablePersistenceMode.Journaled"/>
    /// tables; callers only invoke this when <c>_persistenceMode == Journaled</c> (mirrors
    /// <see cref="ReflectDeltasInSearchIndex"/>'s <c>_searchIndex is not null</c> guard shape — same per-batch,
    /// dedup-by-key pattern). Looks each touched key up in the ALREADY-UPDATED consolidated snapshot (classic
    /// mode's <see cref="_executor"/> or coordinator mode's <see cref="_coordinatorLedger"/>, exactly like
    /// ReflectDeltasInSearchIndex does): present with weight &gt; 0 records a live entry; absent means the
    /// key's running weight just dropped to &lt;= 0, recorded as an explicit removal tombstone (Weight = 0) —
    /// see <see cref="TableJournalEntry"/>'s own doc comment for why skipping this instead would resurrect
    /// the row on replay.</summary>
    private void RecordJournalEntries(List<string> touched)
    {
        IReadOnlyDictionary<string, (EventRecord Row, long Weight)> snapshot = _coordinatorMode ? _coordinatorLedger.Visible : _executor!.Snapshot();
        foreach (var key in touched)
        {
            _pendingJournalEntries[key] = snapshot.TryGetValue(key, out var current)
                ? new TableJournalEntry { Row = new Dictionary<string, object?>(current.Row), Weight = current.Weight }
                : new TableJournalEntry { Row = [], Weight = 0 };
        }
    }

    /// <summary>Plan 009 A2 — first half of resuming: clears any leftover <see cref="_pendingJournalEntries"/>
    /// from a prior activation (in-memory only, never persisted) and folds whatever the persisted journal
    /// holds onto <c>state.State.Snapshot</c> via <see cref="ReplayJournalIntoSnapshot"/> BEFORE
    /// StartClassicAsync/StartCoordinatorAsync's own "non-empty snapshot means resume" check runs.
    ///
    /// <b>Deliberately UNCONDITIONAL — not gated on <c>_persistenceMode == Journaled</c>.</b> A non-empty
    /// journal can only exist here because a PRIOR activation ran in Journaled mode; the persistence mode
    /// this activation is starting in may since have changed (a mode switch — see class doc's "RESUME"
    /// paragraph). Gating replay on the NEW mode would lose exactly the entries a Journaled-&gt;Batched
    /// switch needs folded in: StopAsync's own final flush (still under the OLD mode) writes those latest
    /// changes to the SMALL journal, not to the snapshot, so skipping replay here because the mode has since
    /// changed would silently drop them. ReplayJournalIntoSnapshot is a no-op when the journal is empty, so
    /// this costs nothing for a table that was never Journaled.</summary>
    private void ResumeJournaledState()
    {
        _pendingJournalEntries.Clear();
        ReplayJournalIntoSnapshot();
    }

    /// <summary>Plan 009 A2 — merges <c>journalState.State.Entries</c> onto <c>state.State.Snapshot</c> in
    /// place: a Weight &gt; 0 entry inserts/overwrites that key, a Weight &lt;= 0 entry (an explicit removal
    /// tombstone — see <see cref="TableJournalEntry"/>'s own doc comment) removes it. Called once, from
    /// <see cref="ResumeJournaledState"/>, before the shared resume-detection check.</summary>
    private void ReplayJournalIntoSnapshot()
    {
        foreach (var (key, entry) in journalState.State.Entries)
        {
            if (entry.Weight > 0)
            {
                state.State.Snapshot[key] = new TableRowDto { Row = new Dictionary<string, object?>(entry.Row), Weight = entry.Weight };
            }
            else
            {
                state.State.Snapshot.Remove(key);
            }
        }
    }

    /// <summary>Plan 009 A2 — second half of resuming (called AFTER StartClassicAsync/StartCoordinatorAsync's
    /// resume-detection reset): unconditionally clears the journal, in memory and on disk, whenever it holds
    /// anything — regardless of which persistence mode this activation is starting in. Whatever it held is
    /// now either folded into the reset-to-empty snapshot above (this activation IS Journaled) or simply
    /// STALE (a different/former mode) — see class doc's Plan 009 A2 "RESUME" paragraph for why leaving it
    /// behind would let a later switch back to Journaled replay pre-restart data onto an already-rebuilt
    /// table.</summary>
    /// <summary>Plan 011 D2 — the shared half of both start paths' resume detection, factored out because
    /// there are now TWO markers to check and clearing one but not the other would leave a table that
    /// changed its ShardBy permanently claiming to be resuming.
    ///
    /// Returns true when this activation is a RESUME (something was here before) rather than a first-ever
    /// start, and unconditionally clears both markers so the table rebuilds from live traffic — the
    /// RESTART-RESUME LIMITATION in the class doc, unchanged by D2.</summary>
    private bool ClearResumeMarkersAndDetect()
    {
        var resuming = state.State.Snapshot.Count > 0 || state.State.HadRows;
        if (!resuming) return false;

        state.State.Snapshot = [];
        state.State.HadRows = false;
        state.State.Seq = 0;
        _dirty = true;
        return true;
    }

    private async Task ClearStaleJournalAsync()
    {
        if (journalState.State.Entries.Count == 0) return;

        journalState.State.Entries = [];
        await journalState.WriteStateAsync();
    }

    /// <summary>Plan 009 A2 — <see cref="TablePersistenceMode.Journaled"/>'s flush: merges
    /// <see cref="_pendingJournalEntries"/> (this tick's touched keys, already deduped by key) into the
    /// PERSISTED <c>journalState.State.Entries</c> — later values simply overwrite earlier ones for the same
    /// key, exactly like <c>state.State.Snapshot</c> itself does, which is what keeps the journal's size
    /// bounded by DISTINCT keys touched since the last compaction rather than by delta volume — then writes
    /// ONLY that small journal state, not the whole snapshot. See class doc's Plan 009 A2 paragraph for why
    /// this can never race a compaction it triggers.</summary>
    private async Task JournalFlushAsync()
    {
        if (_pendingJournalEntries.Count == 0)
        {
            _dirty = false;
            return;
        }

        foreach (var (key, entry) in _pendingJournalEntries)
        {
            journalState.State.Entries[key] = entry;
        }
        _pendingJournalEntries.Clear();
        _dirty = false;
        state.State.Seq++; // in-memory only (not written here) — keeps GetSeqAsync's "advances every flush" contract true for Journaled too.

        await journalState.WriteStateAsync();

        if (journalState.State.Entries.Count >= EffectiveJournalMaxEntries)
        {
            await CompactAsync();
        }
    }

    /// <summary>Plan 009 A2 — writes the full consolidated snapshot ONCE (the same capture
    /// <see cref="FlushAsync"/> uses) and truncates the journal back to empty, resetting the
    /// O(changed-since-compaction) counter to zero. Only ever called from <see cref="JournalFlushAsync"/>,
    /// immediately after that method's own awaited journal write.</summary>
    private async Task CompactAsync()
    {
        CaptureSnapshotIntoState(); // false only if _executor is null, which can't happen mid-flush (see FlushAsync's identical guard).
        await state.WriteStateAsync();
        journalState.State.Entries = [];
        await journalState.WriteStateAsync();
    }

    /// <summary>Plan 009 A2 — 0 (unset) resolves to <see cref="DefaultJournalMaxEntries"/>; any positive
    /// configured value is used verbatim. Mirrors <see cref="TableDefinition.FlushMs"/>'s identical
    /// 0-means-default convention.</summary>
    private int EffectiveJournalMaxEntries => _def!.JournalMaxEntries > 0 ? _def.JournalMaxEntries : DefaultJournalMaxEntries;

    /// <summary>Plan 009 A2 — dispatches an awaited flush to the mode-appropriate write: Journaled goes
    /// through <see cref="JournalFlushAsync"/> (O(changed)), everything else — Batched, and every FINAL flush
    /// (StopAsync/OnDeactivateAsync, all modes) — through the existing <see cref="FlushAsync"/> (O(|table|)).</summary>
    /// <summary>Plan 011 D2: a sharded table always takes the plain path, whatever its mode says. There is
    /// no snapshot to journal — see the class doc's D2 paragraph on why that MEETS Journaled's contract
    /// (Batched durability at O(changed) write cost) rather than quietly redefining it: each shard's own
    /// write is already O(one key), and this grain's write is now O(1).</summary>
    private Task FlushDirtyAsync() =>
        _persistenceMode == TablePersistenceMode.Journaled && !_shardMirrorSuppressed
            ? JournalFlushAsync()
            : FlushAsync();

    /// <summary>Captures the live consolidated Z-set into <c>state.State</c> — MUST stay synchronous, on the
    /// grain turn: <see cref="_executor"/>/<see cref="_coordinatorLedger"/> are not thread-safe and this is
    /// the only safe place to read them. Returns false (and clears <see cref="_dirty"/> without touching
    /// state.State) when there is nothing to capture, exactly like the pre-008 FlushAsync's own null-executor
    /// guard.
    ///
    /// PLAN 011 WAVE C — O(CHANGED), NOT O(|TABLE|). This used to rebuild a brand-new dictionary containing
    /// a brand-new <c>Dictionary&lt;string, object?&gt;</c> PER ROW from the whole ledger, on the grain turn,
    /// every <see cref="TableDefinition.FlushMs"/> (default 2000ms). At N rows that allocated N row
    /// dictionaries and threw the previous N away every tick, forever — allocation proportional to the
    /// TABLE, at a rate set by the CLOCK, so a table that merely grows linearly produced quadratic total
    /// allocation and eventually a GC/LOH stall (which is what "the machine froze" actually was, ahead of
    /// any real heap exhaustion). It now applies only <see cref="_touchedRowKeys"/> — the keys whose ledger
    /// entry changed since the previous capture — leaving every untouched row's already-captured
    /// <see cref="TableRowDto"/> in place, untouched and un-reallocated.
    ///
    /// THE INVARIANT this relies on: <c>state.State.Snapshot</c>, with <see cref="_touchedRowKeys"/> applied,
    /// always equals the live consolidated ledger. It holds because (a) the ONLY writers of the ledger are
    /// ApplyAndPublishAsync/OnOutputBatchAsync, both of which record every key they touch via
    /// <see cref="RecordTouchedRowKeys"/> before returning; (b) this method drains that set in the same
    /// synchronous, no-await body that reads the ledger, so no turn can slip in between; and (c) the only
    /// places <c>state.State.Snapshot</c> is otherwise replaced — StartClassicAsync/StartCoordinatorAsync's
    /// resume reset — set <see cref="_fullCaptureNeeded"/> at a point where BOTH sides are empty. A key
    /// present in the set but absent from the ledger means its running weight fell to &lt;= 0, i.e. removal,
    /// exactly the tombstone rule <see cref="RecordJournalEntries"/> already encodes.
    ///
    /// The captured value is still a COPY of the ledger's row (never an alias): a background
    /// FireAndForget write can be serializing <c>state.State</c> while the ledger keeps mutating, and the
    /// persisted mirror must not move under it. What changed is only HOW MANY copies each tick makes.</summary>
    private bool CaptureSnapshotIntoState()
    {
        if (_executor is null)
        {
            _dirty = false;
            return false;
        }

        var snapshot = _coordinatorMode ? _coordinatorLedger.Visible : _executor.Snapshot();

        // Plan 011 D2 — THE SHED. A sharded table captures a boolean instead of its rows: the shards are
        // the durable per-key copy (see the class doc's D2 paragraph), and the only thing the mirror was
        // ever load-bearing for is the resume/first-start distinction that TableGrainState.HadRows now
        // carries. state.State.Snapshot is left empty and stays empty, so the write below is O(1) rather
        // than O(|table|) and nothing here holds a second copy of any row.
        if (_shardMirrorSuppressed)
        {
            state.State.HadRows = snapshot.Count > 0;
            state.State.Seq++;
            _dirty = false;
            _fullCaptureNeeded = false;
            return true;
        }

        if (_fullCaptureNeeded)
        {
            state.State.Snapshot = snapshot.ToDictionary(
                kv => kv.Key,
                kv => new TableRowDto { Row = new Dictionary<string, object?>(kv.Value.Row), Weight = kv.Value.Weight });
            _fullCaptureNeeded = false;
        }
        else
        {
            foreach (var key in _touchedRowKeys)
            {
                if (snapshot.TryGetValue(key, out var current))
                {
                    state.State.Snapshot[key] = new TableRowDto { Row = new Dictionary<string, object?>(current.Row), Weight = current.Weight };
                }
                else
                {
                    state.State.Snapshot.Remove(key);
                }
            }
        }

        _touchedRowKeys.Clear();
        state.State.Seq++;
        _dirty = false;
        return true;
    }

    /// <summary>Batched (and every final flush, see StopAsync/OnDeactivateAsync): capture + AWAIT the write,
    /// exactly as pre-008 — the grain turn stalls for the duration of the write.</summary>
    private async Task FlushAsync()
    {
        if (!CaptureSnapshotIntoState()) return;
        await state.WriteStateAsync();
    }

    /// <summary>Background half of FireAndForget's write — never throws (caught and logged here), so
    /// <see cref="_pendingWrite"/> can always be awaited/polled without a try/catch at the call site. Per the
    /// brief: log write failures, never let a background failure take down the grain or be swallowed
    /// silently — this is the one place that failure surfaces.</summary>
    private async Task WriteStateBestEffortAsync()
    {
        try
        {
            await state.WriteStateAsync();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Background (FireAndForget) flush failed for table '{Table}'", _def?.Name);
        }
    }

    private async Task OnFlushTickAsync()
    {
        if (!_dirty) return;

        if (_persistenceMode == TablePersistenceMode.FireAndForget)
        {
            // Single-flight: a write from a previous tick still in flight means this tick is skipped
            // outright rather than overlapped — see class doc's persistence-mode paragraph for why that
            // matters (JsonFileGrainStorage is not safe against two concurrent writers of the same file/
            // state object).
            if (!_pendingWrite.IsCompleted) return;

            if (CaptureSnapshotIntoState())
            {
                // Capture happened synchronously above (still on this turn); the write itself is NOT
                // awaited — the turn returns as soon as this method returns, and the write completes in the
                // background, observed by the next tick's _pendingWrite.IsCompleted check.
                _pendingWrite = WriteStateBestEffortAsync();
            }
            return;
        }

        // Batched and Journaled (MemoryOnly never registers this timer at all — see
        // StartClassicAsync/StartCoordinatorAsync): awaited flush, dispatched to the mode-appropriate write.
        await FlushDirtyAsync();
    }

    private static FieldKind MapFieldKind(FieldType type) => type switch
    {
        FieldType.String => FieldKind.String,
        FieldType.Double => FieldKind.Double,
        FieldType.Long => FieldKind.Long,
        FieldType.Bool => FieldKind.Bool,
        FieldType.Timestamp => FieldKind.Timestamp,
        FieldType.Json => FieldKind.Json,
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unknown field type"),
    };
}
