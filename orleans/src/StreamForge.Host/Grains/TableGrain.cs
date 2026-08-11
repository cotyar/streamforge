using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Serialization.Invocation;
using Orleans.Streams;
using StreamForge.Abstractions;
using StreamForge.Engine;
using StreamForge.Engine.Dataflow;
using StreamForge.Host.Search;

namespace StreamForge.Host.Grains;

public sealed class TableGrainState
{
    /// <summary>Consolidated output snapshot: canonical rowKey -> (row, weight). Write-behind persisted
    /// (dirty flag + periodic flush) — see TableGrain's class comment for the restart-resume tradeoff.</summary>
    public Dictionary<string, TableRowDto> Snapshot { get; set; } = [];
    public long Seq { get; set; }
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
/// partition, and only consolidates a batch into _coordinatorSnapshot/the search index once EVERY terminal
/// partition has reported reaching that epoch. <see cref="GetSnapshotFrontierEpochAsync"/> (mirrored on
/// TableMetrics.SnapshotFrontierEpoch) then reports that same epoch.
///
/// THE CONSISTENCY STATEMENT this makes true (not just documented, but true BY CONSTRUCTION — see
/// OnOutputBatchAsync): at any point a caller observes SnapshotFrontierEpoch == F via GetRowsAsync/
/// GetMetricsAsync/GetSnapshotFrontierEpochAsync, the rows/search results served AT THAT SAME MOMENT
/// reflect ALL deltas whose epoch is &lt;= F and NONE beyond it. This holds because (a)
/// _coordinatorSnapshot is only ever mutated inside OnOutputBatchAsync, synchronously, with no `await`
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
/// part MUST stay on the turn — TableExecutor/_coordinatorSnapshot are not thread-safe and nothing else may
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
    /// coordinator-mode analogue of TableExecutor's internal `_consolidated` (not reachable from Host —
    /// see class doc), fed by <see cref="OnOutputBatchAsync"/> and read by
    /// ReflectDeltasInSearchIndex/FlushAsync/SearchAsync exactly where the classic path reads
    /// `_executor.Snapshot()`.</summary>
    private readonly Dictionary<string, (EventRecord Row, long Weight)> _coordinatorSnapshot = [];

    /// <summary>Outstanding NEGATIVE running weight per canonical row key, for keys whose net weight-so-far
    /// is &lt;= 0 and are therefore not (or no longer) in <see cref="_coordinatorSnapshot"/> — the same side
    /// table (and the same reasoning) as TableExecutorImpl's own `_debtWeights`, which _coordinatorSnapshot
    /// is the coordinator-mode analogue of. A negative delta can legitimately arrive here for a key with no
    /// prior positive weight: this grain fans terminal-partition batches in from N partitions (and an
    /// upstream outer join emits retraction-driven pads), so per-key causal order is not guaranteed across
    /// that fan-in, replay, or a restart-resume. Discarding that negative (the old bug) loses information —
    /// a later positive delta for the same key would then start fresh instead of netting against the
    /// outstanding debt, so a row whose true total weight is 0 could resurface at a positive weight
    /// depending on arrival order alone.
    ///
    /// Kept separate from _coordinatorSnapshot rather than letting that dictionary hold weight &lt;= 0
    /// entries so it stays exactly the user-visible positive rows — GetRowsAsync/RowCount/FlushAsync/
    /// ReflectDeltasInSearchIndex all read it unfiltered. The two are disjoint by construction
    /// (<see cref="ApplyCoordinatorConsolidation"/> always removes from whichever one a key is NOT written
    /// into) and a key netting to exactly 0 leaves both, so neither accumulates residue for fully cancelled
    /// rows. That is the DBSP invariant making this ledger order-independent: the value looked up before
    /// folding in a new delta is always the exact running sum of every delta seen so far for that key, so —
    /// integer addition being commutative and associative — a key's final classification (positive/zero/
    /// negative) depends only on the SUM of its deltas, never on the order they arrived in.</summary>
    private readonly Dictionary<string, long> _coordinatorDebt = [];

    /// <summary>Plan 003 M4 — the M0 primitives (see FrontierTracker/EpochBuffer's own doc comments),
    /// registered over every terminal-stage partition (one UpstreamId per partition, keyed on the
    /// compiled dataflow's TerminalEdge.EdgeId — the one edge every terminal partition's own
    /// RouteDownstreamAsync call reports on), driving <see cref="OnOutputBatchAsync"/>. Null on the
    /// Parallelism==1 path and before StartCoordinatorAsync has run.</summary>
    private FrontierTracker? _outputFrontier;
    private EpochBuffer? _outputBuffer;
    private EdgeId _terminalEdgeId;
    /// <summary>Plan 003 M4 — the epoch _coordinatorSnapshot currently, honestly, fully reflects (see class
    /// doc's consistency statement). Null until OnOutputBatchAsync has observed at least one full round
    /// (every terminal partition reporting) since the last StartCoordinatorAsync.</summary>
    private long? _snapshotFrontierEpoch;

    public async Task StartAsync(TableDefinition def)
    {
        await StopAsync();

        _def = def;
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
        _status = PipelineStatus.Running;

        ResumeJournaledState();

        // See class-level comment: a non-empty persisted snapshot means this is a resume (not a first
        // start) — operator internal state can't be rebuilt from it, so mark rebuilding and reset to empty.
        if (state.State.Snapshot.Count > 0)
        {
            _rebuilding = true;
            state.State.Snapshot = [];
            state.State.Seq = 0;
            _dirty = true;
        }

        await ClearStaleJournalAsync();

        // Either branch above leaves the current row set empty (fresh start, or reset-for-rebuild), so a
        // freshly built (empty) index is accurate here — it fills back in incrementally as
        // ApplyAndPublishAsync observes deltas going forward, exactly like state.State.Snapshot does via
        // FlushAsync (just without the 2s lag, since Snapshot() is an O(1) live dictionary reference).
        _searchIndex = def.SearchEnabled ? new TableSearchIndex(def.SearchMode) : null;

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

        // _coordinatorSnapshot (unlike _executor above) is a grain-instance field that outlives a single
        // StartAsync/StopAsync cycle — the grain activation itself isn't torn down on StopAsync, only its
        // subscriptions/sub-grains are. Without clearing it here, a restart-resume would silently resurrect
        // pre-restart rows into the freshly-reset state.State.Snapshot on the next flush, breaking the same
        // "rebuild purely from live traffic" contract the classic path gets for free by allocating a brand
        // new (empty) TableExecutor on every StartClassicAsync call.
        _coordinatorSnapshot.Clear();
        _coordinatorDebt.Clear();

        ResumeJournaledState();

        if (state.State.Snapshot.Count > 0)
        {
            _rebuilding = true;
            state.State.Snapshot = [];
            state.State.Seq = 0;
            _dirty = true;
        }

        await ClearStaleJournalAsync();

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
        // ? _coordinatorSnapshot : _executor.Snapshot()` branch must still see _coordinatorMode==true here,
        // otherwise a coordinator-mode table's final on-stop flush silently persists the scratch executor's
        // (always-empty — see class doc, "never fed an event") snapshot instead of the real
        // _coordinatorSnapshot, losing every row from the persisted state.State.Snapshot on every stop. This
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
    /// <see cref="_coordinatorSnapshot"/> rather than the write-behind-persisted state.State.Snapshot (which
    /// can lag up to the 2s flush-timer interval — see class doc / OnFlushTickAsync). This is required for
    /// the frontier-consistency statement to actually hold: <see cref="_snapshotFrontierEpoch"/> is advanced
    /// synchronously, in the same OnOutputBatchAsync call that updates _coordinatorSnapshot, so reading rows
    /// from anything else (state.State.Snapshot included) could report a frontier ahead of what the rows
    /// served actually reflect. state.State.Snapshot remains the PERSISTED copy either way — still flushed
    /// on the same 2s cadence, still what restart-resume reads (see StartCoordinatorAsync's Rebuilding
    /// logic) — this change only affects which copy live reads are served from, for coordinator mode only;
    /// the Parallelism==1 path is completely unchanged (still reads state.State.Snapshot, exactly as
    /// before M4).</summary>
    public Task<List<TableRowDto>> GetRowsAsync(int limit, int offset)
    {
        var source = _coordinatorMode
            ? _coordinatorSnapshot.Values.Select(v => new TableRowDto { Row = new Dictionary<string, object?>(v.Row), Weight = v.Weight })
            : ClassicModeRows();
        var rows = source
            .Skip(Math.Max(0, offset))
            .Take(Math.Max(0, limit))
            .ToList();
        return Task.FromResult(rows);
    }

    public Task<int> GetRowCountAsync() =>
        Task.FromResult(_coordinatorMode ? _coordinatorSnapshot.Count : ClassicModeRowCount());

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
        _persistenceMode is TablePersistenceMode.MemoryOnly or TablePersistenceMode.Journaled && _executor is not null
            ? _executor.Snapshot().Values.Select(v => new TableRowDto { Row = new Dictionary<string, object?>(v.Row), Weight = v.Weight })
            : state.State.Snapshot.Values;

    private int ClassicModeRowCount() =>
        _persistenceMode is TablePersistenceMode.MemoryOnly or TablePersistenceMode.Journaled && _executor is not null
            ? _executor.Snapshot().Count
            : state.State.Snapshot.Count;

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
            RowCount = _coordinatorMode ? _coordinatorSnapshot.Count : ClassicModeRowCount(),
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

        IReadOnlyDictionary<string, (EventRecord Row, long Weight)>? snapshot = _coordinatorMode ? _coordinatorSnapshot : _executor?.Snapshot();
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

    private async Task OnTableDeltaBatchAsync(string table, List<TableDeltaDto> batch)
    {
        if (_executor is null) return;

        var outAll = new List<TableDelta>();
        foreach (var d in batch)
        {
            _deltasIn++;
            var result = _executor.OnTableDelta(table, new TableDelta(new EventRecord(d.Row), d.Weight));
            outAll.AddRange(result);
        }
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
    /// <see cref="_coordinatorSnapshot"/>/the search index AND _snapshotFrontierEpoch is advanced to match.
    /// That synchronous, no-await body is exactly what makes the class doc's consistency statement true —
    /// and still holds despite this method being [MayInterleave] (see class doc): interleaving only changes
    /// which QUEUED turn Orleans picks up next when another turn is suspended at an `await`; it never
    /// interrupts a synchronous method's body mid-statement. Since neither this method nor GetRowsAsync
    /// ever yields, one always runs to completion before the other can start — no GetRowsAsync call can
    /// ever observe _coordinatorSnapshot mid-update, and _snapshotFrontierEpoch never claims more than what
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

            if (_persistenceMode == TablePersistenceMode.Journaled)
            {
                RecordJournalEntries(allDeltas);
            }

            if (_searchIndex is not null)
            {
                ReflectDeltasInSearchIndex(allDeltas);
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
        var key = _executor!.CanonicalRowKey(delta.Row);

        // Running weight for this key BEFORE folding in `delta`, wherever it currently lives (positive in
        // _coordinatorSnapshot, negative in _coordinatorDebt, or 0/absent from both — never in both at
        // once). See _coordinatorDebt's doc comment.
        long currentWeight = _coordinatorSnapshot.TryGetValue(key, out var existing)
            ? existing.Weight
            : _coordinatorDebt.GetValueOrDefault(key);

        long newWeight = currentWeight + delta.Weight;

        if (newWeight > 0)
        {
            // Same canonical key => same row content, so either representative is equivalent (the Engine's
            // ApplyConsolidation likewise always stores the incoming row).
            _coordinatorSnapshot[key] = (delta.Row, newWeight);
            _coordinatorDebt.Remove(key);
        }
        else if (newWeight < 0)
        {
            _coordinatorSnapshot.Remove(key);
            _coordinatorDebt[key] = newWeight;
        }
        else // newWeight == 0: fully cancelled out — no residue in either structure.
        {
            _coordinatorSnapshot.Remove(key);
            _coordinatorDebt.Remove(key);
        }
    }

    private async Task ApplyAndPublishAsync(IReadOnlyList<TableDelta> deltas)
    {
        _dirty = true;
        _deltasOut += deltas.Count;

        if (_searchIndex is not null)
        {
            ReflectDeltasInSearchIndex(deltas);
        }

        if (_persistenceMode == TablePersistenceMode.Journaled)
        {
            RecordJournalEntries(deltas);
        }

        var dtos = deltas.Select(d => new TableDeltaDto { Row = new Dictionary<string, object?>(d.Row), Weight = d.Weight }).ToList();
        var stream = this.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, _def!.Name));
        await stream.OnNextAsync(dtos);
    }

    /// <summary>Keeps the search index in sync with the consolidated Z-set as deltas land: for each row
    /// touched by this batch, look its canonical key up in the (already-updated, O(1) live) consolidated
    /// snapshot — present with weight &gt; 0 means Add/update, absent means the row's weight returned to 0
    /// (Remove). Only rows actually touched by this batch are re-checked, not the whole table.</summary>
    private void ReflectDeltasInSearchIndex(IReadOnlyList<TableDelta> deltas)
    {
        IReadOnlyDictionary<string, (EventRecord Row, long Weight)> snapshot = _coordinatorMode ? _coordinatorSnapshot : _executor!.Snapshot();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var delta in deltas)
        {
            var key = _executor.CanonicalRowKey(delta.Row);
            if (!seen.Add(key)) continue; // a batch can touch the same row's key more than once

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
    /// mode's <see cref="_executor"/> or coordinator mode's <see cref="_coordinatorSnapshot"/>, exactly like
    /// ReflectDeltasInSearchIndex does): present with weight &gt; 0 records a live entry; absent means the
    /// key's running weight just dropped to &lt;= 0, recorded as an explicit removal tombstone (Weight = 0) —
    /// see <see cref="TableJournalEntry"/>'s own doc comment for why skipping this instead would resurrect
    /// the row on replay.</summary>
    private void RecordJournalEntries(IReadOnlyList<TableDelta> deltas)
    {
        IReadOnlyDictionary<string, (EventRecord Row, long Weight)> snapshot = _coordinatorMode ? _coordinatorSnapshot : _executor!.Snapshot();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var delta in deltas)
        {
            var key = _executor!.CanonicalRowKey(delta.Row);
            if (!seen.Add(key)) continue; // a batch can touch the same row's key more than once

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
    private Task FlushDirtyAsync() => _persistenceMode == TablePersistenceMode.Journaled ? JournalFlushAsync() : FlushAsync();

    /// <summary>Captures the live consolidated Z-set into <c>state.State</c> — MUST stay synchronous, on the
    /// grain turn: <see cref="_executor"/>/<see cref="_coordinatorSnapshot"/> are not thread-safe and this is
    /// the only safe place to read them. Returns false (and clears <see cref="_dirty"/> without touching
    /// state.State) when there is nothing to capture, exactly like the pre-008 FlushAsync's own null-executor
    /// guard.</summary>
    private bool CaptureSnapshotIntoState()
    {
        if (_executor is null)
        {
            _dirty = false;
            return false;
        }

        var snapshot = _coordinatorMode ? _coordinatorSnapshot : _executor.Snapshot();
        state.State.Snapshot = snapshot.ToDictionary(
            kv => kv.Key,
            kv => new TableRowDto { Row = new Dictionary<string, object?>(kv.Value.Row), Weight = kv.Value.Weight });
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
