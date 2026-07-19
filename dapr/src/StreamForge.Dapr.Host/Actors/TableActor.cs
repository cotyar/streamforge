using Dapr.Actors.Runtime;
using Dapr.Client;
using StreamForge.Abstractions;
using StreamForge.Abstractions.Streaming;
using StreamForge.AppCore.Json;
using StreamForge.Dapr.Host.Streaming;
using StreamForge.Engine;
using StreamForge.Host.Search;

namespace StreamForge.Dapr.Host.Actors;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W7-A: Dapr counterpart of Orleans' <c>TableGrain</c>
/// (orleans/src/StreamForge.Host/Grains/TableGrain.cs) — one actor per running table (actor type
/// "TableActor", key = the table's <see cref="TableDefinition.Name"/>), CLASSIC (Parallelism==1) PATH
/// ONLY (see <see cref="ITableActor"/>'s class doc for the D-F descope). Compiles the table's SQL via the
/// shared <see cref="StreamForge.Engine"/> Z-set (DBSP-style) <see cref="TableExecutor"/>, feeds it batches
/// of source events / upstream-table deltas routed in by <see cref="Streaming.TableEventRouter"/>,
/// publishes emitted delta batches to Dapr pub/sub (<c>sf-table-delta</c>) for <see cref="Streaming.
/// DaprStreamBridge"/>/W7-B's <c>TableHistoryActor</c> to relay/consume, and persists a consolidated
/// snapshot with write-behind (dirty flag, flushed every 2s or on stop/deactivate — mirrors
/// <c>TableGrain</c>'s identical cadence rationale: one JSON write per delta would thrash). Read every
/// method next to its Orleans equivalent; deviations are called out explicitly.
///
/// <para><b>Acyclic by construction — see <see cref="ITableActor"/>'s class doc.</b> Everything this actor
/// needs (definition, full source/table catalog, per-batch events/deltas) arrives as a method parameter; it
/// never resolves <see cref="ICatalogFacade"/> or any other actor.</para>
///
/// <para><b>State: the definition, source/table lists, running flag, AND the write-behind snapshot/seq ARE
/// persisted</b> — a superset of <see cref="PipelineActor"/>'s own persisted state (which has no
/// snapshot-equivalent to persist: a pipeline's "output" is a transient results ring, not a durable Z-set).
/// Same self-heal rationale as <see cref="PipelineActor"/>: Dapr actor timers do NOT survive deactivation/
/// reactivation, so <see cref="OnActivateAsync"/> recompiles from persisted state and immediately re-arms
/// the flush timer if the last known state was Running, instead of waiting for
/// <see cref="Services.TableSupervisorService"/>'s next sweep.</para>
///
/// <para><b>RESTART-RESUME LIMITATION — identical to <c>TableGrain</c>'s, not a new one:</b> the persisted
/// snapshot only ever captures this table's OUTPUT rows, not its operators' internal state (join indexes,
/// GROUP BY multisets/accumulators) — that cannot be reconstructed from the output alone. Exactly like
/// <c>TableGrain.StartClassicAsync</c>, <see cref="ActivateExecutor"/> (shared by both <see cref="StartAsync"/>
/// and <see cref="OnActivateAsync"/>'s self-heal branch) detects a non-empty persisted snapshot as "this is
/// a resume, not a first start" and wipes it (marking <see cref="_rebuilding"/>) rather than serving stale
/// rows behind a freshly-empty executor — the table rebuilds purely from live traffic going forward.
/// <b>One honest deviation from Orleans' incidental behavior</b> (see <see cref="OnActivateAsync"/>'s own
/// doc comment): because Dapr actors activate on-demand (any RPC — including a read call — triggers
/// <see cref="OnActivateAsync"/> first), there is no window where a stale pre-restart snapshot is briefly
/// served before the resume-reset runs, the way there incidentally is on Orleans (a REST read arriving
/// between silo boot and <c>RegistryGrain</c>'s resume loop reaching this specific grain). The very first
/// read after a Dapr restart already reflects <c>Rebuilding=true</c> with empty rows — earlier/more honest
/// disclosure, not later.</para>
///
/// <para><b>Two distinct sequence counters — do not conflate them:</b> <see cref="GetSeqAsync"/> exposes a
/// FLUSH-GENERATION counter (incremented once per <see cref="FlushAsync"/> call, mirroring
/// <c>TableGrain.state.State.Seq</c> exactly — a REST read-cursor concept), while <c>TableDeltaEnvelope.Seq</c>
/// (published on <c>sf-table-delta</c>, see <see cref="ApplyAndPublishAsync"/>) is a SEPARATE, transient,
/// per-published-BATCH counter this actor owns — see <c>Streaming.DaprStreamBridge.OnTableDeltaAsync</c>'s
/// own doc comment: "unlike the Orleans side (which assigns its own monotonic <c>_tableSeq</c> counter
/// locally per subscription), the Dapr envelope already carries the table's own <c>TableDeltaEnvelope.Seq</c>
/// — this bridge only relays it, it never invents one; <see cref="TableActor"/> is the single source of
/// truth for sequence numbers on this flavor." Not persisted (resets to 0 across a reactivation, exactly
/// like <see cref="PipelineActor"/>'s own unpersisted <c>_seq</c> for <c>ResultEnvelope.Seq</c>) — it is a
/// live-ordering aid for SignalR subscribers, not a durability guarantee.</para>
///
/// <para><b>Reads: flushed snapshot vs. live executor — same split as <c>TableGrain</c>'s classic path.</b>
/// <see cref="GetRowsAsync"/>/<see cref="GetRowCountAsync"/>/<see cref="GetSeqAsync"/> serve the
/// write-behind-flushed copy (up to ~2s stale — <c>TableGrain</c>'s classic path has this exact same
/// staleness; only its Parallelism&gt;=2 coordinator mode reads live). <see cref="SearchAsync"/> instead
/// reads the LIVE <see cref="TableExecutor.Snapshot"/> for weight lookup (the search INDEX itself is kept
/// live too, updated incrementally on every delta in <see cref="ReflectDeltasInSearchIndex"/>) — mirroring
/// <c>TableGrain.SearchAsync</c>'s identical live-vs-flushed split precisely.</para>
/// </summary>
public sealed class TableActor(ActorHost host, DaprClient daprClient, ILogger<TableActor> logger)
    : Actor(host), ITableActor
{
    private const string StateName = "table";
    private const string FlushTimerName = "table-flush";

    /// <summary>Same write-behind cadence as <c>TableGrain</c>'s own flush timer — one JSON write per
    /// delta would thrash; 2s bounds staleness without hammering the Redis-backed actor state store.</summary>
    private static readonly TimeSpan FlushPeriod = TimeSpan.FromSeconds(2);

    private TableDefinition? _def;
    private List<SourceDefinition> _sources = [];
    private List<TableDefinition> _tables = [];
    private bool _running;
    private bool _timerArmed;

    private TableExecutor? _executor;
    private TableSearchIndex? _searchIndex;
    private List<string> _streamInputs = [];
    private List<string> _tableInputs = [];
    private string? _lastCompileError;

    /// <summary>Write-behind-flushed consolidated snapshot (canonical row key -&gt; row/weight) —
    /// <see cref="GetRowsAsync"/>/<see cref="GetRowCountAsync"/> read THIS, not the live executor (see
    /// class doc's "flushed vs. live" split). Persisted verbatim as <see cref="TableActorState.Snapshot"/>.</summary>
    private Dictionary<string, TableRowDto> _flushed = [];

    /// <summary>Flush-generation counter — see class doc's "two distinct sequence counters" note.</summary>
    private long _seq;

    /// <summary>Per-published-delta-batch counter riding on <c>TableDeltaEnvelope.Seq</c> — see class doc's
    /// "two distinct sequence counters" note. Deliberately NOT persisted (transient live-ordering aid).</summary>
    private long _deltaSeq;

    private bool _dirty;
    private bool _rebuilding;
    private long _deltasIn;
    private long _deltasOut;
    private long _lastUpdateMs;

    /// <summary>Self-heal on (re)activation — same rationale as <see cref="PipelineActor.OnActivateAsync"/>:
    /// Dapr actor timers do not survive deactivation, so a fresh activation whose persisted state says
    /// "Running" recompiles and re-arms the flush timer immediately rather than waiting for
    /// <see cref="Services.TableSupervisorService"/>'s next ~15s sweep.
    ///
    /// <para>See class doc's "RESTART-RESUME LIMITATION" paragraph for why this collapses the
    /// stale-snapshot-serving window Orleans incidentally has: <see cref="ActivateExecutor"/> runs
    /// synchronously here, before ANY method on this actor (including a read call that itself triggered
    /// this very activation) can execute — so the resume-reset always happens before the first
    /// post-restart read is observable, not after.</para></summary>
    protected override async Task OnActivateAsync()
    {
        var existing = await StateManager.TryGetStateAsync<TableActorState>(StateName);
        if (existing.HasValue)
        {
            _def = existing.Value.Def;
            _sources = existing.Value.Sources;
            _tables = existing.Value.Tables;
            _running = existing.Value.Running;
            _flushed = existing.Value.Snapshot;
            _seq = existing.Value.Seq;
        }

        if (_running && _def is not null)
        {
            ActivateExecutor();
            if (_executor is not null)
            {
                await ArmTimerAsync();
            }
            else
            {
                logger.LogWarning(
                    "TableActor[{Name}]: self-heal compile failed on reactivation — leaving stopped: {Error}",
                    _def.Name, _lastCompileError);
                _running = false;
            }
        }
    }

    public async Task<ActorResult<TableInputNames>> StartAsync(TableStartRequest request)
    {
        await DisarmTimerIfArmedAsync();

        _def = request.Def;
        _sources = request.Sources;
        _tables = request.Tables;

        // Defensive-only: Catalog.CatalogStore.ValidateParallelism already rejects Parallelism != 1 at
        // CRUD time (decision D-F) — partitioned execution never legitimately reaches this actor. Assert
        // anyway per the plan brief, so a TableActor can never silently run a partitioned definition if
        // some future caller ever skips that validation.
        if (_def.Parallelism > 1)
        {
            _executor = null;
            _running = false;
            await SaveAsync();
            return ActorResult<TableInputNames>.Failure(
                $"Parallelism must be 1 on the Dapr flavor (got {_def.Parallelism}) — partitioned execution is Orleans-only.");
        }

        ActivateExecutor();
        if (_executor is null)
        {
            _running = false;
            await SaveAsync();
            return ActorResult<TableInputNames>.Failure(_lastCompileError!);
        }

        _running = true;
        await SaveAsync();
        await ArmTimerAsync();

        return ActorResult<TableInputNames>.Success(new TableInputNames(_streamInputs.ToList(), _tableInputs.ToList()));
    }

    public async Task StopAsync()
    {
        await DisarmTimerIfArmedAsync();

        if (_dirty)
        {
            await FlushAsync();
        }

        _executor = null;
        _searchIndex = null;
        _running = false;
        await SaveAsync();
    }

    /// <summary>Best-effort final flush before this activation is evicted — mirrors
    /// <c>TableGrain.OnDeactivateAsync</c> exactly (same "don't lose the last &lt;2s of deltas just because
    /// the flush timer hadn't ticked yet" rationale).</summary>
    protected override async Task OnDeactivateAsync()
    {
        if (_dirty)
        {
            try { await FlushAsync(); } catch { /* best-effort */ }
        }
    }

    public Task<bool> IsRunningAsync() => Task.FromResult(_running);

    public Task<TableInputNames> GetInputNamesAsync() => Task.FromResult(
        _running ? new TableInputNames(_streamInputs.ToList(), _tableInputs.ToList()) : new TableInputNames([], []));

    public async Task ProcessSourceEventsAsync(SourceEventsEnvelope envelope)
    {
        if (_executor is null || !_running)
        {
            return;
        }

        foreach (var raw in envelope.Events)
        {
            // See ITableActor.ProcessSourceEventsAsync's doc comment: this envelope crosses the Dapr
            // actor-invocation wire, which re-boxes every Dictionary<string, object?> value as a
            // JsonElement regardless of whether it was already normalized once at the sf-sources pub/sub
            // ingress. Re-normalize before the Engine ever sees it.
            JsonValueNormalizer.NormalizeInPlace(raw);

            var evt = new EventRecord(raw);
            _deltasIn++;
            _lastUpdateMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            _rebuilding = false; // live traffic observed since resume (or this is a first-ever start — already false)

            var deltas = _executor.OnStreamEvent(envelope.Source, evt);
            if (deltas.Count > 0)
            {
                await ApplyAndPublishAsync(deltas);
            }
        }
    }

    public async Task ProcessTableDeltasAsync(TableDeltaEnvelope envelope)
    {
        if (_executor is null || !_running)
        {
            return;
        }

        var outAll = new List<TableDelta>();
        foreach (var d in envelope.Deltas)
        {
            // Same actor-wire re-normalization requirement as ProcessSourceEventsAsync.
            JsonValueNormalizer.NormalizeInPlace(d.Row);

            _deltasIn++;
            var result = _executor.OnTableDelta(envelope.Table, new TableDelta(new EventRecord(d.Row), d.Weight));
            outAll.AddRange(result);
        }
        _lastUpdateMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _rebuilding = false;

        if (outAll.Count > 0)
        {
            await ApplyAndPublishAsync(outAll);
        }
    }

    public Task<List<TableRowDto>> GetRowsAsync(int limit, int offset)
    {
        var rows = _flushed.Values
            .Skip(Math.Max(0, offset))
            .Take(Math.Max(0, limit))
            .ToList();
        return Task.FromResult(rows);
    }

    public Task<int> GetRowCountAsync() => Task.FromResult(_flushed.Count);

    public Task<long> GetSeqAsync() => Task.FromResult(_seq);

    public Task<TableMetrics> GetMetricsAsync() => Task.FromResult(new TableMetrics
    {
        TableId = _def?.Id ?? Id.ToString(),
        Status = _running ? PipelineStatus.Running : PipelineStatus.Stopped,
        RowCount = _flushed.Count,
        DeltasIn = _deltasIn,
        DeltasOut = _deltasOut,
        LastUpdateMs = _lastUpdateMs,
        Rebuilding = _rebuilding,
        // Partitioned execution (and therefore per-partition detail / shared arrangements / frontier
        // epoch) is Orleans-only — decision D-F. Always null/absent on this flavor, independent of
        // Parallelism (which is always 1 here by construction).
        Partitions = null,
        ArrangedInputs = null,
        SnapshotFrontierEpoch = null,
    });

    public Task<List<TableRowDto>> SearchAsync(string query, int limit)
    {
        if (_searchIndex is null || string.IsNullOrWhiteSpace(query))
        {
            return Task.FromResult(new List<TableRowDto>());
        }

        // Live executor snapshot for weight lookup — NOT the flushed copy GetRowsAsync reads (see class
        // doc's "flushed vs. live" split; mirrors TableGrain.SearchAsync exactly).
        var snapshot = _executor?.Snapshot();
        var hits = _searchIndex.Search(query, limit);
        var rows = hits.Select(h =>
        {
            long weight = snapshot is not null && snapshot.TryGetValue(h.RowKey, out var current) ? current.Weight : 1;
            return new TableRowDto { Row = new Dictionary<string, object?>(h.Row), Weight = weight };
        }).ToList();
        return Task.FromResult(rows);
    }

    /// <summary>Shared by <see cref="StartAsync"/> and <see cref="OnActivateAsync"/>'s self-heal branch —
    /// compiles <see cref="_def"/>'s SQL (via <see cref="TableCompilation.TryCompile"/>) and, on success,
    /// applies the SAME "non-empty persisted snapshot means resume, not first start" reset
    /// <c>TableGrain.StartClassicAsync</c> applies (see class doc's restart-resume paragraph), then builds
    /// the search index FROM the (now-empty-either-way) snapshot. Sets <see cref="_executor"/> to null and
    /// <see cref="_lastCompileError"/> on failure; callers check <see cref="_executor"/>.</summary>
    private void ActivateExecutor()
    {
        var (executor, streamInputs, tableInputs, error) = TableCompilation.TryCompile(_def!, _sources, _tables);
        if (executor is null)
        {
            _executor = null;
            _lastCompileError = error;
            return;
        }

        _executor = executor;
        _streamInputs = streamInputs;
        _tableInputs = tableInputs;
        _lastCompileError = null;

        // See TableGrain.StartClassicAsync's identical comment: a non-empty persisted snapshot means this
        // is a resume (not a first start) — operator internal state (join indexes/GROUP BY multisets)
        // can't be reconstructed from the output alone, so mark rebuilding and reset to empty; it rebuilds
        // purely from live traffic going forward.
        if (_flushed.Count > 0)
        {
            _rebuilding = true;
            _flushed = [];
            _seq = 0;
            _dirty = true;
        }

        // Either branch above leaves the row set empty (fresh start, or reset-for-rebuild) — see
        // TableGrain's identical comment — so rebuilding the index from _flushed here is accurate (empty
        // in, empty out); it fills back in incrementally as Process*Async observes deltas going forward.
        _searchIndex = _def!.SearchEnabled ? BuildSearchIndex(_def.SearchMode) : null;
    }

    private TableSearchIndex BuildSearchIndex(TableSearchMode mode)
    {
        var index = new TableSearchIndex(mode);
        index.Rebuild(_flushed.Select(kv =>
            new KeyValuePair<string, IReadOnlyDictionary<string, object?>>(kv.Key, kv.Value.Row)));
        return index;
    }

    private async Task ApplyAndPublishAsync(IReadOnlyList<TableDelta> deltas)
    {
        _dirty = true;
        _deltasOut += deltas.Count;

        if (_searchIndex is not null)
        {
            ReflectDeltasInSearchIndex(deltas);
        }

        _deltaSeq++;
        var dtos = deltas.Select(d => new TableDeltaDto { Row = new Dictionary<string, object?>(d.Row), Weight = d.Weight }).ToList();

        try
        {
            await daprClient.PublishEventAsync(
                StreamingRuntimeSetup.PubsubName,
                StreamingRuntimeSetup.TableDeltaTopic,
                new TableDeltaEnvelope { Table = _def!.Name, Seq = _deltaSeq, Deltas = dtos });
        }
        catch (Exception ex)
        {
            // A transient sidecar hiccup must not tear down the timer or lose the in-memory
            // counters/search-index updates above — mirrors PipelineActor.PublishRowsAsync's own
            // try/catch rationale (drop this publish, the next delta/flush tick tries again).
            logger.LogWarning(ex, "TableActor[{Name}]: failed to publish {Count} delta(s).", _def?.Name, dtos.Count);
        }
    }

    /// <summary>Keeps the search index in sync with the consolidated Z-set as deltas land — verbatim port
    /// of <c>TableGrain.ReflectDeltasInSearchIndex</c>: for each row touched by this batch, look its
    /// canonical key up in the already-updated, live <see cref="TableExecutor.Snapshot"/> — present with
    /// weight &gt; 0 means Add/update, absent means the row's weight returned to 0 (Remove).</summary>
    private void ReflectDeltasInSearchIndex(IReadOnlyList<TableDelta> deltas)
    {
        var snapshot = _executor!.Snapshot();
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

    private async Task FlushAsync()
    {
        if (_executor is null)
        {
            _dirty = false;
            return;
        }

        var snapshot = _executor.Snapshot();
        _flushed = snapshot.ToDictionary(
            kv => kv.Key,
            kv => new TableRowDto { Row = new Dictionary<string, object?>(kv.Value.Row), Weight = kv.Value.Weight });
        _seq++;
        _dirty = false;

        await SaveAsync();
    }

    private async Task OnFlushTickAsync()
    {
        if (_dirty)
        {
            await FlushAsync();
        }
    }

    private Task SaveAsync() => StateManager.SetStateAsync(StateName, new TableActorState
    {
        Def = _def,
        Sources = _sources,
        Tables = _tables,
        Running = _running,
        Snapshot = _flushed,
        Seq = _seq,
    });

    private async Task ArmTimerAsync()
    {
        await RegisterTimerAsync(FlushTimerName, nameof(OnFlushTickAsync), null, FlushPeriod, FlushPeriod);
        _timerArmed = true;
    }

    private async Task DisarmTimerIfArmedAsync()
    {
        if (!_timerArmed)
        {
            return;
        }

        await UnregisterTimerAsync(FlushTimerName);
        _timerArmed = false;
    }
}

/// <summary>Persisted shape of a TableActor's state — see that class's doc comment for why the definition,
/// source/table lists, running flag, AND the write-behind snapshot/seq are all persisted (self-healing
/// across deactivation/reactivation, plus read availability of the last-flushed rows). Plain get/set
/// properties, same style as <see cref="PipelineActorState"/>/<see cref="Catalog.CatalogState"/>, for a
/// clean System.Text.Json round trip through Dapr's actor state store.</summary>
public sealed class TableActorState
{
    public TableDefinition? Def { get; set; }

    public List<SourceDefinition> Sources { get; set; } = [];

    public List<TableDefinition> Tables { get; set; } = [];

    public bool Running { get; set; }

    public Dictionary<string, TableRowDto> Snapshot { get; set; } = [];

    public long Seq { get; set; }
}

/// <summary>
/// Pure SQL-compile-to-executor logic, extracted from <see cref="TableActor"/> specifically so it can be
/// unit tested without any actor/timer/Dapr-sidecar machinery (mirrors <see cref="PipelineCompilation"/>'s
/// own extraction rationale) — see dapr/tests/StreamForge.Dapr.Tests/TableCompilationTests.cs. Builds the
/// same stream/table schema dictionaries + <see cref="SqlCompiler.CompileTable"/> call
/// <c>TableGrain.StartClassicAsync</c> makes.
/// </summary>
public static class TableCompilation
{
    public static (TableExecutor? Executor, List<string> StreamInputs, List<string> TableInputs, string? Error) TryCompile(
        TableDefinition def, IReadOnlyList<SourceDefinition> sources, IReadOnlyList<TableDefinition> tables)
    {
        var streamSchemas = sources.ToDictionary(
            s => s.Name,
            s => new SourceSchema(s.Name, s.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));

        var tableSchemas = tables
            .Where(t => t.OutputFields.Count > 0)
            .ToDictionary(
                t => t.Name,
                t => new SourceSchema(t.Name, t.OutputFields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));

        var compileResult = SqlCompiler.CompileTable(def.Sql, streamSchemas, tableSchemas);
        if (!compileResult.Ok || compileResult.Plan is null)
        {
            var message = string.Join("; ", compileResult.Diagnostics.Select(d => $"{d.Line}:{d.Column} {d.Message}"));
            return (null, [], [], message);
        }

        return (
            compileResult.Plan.CreateExecutor(),
            compileResult.StreamInputs.Distinct().ToList(),
            compileResult.TableInputs.Distinct().ToList(),
            null);
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
