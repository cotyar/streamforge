using Dapr.Actors;
using Dapr.Actors.Client;
using StreamsForge.Abstractions.Streaming;
using StreamsForge.Dapr.Host.Actors;

namespace StreamsForge.Dapr.Host.Streaming;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W7-A: <c>Sinks.cs</c>'s class doc anticipates this exactly ("W7's
/// TableActor routes matching sources into Z-set ingestion... both register an extra sink, they don't
/// replace this one") — registered as BOTH <see cref="ISourceEventsSink"/> AND <see cref="ITableDeltaSink"/>
/// (in <c>Actors/TableRuntimeSetup.cs</c>'s <c>AddServices</c>, per the wave brief — NOT
/// <c>StreamingRuntimeSetup.cs</c>, to keep this router's entire registration surface inside this wave's
/// own owned file), so every <c>sf-sources</c> envelope this process receives is fanned out to it exactly
/// like <see cref="PipelineEventRouter"/>, AND every <c>sf-table-delta</c> envelope is fanned out to it
/// alongside W7-B's <c>TableHistoryActor</c> sink.
///
/// <para><b>What it tracks — two independent routing tables, split by input kind:</b> a table's SQL can
/// read stream sources directly (<c>TableCompileResult.StreamInputs</c>) and/or other tables' output
/// directly (<c>TableCompileResult.TableInputs</c>, table-over-table chaining — e.g. the seeded
/// "hot_symbols" FROM "positions" demo). <see cref="OnSourceEventsAsync"/> fans a <c>sf-sources</c>
/// envelope out to every table subscribed to that STREAM source; <see cref="OnTableDeltaAsync"/> fans a
/// <c>sf-table-delta</c> envelope out to every table subscribed to that UPSTREAM TABLE. Both indexes are
/// pure in-memory (never persisted) many-to-many maps.</para>
///
/// <para><b>Who registers, and when — changed by PARITY.md debt item D2:</b> <see cref="Register"/> used
/// to be called only from <see cref="Lifecycle.DaprLifecycleOrchestrator.StartTableAsync"/>, AFTER
/// <see cref="Actors.TableActor.StartAsync"/> had already returned. It is now called from INSIDE
/// <see cref="Actors.TableActor.StartAsync"/>/<c>OnActivateAsync</c>'s self-heal branch themselves
/// (<c>RegisterRouterAndAttachToTableInputsAsync</c>), BEFORE those methods read any upstream table's
/// snapshot for a table-over-table warm attach — see that method's own doc comment for why registering
/// first is what makes a subsequent <see cref="OnTableDeltaAsync"/>/<see cref="OnSourceEventsAsync"/> call
/// against the newly-registered table queue behind the still-in-flight StartAsync/OnActivateAsync turn
/// (Dapr actors process one invocation at a time per actor id) rather than arrive too early and be
/// dropped. <see cref="Unregister"/> is still called from <see cref="Lifecycle.
/// DaprLifecycleOrchestrator.StopTableAsync"/> (and defensively from <see cref="Lifecycle.
/// DaprLifecycleOrchestrator.StartTableAsync"/> on a pre-registration failure) — there is no equivalent
/// ordering hazard on the way down. <see cref="Services.TableSupervisorService"/>'s sweep still repairs
/// this router for a <see cref="TableActor"/> that self-healed on reactivation without going through
/// either of those two calls, unchanged by this item — the same relationship <see cref="PipelineEventRouter"/>
/// has to <see cref="Services.PipelineSupervisorService"/>.</para>
///
/// <para><b>Self-filter (a table must never receive its own output deltas):</b>
/// <see cref="OnTableDeltaAsync"/> skips any consumer whose name equals the delta batch's own
/// <see cref="TableDeltaEnvelope.Table"/> — the SQL compiler would never legitimately produce a
/// self-referential <c>TableInputs</c> entry (a table can't FROM/JOIN itself), so this is cheap defensive
/// insurance against that specific footgun rather than a case this router expects to hit in practice.</para>
///
/// <para><b>Fan-out, not a direct subscription</b> — same rationale as <see cref="PipelineEventRouter"/>'s
/// own doc comment: Dapr's fixed topics (decision D-D) mean this router is what makes them behave like a
/// per-source/per-upstream-table subscription from a <see cref="TableActor"/>'s point of view.</para>
///
/// <para><b>Table-over-pipeline (plan 025) added a THIRD routing table,</b> <c>_byPipeline</c>, and this
/// class now also implements <see cref="IPipelineResultsSink"/> (registered in
/// <c>Actors/TableRuntimeSetup.cs</c>'s <c>AddServices</c>, alongside the other two, per that wave's
/// PARITY.md D6 "table-over-pipeline inputs" entry). It is registered/unregistered through a SEPARATE
/// public method, <see cref="RegisterPipelineInputs"/> — not a wider <see cref="Register"/> parameter list
/// — specifically so <c>Services/TableSupervisorService.cs</c>'s existing 3-argument
/// <see cref="Register"/> call site needs no change; see that method's own doc comment for the full
/// reasoning.</para>
/// </summary>
public sealed class TableEventRouter(ILogger<TableEventRouter> logger) : ISourceEventsSink, ITableDeltaSink, IPipelineResultsSink
{
    private readonly object _gate = new();

    /// <summary>Stream source name → set of table names consuming it directly.</summary>
    private readonly Dictionary<string, HashSet<string>> _byStreamSource = new(StringComparer.Ordinal);

    /// <summary>Upstream table name → set of (downstream) table names consuming it directly.</summary>
    private readonly Dictionary<string, HashSet<string>> _byUpstreamTable = new(StringComparer.Ordinal);

    /// <summary>Table name → (its own stream inputs, its own table inputs) — reverse index so
    /// <see cref="Unregister"/> is O(subscriptions for this table) instead of a full scan of both maps
    /// above.</summary>
    private readonly Dictionary<string, (HashSet<string> Streams, HashSet<string> Tables)> _byTable = new(StringComparer.Ordinal);

    /// <summary>Table-over-pipeline (plan 025): pipeline id (BARE — pipeline ids are GUIDs and already
    /// globally unique, so unlike <see cref="_byStreamSource"/>/<see cref="_byUpstreamTable"/>'s
    /// environment-qualified NAME keys, no <c>EnvKeys.Qualify</c> is needed here; see
    /// <see cref="RegisterPipelineInputs"/>'s own doc comment) → set of (downstream) table names consuming
    /// its published output directly.</summary>
    private readonly Dictionary<string, HashSet<string>> _byPipeline = new(StringComparer.Ordinal);

    /// <summary>Table name → the pipeline ids it is currently registered against — the reverse index
    /// <see cref="RegisterPipelineInputs"/>/<see cref="UnregisterLocked"/> use to clean up
    /// <see cref="_byPipeline"/> in O(this table's own pipeline inputs). Deliberately a SEPARATE reverse
    /// index from <see cref="_byTable"/> (which tracks stream/table inputs only) rather than a wider
    /// tuple there — see <see cref="RegisterPipelineInputs"/>'s own doc comment for why that separation is
    /// what lets <c>TableActor</c> call <see cref="Register"/> and <see cref="RegisterPipelineInputs"/>
    /// independently, each replacing only its own half of a table's subscription set, without one
    /// clobbering the other.</summary>
    private readonly Dictionary<string, HashSet<string>> _pipelinesByTable = new(StringComparer.Ordinal);

    /// <summary>Replaces (or creates) <paramref name="tableName"/>'s subscription sets with exactly
    /// <paramref name="streamInputs"/>/<paramref name="tableInputs"/> — idempotent; safe to call repeatedly
    /// with the same sets (e.g. a supervisor sweep re-registering an already-routed table).</summary>
    public void Register(string tableName, IReadOnlyList<string> streamInputs, IReadOnlyList<string> tableInputs)
    {
        lock (_gate)
        {
            UnregisterLocked(tableName);

            if (streamInputs.Count == 0 && tableInputs.Count == 0)
            {
                return;
            }

            var streams = new HashSet<string>(streamInputs, StringComparer.Ordinal);
            var tables = new HashSet<string>(tableInputs, StringComparer.Ordinal);
            _byTable[tableName] = (streams, tables);

            foreach (var source in streams)
            {
                if (!_byStreamSource.TryGetValue(source, out var consumers))
                {
                    consumers = new HashSet<string>(StringComparer.Ordinal);
                    _byStreamSource[source] = consumers;
                }
                consumers.Add(tableName);
            }

            foreach (var upstream in tables)
            {
                if (!_byUpstreamTable.TryGetValue(upstream, out var consumers))
                {
                    consumers = new HashSet<string>(StringComparer.Ordinal);
                    _byUpstreamTable[upstream] = consumers;
                }
                consumers.Add(tableName);
            }
        }
    }

    /// <summary>Removes every subscription for <paramref name="tableName"/> — stream, table, AND (plan
    /// 025) pipeline inputs alike. Idempotent — a no-op if it wasn't registered.</summary>
    public void Unregister(string tableName)
    {
        lock (_gate)
        {
            UnregisterLocked(tableName);
        }
    }

    private void UnregisterLocked(string tableName)
    {
        UnregisterPipelinesLocked(tableName);

        if (!_byTable.Remove(tableName, out var prev))
        {
            return;
        }

        foreach (var source in prev.Streams)
        {
            if (_byStreamSource.TryGetValue(source, out var consumers))
            {
                consumers.Remove(tableName);
                if (consumers.Count == 0)
                {
                    _byStreamSource.Remove(source);
                }
            }
        }

        foreach (var upstream in prev.Tables)
        {
            if (_byUpstreamTable.TryGetValue(upstream, out var consumers))
            {
                consumers.Remove(tableName);
                if (consumers.Count == 0)
                {
                    _byUpstreamTable.Remove(upstream);
                }
            }
        }
    }

    /// <summary>Table-over-pipeline (plan 025): replaces (or creates) <paramref name="tableName"/>'s
    /// pipeline-input subscription set with exactly <paramref name="pipelineIds"/> — idempotent, same
    /// "replace whole set" contract as <see cref="Register"/>, but for the pipeline half only.
    ///
    /// <para><b>Why a separate method, alongside <see cref="Register"/>, rather than a wider parameter
    /// list on it:</b> <see cref="Services.TableSupervisorService"/> (a concurrent file this router does
    /// not own the caller side of) already calls the existing 3-argument <see cref="Register"/> and must
    /// keep compiling and meaning exactly what it means today. <c>TableActor.
    /// RegisterRouterAndAttachToTableInputsAsync</c> calls <see cref="Register"/> for stream/table inputs
    /// FIRST (as it always has — see that method's own doc comment for the D2 registration-ordering
    /// argument, which this does not change) and THEN this method for pipeline inputs — the two calls are
    /// independent because this method never touches <see cref="_byTable"/>/<see cref="_byStreamSource"/>/
    /// <see cref="_byUpstreamTable"/>, only its own <see cref="_pipelinesByTable"/>/<see cref="_byPipeline"/>
    /// pair.</para>
    ///
    /// <para>Pipeline ids need no <c>EnvKeys.Qualify</c> — unlike a source/table NAME (only unique within
    /// one environment's catalog, hence qualified at the router boundary so two environments' same-named
    /// entities don't collide in this one shared process-wide router), a pipeline id is a GUID assigned
    /// once at <c>CreatePipelineAsync</c> and is already globally unique. Pass <c>TableStartRequest.
    /// Pipelines</c>' ids straight through.</para></summary>
    public void RegisterPipelineInputs(string tableName, IReadOnlyList<string> pipelineIds)
    {
        lock (_gate)
        {
            UnregisterPipelinesLocked(tableName);

            if (pipelineIds.Count == 0)
            {
                return;
            }

            var pipelines = new HashSet<string>(pipelineIds, StringComparer.Ordinal);
            _pipelinesByTable[tableName] = pipelines;

            foreach (var pipelineId in pipelines)
            {
                if (!_byPipeline.TryGetValue(pipelineId, out var consumers))
                {
                    consumers = new HashSet<string>(StringComparer.Ordinal);
                    _byPipeline[pipelineId] = consumers;
                }
                consumers.Add(tableName);
            }
        }
    }

    private void UnregisterPipelinesLocked(string tableName)
    {
        if (!_pipelinesByTable.Remove(tableName, out var prev))
        {
            return;
        }

        foreach (var pipelineId in prev)
        {
            if (_byPipeline.TryGetValue(pipelineId, out var consumers))
            {
                consumers.Remove(tableName);
                if (consumers.Count == 0)
                {
                    _byPipeline.Remove(pipelineId);
                }
            }
        }
    }

    /// <summary>Point-in-time snapshot of the table names subscribed to stream source
    /// <paramref name="sourceName"/> — exposed for tests (see dapr/tests/StreamsForge.Dapr.Tests/
    /// TableEventRouterTests.cs); the dispatch path below takes its own lock-protected snapshot inline
    /// rather than calling this.</summary>
    public IReadOnlyCollection<string> StreamSubscribersOf(string sourceName)
    {
        lock (_gate)
        {
            return _byStreamSource.TryGetValue(sourceName, out var consumers) ? consumers.ToList() : [];
        }
    }

    /// <summary>Point-in-time snapshot of the table names subscribed to upstream table
    /// <paramref name="upstreamTableName"/> — exposed for tests, same rationale as
    /// <see cref="StreamSubscribersOf"/>.</summary>
    public IReadOnlyCollection<string> TableSubscribersOf(string upstreamTableName)
    {
        lock (_gate)
        {
            return _byUpstreamTable.TryGetValue(upstreamTableName, out var consumers) ? consumers.ToList() : [];
        }
    }

    /// <summary>Point-in-time snapshot of the table names subscribed to pipeline <paramref name="pipelineId"/>
    /// (BARE — see <see cref="RegisterPipelineInputs"/>'s own doc comment) — exposed for tests, same
    /// rationale as <see cref="StreamSubscribersOf"/>/<see cref="TableSubscribersOf"/>.</summary>
    public IReadOnlyCollection<string> PipelineSubscribersOf(string pipelineId)
    {
        lock (_gate)
        {
            return _byPipeline.TryGetValue(pipelineId, out var consumers) ? consumers.ToList() : [];
        }
    }

    public async Task OnSourceEventsAsync(SourceEventsEnvelope envelope)
    {
        List<string> tableNames;
        lock (_gate)
        {
            if (!_byStreamSource.TryGetValue(envelope.Source, out var consumers) || consumers.Count == 0)
            {
                return;
            }
            tableNames = consumers.ToList();
        }

        foreach (var tableName in tableNames)
        {
            try
            {
                var actor = ActorProxy.Create<ITableActor>(new ActorId(tableName), nameof(TableActor), ActorProxyDefaults.Options);
                await actor.ProcessSourceEventsAsync(envelope);
            }
            catch (Exception ex)
            {
                // Best-effort per table, mirroring PipelineEventRouter's own per-subscriber try/catch —
                // one misbehaving/unreachable table actor must never stop this batch from reaching the
                // rest, nor tear down the router.
                logger.LogWarning(
                    ex,
                    "TableEventRouter: failed to forward {Count} event(s) from source '{Source}' to table '{Table}'.",
                    envelope.Events.Count, envelope.Source, tableName);
            }
        }
    }

    public async Task OnTableDeltaAsync(TableDeltaEnvelope envelope)
    {
        List<string> tableNames;
        lock (_gate)
        {
            if (!_byUpstreamTable.TryGetValue(envelope.Table, out var consumers) || consumers.Count == 0)
            {
                return;
            }
            tableNames = consumers.ToList();
        }

        foreach (var tableName in ExcludeSelf(tableNames, envelope.Table))
        {
            try
            {
                var actor = ActorProxy.Create<ITableActor>(new ActorId(tableName), nameof(TableActor), ActorProxyDefaults.Options);
                await actor.ProcessTableDeltasAsync(envelope);
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "TableEventRouter: failed to forward {Count} delta(s) from table '{Upstream}' to table '{Table}'.",
                    envelope.Deltas.Count, envelope.Table, tableName);
            }
        }
    }

    /// <summary>Table-over-pipeline (plan 025): fans a <c>sf-pipeline-out</c> envelope out to every table
    /// subscribed to <see cref="PipelineResultsEnvelope.PipelineId"/> (BARE — see
    /// <see cref="RegisterPipelineInputs"/>'s own doc comment). No self-filter is needed here the way
    /// <see cref="OnTableDeltaAsync"/> needs <see cref="ExcludeSelf"/> — a TABLE can never be its own
    /// pipeline input (the two id spaces, GUID pipeline ids and table names, never collide), so there is
    /// no self-reference to guard against by construction.</summary>
    public async Task OnPipelineResultsAsync(PipelineResultsEnvelope envelope)
    {
        List<string> tableNames;
        lock (_gate)
        {
            if (!_byPipeline.TryGetValue(envelope.PipelineId, out var consumers) || consumers.Count == 0)
            {
                return;
            }
            tableNames = consumers.ToList();
        }

        foreach (var tableName in tableNames)
        {
            try
            {
                var actor = ActorProxy.Create<ITableActor>(new ActorId(tableName), nameof(TableActor), ActorProxyDefaults.Options);
                await actor.ProcessPipelineResultsAsync(envelope);
            }
            catch (Exception ex)
            {
                // Best-effort per table, mirroring OnSourceEventsAsync/OnTableDeltaAsync's own per-
                // subscriber try/catch — one misbehaving/unreachable table actor must never stop this
                // batch from reaching the rest, nor tear down the router.
                logger.LogWarning(
                    ex,
                    "TableEventRouter: failed to forward {Count} pipeline result row(s) from pipeline '{Pipeline}' to table '{Table}'.",
                    envelope.Results.Count, envelope.PipelineId, tableName);
            }
        }
    }

    /// <summary>Pure self-filter — a table must never receive its own output deltas (see class doc). Kept
    /// as its own static method, rather than inlined in <see cref="OnTableDeltaAsync"/>, purely so this
    /// safety rule is unit-testable without a live actor/sidecar — see dapr/tests/StreamsForge.Dapr.Tests/
    /// TableEventRouterTests.cs.</summary>
    public static IEnumerable<string> ExcludeSelf(IEnumerable<string> consumers, string upstreamTableName) =>
        consumers.Where(name => !string.Equals(name, upstreamTableName, StringComparison.Ordinal));
}
